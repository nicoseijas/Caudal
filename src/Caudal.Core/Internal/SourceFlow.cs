using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace Caudal.Internal;

/// <summary>
/// The head of every pipeline: pumps an <see cref="IAsyncEnumerable{T}"/> into a
/// bounded channel. When the channel is full the pump suspends, which is what
/// propagates backpressure into the source.
/// </summary>
internal sealed class SourceFlow<T> : FlowBase<T>
{
    private readonly IAsyncEnumerable<T> _source;

    internal SourceFlow(IAsyncEnumerable<T> source, FlowOptions options)
        : base(null, "Source", options)
        => _source = source;

    public override async IAsyncEnumerable<T> Enumerate(
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        Stats?.MarkStarted();

        var channel = Channel.CreateBounded<T>(new BoundedChannelOptions(Options.Capacity)
        {
            SingleWriter = true,
            SingleReader = true,
        });

        if (Stats is { } stats)
        {
            stats.QueueCapacity = Options.Capacity;
            stats.QueueLengthProbe = () => channel.Reader.Count;
        }

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var pump = PumpAsync(channel.Writer, cts.Token);

        try
        {
            await foreach (var item in channel.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
            {
                Stats?.ItemCompleted();
                yield return item;
            }
        }
        finally
        {
            cts.Cancel();
            await TaskHelpers.IgnoreErrorsAsync(pump).ConfigureAwait(false);
        }
    }

    private async Task PumpAsync(ChannelWriter<T> writer, CancellationToken cancellationToken)
    {
        try
        {
            await foreach (var item in _source.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                // Before the write, so a snapshot can never see completed > received.
                Stats?.ItemReceived();
                await writer.WriteAsync(item, cancellationToken).ConfigureAwait(false);
            }

            writer.Complete();
        }
        catch (Exception ex)
        {
            // The original exception travels through the channel to the consumer.
            writer.TryComplete(ex);
        }
    }
}
