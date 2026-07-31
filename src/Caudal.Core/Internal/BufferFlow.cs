using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace Caudal.Internal;

/// <summary>
/// An explicit decoupling stage: a bounded channel with a declared full-buffer
/// policy. Wait propagates backpressure; the drop modes shed load and count every
/// dropped item; Reject faults the pipeline visibly.
/// </summary>
internal sealed class BufferFlow<T> : FlowBase<T>
{
    private readonly FlowBase<T> _upstream;
    private readonly int _capacity;
    private readonly BufferFullMode _fullMode;
    private long _dropped;

    internal BufferFlow(FlowBase<T> upstream, int capacity, BufferFullMode fullMode)
        : base(upstream, "Buffer", upstream.Options)
    {
        _upstream = upstream;
        _capacity = capacity;
        _fullMode = fullMode;
    }

    /// <summary>How many items the drop policies have discarded.</summary>
    internal long DroppedCount => Interlocked.Read(ref _dropped);

    public override async IAsyncEnumerable<T> Enumerate(
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        Stats?.MarkStarted();

        var channel = CreateChannel();

        if (Stats is { } stats)
        {
            stats.QueueCapacity = _capacity;
            stats.QueueLengthProbe = () => channel.Reader.Count;
        }

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var pump = PumpAsync(channel.Writer, cts.Token);

        try
        {
            await foreach (var item in channel.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
            {
                Stats?.OutputEmitted();
                yield return item;
            }
        }
        finally
        {
            cts.Cancel();
            await TaskHelpers.IgnoreErrorsAsync(pump).ConfigureAwait(false);
        }
    }

    private Channel<T> CreateChannel()
    {
        var options = new BoundedChannelOptions(_capacity)
        {
            SingleWriter = true,
            SingleReader = true,
            FullMode = _fullMode switch
            {
                BufferFullMode.DropNewest => BoundedChannelFullMode.DropWrite,
                BufferFullMode.DropOldest => BoundedChannelFullMode.DropOldest,
                _ => BoundedChannelFullMode.Wait,
            },
        };

        return _fullMode is BufferFullMode.DropNewest or BufferFullMode.DropOldest
            ? Channel.CreateBounded<T>(options, _ =>
            {
                Interlocked.Increment(ref _dropped);
                Stats?.InputDropped();
            })
            : Channel.CreateBounded<T>(options);
    }

    private async Task PumpAsync(ChannelWriter<T> writer, CancellationToken cancellationToken)
    {
        try
        {
            await foreach (var item in _upstream.Enumerate(cancellationToken).ConfigureAwait(false))
            {
                Stats?.InputReceived();
                if (_fullMode == BufferFullMode.Reject)
                {
                    if (!writer.TryWrite(item))
                    {
                        throw new FlowBufferFullException(
                            $"Buffer (capacity {_capacity}) is full and its full-mode is Reject.");
                    }
                }
                else
                {
                    // The drop modes never block here: the channel accepts the write
                    // and applies the policy, invoking the drop counter callback.
                    await writer.WriteAsync(item, cancellationToken).ConfigureAwait(false);
                }
            }

            writer.Complete();
        }
        catch (Exception ex)
        {
            writer.TryComplete(ex);
        }
    }
}
