using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace Caudal.Internal;

/// <summary>
/// Interleaves several flows into one, in arrival order with no inter-source
/// ordering guarantee. Backpressure propagates to every source through the shared
/// bounded output channel. Completes when all sources complete; the first failure
/// cancels the rest (Stop semantics).
/// </summary>
internal sealed class MergeFlow<T> : FlowBase<T>
{
    private readonly FlowBase<T>[] _sources;

    internal MergeFlow(FlowBase<T>[] sources, FlowOptions options)
        // The spine only carries a single upstream link; Merge's fan-in of multiple
        // sources is accepted here as a one-source-deep simplification — diagnostics
        // walking the spine sees only sources[0].
        : base(sources[0], "Merge", options)
        => _sources = sources;

    public override async IAsyncEnumerable<T> Enumerate(
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        Stats?.MarkStarted();

        var output = Channel.CreateBounded<T>(new BoundedChannelOptions(Options.Capacity)
        {
            SingleReader = true,
        });

        if (Stats is { } stats)
        {
            stats.QueueCapacity = Options.Capacity;
            stats.QueueLengthProbe = () => output.Reader.Count;
        }

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        var pumps = new Task[_sources.Length];
        for (var i = 0; i < _sources.Length; i++)
        {
            pumps[i] = PumpAsync(_sources[i], output.Writer, cts, Stats);
        }

        var completion = CompleteWhenDrainedAsync(pumps, output.Writer);

        try
        {
            await foreach (var item in output.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
            {
                Stats?.ItemCompleted();
                yield return item;
            }
        }
        finally
        {
            cts.Cancel();
            await TaskHelpers.IgnoreErrorsAsync(completion).ConfigureAwait(false);
        }
    }

    private static async Task PumpAsync(
        FlowBase<T> source,
        ChannelWriter<T> writer,
        CancellationTokenSource cts,
        StageStats? stats)
    {
        try
        {
            await foreach (var item in source.Enumerate(cts.Token).ConfigureAwait(false))
            {
                // Before the write, so a snapshot can never see completed > received.
                stats?.ItemReceived();
                await writer.WriteAsync(item, cts.Token).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException oce)
            when (oce.CancellationToken == cts.Token && cts.Token.IsCancellationRequested)
        {
            // Both conditions required: an OperationCanceledException that merely
            // forwards our token while nothing was cancelled is a source failure —
            // treating it as teardown would skip TryComplete/Cancel and hang the
            // merge with its healthy siblings running forever.
            throw;
        }
        catch (Exception ex)
        {
            // First failing source wins and stops the merge promptly.
            if (writer.TryComplete(ex))
            {
                cts.Cancel();
            }

            throw;
        }
    }

    private static async Task CompleteWhenDrainedAsync(Task[] pumps, ChannelWriter<T> writer)
    {
        try
        {
            await Task.WhenAll(pumps).ConfigureAwait(false);
            writer.TryComplete();
        }
        catch (Exception ex)
        {
            writer.TryComplete(ex);
        }
    }
}
