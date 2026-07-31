using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace Caudal.Internal;

/// <summary>
/// Groups items into batches emitted when whichever comes first: the batch reaches
/// <c>maximumSize</c>, <c>maximumDelay</c> has elapsed since the batch's first item
/// arrived, or the source completes (the final partial batch is always emitted).
/// All timing goes through <see cref="TimeProvider"/> so tests can use a fake clock.
/// </summary>
internal sealed class BatchFlow<T> : FlowBase<IReadOnlyList<T>>
{
    private readonly FlowBase<T> _upstream;
    private readonly int _maximumSize;
    private readonly TimeSpan _maximumDelay;
    private readonly TimeProvider _timeProvider;

    internal BatchFlow(
        FlowBase<T> upstream,
        int maximumSize,
        TimeSpan maximumDelay,
        TimeProvider timeProvider)
        : base(upstream, "Batch", upstream.Options)
    {
        _upstream = upstream;
        _maximumSize = maximumSize;
        _maximumDelay = maximumDelay;
        _timeProvider = timeProvider;
    }

    public override async IAsyncEnumerable<IReadOnlyList<T>> Enumerate(
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        Stats?.MarkStarted();

        var input = Channel.CreateBounded<T>(new BoundedChannelOptions(Options.Capacity)
        {
            SingleWriter = true,
            SingleReader = true,
        });

        if (Stats is { } stats)
        {
            stats.QueueCapacity = Options.Capacity;
            stats.QueueLengthProbe = () => input.Reader.Count;
        }

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var pump = FlowPump.RunAsync(_upstream, input.Writer, cts.Token, Stats);

        try
        {
            var reader = input.Reader;
            var batch = NewBatch();
            long batchStart = 0;

            while (true)
            {
                if (batch.Count == 0)
                {
                    // No open batch: wait indefinitely for a first item; the timer
                    // only runs while a batch is open.
                    if (!await reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        break;
                    }

                    batchStart = _timeProvider.GetTimestamp();
                    Drain(reader, batch);
                    if (batch.Count >= _maximumSize)
                    {
                        // outputs.emitted counts batches, not the items inside them —
                        // batch.items.included (below) carries the item-level count.
                        EmitBatch(batch);
                        yield return batch;
                        batch = NewBatch();
                    }

                    continue;
                }

                var remaining = _maximumDelay - _timeProvider.GetElapsedTime(batchStart);
                if (remaining <= TimeSpan.Zero)
                {
                    EmitBatch(batch);
                    yield return batch;
                    batch = NewBatch();
                    continue;
                }

                // Race the open batch's deadline against the next arrival.
                var outcome = await TimerRace
                    .WaitToReadOrTimeoutAsync(reader, remaining, _timeProvider, cancellationToken)
                    .ConfigureAwait(false);

                if (outcome == TimedWaitOutcome.TimerElapsed)
                {
                    EmitBatch(batch);
                    yield return batch;
                    batch = NewBatch();
                    continue;
                }

                if (outcome == TimedWaitOutcome.Completed)
                {
                    EmitBatch(batch);
                    yield return batch;
                    batch = [];
                    break;
                }

                Drain(reader, batch);
                if (batch.Count >= _maximumSize)
                {
                    EmitBatch(batch);
                    yield return batch;
                    batch = NewBatch();
                }
            }

            if (batch.Count > 0)
            {
                EmitBatch(batch);
                yield return batch;
            }
        }
        finally
        {
            cts.Cancel();
            await TaskHelpers.IgnoreErrorsAsync(pump).ConfigureAwait(false);
        }
    }

    private List<T> NewBatch()
        => new(Math.Min(_maximumSize, 512));

    private void Drain(ChannelReader<T> reader, List<T> batch)
    {
        while (batch.Count < _maximumSize && reader.TryRead(out var item))
        {
            batch.Add(item);
        }
    }

    // outputs.emitted counts the batch itself (what this stage actually handed
    // downstream); batch.items.included separately accumulates the logical items
    // folded into it. Both are true — there is no pretending the two are equal.
    private void EmitBatch(List<T> batch)
    {
        Stats?.OutputEmitted();
        Stats?.RecordBatchItems(batch.Count);
    }
}
