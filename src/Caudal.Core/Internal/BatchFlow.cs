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
        : base(upstream.Options)
    {
        _upstream = upstream;
        _maximumSize = maximumSize;
        _maximumDelay = maximumDelay;
        _timeProvider = timeProvider;
    }

    public override async IAsyncEnumerable<IReadOnlyList<T>> Enumerate(
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var input = Channel.CreateBounded<T>(new BoundedChannelOptions(Options.Capacity)
        {
            SingleWriter = true,
            SingleReader = true,
        });

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var pump = FlowPump.RunAsync(_upstream, input.Writer, cts.Token);

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
                        yield return batch;
                        batch = NewBatch();
                    }

                    continue;
                }

                var remaining = _maximumDelay - _timeProvider.GetElapsedTime(batchStart);
                if (remaining <= TimeSpan.Zero)
                {
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
                    yield return batch;
                    batch = NewBatch();
                    continue;
                }

                if (outcome == TimedWaitOutcome.Completed)
                {
                    yield return batch;
                    batch = [];
                    break;
                }

                Drain(reader, batch);
                if (batch.Count >= _maximumSize)
                {
                    yield return batch;
                    batch = NewBatch();
                }
            }

            if (batch.Count > 0)
            {
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

}
