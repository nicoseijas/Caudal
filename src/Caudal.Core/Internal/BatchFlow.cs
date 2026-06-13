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
        var pump = PumpAsync(input.Writer, cts.Token);

        try
        {
            var reader = input.Reader;
            var batch = new List<T>(_maximumSize);
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
                        batch = new List<T>(_maximumSize);
                    }

                    continue;
                }

                var remaining = _maximumDelay - _timeProvider.GetElapsedTime(batchStart);
                if (remaining <= TimeSpan.Zero)
                {
                    yield return batch;
                    batch = new List<T>(_maximumSize);
                    continue;
                }

                // Race the open batch's deadline against the next arrival.
                bool timerWon;
                bool hasMore = true;
                using (var raceCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken))
                {
                    var waitTask = reader.WaitToReadAsync(raceCts.Token).AsTask();
                    var timerTask = Task.Delay(remaining, _timeProvider, raceCts.Token);
                    var winner = await Task.WhenAny(waitTask, timerTask).ConfigureAwait(false);
                    raceCts.Cancel();

                    timerWon = winner == timerTask;
                    if (timerWon)
                    {
                        // The loser was cancelled by raceCts; observe it. A channel
                        // fault swallowed here resurfaces on the next WaitToReadAsync.
                        await TaskHelpers.IgnoreErrorsAsync(waitTask).ConfigureAwait(false);
                    }
                    else
                    {
                        await TaskHelpers.IgnoreErrorsAsync(timerTask).ConfigureAwait(false);

                        // Completed before the cancel: rethrows the original channel
                        // fault if the upstream failed.
                        hasMore = await waitTask.ConfigureAwait(false);
                    }
                }

                if (timerWon)
                {
                    yield return batch;
                    batch = new List<T>(_maximumSize);
                    continue;
                }

                if (!hasMore)
                {
                    yield return batch;
                    batch = [];
                    break;
                }

                Drain(reader, batch);
                if (batch.Count >= _maximumSize)
                {
                    yield return batch;
                    batch = new List<T>(_maximumSize);
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

    private void Drain(ChannelReader<T> reader, List<T> batch)
    {
        while (batch.Count < _maximumSize && reader.TryRead(out var item))
        {
            batch.Add(item);
        }
    }

    private async Task PumpAsync(ChannelWriter<T> writer, CancellationToken cancellationToken)
    {
        try
        {
            await foreach (var item in _upstream.Enumerate(cancellationToken).ConfigureAwait(false))
            {
                await writer.WriteAsync(item, cancellationToken).ConfigureAwait(false);
            }

            writer.Complete();
        }
        catch (Exception ex)
        {
            writer.TryComplete(ex);
        }
    }
}
