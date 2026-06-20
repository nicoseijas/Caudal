using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace Caudal.Internal;

/// <summary>
/// Emits an item only after a full period of silence follows it. A new arrival
/// within the period replaces the pending item and restarts the timer, so a bursty
/// stream collapses to its last value per burst. Completion flushes the pending
/// item immediately; replacements are counted.
/// </summary>
internal sealed class DebounceFlow<T> : FlowBase<T>
{
    private readonly FlowBase<T> _upstream;
    private readonly TimeSpan _period;
    private readonly TimeProvider _timeProvider;
    private long _replaced;

    internal DebounceFlow(FlowBase<T> upstream, TimeSpan period, TimeProvider timeProvider)
        : base(upstream.Options)
    {
        _upstream = upstream;
        _period = period;
        _timeProvider = timeProvider;
    }

    /// <summary>How many pending items were replaced by a newer arrival.</summary>
    internal long ReplacedCount => Interlocked.Read(ref _replaced);

    public override async IAsyncEnumerable<T> Enumerate(
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
            var hasPending = false;
            var pending = default(T);
            long pendingSince = 0;

            while (true)
            {
                if (!hasPending)
                {
                    if (!await reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        break;
                    }

                    (hasPending, pending, pendingSince) = DrainToLatest(reader, hasPending, pending);
                    continue;
                }

                var remaining = _period - _timeProvider.GetElapsedTime(pendingSince);
                if (remaining <= TimeSpan.Zero)
                {
                    yield return pending!;
                    hasPending = false;
                    continue;
                }

                var outcome = await TimerRace
                    .WaitToReadOrTimeoutAsync(reader, remaining, _timeProvider, cancellationToken)
                    .ConfigureAwait(false);

                if (outcome == TimedWaitOutcome.TimerElapsed)
                {
                    yield return pending!;
                    hasPending = false;
                    continue;
                }

                if (outcome == TimedWaitOutcome.Completed)
                {
                    // Completion flushes the pending item instead of losing it.
                    yield return pending!;
                    break;
                }

                // A new arrival replaces the pending item and restarts the timer.
                (hasPending, pending, pendingSince) = DrainToLatest(reader, hasPending, pending);
            }
        }
        finally
        {
            cts.Cancel();
            await TaskHelpers.IgnoreErrorsAsync(pump).ConfigureAwait(false);
        }
    }

    private (bool HasPending, T? Pending, long Since) DrainToLatest(
        ChannelReader<T> reader,
        bool hasPending,
        T? pending)
    {
        while (reader.TryRead(out var item))
        {
            if (hasPending)
            {
                Interlocked.Increment(ref _replaced);
            }

            pending = item;
            hasPending = true;
        }

        return (hasPending, pending, _timeProvider.GetTimestamp());
    }
}
