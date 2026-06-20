using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace Caudal.Internal;

/// <summary>
/// Emits, at each fixed interval tick, the latest item received since the previous
/// emission; ticks with nothing new emit nothing. Items overwritten between ticks
/// are counted as replaced. Completion flushes the last unsampled item.
/// </summary>
internal sealed class SampleFlow<T> : FlowBase<T>
{
    private readonly FlowBase<T> _upstream;
    private readonly TimeSpan _interval;
    private readonly TimeProvider _timeProvider;
    private long _replaced;

    internal SampleFlow(FlowBase<T> upstream, TimeSpan interval, TimeProvider timeProvider)
        : base(upstream.Options)
    {
        _upstream = upstream;
        _interval = interval;
        _timeProvider = timeProvider;
    }

    /// <summary>How many items were overwritten before a tick could sample them.</summary>
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
            var hasNew = false;
            var latest = default(T);
            var lastTick = _timeProvider.GetTimestamp();

            while (true)
            {
                var remaining = _interval - _timeProvider.GetElapsedTime(lastTick);
                if (remaining <= TimeSpan.Zero)
                {
                    if (hasNew)
                    {
                        yield return latest!;
                        hasNew = false;
                    }

                    lastTick = _timeProvider.GetTimestamp();
                    continue;
                }

                var outcome = await TimerRace
                    .WaitToReadOrTimeoutAsync(reader, remaining, _timeProvider, cancellationToken)
                    .ConfigureAwait(false);

                if (outcome == TimedWaitOutcome.TimerElapsed)
                {
                    if (hasNew)
                    {
                        yield return latest!;
                        hasNew = false;
                    }

                    lastTick = _timeProvider.GetTimestamp();
                    continue;
                }

                if (outcome == TimedWaitOutcome.Completed)
                {
                    // Completion flushes the last unsampled value instead of losing it.
                    if (hasNew)
                    {
                        yield return latest!;
                    }

                    break;
                }

                while (reader.TryRead(out var item))
                {
                    if (hasNew)
                    {
                        Interlocked.Increment(ref _replaced);
                    }

                    latest = item;
                    hasNew = true;
                }
            }
        }
        finally
        {
            cts.Cancel();
            await TaskHelpers.IgnoreErrorsAsync(pump).ConfigureAwait(false);
        }
    }
}
