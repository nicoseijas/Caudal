using System.Runtime.CompilerServices;

namespace Caudal.Internal;

/// <summary>
/// Leading-edge rate limiting: the first item is emitted immediately, then every
/// arrival during the following period is dropped (and counted). A pure
/// pass-through stage — no channel, no timers; it only reads the clock.
/// </summary>
internal sealed class ThrottleFlow<T> : FlowBase<T>
{
    private readonly FlowBase<T> _upstream;
    private readonly TimeSpan _period;
    private readonly TimeProvider _timeProvider;
    private long _dropped;

    internal ThrottleFlow(FlowBase<T> upstream, TimeSpan period, TimeProvider timeProvider)
        : base(upstream, "Throttle", upstream.Options)
    {
        _upstream = upstream;
        _period = period;
        _timeProvider = timeProvider;
    }

    /// <summary>How many items were dropped inside a throttle window.</summary>
    internal long DroppedCount => Interlocked.Read(ref _dropped);

    public override async IAsyncEnumerable<T> Enumerate(
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        Stats?.MarkStarted();

        var emittedAny = false;
        long lastEmit = 0;

        await foreach (var item in _upstream.Enumerate(cancellationToken).ConfigureAwait(false))
        {
            Stats?.ItemReceived();
            if (!emittedAny || _timeProvider.GetElapsedTime(lastEmit) >= _period)
            {
                emittedAny = true;
                lastEmit = _timeProvider.GetTimestamp();
                Stats?.ItemCompleted();
                yield return item;
            }
            else
            {
                Interlocked.Increment(ref _dropped);
                Stats?.ItemDropped();
            }
        }
    }
}
