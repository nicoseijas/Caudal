using Caudal.Internal;

namespace Caudal;

/// <summary>
/// Time-based operators. All timing goes through <see cref="TimeProvider"/> (system
/// clock by default) so tests can drive a fake clock. Exact semantics, including
/// timing diagrams, are documented in <c>docs/time-operators.md</c> — these names
/// are notoriously ambiguous across libraries.
/// </summary>
public static class FlowTimeExtensions
{
    /// <summary>
    /// Emits an item only after <paramref name="period"/> of silence follows it.
    /// A new arrival within the period replaces the pending item and restarts the
    /// timer; completion flushes the pending item. Replacements are counted.
    /// </summary>
    public static IFlow<T> Debounce<T>(
        this IFlow<T> flow,
        TimeSpan period,
        TimeProvider? timeProvider = null)
    {
        var upstream = FlowBase<T>.FromFlow(flow, nameof(flow));
        ValidatePositive(period, nameof(period));
        return new DebounceFlow<T>(upstream, period, timeProvider ?? TimeProvider.System);
    }

    /// <summary>
    /// Leading-edge rate limit: emits the first item immediately, then drops (and
    /// counts) every arrival during the following <paramref name="period"/>.
    /// </summary>
    public static IFlow<T> Throttle<T>(
        this IFlow<T> flow,
        TimeSpan period,
        TimeProvider? timeProvider = null)
    {
        var upstream = FlowBase<T>.FromFlow(flow, nameof(flow));
        ValidatePositive(period, nameof(period));
        return new ThrottleFlow<T>(upstream, period, timeProvider ?? TimeProvider.System);
    }

    /// <summary>
    /// Emits, every <paramref name="interval"/>, the latest item received since the
    /// previous emission; ticks with nothing new emit nothing. Items overwritten
    /// between ticks are counted; completion flushes the last unsampled item.
    /// </summary>
    public static IFlow<T> Sample<T>(
        this IFlow<T> flow,
        TimeSpan interval,
        TimeProvider? timeProvider = null)
    {
        var upstream = FlowBase<T>.FromFlow(flow, nameof(flow));
        ValidatePositive(interval, nameof(interval));
        return new SampleFlow<T>(upstream, interval, timeProvider ?? TimeProvider.System);
    }

    /// <summary>
    /// Groups everything received while a batch is open into one emission per
    /// <paramref name="interval"/>. Sugar over
    /// <see cref="FlowShapingExtensions.Batch{T}(IFlow{T}, int, TimeSpan, TimeProvider?)"/>
    /// with an effectively unbounded size: the window is anchored at the batch's
    /// first item, not at wall-clock alignment.
    /// </summary>
    public static IFlow<IReadOnlyList<T>> BatchEvery<T>(
        this IFlow<T> flow,
        TimeSpan interval,
        int maximumSize = int.MaxValue,
        TimeProvider? timeProvider = null)
        => flow.Batch(maximumSize, interval, timeProvider);

    /// <summary>
    /// Bounds the silence between consecutive items: if none arrives within
    /// <paramref name="timeout"/>, the pipeline faults with
    /// <see cref="TimeoutException"/>. This times upstream production. To time out
    /// an individual item's processing, attach a timeout resilience strategy to the
    /// processing stage (Caudal.Resilience) instead.
    /// </summary>
    public static IFlow<T> TimeoutEach<T>(
        this IFlow<T> flow,
        TimeSpan timeout,
        TimeProvider? timeProvider = null)
    {
        var upstream = FlowBase<T>.FromFlow(flow, nameof(flow));
        ValidatePositive(timeout, nameof(timeout));
        return new TimeoutEachFlow<T>(upstream, timeout, timeProvider ?? TimeProvider.System);
    }

    /// <summary>
    /// Paces the flow: waits <paramref name="delay"/> before emitting each item,
    /// bounding throughput to one item per delay with natural backpressure.
    /// </summary>
    public static IFlow<T> DelayEach<T>(
        this IFlow<T> flow,
        TimeSpan delay,
        TimeProvider? timeProvider = null)
    {
        var upstream = FlowBase<T>.FromFlow(flow, nameof(flow));
        ValidatePositive(delay, nameof(delay));
        return new DelayEachFlow<T>(upstream, delay, timeProvider ?? TimeProvider.System);
    }

    private static void ValidatePositive(TimeSpan value, string parameterName)
    {
        if (value <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(parameterName, value, "The duration must be positive.");
        }
    }
}
