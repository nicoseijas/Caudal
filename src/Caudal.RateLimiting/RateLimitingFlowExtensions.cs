using System.Threading.RateLimiting;
using Caudal.Internal;

namespace Caudal;

/// <summary>
/// Rate limiting operators built on <see cref="System.Threading.RateLimiting"/>.
/// Every overload paces the flow by waiting for a permit before emitting each
/// item: a full or exhausted limiter suspends the stage exactly like a full
/// downstream buffer, so waiting for permits is backpressure, not buffering.
/// The limiter produced by a factory is owned by the stage and disposed when
/// the pipeline ends — normally, by fault, or by cancellation.
/// </summary>
public static class RateLimitingFlowExtensions
{
    /// <summary>
    /// Limits the flow to <paramref name="permitLimit"/> items per
    /// <paramref name="window"/>, using a fixed-window limiter. Sugar over the
    /// factory overload: builds a <see cref="FixedWindowRateLimiter"/> with a
    /// queue limit of one, which suffices because this stage acquires permits
    /// sequentially (at most one waiter at a time).
    /// </summary>
    /// <param name="flow">The upstream flow.</param>
    /// <param name="permitLimit">The maximum number of items allowed per window. Must be at least 1.</param>
    /// <param name="window">The window duration. Must be positive.</param>
    public static IFlow<T> RateLimit<T>(this IFlow<T> flow, int permitLimit, TimeSpan window)
    {
        var upstream = FlowBase<T>.FromFlow(flow, nameof(flow));
        ValidatePermitLimit(permitLimit, nameof(permitLimit));
        ValidateWindow(window, nameof(window));

        return new RateLimitFlow<T>(upstream, () => new FixedWindowRateLimiter(new FixedWindowRateLimiterOptions
        {
            PermitLimit = permitLimit,
            Window = window,
            QueueLimit = 1,
            QueueProcessingOrder = QueueProcessingOrder.OldestFirst,
            AutoReplenishment = true,
        }));
    }

    /// <summary>
    /// Limits the flow through a caller-supplied <see cref="RateLimiter"/>. The
    /// factory is invoked once per pipeline run, and the stage disposes the
    /// resulting limiter when the pipeline ends.
    /// </summary>
    /// <remarks>
    /// This stage acquires and releases at most one permit at a time, sequentially;
    /// the lease is released once the next item is requested, not once this item's
    /// downstream processing finishes. Limiter types whose permit release depends on
    /// lease disposal — <c>ConcurrencyLimiter</c> in particular — will therefore NOT
    /// cap concurrent downstream work when used here; they degrade to a near no-op.
    /// To cap concurrent work, use the stage's own <c>concurrency</c> parameter, or
    /// acquire and dispose the permit inside the selector itself. Time-released
    /// limiters (fixed window, sliding window, token bucket) behave as expected.
    /// </remarks>
    /// <param name="flow">The upstream flow.</param>
    /// <param name="limiterFactory">Creates the limiter for a single run of the pipeline.</param>
    public static IFlow<T> RateLimit<T>(this IFlow<T> flow, Func<RateLimiter> limiterFactory)
    {
        var upstream = FlowBase<T>.FromFlow(flow, nameof(flow));
        ArgumentNullException.ThrowIfNull(limiterFactory);

        return new RateLimitFlow<T>(upstream, limiterFactory);
    }

    /// <summary>
    /// Limits the flow to <paramref name="permitLimit"/> items per
    /// <paramref name="window"/>, per key produced by
    /// <paramref name="keySelector"/>, using a fixed-window limiter per
    /// partition. Sugar over the factory overload.
    /// </summary>
    /// <param name="flow">The upstream flow.</param>
    /// <param name="keySelector">Extracts the partition key for an item.</param>
    /// <param name="permitLimit">The maximum number of items allowed per window, per key. Must be at least 1.</param>
    /// <param name="window">The window duration. Must be positive.</param>
    public static IFlow<T> RateLimitBy<T, TKey>(
        this IFlow<T> flow,
        Func<T, TKey> keySelector,
        int permitLimit,
        TimeSpan window)
        where TKey : notnull
    {
        var upstream = FlowBase<T>.FromFlow(flow, nameof(flow));
        ArgumentNullException.ThrowIfNull(keySelector);
        ValidatePermitLimit(permitLimit, nameof(permitLimit));
        ValidateWindow(window, nameof(window));

        return new RateLimitByFlow<T, TKey>(
            upstream,
            keySelector,
            () => PartitionedRateLimiter.Create<TKey, TKey>(key => RateLimitPartition.GetFixedWindowLimiter(
                key,
                _ => new FixedWindowRateLimiterOptions
                {
                    PermitLimit = permitLimit,
                    Window = window,
                    QueueLimit = 1,
                    QueueProcessingOrder = QueueProcessingOrder.OldestFirst,
                    AutoReplenishment = true,
                })));
    }

    /// <summary>
    /// Limits the flow through a caller-supplied
    /// <see cref="PartitionedRateLimiter{TKey}"/>, keyed by
    /// <paramref name="keySelector"/>. The factory is invoked once per pipeline
    /// run, and the stage disposes the resulting limiter when the pipeline ends.
    /// </summary>
    /// <remarks>
    /// The same lease-lifetime caveat as
    /// <see cref="RateLimit{T}(IFlow{T}, Func{RateLimiter})"/> applies: permits are
    /// acquired sequentially and released when the next item is requested, so
    /// lease-disposal-driven limiters (<c>ConcurrencyLimiter</c>) do not cap
    /// concurrent downstream work here. Use time-released limiter types.
    /// </remarks>
    /// <param name="flow">The upstream flow.</param>
    /// <param name="keySelector">Extracts the partition key for an item.</param>
    /// <param name="limiterFactory">Creates the partitioned limiter for a single run of the pipeline.</param>
    public static IFlow<T> RateLimitBy<T, TKey>(
        this IFlow<T> flow,
        Func<T, TKey> keySelector,
        Func<PartitionedRateLimiter<TKey>> limiterFactory)
        where TKey : notnull
    {
        var upstream = FlowBase<T>.FromFlow(flow, nameof(flow));
        ArgumentNullException.ThrowIfNull(keySelector);
        ArgumentNullException.ThrowIfNull(limiterFactory);

        return new RateLimitByFlow<T, TKey>(upstream, keySelector, limiterFactory);
    }

    private static void ValidatePermitLimit(int permitLimit, string parameterName)
    {
        if (permitLimit < 1)
        {
            throw new ArgumentOutOfRangeException(parameterName, permitLimit, "The permit limit must be at least 1.");
        }
    }

    private static void ValidateWindow(TimeSpan window, string parameterName)
    {
        if (window <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(parameterName, window, "The window must be positive.");
        }
    }
}
