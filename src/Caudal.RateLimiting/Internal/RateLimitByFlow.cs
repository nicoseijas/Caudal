using System.Runtime.CompilerServices;
using System.Threading.RateLimiting;

namespace Caudal.Internal;

/// <summary>
/// Paces the flow per key through a single <see cref="PartitionedRateLimiter{TKey}"/>:
/// one permit is acquired per item, scoped to the key produced by the key
/// selector. Waiting for a permit is backpressure, not
/// buffering — while an item waits for its key's permit, upstream fills its
/// bounded buffers and then waits too. The limiter created by the factory is
/// owned by this stage and disposed when enumeration ends, whether by
/// completion, fault, or cancellation.
/// </summary>
internal sealed class RateLimitByFlow<T, TKey> : FlowBase<T>
    where TKey : notnull
{
    private readonly FlowBase<T> _upstream;
    private readonly Func<T, TKey> _keySelector;
    private readonly Func<PartitionedRateLimiter<TKey>> _limiterFactory;

    internal RateLimitByFlow(
        FlowBase<T> upstream,
        Func<T, TKey> keySelector,
        Func<PartitionedRateLimiter<TKey>> limiterFactory)
        : base(upstream, "RateLimitBy", upstream.Options)
    {
        _upstream = upstream;
        _keySelector = keySelector;
        _limiterFactory = limiterFactory;
    }

    public override async IAsyncEnumerable<T> Enumerate(
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        Stats?.MarkStarted();

        var limiter = _limiterFactory();
        try
        {
            await foreach (var item in _upstream.Enumerate(cancellationToken).ConfigureAwait(false))
            {
                Stats?.ItemReceived();
                var key = _keySelector(item);
                using var lease = await limiter.AcquireAsync(key, 1, cancellationToken).ConfigureAwait(false);
                if (!lease.IsAcquired)
                {
                    throw new InvalidOperationException(
                        "The rate limiter rejected the acquisition (queue limit exceeded).");
                }

                Stats?.ItemCompleted();
                yield return item;
            }
        }
        finally
        {
            await limiter.DisposeAsync().ConfigureAwait(false);
        }
    }
}
