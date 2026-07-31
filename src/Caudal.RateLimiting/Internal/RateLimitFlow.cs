using System.Runtime.CompilerServices;
using System.Threading.RateLimiting;

namespace Caudal.Internal;

/// <summary>
/// Paces the flow through a single <see cref="RateLimiter"/>: one permit is
/// acquired per item before it is emitted. Waiting for a permit is backpressure,
/// not buffering — while this stage waits, upstream fills its bounded buffers and
/// then waits too. The limiter created by the factory is owned by this stage and
/// disposed when enumeration ends, whether by completion, fault, or cancellation.
/// </summary>
internal sealed class RateLimitFlow<T> : FlowBase<T>
{
    private readonly FlowBase<T> _upstream;
    private readonly Func<RateLimiter> _limiterFactory;

    internal RateLimitFlow(FlowBase<T> upstream, Func<RateLimiter> limiterFactory)
        : base(upstream, "RateLimit", upstream.Options)
    {
        _upstream = upstream;
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
                Stats?.InputReceived();
                using var lease = await limiter.AcquireAsync(1, cancellationToken).ConfigureAwait(false);
                if (!lease.IsAcquired)
                {
                    throw new InvalidOperationException(
                        "The rate limiter rejected the acquisition (queue limit exceeded).");
                }

                Stats?.OutputEmitted();
                yield return item;
            }
        }
        finally
        {
            await limiter.DisposeAsync().ConfigureAwait(false);
        }
    }
}
