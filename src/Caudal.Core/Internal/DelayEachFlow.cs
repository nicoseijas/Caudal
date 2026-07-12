using System.Runtime.CompilerServices;

namespace Caudal.Internal;

/// <summary>
/// Paces the flow: waits a fixed delay before emitting each item, bounding
/// throughput to one item per delay. Backpressure propagates naturally — while
/// this stage waits, upstream fills its bounded buffers and then waits too.
/// </summary>
internal sealed class DelayEachFlow<T> : FlowBase<T>
{
    private readonly FlowBase<T> _upstream;
    private readonly TimeSpan _delay;
    private readonly TimeProvider _timeProvider;

    internal DelayEachFlow(FlowBase<T> upstream, TimeSpan delay, TimeProvider timeProvider)
        : base(upstream, "DelayEach", upstream.Options)
    {
        _upstream = upstream;
        _delay = delay;
        _timeProvider = timeProvider;
    }

    public override async IAsyncEnumerable<T> Enumerate(
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        Stats?.MarkStarted();

        await foreach (var item in _upstream.Enumerate(cancellationToken).ConfigureAwait(false))
        {
            Stats?.ItemReceived();
            await Task.Delay(_delay, _timeProvider, cancellationToken).ConfigureAwait(false);
            Stats?.ItemCompleted();
            yield return item;
        }
    }
}
