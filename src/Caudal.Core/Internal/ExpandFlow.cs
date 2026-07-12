using System.Runtime.CompilerServices;

namespace Caudal.Internal;

/// <summary>
/// Expands each item into a sub-sequence, flattened sequentially in source order.
/// No channel of its own: the sub-sequences are pulled on demand, so backpressure
/// propagates naturally through the enumerators.
/// </summary>
internal sealed class ExpandFlow<TSource, TResult> : FlowBase<TResult>
{
    private readonly FlowBase<TSource> _upstream;
    private readonly Func<TSource, CancellationToken, IAsyncEnumerable<TResult>> _selector;

    internal ExpandFlow(
        FlowBase<TSource> upstream,
        Func<TSource, CancellationToken, IAsyncEnumerable<TResult>> selector)
        : base(upstream, "SelectManyAsync", upstream.Options)
    {
        _upstream = upstream;
        _selector = selector;
    }

    public override async IAsyncEnumerable<TResult> Enumerate(
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        Stats?.MarkStarted();

        await foreach (var item in _upstream.Enumerate(cancellationToken).ConfigureAwait(false))
        {
            Stats?.ItemReceived();
            await foreach (var result in _selector(item, cancellationToken)
                .WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                Stats?.ItemCompleted();
                yield return result;
            }
        }
    }
}
