namespace Caudal.Internal;

/// <summary>
/// A lightweight synchronous-shape stage (no channel, no tasks of its own) that
/// re-shapes the upstream sequence — used for filtering.
/// </summary>
internal sealed class IteratorFlow<TSource, TResult> : FlowBase<TResult>
{
    private readonly FlowBase<TSource> _upstream;
    private readonly Func<IAsyncEnumerable<TSource>, CancellationToken, IAsyncEnumerable<TResult>> _transform;

    internal IteratorFlow(
        FlowBase<TSource> upstream,
        Func<IAsyncEnumerable<TSource>, CancellationToken, IAsyncEnumerable<TResult>> transform)
        : base(upstream.Options)
    {
        _upstream = upstream;
        _transform = transform;
    }

    public override IAsyncEnumerable<TResult> Enumerate(CancellationToken cancellationToken)
        => _transform(_upstream.Enumerate(cancellationToken), cancellationToken);
}
