namespace Caudal.Internal;

internal abstract class FlowBase<T> : FlowNode
{
    protected FlowBase(FlowNode? upstreamNode, string operatorName, FlowOptions options)
        : base(upstreamNode, operatorName, options)
    {
    }

    public string? Name => Options.Name;

    /// <summary>
    /// Starts the stage and yields its results. Implementations must guarantee that
    /// when enumeration ends — normally, by fault, or by cancellation — every task
    /// they started has completed.
    /// </summary>
    public abstract IAsyncEnumerable<T> Enumerate(CancellationToken cancellationToken);
}
