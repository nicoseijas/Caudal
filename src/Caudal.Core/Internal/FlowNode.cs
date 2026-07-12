namespace Caudal.Internal;

/// <summary>
/// The non-generic spine of a pipeline: every stage links to its upstream node,
/// so diagnostics can walk a flow from its terminal stage back to the source
/// without knowing the element types along the way. Statistics are attached here,
/// created only when <see cref="FlowOptions.CaptureStatistics"/> is set.
/// </summary>
internal abstract class FlowNode
{
    protected FlowNode(FlowNode? upstreamNode, string operatorName, FlowOptions options)
    {
        UpstreamNode = upstreamNode;
        OperatorName = operatorName;
        Options = options;
        Stats = options.CaptureStatistics ? new StageStats(operatorName) : null;
    }

    internal FlowNode? UpstreamNode { get; }

    internal string OperatorName { get; }

    internal FlowOptions Options { get; }

    internal StageStats? Stats { get; }
}
