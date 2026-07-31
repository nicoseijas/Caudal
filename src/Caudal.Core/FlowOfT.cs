using Caudal.Internal;

namespace Caudal;

/// <summary>
/// A bounded async pipeline stage producing items of type <typeparamref name="T"/>.
/// Flows are lazy: nothing runs until a sink (<c>ForEachAsync</c>, <c>ToListAsync</c>,
/// <c>ConsumeAsync</c>) is awaited, and awaiting the sink is sufficient to know that
/// all internal work has finished. Flows are created only through <see cref="Flow"/>
/// and the <c>ToFlow</c> extensions — the type is sealed with an internal
/// constructor, so the pipeline contract (bounded buffers, single lifecycle,
/// teardown guarantees) cannot be subverted by external implementations.
/// </summary>
public sealed class Flow<T>
{
    private readonly FlowBase<T> _node;

    internal Flow(FlowBase<T> node) => _node = node;

    internal FlowBase<T> Node => _node;

    /// <summary>The optional name of the pipeline, used for diagnostics.</summary>
    public string? Name => _node.Name;
}
