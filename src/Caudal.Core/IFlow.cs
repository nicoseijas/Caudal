namespace Caudal;

/// <summary>
/// A bounded async pipeline stage producing items of type <typeparamref name="T"/>.
/// Flows are lazy: nothing runs until a sink (<c>ForEachAsync</c>, <c>ToListAsync</c>,
/// <c>ConsumeAsync</c>) is awaited, and awaiting the sink is sufficient to know that
/// all internal work has finished.
/// </summary>
/// <typeparam name="T">The type of the items the flow produces.</typeparam>
/// <remarks>
/// This interface is implemented only by flows created through <see cref="Flow"/> or
/// the <c>ToFlow</c> extensions. Custom implementations are not supported: Caudal
/// operators reject them at runtime, because the pipeline contract (bounded buffers,
/// single lifecycle, teardown guarantees) lives in the internal implementation.
/// </remarks>
public interface IFlow<out T>
{
    /// <summary>The optional name of the pipeline, used for diagnostics.</summary>
    string? Name { get; }
}
