using Caudal.Internal;

namespace Caudal.Testing;

/// <summary>Entry point for asserting on a Caudal flow's runtime behavior.</summary>
public static class FlowTestingExtensions
{
    /// <summary>
    /// Begins a fluent chain of assertions against <paramref name="flow"/>. Nothing
    /// runs until the returned <see cref="FlowAssertions{T}"/> is awaited or its
    /// <see cref="FlowAssertions{T}.RunAsync"/> is called: that is the terminal sink
    /// that runs the pipeline and evaluates every configured check.
    /// </summary>
    /// <typeparam name="T">The type of the items the flow produces.</typeparam>
    /// <param name="flow">The flow to assert on.</param>
    /// <exception cref="ArgumentNullException"><paramref name="flow"/> is <see langword="null"/>.</exception>
    public static FlowAssertions<T> Should<T>(this Flow<T> flow)
    {
        ArgumentNullException.ThrowIfNull(flow);
        return new FlowAssertions<T>(flow, flow.Node);
    }
}
