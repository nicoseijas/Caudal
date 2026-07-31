using Caudal.Internal;

namespace Caudal;

/// <summary>Entry points for building flows.</summary>
public static class Flow
{
    /// <summary>
    /// Creates a flow from an async sequence. The source is read lazily when a sink
    /// is awaited, through a bounded buffer: when the buffer is full, reading pauses
    /// until the pipeline catches up.
    /// </summary>
    /// <typeparam name="T">The type of the items the source produces.</typeparam>
    /// <param name="source">The sequence feeding the pipeline.</param>
    /// <param name="options">Buffer capacity and pipeline name; defaults to <see cref="FlowOptions"/> defaults.</param>
    public static Flow<T> From<T>(IAsyncEnumerable<T> source, FlowOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(source);
        options ??= new FlowOptions();
        options.Validate();
        return new Flow<T>(new SourceFlow<T>(source, options));
    }

    /// <summary>
    /// Interleaves several flows into one, in arrival order with no ordering
    /// guarantee between sources. Backpressure propagates to every source; the
    /// merged flow completes when all sources complete, and the first failure
    /// cancels the rest. Buffer capacity and name come from the first flow.
    /// </summary>
    public static Flow<T> Merge<T>(params Flow<T>[] flows)
        => Merge(flows, options: null);

    /// <summary>
    /// Interleaves several flows into one, with explicit buffer capacity and name;
    /// see <see cref="Merge{T}(Flow{T}[])"/> for the merge semantics.
    /// </summary>
    public static Flow<T> Merge<T>(IEnumerable<Flow<T>> flows, FlowOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(flows);
        var sources = flows
            .Select(flow =>
            {
                ArgumentNullException.ThrowIfNull(flow, nameof(flows));
                return flow.Node;
            })
            .ToArray();

        if (sources.Length == 0)
        {
            throw new ArgumentException("Merge requires at least one flow.", nameof(flows));
        }

        options ??= sources[0].Options;
        options.Validate();
        return new Flow<T>(new MergeFlow<T>(sources, options));
    }
}
