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
    public static IFlow<T> From<T>(IAsyncEnumerable<T> source, FlowOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(source);
        options ??= new FlowOptions();
        options.Validate();
        return new SourceFlow<T>(source, options);
    }
}
