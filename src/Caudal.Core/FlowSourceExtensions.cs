using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace Caudal;

/// <summary>Extensions that turn existing sequences into flows.</summary>
public static class FlowSourceExtensions
{
    /// <summary>Creates a flow from an async sequence with default options.</summary>
    public static IFlow<T> ToFlow<T>(this IAsyncEnumerable<T> source, FlowOptions? options = null)
        => Flow.From(source, options);

    /// <summary>Creates a flow from an async sequence with an explicit buffer capacity.</summary>
    public static IFlow<T> ToFlow<T>(this IAsyncEnumerable<T> source, int capacity, string? name = null)
        => Flow.From(source, new FlowOptions { Capacity = capacity, Name = name });

    /// <summary>Creates a flow from a synchronous sequence with default options.</summary>
    public static IFlow<T> ToFlow<T>(this IEnumerable<T> source, FlowOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(source);
        return Flow.From(AsAsyncEnumerable(source), options);
    }

    /// <summary>Creates a flow from a synchronous sequence with an explicit buffer capacity.</summary>
    public static IFlow<T> ToFlow<T>(this IEnumerable<T> source, int capacity, string? name = null)
    {
        ArgumentNullException.ThrowIfNull(source);
        return Flow.From(AsAsyncEnumerable(source), new FlowOptions { Capacity = capacity, Name = name });
    }

    /// <summary>Creates a flow that drains a channel reader with default options.</summary>
    public static IFlow<T> ToFlow<T>(this ChannelReader<T> reader, FlowOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(reader);
        return Flow.From(reader.ReadAllAsync(), options);
    }

    /// <summary>Creates a flow that drains a channel reader with an explicit buffer capacity.</summary>
    public static IFlow<T> ToFlow<T>(this ChannelReader<T> reader, int capacity, string? name = null)
    {
        ArgumentNullException.ThrowIfNull(reader);
        return Flow.From(reader.ReadAllAsync(), new FlowOptions { Capacity = capacity, Name = name });
    }

    private static async IAsyncEnumerable<T> AsAsyncEnumerable<T>(
        IEnumerable<T> source,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        // Yield synchronously; the await keeps this a valid async iterator and lets
        // a cancelled pipeline stop between items.
        foreach (var item in source)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return item;
        }

        await Task.CompletedTask.ConfigureAwait(false);
    }
}
