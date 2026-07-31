using System.Runtime.CompilerServices;
using Caudal.Internal;

namespace Caudal;

/// <summary>Operators that reshape a flow: conflation, buffering, batching, expansion.</summary>
public static class FlowShapingExtensions
{
    /// <summary>
    /// Keeps at most one pending item per key: a new arrival replaces the pending
    /// item for its key instead of queueing behind it. An item already delivered
    /// downstream is never interrupted; when its processing finishes, the latest
    /// value received meanwhile is next. This stage intentionally absorbs upstream
    /// pressure — memory is bounded by the number of distinct keys, and every
    /// replacement is counted.
    /// </summary>
    public static Flow<T> LatestByKey<T, TKey>(
        this Flow<T> flow,
        Func<T, TKey> keySelector,
        IEqualityComparer<TKey>? comparer = null)
        where TKey : notnull
    {
        ArgumentNullException.ThrowIfNull(flow);
        ArgumentNullException.ThrowIfNull(keySelector);
        return new Flow<T>(new LatestByKeyFlow<T, TKey>(flow.Node, keySelector, comparer));
    }

    /// <summary>
    /// Inserts an explicit bounded buffer with a declared full-buffer policy.
    /// <see cref="BufferFullMode.Wait"/> (default) propagates backpressure; the drop
    /// modes shed load and count what they drop; <see cref="BufferFullMode.Reject"/>
    /// faults the pipeline with <see cref="FlowBufferFullException"/>.
    /// </summary>
    public static Flow<T> Buffer<T>(
        this Flow<T> flow,
        int capacity,
        BufferFullMode fullMode = BufferFullMode.Wait)
    {
        ArgumentNullException.ThrowIfNull(flow);
        if (capacity < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(capacity), capacity, "Capacity must be at least 1.");
        }

        if (!Enum.IsDefined(fullMode))
        {
            throw new ArgumentOutOfRangeException(nameof(fullMode), fullMode, "Unknown buffer full-mode.");
        }

        return new Flow<T>(new BufferFlow<T>(flow.Node, capacity, fullMode));
    }

    /// <summary>
    /// Groups items into batches. A batch is emitted when whichever comes first:
    /// it reaches <paramref name="maximumSize"/>, <paramref name="maximumDelay"/>
    /// has elapsed since its first item arrived, or the source completes — the final
    /// partial batch is always emitted. Timing uses <paramref name="timeProvider"/>
    /// (system time by default) so tests can drive a fake clock.
    /// </summary>
    public static Flow<IReadOnlyList<T>> Batch<T>(
        this Flow<T> flow,
        int maximumSize,
        TimeSpan maximumDelay,
        TimeProvider? timeProvider = null)
    {
        ArgumentNullException.ThrowIfNull(flow);
        if (maximumSize < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(maximumSize), maximumSize, "Maximum size must be at least 1.");
        }

        if (maximumDelay <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(maximumDelay), maximumDelay, "Maximum delay must be positive.");
        }

        return new Flow<IReadOnlyList<T>>(
            new BatchFlow<T>(flow.Node, maximumSize, maximumDelay, timeProvider ?? TimeProvider.System));
    }

    /// <summary>
    /// Expands each item into a sub-sequence, flattened sequentially in source
    /// order. Useful to turn one server, file, or symbol into several work items.
    /// </summary>
    public static Flow<TResult> SelectManyAsync<TSource, TResult>(
        this Flow<TSource> flow,
        Func<TSource, CancellationToken, IAsyncEnumerable<TResult>> selector)
    {
        ArgumentNullException.ThrowIfNull(flow);
        ArgumentNullException.ThrowIfNull(selector);
        return new Flow<TResult>(new ExpandFlow<TSource, TResult>(flow.Node, selector));
    }

    /// <summary>Expands each item through a selector that does not observe cancellation.</summary>
    public static Flow<TResult> SelectManyAsync<TSource, TResult>(
        this Flow<TSource> flow,
        Func<TSource, IAsyncEnumerable<TResult>> selector)
    {
        ArgumentNullException.ThrowIfNull(selector);
        return flow.SelectManyAsync((item, _) => selector(item));
    }

    /// <summary>Expands each item through an async selector returning a materialized sequence.</summary>
    public static Flow<TResult> SelectManyAsync<TSource, TResult>(
        this Flow<TSource> flow,
        Func<TSource, CancellationToken, Task<IEnumerable<TResult>>> selector)
    {
        ArgumentNullException.ThrowIfNull(selector);
        return flow.SelectManyAsync((item, ct) => ExpandAsync(item, selector, ct));
    }

    private static async IAsyncEnumerable<TResult> ExpandAsync<TSource, TResult>(
        TSource item,
        Func<TSource, CancellationToken, Task<IEnumerable<TResult>>> selector,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        foreach (var result in await selector(item, cancellationToken).ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return result;
        }
    }
}
