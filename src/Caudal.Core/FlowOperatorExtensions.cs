using System.Runtime.CompilerServices;
using Caudal.Internal;

namespace Caudal;

/// <summary>Transforming operators.</summary>
public static class FlowOperatorExtensions
{
    /// <summary>
    /// Projects each item through an async selector with bounded concurrency.
    /// With <paramref name="preserveOrder"/> <see langword="false"/> (the default),
    /// results are delivered in completion order; with <see langword="true"/>, in
    /// source order. The first selector exception stops the pipeline and is rethrown
    /// unwrapped at the sink.
    /// </summary>
    public static IFlow<TResult> SelectAsync<TSource, TResult>(
        this IFlow<TSource> flow,
        Func<TSource, CancellationToken, Task<TResult>> selector,
        int concurrency = 1,
        bool preserveOrder = false)
        => flow.SelectAsync(selector, new SelectAsyncOptions
        {
            Concurrency = concurrency,
            PreserveOrder = preserveOrder,
        });

    /// <summary>Projects each item through an async selector that does not observe cancellation.</summary>
    public static IFlow<TResult> SelectAsync<TSource, TResult>(
        this IFlow<TSource> flow,
        Func<TSource, Task<TResult>> selector,
        int concurrency = 1,
        bool preserveOrder = false)
    {
        ArgumentNullException.ThrowIfNull(selector);
        return flow.SelectAsync((item, _) => selector(item), concurrency, preserveOrder);
    }

    /// <summary>Projects each item through an async selector, with full options.</summary>
    public static IFlow<TResult> SelectAsync<TSource, TResult>(
        this IFlow<TSource> flow,
        Func<TSource, CancellationToken, Task<TResult>> selector,
        SelectAsyncOptions options)
    {
        var upstream = FlowBase<TSource>.FromFlow(flow, nameof(flow));
        ArgumentNullException.ThrowIfNull(selector);
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();
        return new SelectFlow<TSource, TResult>(upstream, selector, options);
    }

    /// <summary>
    /// Filters items through an async predicate with bounded concurrency. Items that
    /// pass are delivered in source order relative to each other.
    /// </summary>
    public static IFlow<T> WhereAsync<T>(
        this IFlow<T> flow,
        Func<T, CancellationToken, Task<bool>> predicate,
        int concurrency = 1)
    {
        var upstream = FlowBase<T>.FromFlow(flow, nameof(flow));
        ArgumentNullException.ThrowIfNull(predicate);
        var options = new SelectAsyncOptions { Concurrency = concurrency, PreserveOrder = true };
        options.Validate();

        var evaluated = new SelectFlow<T, (T Item, bool Keep)>(
            upstream,
            async (item, ct) => (item, await predicate(item, ct).ConfigureAwait(false)),
            options);

        return new IteratorFlow<(T Item, bool Keep), T>(evaluated, static (source, ct) => Filter(source, ct));
    }

    /// <summary>Filters items through an async predicate that does not observe cancellation.</summary>
    public static IFlow<T> WhereAsync<T>(
        this IFlow<T> flow,
        Func<T, Task<bool>> predicate,
        int concurrency = 1)
    {
        ArgumentNullException.ThrowIfNull(predicate);
        return flow.WhereAsync((item, _) => predicate(item), concurrency);
    }

    private static async IAsyncEnumerable<T> Filter<T>(
        IAsyncEnumerable<(T Item, bool Keep)> source,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await foreach (var (item, keep) in source.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            if (keep)
            {
                yield return item;
            }
        }
    }
}
