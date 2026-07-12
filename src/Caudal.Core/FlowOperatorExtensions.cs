using Caudal.Internal;

namespace Caudal;

/// <summary>Transforming operators.</summary>
public static class FlowOperatorExtensions
{
    /// <summary>
    /// Projects each item through an async selector with bounded concurrency.
    /// With <paramref name="preserveOrder"/> <see langword="false"/> (the default),
    /// results are delivered in completion order; with <see langword="true"/>, in
    /// source order. <paramref name="failureMode"/> decides what a selector exception
    /// does: <see cref="FlowFailureMode.Stop"/> (default) faults the pipeline with the
    /// original exception; <see cref="FlowFailureMode.Skip"/> drops the item and
    /// continues. For <see cref="FlowFailureMode.Capture"/> use
    /// <see cref="SelectResultAsync{TSource, TResult}(IFlow{TSource}, Func{TSource, CancellationToken, Task{TResult}}, int, bool)"/>.
    /// </summary>
    public static IFlow<TResult> SelectAsync<TSource, TResult>(
        this IFlow<TSource> flow,
        Func<TSource, CancellationToken, Task<TResult>> selector,
        int concurrency = 1,
        bool preserveOrder = false,
        FlowFailureMode failureMode = FlowFailureMode.Stop)
        => flow.SelectAsync(selector, new SelectAsyncOptions
        {
            Concurrency = concurrency,
            PreserveOrder = preserveOrder,
            FailureMode = failureMode,
        });

    /// <summary>Projects each item through an async selector that does not observe cancellation.</summary>
    public static IFlow<TResult> SelectAsync<TSource, TResult>(
        this IFlow<TSource> flow,
        Func<TSource, Task<TResult>> selector,
        int concurrency = 1,
        bool preserveOrder = false,
        FlowFailureMode failureMode = FlowFailureMode.Stop)
    {
        ArgumentNullException.ThrowIfNull(selector);
        return flow.SelectAsync((item, _) => selector(item), concurrency, preserveOrder, failureMode);
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

        if (options.FailureMode == FlowFailureMode.Capture)
        {
            throw new ArgumentException(
                "Capture changes the result type to FlowResult<T>; use SelectResultAsync instead.",
                nameof(options));
        }

        return new SelectFlow<TSource, TResult>(
            upstream,
            "SelectAsync",
            FailurePolicy.Wrap(selector, options.FailureMode),
            options);
    }

    /// <summary>
    /// Projects each item under <see cref="FlowFailureMode.Capture"/>: every item
    /// produces a <see cref="FlowResult{T}"/> — the value on success, the original
    /// exception on failure — and the pipeline never faults on selector exceptions.
    /// Cancellation is still cancellation: it propagates, it is never captured.
    /// </summary>
    public static IFlow<FlowResult<TResult>> SelectResultAsync<TSource, TResult>(
        this IFlow<TSource> flow,
        Func<TSource, CancellationToken, Task<TResult>> selector,
        int concurrency = 1,
        bool preserveOrder = false)
    {
        var upstream = FlowBase<TSource>.FromFlow(flow, nameof(flow));
        ArgumentNullException.ThrowIfNull(selector);
        var options = new SelectAsyncOptions
        {
            Concurrency = concurrency,
            PreserveOrder = preserveOrder,
            FailureMode = FlowFailureMode.Capture,
        };
        options.Validate();

        return new SelectFlow<TSource, FlowResult<TResult>>(
            upstream,
            "SelectResultAsync",
            FailurePolicy.WrapCapture(selector),
            options);
    }

    /// <summary>Projects each item under Capture with a selector that does not observe cancellation.</summary>
    public static IFlow<FlowResult<TResult>> SelectResultAsync<TSource, TResult>(
        this IFlow<TSource> flow,
        Func<TSource, Task<TResult>> selector,
        int concurrency = 1,
        bool preserveOrder = false)
    {
        ArgumentNullException.ThrowIfNull(selector);
        return flow.SelectResultAsync((item, _) => selector(item), concurrency, preserveOrder);
    }

    /// <summary>
    /// Filters items through an async predicate with bounded concurrency. Items that
    /// pass are delivered in source order relative to each other. With
    /// <see cref="FlowFailureMode.Skip"/>, an item whose predicate throws is dropped;
    /// with <see cref="FlowFailureMode.Stop"/> (default) the pipeline faults.
    /// </summary>
    public static IFlow<T> WhereAsync<T>(
        this IFlow<T> flow,
        Func<T, CancellationToken, Task<bool>> predicate,
        int concurrency = 1,
        FlowFailureMode failureMode = FlowFailureMode.Stop)
    {
        var upstream = FlowBase<T>.FromFlow(flow, nameof(flow));
        ArgumentNullException.ThrowIfNull(predicate);

        if (failureMode == FlowFailureMode.Capture)
        {
            throw new ArgumentException(
                "Capture does not apply to a filter; use SelectResultAsync to capture failures.",
                nameof(failureMode));
        }

        var options = new SelectAsyncOptions
        {
            Concurrency = concurrency,
            PreserveOrder = true,
            FailureMode = failureMode,
        };
        options.Validate();

        // A filter is a select whose stage result decides emission per item.
        Func<T, CancellationToken, Task<StageResult<T>>> evaluate = async (item, ct) =>
            await predicate(item, ct).ConfigureAwait(false)
                ? StageResult<T>.From(item)
                : StageResult<T>.Nothing;

        return new SelectFlow<T, T>(
            upstream,
            "WhereAsync",
            failureMode == FlowFailureMode.Skip ? SkipOnFailure(evaluate) : evaluate,
            options);
    }

    /// <summary>Filters items through an async predicate that does not observe cancellation.</summary>
    public static IFlow<T> WhereAsync<T>(
        this IFlow<T> flow,
        Func<T, Task<bool>> predicate,
        int concurrency = 1,
        FlowFailureMode failureMode = FlowFailureMode.Stop)
    {
        ArgumentNullException.ThrowIfNull(predicate);
        return flow.WhereAsync((item, _) => predicate(item), concurrency, failureMode);
    }

    private static Func<T, CancellationToken, Task<StageResult<T>>> SkipOnFailure<T>(
        Func<T, CancellationToken, Task<StageResult<T>>> evaluate)
        => async (item, ct) =>
        {
            try
            {
                return await evaluate(item, ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException oce)
                when (oce.CancellationToken == ct && ct.IsCancellationRequested)
            {
                throw;
            }
            catch
            {
                // Dropped by explicit Skip policy: a failure, distinct from a
                // predicate returning false (a filter miss, not a failure).
                return StageResult<T>.SkippedFailure;
            }
        };
}
