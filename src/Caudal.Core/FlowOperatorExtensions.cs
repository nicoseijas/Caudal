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
    /// <see cref="SelectResultAsync{TSource, TResult}(Flow{TSource}, Func{TSource, CancellationToken, Task{TResult}}, int, bool)"/>.
    /// </summary>
    public static Flow<TResult> SelectAsync<TSource, TResult>(
        this Flow<TSource> flow,
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
    public static Flow<TResult> SelectAsync<TSource, TResult>(
        this Flow<TSource> flow,
        Func<TSource, Task<TResult>> selector,
        int concurrency = 1,
        bool preserveOrder = false,
        FlowFailureMode failureMode = FlowFailureMode.Stop)
    {
        ArgumentNullException.ThrowIfNull(selector);
        return flow.SelectAsync((item, _) => selector(item), concurrency, preserveOrder, failureMode);
    }

    /// <summary>Projects each item through an async selector, with full options.</summary>
    public static Flow<TResult> SelectAsync<TSource, TResult>(
        this Flow<TSource> flow,
        Func<TSource, CancellationToken, Task<TResult>> selector,
        SelectAsyncOptions options)
    {
        ArgumentNullException.ThrowIfNull(flow);
        ArgumentNullException.ThrowIfNull(selector);
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();

        if (options.FailureMode == FlowFailureMode.Capture)
        {
            throw new ArgumentException(
                "Capture changes the result type to FlowResult<T>; use SelectResultAsync instead.",
                nameof(options));
        }

        return new Flow<TResult>(new SelectFlow<TSource, TResult>(
            flow.Node,
            "SelectAsync",
            FailurePolicy.Wrap(selector, options.FailureMode),
            options));
    }

    /// <summary>
    /// Projects each item under <see cref="FlowFailureMode.Capture"/>: every item
    /// produces a <see cref="FlowResult{T}"/> — the value on success, the original
    /// exception on failure — and the pipeline never faults on selector exceptions.
    /// Cancellation is still cancellation: it propagates, it is never captured.
    /// </summary>
    public static Flow<FlowResult<TResult>> SelectResultAsync<TSource, TResult>(
        this Flow<TSource> flow,
        Func<TSource, CancellationToken, Task<TResult>> selector,
        int concurrency = 1,
        bool preserveOrder = false)
    {
        ArgumentNullException.ThrowIfNull(flow);
        ArgumentNullException.ThrowIfNull(selector);
        var options = new SelectAsyncOptions
        {
            Concurrency = concurrency,
            PreserveOrder = preserveOrder,
            FailureMode = FlowFailureMode.Capture,
        };
        options.Validate();

        return new Flow<FlowResult<TResult>>(new SelectFlow<TSource, FlowResult<TResult>>(
            flow.Node,
            "SelectResultAsync",
            FailurePolicy.WrapCapture(selector),
            options));
    }

    /// <summary>Projects each item under Capture with a selector that does not observe cancellation.</summary>
    public static Flow<FlowResult<TResult>> SelectResultAsync<TSource, TResult>(
        this Flow<TSource> flow,
        Func<TSource, Task<TResult>> selector,
        int concurrency = 1,
        bool preserveOrder = false)
    {
        ArgumentNullException.ThrowIfNull(selector);
        return flow.SelectResultAsync((item, _) => selector(item), concurrency, preserveOrder);
    }

    /// <summary>
    /// Projects each item through an async selector, latest-wins per key: at most one
    /// invocation runs for a given key at a time, and a value arriving while its key is
    /// executing replaces any other value waiting for that key instead of queueing
    /// behind it. When an execution finishes, the latest value received meanwhile is
    /// the one that runs next. <paramref name="concurrency"/> still bounds how many
    /// keys execute at once.
    /// <para>
    /// This is the serializing form of conflation, and it is deliberately not
    /// <c>LatestByKey().SelectAsync()</c>: a standalone <c>LatestByKey</c> conflates
    /// only until it emits, so with any concurrency or buffering downstream two values
    /// for one key can be processed at the same time. Use that one to thin a feed, and
    /// this one when processing a key twice at once is wrong.
    /// </para>
    /// <para>
    /// Memory is bounded by <paramref name="maximumKeys"/> tracked keys — executing or
    /// waiting. Replacements are free and counted (<c>inputs.replaced</c>); a genuinely
    /// new key arriving once that many keys are tracked faults the pipeline with
    /// <see cref="FlowKeyCapacityException"/>. Results are delivered in completion
    /// order across keys, and in execution order within one key.
    /// </para>
    /// </summary>
    public static Flow<TResult> SelectLatestByKeyAsync<TSource, TResult, TKey>(
        this Flow<TSource> flow,
        Func<TSource, TKey> keySelector,
        Func<TSource, CancellationToken, Task<TResult>> selector,
        int concurrency,
        int maximumKeys,
        FlowFailureMode failureMode = FlowFailureMode.Stop,
        IEqualityComparer<TKey>? comparer = null)
        where TKey : notnull
    {
        ArgumentNullException.ThrowIfNull(flow);
        ArgumentNullException.ThrowIfNull(keySelector);
        ArgumentNullException.ThrowIfNull(selector);
        if (concurrency < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(concurrency), concurrency, "Concurrency must be at least 1.");
        }

        if (maximumKeys < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(maximumKeys), maximumKeys, "Maximum keys must be at least 1.");
        }

        if (!Enum.IsDefined(failureMode))
        {
            throw new ArgumentOutOfRangeException(nameof(failureMode), failureMode, "Unknown failure mode.");
        }

        if (failureMode == FlowFailureMode.Capture)
        {
            throw new ArgumentException(
                "Capture changes the result type to FlowResult<T>, which this operator does not express; use Stop or Skip.",
                nameof(failureMode));
        }

        return new Flow<TResult>(new SelectLatestByKeyFlow<TSource, TResult, TKey>(
            flow.Node,
            keySelector,
            FailurePolicy.Wrap(selector, failureMode),
            concurrency,
            maximumKeys,
            comparer));
    }

    /// <summary>Projects each item latest-wins per key with a selector that does not observe cancellation.</summary>
    public static Flow<TResult> SelectLatestByKeyAsync<TSource, TResult, TKey>(
        this Flow<TSource> flow,
        Func<TSource, TKey> keySelector,
        Func<TSource, Task<TResult>> selector,
        int concurrency,
        int maximumKeys,
        FlowFailureMode failureMode = FlowFailureMode.Stop,
        IEqualityComparer<TKey>? comparer = null)
        where TKey : notnull
    {
        ArgumentNullException.ThrowIfNull(selector);
        return flow.SelectLatestByKeyAsync(
            keySelector, (item, _) => selector(item), concurrency, maximumKeys, failureMode, comparer);
    }

    /// <summary>
    /// Filters items through an async predicate with bounded concurrency. Items that
    /// pass are delivered in source order relative to each other. With
    /// <see cref="FlowFailureMode.Skip"/>, an item whose predicate throws is dropped;
    /// with <see cref="FlowFailureMode.Stop"/> (default) the pipeline faults.
    /// </summary>
    public static Flow<T> WhereAsync<T>(
        this Flow<T> flow,
        Func<T, CancellationToken, Task<bool>> predicate,
        int concurrency = 1,
        FlowFailureMode failureMode = FlowFailureMode.Stop)
    {
        ArgumentNullException.ThrowIfNull(flow);
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

        return new Flow<T>(new SelectFlow<T, T>(
            flow.Node,
            "WhereAsync",
            failureMode == FlowFailureMode.Skip ? SkipOnFailure(evaluate) : evaluate,
            options));
    }

    /// <summary>Filters items through an async predicate that does not observe cancellation.</summary>
    public static Flow<T> WhereAsync<T>(
        this Flow<T> flow,
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
