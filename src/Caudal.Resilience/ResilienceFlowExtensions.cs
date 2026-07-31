using Polly;

namespace Caudal;

/// <summary>
/// Extension methods that run a flow stage's selector through a Polly
/// <see cref="ResiliencePipeline"/> — retry, timeout, circuit breaker, and fallback —
/// without Caudal reimplementing any of that behavior itself.
/// </summary>
public static class ResilienceFlowExtensions
{
    /// <summary>
    /// Projects each item through <paramref name="selector"/>, executing every
    /// invocation through <paramref name="resiliencePipeline"/>. Semantics otherwise
    /// match <c>Flow{TSource}.SelectAsync</c>: <paramref name="concurrency"/>,
    /// <paramref name="preserveOrder"/>, and <paramref name="failureMode"/> apply to
    /// the pipeline-wrapped invocation as a whole — a retry that ultimately fails is
    /// one failed item, handled per <paramref name="failureMode"/>.
    /// </summary>
    public static Flow<TResult> SelectAsync<TSource, TResult>(
        this Flow<TSource> flow,
        Func<TSource, CancellationToken, Task<TResult>> selector,
        ResiliencePipeline resiliencePipeline,
        int concurrency = 1,
        bool preserveOrder = false,
        FlowFailureMode failureMode = FlowFailureMode.Stop)
    {
        ArgumentNullException.ThrowIfNull(flow);
        ArgumentNullException.ThrowIfNull(selector);
        ArgumentNullException.ThrowIfNull(resiliencePipeline);

        return flow.SelectAsync(
            WrapWithPipeline(selector, resiliencePipeline),
            concurrency,
            preserveOrder,
            failureMode);
    }

    /// <summary>
    /// Projects each item through <paramref name="selector"/> under
    /// <see cref="FlowFailureMode.Capture"/>, executing every invocation through
    /// <paramref name="resiliencePipeline"/>. A retry that exhausts its attempts, a
    /// timeout, or an open circuit all become the item's captured
    /// <see cref="FlowResult{T}.Exception"/> rather than faulting the pipeline.
    /// Pipeline cancellation is still cancellation: it propagates, it is never
    /// captured.
    /// </summary>
    public static Flow<FlowResult<TResult>> SelectResultAsync<TSource, TResult>(
        this Flow<TSource> flow,
        Func<TSource, CancellationToken, Task<TResult>> selector,
        ResiliencePipeline resiliencePipeline,
        int concurrency = 1,
        bool preserveOrder = false)
    {
        ArgumentNullException.ThrowIfNull(flow);
        ArgumentNullException.ThrowIfNull(selector);
        ArgumentNullException.ThrowIfNull(resiliencePipeline);

        return flow.SelectResultAsync(
            WrapWithPipeline(selector, resiliencePipeline),
            concurrency,
            preserveOrder);
    }

    /// <summary>
    /// Binds <paramref name="resiliencePipeline"/> to <paramref name="flow"/>,
    /// returning a <see cref="ResilientFlow{T}"/> whose own <c>SelectAsync</c> and
    /// <c>SelectResultAsync</c> methods run every selector invocation through it.
    /// </summary>
    public static ResilientFlow<T> WithResiliencePipeline<T>(
        this Flow<T> flow,
        ResiliencePipeline resiliencePipeline)
    {
        ArgumentNullException.ThrowIfNull(flow);
        ArgumentNullException.ThrowIfNull(resiliencePipeline);

        return new ResilientFlow<T>(flow, resiliencePipeline);
    }

    private static Func<TSource, CancellationToken, Task<TResult>> WrapWithPipeline<TSource, TResult>(
        Func<TSource, CancellationToken, Task<TResult>> selector,
        ResiliencePipeline resiliencePipeline)
        => async (item, ct) =>
        {
            try
            {
                return await resiliencePipeline.ExecuteAsync(
                    token => new ValueTask<TResult>(selector(item, token)), ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Polly may run the callback under its own linked attempt token (a
                // Timeout strategy always does), so cancellation caused by OUR token
                // can surface carrying a different token instance. Re-anchor it to
                // the stage's token so Caudal's strict token-identity classification
                // recognizes pipeline cancellation instead of misfiling it as an
                // ordinary Skip-dropped or Capture-recorded item failure.
                ct.ThrowIfCancellationRequested();

                // Not caused by our token: a business-level cancellation-shaped
                // failure (e.g. Polly's own timeout surfacing as OCE). Let it
                // classify as an ordinary failure.
                throw;
            }
        };
}
