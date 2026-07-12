namespace Caudal.Internal;

/// <summary>
/// Wraps user selectors with a <see cref="FlowFailureMode"/>. Cancellation caused by
/// the pipeline's own token is always rethrown — it is never a failure, so neither
/// Skip nor Capture may absorb it.
/// </summary>
internal static class FailurePolicy
{
    public static Func<TSource, CancellationToken, Task<StageResult<TResult>>> Wrap<TSource, TResult>(
        Func<TSource, CancellationToken, Task<TResult>> selector,
        FlowFailureMode mode)
        => mode switch
        {
            FlowFailureMode.Stop => async (item, ct) =>
                StageResult<TResult>.From(await selector(item, ct).ConfigureAwait(false)),

            FlowFailureMode.Skip => async (item, ct) =>
            {
                try
                {
                    return StageResult<TResult>.From(await selector(item, ct).ConfigureAwait(false));
                }
                catch (OperationCanceledException oce)
                    when (oce.CancellationToken == ct && ct.IsCancellationRequested)
                {
                    // Only cancellation genuinely produced by the pipeline's own token
                    // passes through: the exception must carry that token AND the token
                    // must actually be cancelled. The ambient flag alone would
                    // misclassify a selector's internal timeout during a teardown race;
                    // token identity alone would misclassify an exception that merely
                    // forwards our token without any cancellation having happened.
                    throw;
                }
                catch
                {
                    // Dropped by explicit policy: this is a failure, distinct from a
                    // filter miss, so diagnostics can tell the two apart.
                    return StageResult<TResult>.SkippedFailure;
                }
            },

            // Defense in depth for future internal callers: the public operators
            // already reject Capture (and undefined values) before reaching here.
            _ => throw new ArgumentOutOfRangeException(nameof(mode), mode,
                "Unsupported failure mode. Capture is expressed through SelectResultAsync."),
        };

    public static Func<TSource, CancellationToken, Task<StageResult<FlowResult<TResult>>>> WrapCapture<TSource, TResult>(
        Func<TSource, CancellationToken, Task<TResult>> selector)
        => async (item, ct) =>
        {
            try
            {
                return StageResult<FlowResult<TResult>>.From(
                    FlowResult.Success(await selector(item, ct).ConfigureAwait(false)));
            }
            catch (OperationCanceledException oce)
                when (oce.CancellationToken == ct && ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                return StageResult<FlowResult<TResult>>.From(FlowResult.Failure<TResult>(ex));
            }
        };
}
