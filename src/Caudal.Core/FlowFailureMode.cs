namespace Caudal;

/// <summary>
/// What a stage does when a selector or predicate throws. The mode is a per-stage,
/// explicit decision; <see cref="Stop"/> is always the default.
/// </summary>
public enum FlowFailureMode
{
    /// <summary>
    /// The first exception cancels the rest of the pipeline and is rethrown,
    /// unwrapped, at the sink. The default.
    /// </summary>
    Stop = 0,

    /// <summary>
    /// The failed item is dropped and the pipeline continues. Skipped failures are
    /// counted by diagnostics (<c>items.failed</c>); they never fault the pipeline.
    /// </summary>
    Skip = 1,

    /// <summary>
    /// The failure becomes data: the stage produces <see cref="FlowResult{T}"/>
    /// values instead of faulting. Selected through <c>SelectResultAsync</c>, not
    /// through a <c>SelectAsync</c> parameter, because it changes the result type.
    /// </summary>
    Capture = 2,
}
