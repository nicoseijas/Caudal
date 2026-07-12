namespace Caudal.Internal;

/// <summary>
/// The internal outcome of one selector invocation: a value to emit downstream, or
/// nothing (a filtered or skipped item). Failure policies and filtering both reduce
/// to this shape, so <see cref="SelectFlow{TSource, TResult}"/> stays policy-agnostic.
/// </summary>
internal readonly record struct StageResult<T>(bool Emit, T? Value, bool Failed = false)
{
    public static StageResult<T> From(T value) => new(true, value);

    /// <summary>A filtered-out item: nothing emitted, nothing failed.</summary>
    public static StageResult<T> Nothing { get; } = new(false, default);

    /// <summary>
    /// A Skip-dropped failure: nothing emitted, but the item counts as failed for
    /// diagnostics — distinguishing an explicit policy drop from a filter miss.
    /// </summary>
    public static StageResult<T> SkippedFailure { get; } = new(false, default, true);
}
