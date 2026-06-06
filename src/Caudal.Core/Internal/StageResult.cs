namespace Caudal.Internal;

/// <summary>
/// The internal outcome of one selector invocation: a value to emit downstream, or
/// nothing (a filtered or skipped item). Failure policies and filtering both reduce
/// to this shape, so <see cref="SelectFlow{TSource, TResult}"/> stays policy-agnostic.
/// </summary>
internal readonly record struct StageResult<T>(bool Emit, T? Value)
{
    public static StageResult<T> From(T value) => new(true, value);

    public static StageResult<T> Nothing { get; } = new(false, default);
}
