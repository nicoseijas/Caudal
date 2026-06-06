namespace Caudal;

/// <summary>
/// The outcome of processing one item under <see cref="FlowFailureMode.Capture"/>:
/// either a value or the original exception, never both.
/// </summary>
/// <typeparam name="T">The type of the successful value.</typeparam>
public readonly record struct FlowResult<T>(
    T? Value,
    Exception? Exception,
    bool IsSuccess);

/// <summary>Factory methods for <see cref="FlowResult{T}"/>.</summary>
public static class FlowResult
{
    /// <summary>Creates a successful result carrying <paramref name="value"/>.</summary>
    public static FlowResult<T> Success<T>(T value) => new(value, null, true);

    /// <summary>Creates a failed result carrying the original <paramref name="exception"/>.</summary>
    public static FlowResult<T> Failure<T>(Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);
        return new(default, exception, false);
    }
}
