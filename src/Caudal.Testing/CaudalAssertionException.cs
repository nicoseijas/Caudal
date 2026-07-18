namespace Caudal.Testing;

/// <summary>
/// Thrown by <see cref="FlowAssertions{T}"/> when a configured assertion is violated
/// after a flow has run to completion (or been torn down by a timeout). Never thrown
/// for a pipeline fault: those propagate unwrapped, exactly as raised by the flow.
/// </summary>
public sealed class CaudalAssertionException : Exception
{
    /// <summary>Initializes a new instance with no message.</summary>
    public CaudalAssertionException()
    {
    }

    /// <summary>Initializes a new instance with the specified error message.</summary>
    /// <param name="message">The message describing the assertion failure.</param>
    public CaudalAssertionException(string message)
        : base(message)
    {
    }

    /// <summary>Initializes a new instance with the specified error message and inner exception.</summary>
    /// <param name="message">The message describing the assertion failure.</param>
    /// <param name="innerException">The exception that caused this assertion failure.</param>
    public CaudalAssertionException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
