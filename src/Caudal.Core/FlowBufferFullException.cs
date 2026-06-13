namespace Caudal;

/// <summary>
/// Thrown when a <c>Buffer</c> stage configured with
/// <see cref="BufferFullMode.Reject"/> receives an item while full.
/// </summary>
public sealed class FlowBufferFullException : InvalidOperationException
{
    /// <summary>Creates the exception with a default message.</summary>
    public FlowBufferFullException()
        : base("The buffer is full and its full-mode is Reject.")
    {
    }

    /// <summary>Creates the exception with a specific message.</summary>
    public FlowBufferFullException(string message)
        : base(message)
    {
    }

    /// <summary>Creates the exception with a message and an inner exception.</summary>
    public FlowBufferFullException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
