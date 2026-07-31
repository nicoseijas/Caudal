namespace Caudal;

/// <summary>
/// Thrown when a <c>LatestByKey</c> stage configured with
/// <see cref="KeyOverflowMode.Reject"/> receives a new key while already tracking
/// <c>maximumKeys</c> distinct pending keys.
/// </summary>
public sealed class FlowKeyCapacityException : InvalidOperationException
{
    /// <summary>Creates the exception with a default message.</summary>
    public FlowKeyCapacityException()
        : base("LatestByKey is at its key capacity and its overflow-mode is Reject.")
    {
    }

    /// <summary>Creates the exception with a specific message.</summary>
    public FlowKeyCapacityException(string message)
        : base(message)
    {
    }

    /// <summary>Creates the exception with a message and an inner exception.</summary>
    public FlowKeyCapacityException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
