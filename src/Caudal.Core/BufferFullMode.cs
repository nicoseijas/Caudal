namespace Caudal;

/// <summary>
/// What a <c>Buffer</c> stage does when it is full. Dropping is never implicit:
/// <see cref="Wait"/> is the only default anywhere in Caudal, and dropped items are
/// always counted, never silently lost.
/// </summary>
public enum BufferFullMode
{
    /// <summary>The producer suspends until space frees up. The default.</summary>
    Wait = 0,

    /// <summary>The incoming item is discarded.</summary>
    DropNewest = 1,

    /// <summary>The oldest buffered item is discarded to make room.</summary>
    DropOldest = 2,

    /// <summary>
    /// The write fails visibly: the pipeline faults with
    /// <see cref="FlowBufferFullException"/>.
    /// </summary>
    Reject = 3,
}
