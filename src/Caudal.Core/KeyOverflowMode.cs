namespace Caudal;

/// <summary>
/// What <c>LatestByKey</c> does when a NEW key arrives while the stage is already
/// tracking <c>maximumKeys</c> distinct pending keys. Replacements of already-pending
/// keys never trigger overflow — only growth does.
/// </summary>
public enum KeyOverflowMode
{
    /// <summary>
    /// The pipeline faults with <see cref="FlowKeyCapacityException"/>. The only
    /// policy for now: eviction semantics (which key, whether an in-flight key can
    /// be evicted) are questions we refuse to answer implicitly. The default.
    /// </summary>
    Reject = 0,
}
