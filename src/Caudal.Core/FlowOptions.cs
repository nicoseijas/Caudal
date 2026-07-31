namespace Caudal;

/// <summary>Options governing a flow's source buffer.</summary>
public sealed record FlowOptions
{
    /// <summary>
    /// The bounded capacity of the stage buffer. When the buffer is full the
    /// producer waits; every stage buffer in Caudal is bounded, and this is the
    /// bound for stages backed by one. <c>LatestByKey</c> has no such buffer to
    /// size — its own bound is the required <c>maximumKeys</c> parameter, which
    /// caps its key set instead. Must be at least 1.
    /// </summary>
    public int Capacity { get; init; } = 128;

    /// <summary>The optional name of the pipeline, used for diagnostics.</summary>
    public string? Name { get; init; }

    /// <summary>
    /// When <see langword="true"/>, every stage collects per-stage statistics
    /// (counters, queue lengths, timing averages) that <c>Caudal.Diagnostics</c> can
    /// snapshot, render, and publish as metrics. Off by default: telemetry never
    /// changes semantics, only cost, and the cost is zero when disabled.
    /// </summary>
    public bool CaptureStatistics { get; init; }

    internal void Validate()
    {
        if (Capacity < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(Capacity), Capacity, "Capacity must be at least 1.");
        }
    }
}
