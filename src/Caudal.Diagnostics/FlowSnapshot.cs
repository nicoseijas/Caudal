namespace Caudal;

/// <summary>
/// An immutable, point-in-time copy of an entire pipeline's statistics: one
/// <see cref="StageSnapshot"/> per stage, source first and terminal stage last, plus
/// pipeline-wide aggregates. Produced by
/// <see cref="FlowDiagnosticsExtensions.GetSnapshot{T}"/> and rendered by
/// <see cref="FlowDiagnosticsExtensions.Render"/>.
/// </summary>
/// <param name="Name">The pipeline's name, or <see langword="null"/> if it was not given one.</param>
/// <param name="Stages">Every stage in the pipeline, ordered from the source to the terminal stage.</param>
/// <param name="PipelineDuration">Wall time since the source stage started enumerating.</param>
/// <remarks>
/// <see cref="InputsReceived"/> and <see cref="OutputsEmitted"/> are read from the
/// first and last stage respectively and are NOT comparable to each other whenever
/// the pipeline contains a cardinality-changing stage (<c>Where</c>, <c>Batch</c>,
/// <c>SelectMany</c>, <c>LatestByKey</c>, a drop operator, or <c>Capture</c>
/// filtering). There is no global in-equals-out invariant for a pipeline — each
/// stage's own semantics decide what, if anything, its inputs and outputs should
/// have in common. Use each <see cref="StageSnapshot"/>'s own counters (and its
/// <see cref="StageSnapshot.OperatorCounters"/>) to reason about a specific stage.
/// </remarks>
public sealed record FlowSnapshot(string? Name, IReadOnlyList<StageSnapshot> Stages, TimeSpan PipelineDuration)
{
    /// <summary>The number of items accepted by the source stage; zero if the pipeline has no stages.</summary>
    public long InputsReceived => Stages.Count == 0 ? 0 : Stages[0].InputsReceived;

    /// <summary>The number of values produced by the terminal stage; zero if the pipeline has no stages.</summary>
    public long OutputsEmitted => Stages.Count == 0 ? 0 : Stages[^1].OutputsEmitted;

    /// <summary>The total number of inputs failed across every stage.</summary>
    public long InputsFailed => Sum(static stage => stage.InputsFailed);

    /// <summary>The total number of inputs dropped across every stage.</summary>
    public long InputsDropped => Sum(static stage => stage.InputsDropped);

    /// <summary>The total number of inputs replaced across every stage.</summary>
    public long InputsReplaced => Sum(static stage => stage.InputsReplaced);

    /// <summary>The total number of inputs filtered out across every stage.</summary>
    public long InputsFiltered => Sum(static stage => stage.InputsFiltered);

    /// <summary>The total number of items currently queued across every stage.</summary>
    public int Queued => (int)Sum(static stage => stage.Queued);

    /// <summary>The total number of workers currently active across every stage.</summary>
    public int Active => (int)Sum(static stage => stage.Active);

    /// <summary>
    /// The sample-count-weighted average queue time across every stage;
    /// <see cref="TimeSpan.Zero"/> when no stage has recorded a queue-time sample.
    /// </summary>
    public TimeSpan AverageQueueTime => WeightedAverage(
        static stage => stage.AverageQueueTime, static stage => stage.QueueTimeSamples);

    /// <summary>
    /// The sample-count-weighted average processing time across every stage;
    /// <see cref="TimeSpan.Zero"/> when no stage has recorded a processing-time sample.
    /// </summary>
    public TimeSpan AverageProcessingTime => WeightedAverage(
        static stage => stage.AverageProcessingTime, static stage => stage.ProcessingTimeSamples);

    private long Sum(Func<StageSnapshot, long> selector)
    {
        long total = 0;
        foreach (var stage in Stages)
        {
            total += selector(stage);
        }

        return total;
    }

    private TimeSpan WeightedAverage(Func<StageSnapshot, TimeSpan> averageSelector, Func<StageSnapshot, long> sampleSelector)
    {
        double weightedTicksTotal = 0;
        long samplesTotal = 0;

        foreach (var stage in Stages)
        {
            var samples = sampleSelector(stage);
            if (samples <= 0)
            {
                continue;
            }

            weightedTicksTotal += averageSelector(stage).Ticks * (double)samples;
            samplesTotal += samples;
        }

        return samplesTotal == 0 ? TimeSpan.Zero : TimeSpan.FromTicks((long)(weightedTicksTotal / samplesTotal));
    }
}
