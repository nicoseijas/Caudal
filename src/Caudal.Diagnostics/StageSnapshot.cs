namespace Caudal;

/// <summary>
/// An immutable, point-in-time copy of a single stage's statistics, produced by
/// <see cref="FlowDiagnosticsExtensions.GetSnapshot{T}"/>. Unlike the internal
/// <c>StageStats</c> it mirrors, a snapshot never changes after it is taken.
/// </summary>
/// <param name="Operator">The name of the operator that produced this stage (for example <c>"Source"</c> or <c>"SelectAsync"</c>).</param>
/// <param name="InputsReceived">The number of items the stage has accepted from upstream.</param>
/// <param name="OutputsEmitted">
/// The number of values the stage has actually handed downstream. For
/// cardinality-changing stages (for example <c>Batch</c>, which emits batches
/// rather than items) this is intentionally not comparable to
/// <see cref="InputsReceived"/> — there is no global in-equals-out invariant.
/// See <see cref="OperatorCounters"/> for the cardinality context those stages
/// carry separately.
/// </param>
/// <param name="InputsFailed">The number of inputs that failed processing in this stage.</param>
/// <param name="InputsDropped">The number of inputs discarded by a drop policy (for example <c>Buffer(DropNewest)</c>).</param>
/// <param name="InputsReplaced">The number of inputs replaced under key-based or time-based shedding (for example <c>LatestByKey</c>).</param>
/// <param name="InputsFiltered">The number of inputs a predicate rejected — a filter miss (for example <c>WhereAsync</c>), never counted as a failure.</param>
/// <param name="Queued">The number of items currently sitting in the stage's internal queue.</param>
/// <param name="QueueCapacity">The stage's bounded queue capacity; zero for stages without a queue.</param>
/// <param name="Active">The number of workers currently processing an item in this stage.</param>
/// <param name="ConfiguredConcurrency">The stage's configured concurrency; one for sequential stages.</param>
/// <param name="QueueTimeSamples">The number of queue-time samples recorded so far.</param>
/// <param name="ProcessingTimeSamples">The number of processing-time samples recorded so far.</param>
/// <param name="AverageQueueTime">The average time items spent waiting in this stage's queue.</param>
/// <param name="AverageProcessingTime">The average time items spent being processed by this stage.</param>
/// <param name="OperatorCounters">
/// Operator-specific counters that carry cardinality context the generic
/// inputs/outputs counters intentionally do not pretend to capture; defaults to an
/// empty dictionary. <c>Batch</c> contributes <c>"batch.items.included"</c> — the
/// number of logical items folded into the batches this stage emitted — whenever
/// that count is greater than zero.
/// </param>
public sealed record StageSnapshot(
    string Operator,
    long InputsReceived,
    long OutputsEmitted,
    long InputsFailed,
    long InputsDropped,
    long InputsReplaced,
    long InputsFiltered,
    int Queued,
    int QueueCapacity,
    int Active,
    int ConfiguredConcurrency,
    long QueueTimeSamples,
    long ProcessingTimeSamples,
    TimeSpan AverageQueueTime,
    TimeSpan AverageProcessingTime,
    IReadOnlyDictionary<string, long>? OperatorCounters = null)
{
    private static readonly IReadOnlyDictionary<string, long> Empty = new Dictionary<string, long>();

    /// <inheritdoc cref="StageSnapshot" path="/param[@name='OperatorCounters']"/>
    public IReadOnlyDictionary<string, long> OperatorCounters { get; init; } = OperatorCounters ?? Empty;
}
