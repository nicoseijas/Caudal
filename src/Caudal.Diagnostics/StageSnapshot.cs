namespace Caudal;

/// <summary>
/// An immutable, point-in-time copy of a single stage's statistics, produced by
/// <see cref="FlowDiagnosticsExtensions.GetSnapshot{T}"/>. Unlike the internal
/// <c>StageStats</c> it mirrors, a snapshot never changes after it is taken.
/// </summary>
/// <param name="Operator">The name of the operator that produced this stage (for example <c>"Source"</c> or <c>"SelectAsync"</c>).</param>
/// <param name="Received">The number of items the stage has accepted from upstream.</param>
/// <param name="Completed">The number of items the stage has successfully produced downstream.</param>
/// <param name="Failed">The number of items that failed processing in this stage.</param>
/// <param name="Dropped">The number of items discarded by a drop policy (for example <c>Buffer(DropNewest)</c>).</param>
/// <param name="Replaced">The number of items replaced under key-based shedding (for example <c>LatestByKey</c>).</param>
/// <param name="Queued">The number of items currently sitting in the stage's internal queue.</param>
/// <param name="QueueCapacity">The stage's bounded queue capacity; zero for stages without a queue.</param>
/// <param name="Active">The number of workers currently processing an item in this stage.</param>
/// <param name="ConfiguredConcurrency">The stage's configured concurrency; one for sequential stages.</param>
/// <param name="QueueTimeSamples">The number of queue-time samples recorded so far.</param>
/// <param name="ProcessingTimeSamples">The number of processing-time samples recorded so far.</param>
/// <param name="AverageQueueTime">The average time items spent waiting in this stage's queue.</param>
/// <param name="AverageProcessingTime">The average time items spent being processed by this stage.</param>
public sealed record StageSnapshot(
    string Operator,
    long Received,
    long Completed,
    long Failed,
    long Dropped,
    long Replaced,
    int Queued,
    int QueueCapacity,
    int Active,
    int ConfiguredConcurrency,
    long QueueTimeSamples,
    long ProcessingTimeSamples,
    TimeSpan AverageQueueTime,
    TimeSpan AverageProcessingTime);
