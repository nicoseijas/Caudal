using System.Diagnostics;

namespace Caudal.Internal;

/// <summary>
/// Per-stage statistics, collected only when <see cref="FlowOptions.CaptureStatistics"/>
/// is set. Plain interlocked counters — no locks, no allocation per item — so that
/// enabling telemetry changes cost, never semantics. Timing uses
/// <see cref="Stopwatch"/> timestamps directly: statistics measure wall time and are
/// not part of the pipeline's TimeProvider-driven semantics.
///
/// The counters measure inputs and emissions, never a pretended item-for-item
/// equality between them: <see cref="InputsReceived"/> counts what a stage
/// accepted, <see cref="OutputsEmitted"/> counts what it actually handed
/// downstream. For cardinality-changing operators (<c>Batch</c>, <c>Where</c>,
/// <c>SelectMany</c>, <c>LatestByKey</c>) those two numbers are not comparable —
/// there is no global in-equals-out invariant. Operator-specific counters
/// (<see cref="BatchItemsIncluded"/>) carry the cardinality context that the
/// generic counters intentionally do not.
/// </summary>
internal sealed class StageStats
{
    private long _received;
    private long _emitted;
    private long _failed;
    private long _dropped;
    private long _replaced;
    private long _filtered;
    private long _batchItemsIncluded;
    private int _active;
    private int _maxActive;
    private long _queueTicks;
    private long _queueSamples;
    private long _processingTicks;
    private long _processingSamples;
    private long _startTimestamp;

    public StageStats(string operatorName) => OperatorName = operatorName;

    public string OperatorName { get; }

    /// <summary>Reads the stage's current internal queue length; assigned by the stage when it creates its channel.</summary>
    public volatile Func<int>? QueueLengthProbe;

    /// <summary>The stage's bounded queue capacity; 0 for stages without a queue.</summary>
    public int QueueCapacity { get; set; }

    /// <summary>The stage's configured concurrency; 1 for sequential stages.</summary>
    public int ConfiguredConcurrency { get; set; } = 1;

    public void MarkStarted()
        => Interlocked.CompareExchange(ref _startTimestamp, Stopwatch.GetTimestamp(), 0);

    /// <summary>Records one item accepted from upstream.</summary>
    public void InputReceived() => Interlocked.Increment(ref _received);

    /// <summary>Records one value the stage actually handed downstream.</summary>
    public void OutputEmitted() => Interlocked.Increment(ref _emitted);

    /// <summary>Records one input that failed processing in this stage.</summary>
    public void InputFailed() => Interlocked.Increment(ref _failed);

    /// <summary>Records one input discarded by a drop policy (for example <c>Buffer(DropNewest)</c>).</summary>
    public void InputDropped() => Interlocked.Increment(ref _dropped);

    /// <summary>Records one input replaced under key-based or time-based shedding.</summary>
    public void InputReplaced() => Interlocked.Increment(ref _replaced);

    /// <summary>Records one input that a predicate rejected — a filter miss, not a failure.</summary>
    public void InputFiltered() => Interlocked.Increment(ref _filtered);

    /// <summary>Records <paramref name="count"/> logical items folded into an emitted batch.</summary>
    public void RecordBatchItems(int count) => Interlocked.Add(ref _batchItemsIncluded, count);

    public void WorkerStarted()
    {
        var now = Interlocked.Increment(ref _active);
        int seen;
        while (now > (seen = Volatile.Read(ref _maxActive)))
        {
            if (Interlocked.CompareExchange(ref _maxActive, now, seen) == seen)
            {
                break;
            }
        }
    }

    public void WorkerFinished() => Interlocked.Decrement(ref _active);

    public void RecordQueueTicks(long elapsedStopwatchTicks)
    {
        Interlocked.Add(ref _queueTicks, elapsedStopwatchTicks);
        Interlocked.Increment(ref _queueSamples);
    }

    public void RecordProcessingTicks(long elapsedStopwatchTicks)
    {
        Interlocked.Add(ref _processingTicks, elapsedStopwatchTicks);
        Interlocked.Increment(ref _processingSamples);
    }

    public long InputsReceived => Interlocked.Read(ref _received);

    public long OutputsEmitted => Interlocked.Read(ref _emitted);

    public long InputsFailed => Interlocked.Read(ref _failed);

    public long InputsDropped => Interlocked.Read(ref _dropped);

    public long InputsReplaced => Interlocked.Read(ref _replaced);

    public long InputsFiltered => Interlocked.Read(ref _filtered);

    /// <summary>The number of logical items folded into batches this stage emitted; zero for non-batching stages.</summary>
    public long BatchItemsIncluded => Interlocked.Read(ref _batchItemsIncluded);

    public int Active => Volatile.Read(ref _active);

    /// <summary>The highest number of concurrently active workers ever observed.</summary>
    public int MaxActive => Volatile.Read(ref _maxActive);

    public int QueueLength => QueueLengthProbe?.Invoke() ?? 0;

    public long QueueTimeSampleCount => Interlocked.Read(ref _queueSamples);

    public long ProcessingTimeSampleCount => Interlocked.Read(ref _processingSamples);

    public TimeSpan AverageQueueTime
        => Average(Interlocked.Read(ref _queueTicks), Interlocked.Read(ref _queueSamples));

    public TimeSpan AverageProcessingTime
        => Average(Interlocked.Read(ref _processingTicks), Interlocked.Read(ref _processingSamples));

    /// <summary>Wall time since the stage started enumerating; zero if never started.</summary>
    public TimeSpan Elapsed
    {
        get
        {
            var start = Interlocked.Read(ref _startTimestamp);
            return start == 0 ? TimeSpan.Zero : Stopwatch.GetElapsedTime(start);
        }
    }

    // The two values are read atomically each but not as a pair; a concurrent
    // recording can tear ticks-vs-samples by one sample. That momentary skew is
    // acceptable for a diagnostics average and self-corrects on the next read.
    private static TimeSpan Average(long ticksTotal, long samples)
        => samples == 0
            ? TimeSpan.Zero
            : TimeSpan.FromSeconds(ticksTotal / (double)samples / Stopwatch.Frequency);
}
