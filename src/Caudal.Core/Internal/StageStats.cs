using System.Diagnostics;

namespace Caudal.Internal;

/// <summary>
/// Per-stage statistics, collected only when <see cref="FlowOptions.CaptureStatistics"/>
/// is set. Plain interlocked counters — no locks, no allocation per item — so that
/// enabling telemetry changes cost, never semantics. Timing uses
/// <see cref="Stopwatch"/> timestamps directly: statistics measure wall time and are
/// not part of the pipeline's TimeProvider-driven semantics.
/// </summary>
internal sealed class StageStats
{
    private long _received;
    private long _completed;
    private long _failed;
    private long _dropped;
    private long _replaced;
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

    public void ItemReceived() => Interlocked.Increment(ref _received);

    public void ItemCompleted() => Interlocked.Increment(ref _completed);

    public void ItemFailed() => Interlocked.Increment(ref _failed);

    public void ItemDropped() => Interlocked.Increment(ref _dropped);

    public void ItemReplaced() => Interlocked.Increment(ref _replaced);

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

    public long Received => Interlocked.Read(ref _received);

    public long Completed => Interlocked.Read(ref _completed);

    public long Failed => Interlocked.Read(ref _failed);

    public long Dropped => Interlocked.Read(ref _dropped);

    public long Replaced => Interlocked.Read(ref _replaced);

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
