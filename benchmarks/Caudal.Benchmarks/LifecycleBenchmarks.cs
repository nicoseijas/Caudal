using BenchmarkDotNet.Attributes;

namespace Caudal.Benchmarks;

/// <summary>
/// Isolates the cost of Caudal's lifecycle guarantees: cancellation that leaves
/// no orphaned tasks, and the fixed overhead of starting and draining a pipeline.
/// Neither benchmark here is about throughput — for both, Mean itself IS the
/// thing being measured (shutdown latency, and fixed lifecycle overhead), so read
/// them differently from the throughput-oriented benchmarks in this project.
/// </summary>
[MemoryDiagnoser]
public class LifecycleBenchmarks
{
    private static IEnumerable<int> InfiniteSequence()
    {
        var i = 0;
        while (true)
        {
            yield return i++;
        }
    }

    private static async Task<int> YieldSelectAsync(int item, CancellationToken cancellationToken)
    {
        await Task.Yield();
        cancellationToken.ThrowIfCancellationRequested();
        return item;
    }

    private CancellationTokenSource? _warmCts;
    private Task? _warmPipeline;

    /// <summary>
    /// Builds an infinite-source pipeline and waits for it to reach a warm,
    /// steady state (the first item observed downstream). Runs OUTSIDE the
    /// measured window: BenchmarkDotNet excludes IterationSetup time, so the
    /// benchmark's Mean measures only cancel-to-complete.
    /// </summary>
    [IterationSetup(Target = nameof(CancellationLatency))]
    public void WarmUpInfinitePipeline()
    {
        _warmCts = new CancellationTokenSource();
        var firstProcessed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        _warmPipeline = InfiniteSequence()
            .ToFlow(capacity: 128)
            .SelectAsync(YieldSelectAsync, concurrency: 8)
            .ForEachAsync(
                _ =>
                {
                    firstProcessed.TrySetResult();
                    return Task.CompletedTask;
                },
                _warmCts.Token);

        // Synchronous wait is fine here: setup time is not measured.
        firstProcessed.Task.Wait();
    }

    [IterationCleanup(Target = nameof(CancellationLatency))]
    public void DisposeWarmPipeline()
    {
        _warmCts?.Dispose();
        _warmCts = null;
        _warmPipeline = null;
    }

    /// <summary>
    /// Measures the time from requesting cancellation of a warm, running
    /// pipeline to its terminal task actually completing — the price of the
    /// "no orphaned tasks" semantic. Construction and warm-up happen in
    /// IterationSetup, so the Mean here genuinely is the shutdown latency.
    /// </summary>
    [Benchmark]
    public async Task CancellationLatency()
    {
        _warmCts!.Cancel();
        try
        {
            await _warmPipeline!.ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Expected: cancellation is a normal outcome, not an error.
        }
    }

    /// <summary>
    /// A finite run of 10,000 trivial items through ToFlow + SelectAsync(8) +
    /// Consume, with no artificial slow work beyond a single yield per item.
    /// Compared against near-empty-work numbers elsewhere in the suite, this
    /// bounds the fixed start-up and drain overhead of a pipeline's lifecycle —
    /// the cost paid even when there is nothing interesting for the pipeline to do.
    /// </summary>
    [Benchmark(Baseline = true)]
    public async Task GracefulShutdown()
    {
        await Enumerable.Range(0, 10_000)
            .ToFlow(capacity: 128)
            .SelectAsync(YieldSelectAsync, concurrency: 8)
            .ConsumeAsync()
            .ConfigureAwait(false);
    }
}
