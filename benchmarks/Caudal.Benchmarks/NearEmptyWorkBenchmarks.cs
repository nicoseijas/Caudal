using System.Threading.Channels;
using System.Threading.Tasks.Dataflow;
using BenchmarkDotNet.Attributes;

namespace Caudal.Benchmarks;

/// <summary>
/// Isolates the pure overhead Caudal adds over doing nothing: the transform is a
/// single integer increment, so wall-clock time is dominated entirely by
/// scheduling, buffering, and abstraction cost rather than by the "work" itself.
/// This is exactly the scenario ROADMAP Phase 8's honesty mandate expects Caudal
/// to lose in against a plain sequential loop and a hand-rolled
/// Parallel.ForEachAsync — the honest question is by how much, and whether the
/// ordered / statistics-capturing variants cost proportionally more than the
/// unordered baseline.
/// </summary>
[MemoryDiagnoser]
public class NearEmptyWorkBenchmarks
{
    [Params(10_000)]
    public int N { get; set; }

    [Benchmark(Baseline = true)]
    public Task<long> SequentialLoop()
    {
        long sum = 0;
        for (var i = 0; i < N; i++)
        {
            sum += i + 1;
        }

        return Task.FromResult(sum);
    }

    [Benchmark]
    public async Task<long> ParallelForEachAsync()
    {
        long sum = 0;
        await Parallel.ForEachAsync(
            Enumerable.Range(0, N),
            new ParallelOptions { MaxDegreeOfParallelism = 8 },
            (x, _) =>
            {
                Interlocked.Add(ref sum, x + 1);
                return ValueTask.CompletedTask;
            }).ConfigureAwait(false);
        return sum;
    }

    [Benchmark]
    public async Task<long> SemaphoreSlimWhenAll()
    {
        long sum = 0;
        using var semaphore = new SemaphoreSlim(8, 8);
        var tasks = new Task[N];

        for (var i = 0; i < N; i++)
        {
            var x = i;
            tasks[i] = RunAsync(x);
        }

        await Task.WhenAll(tasks).ConfigureAwait(false);
        return sum;

        async Task RunAsync(int x)
        {
            await semaphore.WaitAsync().ConfigureAwait(false);
            try
            {
                Interlocked.Add(ref sum, x + 1);
            }
            finally
            {
                semaphore.Release();
            }
        }
    }

    [Benchmark]
    public async Task<long> ManualChannel()
    {
        long sum = 0;
        var channel = Channel.CreateBounded<int>(128);

        var producer = Task.Run(async () =>
        {
            for (var i = 0; i < N; i++)
            {
                await channel.Writer.WriteAsync(i).ConfigureAwait(false);
            }

            channel.Writer.Complete();
        });

        var consumers = new Task[8];
        for (var c = 0; c < 8; c++)
        {
            consumers[c] = Task.Run(async () =>
            {
                await foreach (var x in channel.Reader.ReadAllAsync().ConfigureAwait(false))
                {
                    Interlocked.Add(ref sum, x + 1);
                }
            });
        }

        await Task.WhenAll(consumers.Append(producer)).ConfigureAwait(false);
        return sum;
    }

    [Benchmark]
    public async Task<long> TplDataflow()
    {
        long sum = 0;
        var transform = new TransformBlock<int, long>(
            x => (long)(x + 1),
            new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = 8, BoundedCapacity = 128 });

        var action = new ActionBlock<long>(
            v => Interlocked.Add(ref sum, v),
            new ExecutionDataflowBlockOptions { BoundedCapacity = 128 });

        transform.LinkTo(action, new DataflowLinkOptions { PropagateCompletion = true });

        for (var i = 0; i < N; i++)
        {
            await transform.SendAsync(i).ConfigureAwait(false);
        }

        transform.Complete();
        await action.Completion.ConfigureAwait(false);
        return sum;
    }

    [Benchmark]
    public async Task<long> CaudalUnordered()
    {
        long sum = 0;
        await Enumerable.Range(0, N)
            .ToFlow(capacity: 128)
            .SelectAsync((x, _) => Task.FromResult((long)x + 1), concurrency: 8)
            .ForEachAsync(v =>
            {
                // The sink runs sequentially: a plain add is honest here, not a
                // hidden race hidden by Interlocked.
                sum += v;
                return Task.CompletedTask;
            }).ConfigureAwait(false);
        return sum;
    }

    [Benchmark]
    public async Task<long> CaudalOrdered()
    {
        long sum = 0;
        await Enumerable.Range(0, N)
            .ToFlow(capacity: 128)
            .SelectAsync((x, _) => Task.FromResult((long)x + 1), concurrency: 8, preserveOrder: true)
            .ForEachAsync(v =>
            {
                sum += v;
                return Task.CompletedTask;
            }).ConfigureAwait(false);
        return sum;
    }

    [Benchmark]
    public async Task<long> CaudalSequential()
    {
        // The honest apples-to-apples comparison against SequentialLoop:
        // concurrency 1 removes parallelism from the equation entirely and
        // isolates the flow/channel machinery's own overhead.
        long sum = 0;
        await Enumerable.Range(0, N)
            .ToFlow(capacity: 128)
            .SelectAsync((x, _) => Task.FromResult((long)x + 1), concurrency: 1)
            .ForEachAsync(v =>
            {
                sum += v;
                return Task.CompletedTask;
            }).ConfigureAwait(false);
        return sum;
    }

    [Benchmark]
    public async Task<long> CaudalWithStatistics()
    {
        // The diagnostics-cost datum: identical to CaudalUnordered except for
        // CaptureStatistics, so the delta between the two IS the cost of
        // per-stage counters/queue-length/timing telemetry.
        long sum = 0;
        var options = new FlowOptions { Capacity = 128, CaptureStatistics = true };
        await Enumerable.Range(0, N)
            .ToFlow(options)
            .SelectAsync((x, _) => Task.FromResult((long)x + 1), concurrency: 8)
            .ForEachAsync(v =>
            {
                sum += v;
                return Task.CompletedTask;
            }).ConfigureAwait(false);
        return sum;
    }
}
