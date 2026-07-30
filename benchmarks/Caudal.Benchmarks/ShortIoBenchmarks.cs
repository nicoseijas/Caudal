using System.Threading.Channels;
using System.Threading.Tasks.Dataflow;
using BenchmarkDotNet.Attributes;

namespace Caudal.Benchmarks;

/// <summary>
/// Isolates the case where per-item work is real I/O-shaped latency (a 1ms
/// delay) rather than CPU work: this is the regime bounded concurrency exists
/// for, so an honest reading expects the gap between Caudal and the hand-rolled
/// contenders to shrink dramatically, or vanish, since <c>Task.Delay</c> yields
/// the thread and every contestant pays the same real timer granularity
/// (~15ms on Windows) equally. There is no sequential baseline here: iterating
/// 200 items at 1ms each sequentially would only measure the sum of delays
/// (~200ms-3s depending on timer granularity), not anything about the
/// concurrency abstractions under test — so ParallelForEachAsync is the
/// baseline instead.
/// </summary>
[MemoryDiagnoser]
public class ShortIoBenchmarks
{
    [Params(200)]
    public int N { get; set; }

    [Benchmark(Baseline = true)]
    public async Task<long> ParallelForEachAsync()
    {
        long sum = 0;
        await Parallel.ForEachAsync(
            Enumerable.Range(0, N),
            new ParallelOptions { MaxDegreeOfParallelism = 8 },
            async (x, ct) =>
            {
                await Task.Delay(1, ct).ConfigureAwait(false);
                Interlocked.Add(ref sum, x + 1);
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
                await Task.Delay(1).ConfigureAwait(false);
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
                    await Task.Delay(1).ConfigureAwait(false);
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
            async x =>
            {
                await Task.Delay(1).ConfigureAwait(false);
                return (long)(x + 1);
            },
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
            .SelectAsync(
                async (x, ct) =>
                {
                    await Task.Delay(1, ct).ConfigureAwait(false);
                    return (long)x + 1;
                },
                concurrency: 8)
            .ForEachAsync(v =>
            {
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
            .SelectAsync(
                async (x, ct) =>
                {
                    await Task.Delay(1, ct).ConfigureAwait(false);
                    return (long)x + 1;
                },
                concurrency: 8,
                preserveOrder: true)
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
        long sum = 0;
        var options = new FlowOptions { Capacity = 128, CaptureStatistics = true };
        await Enumerable.Range(0, N)
            .ToFlow(options)
            .SelectAsync(
                async (x, ct) =>
                {
                    await Task.Delay(1, ct).ConfigureAwait(false);
                    return (long)x + 1;
                },
                concurrency: 8)
            .ForEachAsync(v =>
            {
                sum += v;
                return Task.CompletedTask;
            }).ConfigureAwait(false);
        return sum;
    }
}
