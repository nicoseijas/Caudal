using BenchmarkDotNet.Attributes;

namespace Caudal.Benchmarks;

/// <summary>
/// Isolates skewed per-item latency: 1 item in 10 pays a real 10ms delay, the
/// rest just yield the thread once. The deterministic pattern (index % 10 == 0)
/// stands in for a slow item mixed among fast ones — the kind of tail an
/// occasional slow network call or disk hit produces. The interesting datum
/// this benchmark isolates is head-of-line blocking: <c>CaudalOrdered</c> must
/// hold completed fast items behind an in-flight slow one to preserve source
/// order, while <c>CaudalUnordered</c> (and the hand-rolled contenders, which
/// are inherently unordered) deliver fast items immediately. An honest reading
/// expects ordered throughput to degrade visibly relative to unordered under
/// this skew, roughly in proportion to how much concurrency the slow items tie
/// up.
/// </summary>
[MemoryDiagnoser]
public class VariableIoBenchmarks
{
    [Params(500)]
    public int N { get; set; }

    private static bool IsSlow(int index) => index % 10 == 0;

    private static async Task<long> ProcessAsync(int x, CancellationToken ct)
    {
        if (IsSlow(x))
        {
            await Task.Delay(10, ct).ConfigureAwait(false);
        }
        else
        {
            await Task.Yield();
        }

        return x + 1;
    }

    [Benchmark(Baseline = true)]
    public async Task<long> ParallelForEachAsync()
    {
        long sum = 0;
        await Parallel.ForEachAsync(
            Enumerable.Range(0, N),
            new ParallelOptions { MaxDegreeOfParallelism = 8 },
            async (x, ct) =>
            {
                var value = await ProcessAsync(x, ct).ConfigureAwait(false);
                Interlocked.Add(ref sum, value);
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
                var value = await ProcessAsync(x, CancellationToken.None).ConfigureAwait(false);
                Interlocked.Add(ref sum, value);
            }
            finally
            {
                semaphore.Release();
            }
        }
    }

    [Benchmark]
    public async Task<long> CaudalUnordered()
    {
        long sum = 0;
        await Enumerable.Range(0, N)
            .ToFlow(capacity: 128)
            .SelectAsync(ProcessAsync, concurrency: 8)
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
            .SelectAsync(ProcessAsync, concurrency: 8, preserveOrder: true)
            .ForEachAsync(v =>
            {
                sum += v;
                return Task.CompletedTask;
            }).ConfigureAwait(false);
        return sum;
    }
}
