using System.Collections.Concurrent;
using System.Threading.Channels;
using BenchmarkDotNet.Attributes;

namespace Caudal.Benchmarks;

/// <summary>
/// The ROADMAP market-data scenario: 100,000 updates across 100 keys arriving
/// far faster than a slow downstream selector can consume them. Conflation's
/// entire value proposition is work avoided, so the metric that matters here is
/// <c>processedItems</c>, not Mean — a smaller processed count on the same input
/// means more updates were correctly superseded before they cost a selector
/// invocation. <see cref="CaudalNoConflation"/> processes every update and is
/// expected to be roughly 20x slower; that asymmetry is the point, not a flaw.
/// <see cref="ManualDictionaryConflation"/> is what a competent engineer reaches
/// for by hand, so it prices Caudal's convenience against a real hand-rolled
/// alternative rather than a strawman.
/// </summary>
[MemoryDiagnoser]
public class LatestByKeyBenchmarks
{
    private const int TotalUpdates = 100_000;
    private const int KeyCount = 100;
    private const int YieldsPerItem = 16;
    private const int Concurrency = 4;

    private readonly record struct Update(int Key, int Value);

    private static IEnumerable<Update> GenerateUpdates()
    {
        for (var i = 0; i < TotalUpdates; i++)
        {
            yield return new Update(i % KeyCount, i);
        }
    }

    private static async Task<int> SlowSelectAsync(Update update, CancellationToken cancellationToken)
    {
        for (var i = 0; i < YieldsPerItem; i++)
        {
            await Task.Yield();
        }

        cancellationToken.ThrowIfCancellationRequested();
        return update.Value;
    }

    /// <summary>Caudal's conflating pipeline: lower processedItems is better here.</summary>
    [Benchmark]
    public async Task<long> CaudalLatestByKey()
    {
        long processed = 0;
        await GenerateUpdates()
            .ToFlow(capacity: 1024)
            .LatestByKey(u => u.Key)
            .SelectAsync(SlowSelectAsync, concurrency: Concurrency)
            .ForEachAsync(_ =>
            {
                Interlocked.Increment(ref processed);
                return Task.CompletedTask;
            })
            .ConfigureAwait(false);
        return processed;
    }

    /// <summary>
    /// The identical pipeline with no conflation stage: every update pays for a
    /// selector invocation. The honest cost of not conflating — expect roughly
    /// 20x the Mean of <see cref="CaudalLatestByKey"/> for the same input.
    /// </summary>
    [Benchmark(Baseline = true)]
    public async Task<long> CaudalNoConflation()
    {
        long processed = 0;
        await GenerateUpdates()
            .ToFlow(capacity: 1024)
            .SelectAsync(SlowSelectAsync, concurrency: Concurrency)
            .ForEachAsync(_ =>
            {
                Interlocked.Increment(ref processed);
                return Task.CompletedTask;
            })
            .ConfigureAwait(false);
        return processed;
    }

    /// <summary>
    /// The hand-rolled equivalent: a locked dictionary holds the latest value per
    /// key, a dedup set tracks which keys have unconsumed data, and an unbounded
    /// channel signals workers that a key became dirty. Four workers drain it at
    /// the same concurrency as <see cref="CaudalLatestByKey"/>. Simplification
    /// versus Caudal's LatestByKey: a key can be re-signalled while already
    /// in-flight, so it may occasionally be picked up again slightly sooner than
    /// Caudal's "only after the in-flight item completes" guarantee — a fair
    /// price for the simplicity a hand-rolled version would realistically have.
    /// </summary>
    [Benchmark]
    public async Task<long> ManualDictionaryConflation()
    {
        var latest = new ConcurrentDictionary<int, Update>();
        var pending = new ConcurrentDictionary<int, byte>();
        var dirtyKeys = Channel.CreateUnbounded<int>();
        long processed = 0;

        var producer = Task.Run(async () =>
        {
            foreach (var update in GenerateUpdates())
            {
                latest[update.Key] = update;
                if (pending.TryAdd(update.Key, 0))
                {
                    await dirtyKeys.Writer.WriteAsync(update.Key).ConfigureAwait(false);
                }
            }

            dirtyKeys.Writer.Complete();
        });

        var workers = new Task[Concurrency];
        for (var w = 0; w < Concurrency; w++)
        {
            workers[w] = Task.Run(async () =>
            {
                await foreach (var key in dirtyKeys.Reader.ReadAllAsync().ConfigureAwait(false))
                {
                    pending.TryRemove(key, out _);
                    var value = latest[key];
                    await SlowSelectAsync(value, CancellationToken.None).ConfigureAwait(false);
                    Interlocked.Increment(ref processed);
                }
            });
        }

        await producer.ConfigureAwait(false);
        await Task.WhenAll(workers).ConfigureAwait(false);
        return processed;
    }
}
