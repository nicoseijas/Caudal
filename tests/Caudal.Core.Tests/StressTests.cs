using Caudal.Core.Tests.Support;
using FluentAssertions;
using Xunit;

namespace Caudal.Core.Tests;

public class StressTests
{
    [Fact]
    [Trait("Category", "Stress")]
    public async Task One_million_items_flow_with_bounded_memory()
    {
        const int Count = 1_000_000;
        var processed = 0L;

        var before = GC.GetTotalMemory(forceFullCollection: true);

        await TestSources.Range(Count)
            .ToFlow(capacity: 128)
            .SelectAsync((i, _) => Task.FromResult(i + 1), concurrency: 8)
            .ForEachAsync((_, _) =>
            {
                Interlocked.Increment(ref processed);
                return Task.CompletedTask;
            });

        var after = GC.GetTotalMemory(forceFullCollection: true);

        Interlocked.Read(ref processed).Should().Be(Count);

        // The pipeline buffers at most a few hundred ints at a time; anything close
        // to the size of the stream would mean a buffer is not actually bounded.
        (after - before).Should().BeLessThan(32 * 1024 * 1024);
    }

    [Fact]
    [Trait("Category", "Stress")]
    public async Task An_infinite_source_holds_stable_memory_while_running()
    {
        var processed = 0L;
        using var cts = new CancellationTokenSource();

        var pipeline = TestSources
            .Infinite()
            .ToFlow(capacity: 128)
            .SelectAsync((i, _) => Task.FromResult(i + 1), concurrency: 4)
            .ForEachAsync(
                (_, _) =>
                {
                    Interlocked.Increment(ref processed);
                    return Task.CompletedTask;
                },
                cts.Token);

        // Measure once the pipeline is warm, then again a million items later:
        // a leaky buffer would grow with the stream, a bounded one plateaus.
        await WaitForProcessedAsync(200_000);
        var warm = GC.GetTotalMemory(forceFullCollection: true);

        await WaitForProcessedAsync(1_200_000);
        var later = GC.GetTotalMemory(forceFullCollection: true);

        cts.Cancel();
        await FluentActions.Awaiting(() => pipeline).Should().ThrowAsync<OperationCanceledException>();

        (later - warm).Should().BeLessThan(16 * 1024 * 1024,
            "an infinite source must not accumulate memory while flowing");

        async Task WaitForProcessedAsync(long target)
        {
            while (Interlocked.Read(ref processed) < target)
            {
                await Task.Delay(10);
            }
        }
    }
}
