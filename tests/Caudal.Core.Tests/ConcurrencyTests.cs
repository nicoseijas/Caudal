using Caudal.Core.Tests.Support;
using FluentAssertions;
using Xunit;

namespace Caudal.Core.Tests;

public class ConcurrencyTests
{
    [Fact]
    public async Task Active_work_never_exceeds_the_concurrency_limit()
    {
        var active = 0;
        var maxActive = 0;

        var results = await Enumerable.Range(0, 200)
            .ToFlow(capacity: 16)
            .SelectAsync(
                async (i, ct) =>
                {
                    InterlockedMax.Update(ref maxActive, Interlocked.Increment(ref active));
                    await Task.Delay(1, ct);
                    Interlocked.Decrement(ref active);
                    return i;
                },
                concurrency: 8)
            .ToListAsync();

        results.Should().HaveCount(200);
        maxActive.Should().BeLessThanOrEqualTo(8);
    }

    [Fact]
    public async Task The_stage_actually_reaches_the_requested_concurrency()
    {
        var active = 0;
        var allEightRunning = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var results = await Enumerable.Range(0, 8)
            .ToFlow(capacity: 16)
            .SelectAsync(
                async (i, ct) =>
                {
                    if (Interlocked.Increment(ref active) == 8)
                    {
                        allEightRunning.TrySetResult();
                    }

                    // Each of the 8 items waits until all 8 run at once: only true
                    // parallelism can complete this pipeline.
                    await allEightRunning.Task.WaitAsync(TimeSpan.FromSeconds(10), ct);
                    return i;
                },
                concurrency: 8)
            .ToListAsync();

        results.Should().HaveCount(8);
    }

    [Fact]
    public async Task Default_concurrency_is_sequential()
    {
        var active = 0;
        var maxActive = 0;

        await Enumerable.Range(0, 50)
            .ToFlow()
            .SelectAsync(async (i, ct) =>
            {
                InterlockedMax.Update(ref maxActive, Interlocked.Increment(ref active));
                await Task.Yield();
                Interlocked.Decrement(ref active);
                return i;
            })
            .ConsumeAsync();

        maxActive.Should().Be(1, "concurrency must be explicit, never a default");
    }

    [Fact]
    public async Task Ordered_mode_also_respects_the_concurrency_limit()
    {
        var active = 0;
        var maxActive = 0;

        var results = await Enumerable.Range(0, 200)
            .ToFlow(capacity: 16)
            .SelectAsync(
                async (i, ct) =>
                {
                    InterlockedMax.Update(ref maxActive, Interlocked.Increment(ref active));
                    await Task.Delay(1, ct);
                    Interlocked.Decrement(ref active);
                    return i;
                },
                concurrency: 4,
                preserveOrder: true)
            .ToListAsync();

        results.Should().HaveCount(200);
        maxActive.Should().BeLessThanOrEqualTo(4);
    }
}
