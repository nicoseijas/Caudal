using FluentAssertions;
using Xunit;

namespace Caudal.Core.Tests;

public class OrderingTests
{
    [Fact]
    public async Task PreserveOrder_true_delivers_results_in_source_order()
    {
        // Item 0 cannot finish until item 5 has started, so completion order is
        // guaranteed to differ from source order — delivery order must not.
        var item5Started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var results = await Enumerable.Range(0, 20)
            .ToFlow(capacity: 32)
            .SelectAsync(
                async (i, ct) =>
                {
                    if (i == 5)
                    {
                        item5Started.TrySetResult();
                    }

                    if (i == 0)
                    {
                        await item5Started.Task.WaitAsync(TimeSpan.FromSeconds(10), ct);
                    }

                    await Task.Yield();
                    return i;
                },
                concurrency: 8,
                preserveOrder: true)
            .ToListAsync();

        results.Should().Equal(Enumerable.Range(0, 20));
    }

    [Fact]
    public async Task PreserveOrder_false_delivers_results_in_completion_order()
    {
        // Item 0 is held until items 1..4 have completed, so it cannot be first out.
        var laterItemsCompleted = 0;
        var releaseItem0 = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var results = await Enumerable.Range(0, 10)
            .ToFlow(capacity: 32)
            .SelectAsync(
                async (i, ct) =>
                {
                    if (i == 0)
                    {
                        await releaseItem0.Task.WaitAsync(TimeSpan.FromSeconds(10), ct);
                    }
                    else
                    {
                        await Task.Yield();
                        if (Interlocked.Increment(ref laterItemsCompleted) == 4)
                        {
                            releaseItem0.TrySetResult();
                        }
                    }

                    return i;
                },
                concurrency: 8)
            .ToListAsync();

        results.Should().BeEquivalentTo(Enumerable.Range(0, 10), "no item may be lost");
        results.First().Should().NotBe(0, "item 0 finished after items 1..4, so it cannot be delivered first");
    }

    [Fact]
    public async Task Sequential_stages_preserve_source_order_naturally()
    {
        var results = await Enumerable.Range(0, 100)
            .ToFlow()
            .SelectAsync((i, _) => Task.FromResult(i * 2))
            .ToListAsync();

        results.Should().Equal(Enumerable.Range(0, 100).Select(i => i * 2));
    }
}
