using System.Threading.Channels;
using Caudal.Internal;
using FluentAssertions;
using Xunit;

namespace Caudal.Core.Tests;

public class LatestByKeyTests
{
    [Fact]
    public async Task A_new_item_replaces_the_pending_one_for_its_key()
    {
        var source = Channel.CreateUnbounded<(string Symbol, int Price)>();
        var delivered = new List<(string Symbol, int Price)>();
        var firstDelivered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var conflated = source.Reader
            .ToFlow(capacity: 16)
            .LatestByKey(x => x.Symbol);

        var pipeline = conflated.ForEachAsync(async (item, ct) =>
        {
            delivered.Add(item);
            firstDelivered.TrySetResult();
            if (delivered.Count == 1)
            {
                await gate.Task.WaitAsync(TimeSpan.FromSeconds(10), ct);
            }
        });

        source.Writer.TryWrite(("A", 1));
        await firstDelivered.Task;

        // The consumer is stuck on A@1; these three updates for A must conflate to
        // one pending item, while B keeps its own slot.
        source.Writer.TryWrite(("A", 2));
        source.Writer.TryWrite(("A", 3));
        source.Writer.TryWrite(("A", 4));
        source.Writer.TryWrite(("B", 10));

        var stage = (LatestByKeyFlow<(string Symbol, int Price), string>)conflated.Node;
        while (stage.ReplacedCount < 2)
        {
            await Task.Delay(10);
        }

        source.Writer.Complete();
        gate.TrySetResult();
        await pipeline;

        delivered.Should().Equal(("A", 1), ("A", 4), ("B", 10));
        stage.ReplacedCount.Should().Be(2, "A@2 and A@3 were replaced before delivery");
    }

    [Fact]
    public async Task Distinct_keys_are_never_conflated()
    {
        var conflated = Enumerable.Range(0, 500)
            .ToFlow(capacity: 32)
            .LatestByKey(i => i);

        var results = await conflated.ToListAsync();

        results.Should().Equal(Enumerable.Range(0, 500), "every item has its own key, so nothing can be replaced");
        ((LatestByKeyFlow<int, int>)conflated.Node).ReplacedCount.Should().Be(0);
    }

    [Fact]
    public async Task A_source_failure_reaches_the_consumer_through_the_stage()
    {
        var boom = new InvalidOperationException("source failed");

        var act = () => Failing(boom)
            .ToFlow(capacity: 8)
            .LatestByKey(i => i % 3)
            .ToListAsync();

        var thrown = await act.Should().ThrowAsync<InvalidOperationException>();
        thrown.Which.Should().BeSameAs(boom);
    }

    [Fact]
    public async Task A_key_selector_failure_faults_the_pipeline_with_the_original_exception()
    {
        var boom = new InvalidOperationException("bad key");

        var act = () => Enumerable.Range(0, 100)
            .ToFlow(capacity: 8)
            .LatestByKey(i => i == 50 ? throw boom : i)
            .ConsumeAsync();

        var thrown = await act.Should().ThrowAsync<InvalidOperationException>();
        thrown.Which.Should().BeSameAs(boom);
    }

    private static async IAsyncEnumerable<int> Failing(Exception exception)
    {
        yield return 1;
        yield return 2;
        await Task.Yield();
        throw exception;
    }
}
