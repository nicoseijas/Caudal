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
            .LatestByKey(x => x.Symbol, maximumKeys: 16);

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
    public async Task Conflation_ends_at_emission_and_does_not_serialize_downstream_processing()
    {
        // The boundary of this operator's contract, pinned deliberately. LatestByKey
        // conflates a key only while its value is still waiting to be emitted; once
        // handed downstream, the key is no longer tracked, so a concurrent consumer can
        // process two values for one key at the same time. When that is wrong, the
        // operator to use is SelectLatestByKeyAsync, which owns the selector and can
        // serialize per key. This test passes by not timing out: the second value for A
        // must start while the first is still executing.
        var source = Channel.CreateUnbounded<(string Symbol, int Price)>();
        var firstRunning = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondRunning = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var pipeline = source.Reader
            .ToFlow(capacity: 16)
            .LatestByKey(x => x.Symbol, maximumKeys: 8)
            .SelectAsync(
                async (x, ct) =>
                {
                    if (x.Price == 1)
                    {
                        firstRunning.TrySetResult();
                        await gate.Task.WaitAsync(TimeSpan.FromSeconds(10), ct);
                    }
                    else
                    {
                        secondRunning.TrySetResult();
                    }

                    return x.Price;
                },
                concurrency: 2)
            .ConsumeAsync();

        source.Writer.TryWrite(("A", 1));
        await firstRunning.Task.WaitAsync(TimeSpan.FromSeconds(10));
        source.Writer.TryWrite(("A", 2));

        await secondRunning.Task.WaitAsync(TimeSpan.FromSeconds(10));

        source.Writer.Complete();
        gate.TrySetResult();
        await pipeline.WaitAsync(TimeSpan.FromSeconds(10));
    }

    [Fact]
    public async Task Distinct_keys_are_never_conflated()
    {
        var conflated = Enumerable.Range(0, 500)
            .ToFlow(capacity: 32)
            .LatestByKey(i => i, maximumKeys: 500);

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
            .LatestByKey(i => i % 3, maximumKeys: 3)
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
            .LatestByKey(i => i == 50 ? throw boom : i, maximumKeys: 100)
            .ConsumeAsync();

        var thrown = await act.Should().ThrowAsync<InvalidOperationException>();
        thrown.Which.Should().BeSameAs(boom);
    }

    [Fact]
    public async Task A_new_key_past_the_limit_faults_the_pipeline_with_FlowKeyCapacityException()
    {
        // Every item is its own key. The consumer progresses, but far slower than
        // the pump floods new keys, so the pending set must hit the cap. The sink
        // must keep returning between items — a sink blocked inside its action can
        // never observe the fault (ForEachAsync only rethrows between items).
        var conflated = Enumerable.Range(0, 1_000)
            .ToFlow(capacity: 64)
            .LatestByKey(i => i, maximumKeys: 32);

        var act = () => conflated.ForEachAsync(async (_, ct) => await Task.Delay(20, ct));

        await act.Should().ThrowAsync<FlowKeyCapacityException>().WaitAsync(TimeSpan.FromSeconds(10));
    }

    [Fact]
    public async Task Replacements_of_an_already_pending_key_never_trigger_overflow()
    {
        var source = Channel.CreateUnbounded<int>();
        var delivered = new List<int>();
        var firstDelivered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var conflated = source.Reader
            .ToFlow(capacity: 16)
            .LatestByKey(_ => 0, maximumKeys: 1);

        var pipeline = conflated.ForEachAsync(async (item, ct) =>
        {
            delivered.Add(item);
            firstDelivered.TrySetResult();
            if (delivered.Count == 1)
            {
                await gate.Task.WaitAsync(TimeSpan.FromSeconds(10), ct);
            }
        });

        source.Writer.TryWrite(1);
        await firstDelivered.Task.WaitAsync(TimeSpan.FromSeconds(10));

        // The consumer is stuck on the first update; every one of these targets the
        // same single key, so none of them can be a new key and none can overflow.
        for (var i = 2; i <= 50; i++)
        {
            source.Writer.TryWrite(i);
        }

        var stage = (LatestByKeyFlow<int, int>)conflated.Node;
        while (stage.ReplacedCount < 48)
        {
            await Task.Delay(10);
        }

        source.Writer.Complete();
        gate.TrySetResult();
        await pipeline.WaitAsync(TimeSpan.FromSeconds(10));

        delivered.Should().Equal(1, 50);
        stage.ReplacedCount.Should().Be(48);
    }

    [Fact]
    public void MaximumKeys_less_than_one_is_rejected()
    {
        var flow = Enumerable.Range(0, 10).ToFlow(capacity: 8);

        var act = () => flow.LatestByKey(i => i, maximumKeys: 0);

        act.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Fact]
    public async Task Pending_keys_below_the_limit_are_accepted_after_draining()
    {
        var source = Channel.CreateUnbounded<int>();
        var delivered = new List<int>();

        var conflated = source.Reader
            .ToFlow(capacity: 16)
            .LatestByKey(i => i, maximumKeys: 3);

        var pipeline = conflated.ForEachAsync((item, _) =>
        {
            delivered.Add(item);
            return Task.CompletedTask;
        });

        // 10 distinct keys is well beyond maximumKeys, but each is sent only after
        // the previous one has been drained: the bound is on PENDING keys, not on
        // how many distinct keys have ever passed through.
        for (var i = 0; i < 10; i++)
        {
            source.Writer.TryWrite(i);
            var expectedCount = i + 1;
            var deadline = System.Diagnostics.Stopwatch.StartNew();
            while (delivered.Count < expectedCount && deadline.Elapsed < TimeSpan.FromSeconds(10))
            {
                await Task.Delay(10);
            }
        }

        source.Writer.Complete();
        await pipeline.WaitAsync(TimeSpan.FromSeconds(10));

        delivered.Should().Equal(Enumerable.Range(0, 10));
    }

    private static async IAsyncEnumerable<int> Failing(Exception exception)
    {
        yield return 1;
        yield return 2;
        await Task.Yield();
        throw exception;
    }
}
