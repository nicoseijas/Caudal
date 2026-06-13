using Caudal.Core.Tests.Support;
using Caudal.Internal;
using FluentAssertions;
using Xunit;

namespace Caudal.Core.Tests;

public class BufferTests
{
    [Fact]
    public async Task Wait_mode_delivers_everything_in_order()
    {
        var results = await Enumerable.Range(0, 200)
            .ToFlow(capacity: 8)
            .Buffer(capacity: 4)
            .ToListAsync();

        results.Should().Equal(Enumerable.Range(0, 200));
    }

    [Fact]
    public async Task DropNewest_keeps_the_oldest_items_and_counts_the_drops()
    {
        var (delivered, stage) = await RunDropScenarioAsync(BufferFullMode.DropNewest);

        // DropNewest never evicts an already-buffered item, so the first arrival
        // always survives and arrival order is preserved among survivors.
        delivered[0].Should().Be(0);
        delivered.Should().BeInAscendingOrder().And.OnlyHaveUniqueItems();
        delivered.Count.Should().BeLessThan(100, "the buffer must have shed load");
        stage.DroppedCount.Should().Be(100 - delivered.Count, "every drop must be counted");
    }

    [Fact]
    public async Task DropOldest_keeps_the_newest_items_and_counts_the_drops()
    {
        var (delivered, stage) = await RunDropScenarioAsync(BufferFullMode.DropOldest);

        delivered.Should().BeInAscendingOrder().And.OnlyHaveUniqueItems();
        delivered[^1].Should().Be(99, "the newest item is never the one dropped");
        delivered.Count.Should().BeLessThan(100);
        stage.DroppedCount.Should().Be(100 - delivered.Count);
    }

    [Fact]
    public async Task Reject_faults_the_pipeline_when_the_buffer_overflows()
    {
        var act = () => TestSources
            .Infinite()
            .ToFlow(capacity: 8)
            .Buffer(capacity: 2, BufferFullMode.Reject)
            .ForEachAsync(async (_, ct) => await Task.Delay(20, ct));

        await act.Should()
            .ThrowAsync<FlowBufferFullException>("a full Reject buffer must fail visibly, not silently")
            .WaitAsync(TimeSpan.FromSeconds(30));
    }

    [Fact]
    public void Buffer_rejects_a_non_positive_capacity()
    {
        var flow = Enumerable.Range(0, 10).ToFlow();
        var act = () => flow.Buffer(capacity: 0);
        act.Should().Throw<ArgumentOutOfRangeException>();
    }

    private static async Task<(List<int> Delivered, BufferFlow<int> Stage)> RunDropScenarioAsync(
        BufferFullMode mode)
    {
        var delivered = new List<int>();
        var firstDelivered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var buffered = Enumerable.Range(0, 100)
            .ToFlow(capacity: 8)
            .Buffer(capacity: 4, mode);
        var stage = (BufferFlow<int>)buffered;

        var pipeline = buffered.ForEachAsync(async (item, ct) =>
        {
            delivered.Add(item);
            firstDelivered.TrySetResult();
            if (delivered.Count == 1)
            {
                await gate.Task.WaitAsync(TimeSpan.FromSeconds(10), ct);
            }
        });

        // With the consumer stuck on its first item, the drop-mode buffer absorbs
        // the whole finite source, dropping per policy; wait for that to finish.
        await firstDelivered.Task;
        await Quiesce.WaitForStableAsync(() => (int)stage.DroppedCount);

        gate.TrySetResult();
        await pipeline;
        return (delivered, stage);
    }
}
