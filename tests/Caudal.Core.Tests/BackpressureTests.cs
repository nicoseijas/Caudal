using Caudal.Core.Tests.Support;
using FluentAssertions;
using Xunit;

namespace Caudal.Core.Tests;

public class BackpressureTests
{
    [Fact]
    public async Task Producer_stalls_when_the_buffer_is_full()
    {
        var produced = 0;
        using var cts = new CancellationTokenSource();
        var firstItemReached = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var pipeline = TestSources
            .Infinite(onYield: _ => Interlocked.Increment(ref produced))
            .ToFlow(capacity: 4)
            .ForEachAsync(
                async (_, ct) =>
                {
                    firstItemReached.TrySetResult();
                    await Task.Delay(Timeout.Infinite, ct);
                },
                cts.Token);

        await firstItemReached.Task;

        // Bounded buffer (4) + one item in the writer's hand + one at the sink.
        var stalledAt = await Quiesce.WaitForStableAsync(() => Volatile.Read(ref produced));
        stalledAt.Should().BeLessThan(10, "the producer must stop at the buffer, not run ahead");

        await Task.Delay(200);
        Volatile.Read(ref produced).Should().Be(stalledAt, "a blocked pipeline must not keep producing");

        cts.Cancel();
        await FluentActions.Awaiting(() => pipeline).Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact]
    public async Task Backpressure_propagates_through_a_select_stage()
    {
        var produced = 0;
        using var cts = new CancellationTokenSource();
        var sinkEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var pipeline = TestSources
            .Infinite(onYield: _ => Interlocked.Increment(ref produced))
            .ToFlow(capacity: 8)
            .SelectAsync((i, _) => Task.FromResult(i * 2), concurrency: 2)
            .ForEachAsync(
                async (_, ct) =>
                {
                    sinkEntered.TrySetResult();
                    await Task.Delay(Timeout.Infinite, ct);
                },
                cts.Token);

        await sinkEntered.Task;

        // Source buffer (8) + select input (1) + workers (2) + output buffer (8) + slack.
        var stalledAt = await Quiesce.WaitForStableAsync(() => Volatile.Read(ref produced));
        stalledAt.Should().BeLessThan(30, "every stage buffer is bounded, so total run-ahead is bounded");

        await Task.Delay(200);
        Volatile.Read(ref produced).Should().Be(stalledAt);

        cts.Cancel();
        await FluentActions.Awaiting(() => pipeline).Should().ThrowAsync<OperationCanceledException>();
    }
}
