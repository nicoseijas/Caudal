using Caudal.Core.Tests.Support;
using FluentAssertions;
using Xunit;

namespace Caudal.Core.Tests;

public class CancellationTests
{
    [Fact]
    public async Task Cancellation_stops_producer_workers_and_sink()
    {
        var produced = 0;
        var activeWorkers = 0;
        using var cts = new CancellationTokenSource();
        var workersRunning = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var pipeline = TestSources
            .Infinite(onYield: _ => Interlocked.Increment(ref produced))
            .ToFlow(capacity: 8)
            .SelectAsync(
                async (i, ct) =>
                {
                    if (Interlocked.Increment(ref activeWorkers) == 4)
                    {
                        workersRunning.TrySetResult();
                    }

                    try
                    {
                        await Task.Delay(Timeout.Infinite, ct);
                        return i;
                    }
                    finally
                    {
                        Interlocked.Decrement(ref activeWorkers);
                    }
                },
                concurrency: 4)
            .ForEachAsync((_, _) => Task.CompletedTask, cts.Token);

        await workersRunning.Task;
        cts.Cancel();

        await FluentActions.Awaiting(() => pipeline)
            .Should().ThrowAsync<OperationCanceledException>();

        // The sink task completed, which contractually means all internal tasks finished.
        Volatile.Read(ref activeWorkers).Should().Be(0, "no worker may survive the pipeline");

        var afterCancel = Volatile.Read(ref produced);
        await Task.Delay(200);
        Volatile.Read(ref produced).Should().Be(afterCancel, "the producer must stop on cancellation");
    }

    [Fact]
    public async Task A_pre_cancelled_token_cancels_before_processing()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        var processed = 0;

        var act = () => Enumerable.Range(0, 100)
            .ToFlow()
            .SelectAsync(
                (i, _) =>
                {
                    Interlocked.Increment(ref processed);
                    return Task.FromResult(i);
                },
                concurrency: 2)
            .ConsumeAsync(cts.Token);

        await act.Should().ThrowAsync<OperationCanceledException>();
        processed.Should().Be(0);
    }

    [Fact]
    public async Task Cancellation_in_ordered_mode_leaves_no_running_work()
    {
        var activeWorkers = 0;
        using var cts = new CancellationTokenSource();
        var workersRunning = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var pipeline = TestSources
            .Infinite()
            .ToFlow(capacity: 8)
            .SelectAsync(
                async (i, ct) =>
                {
                    if (Interlocked.Increment(ref activeWorkers) == 4)
                    {
                        workersRunning.TrySetResult();
                    }

                    try
                    {
                        await Task.Delay(Timeout.Infinite, ct);
                        return i;
                    }
                    finally
                    {
                        Interlocked.Decrement(ref activeWorkers);
                    }
                },
                concurrency: 4,
                preserveOrder: true)
            .ConsumeAsync(cts.Token);

        await workersRunning.Task;
        cts.Cancel();

        await FluentActions.Awaiting(() => pipeline)
            .Should().ThrowAsync<OperationCanceledException>();

        Volatile.Read(ref activeWorkers).Should().Be(0);
    }
}
