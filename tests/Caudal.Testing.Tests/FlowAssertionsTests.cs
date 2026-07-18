using FluentAssertions;
using Xunit;

namespace Caudal.Testing.Tests;

public class FlowAssertionsTests
{
    [Fact]
    public async Task A_well_behaved_pipeline_satisfies_every_configured_assertion()
    {
        var flow = Enumerable.Range(0, 100)
            .ToFlow(new FlowOptions { Capacity = 16, CaptureStatistics = true })
            .SelectAsync(
                async (i, ct) =>
                {
                    await Task.Delay(1, ct);
                    return i;
                },
                concurrency: 4,
                preserveOrder: true);

        await flow.Should().UseAtMostConcurrency(4).PreserveOrder().CompleteWithoutLeaks();
    }

    [Fact]
    public async Task UseAtMostConcurrency_violation_names_the_stage_and_prints_the_snapshot()
    {
        var active = 0;
        var allEightRunning = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var flow = Enumerable.Range(0, 8)
            .ToFlow(new FlowOptions { Capacity = 16, CaptureStatistics = true })
            .SelectAsync(
                async (i, ct) =>
                {
                    if (Interlocked.Increment(ref active) == 8)
                    {
                        allEightRunning.TrySetResult();
                    }

                    // Forces real overlap: only true concurrency of 8 lets this pipeline finish.
                    await allEightRunning.Task.WaitAsync(TimeSpan.FromSeconds(10), ct);
                    return i;
                },
                concurrency: 8);

        Func<Task> act = () => flow.Should().UseAtMostConcurrency(2).RunAsync();

        var thrown = await act.Should().ThrowAsync<CaudalAssertionException>();
        thrown.Which.Message.Should().Contain("SelectAsync");
        thrown.Which.Message.Should().Contain("├─");
        thrown.Which.Message.Should().Contain("└─");
    }

    [Fact]
    public async Task PreserveOrder_violation_names_the_out_of_order_pair()
    {
        var laterItemsCompleted = 0;
        var releaseItem0 = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        // PreserveOrder alone must not require CaptureStatistics.
        var flow = Enumerable.Range(0, 10)
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
                concurrency: 8);

        Func<Task> act = () => flow.Should().PreserveOrder().RunAsync();

        var thrown = await act.Should().ThrowAsync<CaudalAssertionException>();
        thrown.Which.Message.Should().Contain("PreserveOrder violated");
    }

    [Fact]
    public async Task Stats_requiring_assertion_without_capture_statistics_throws_invalid_operation()
    {
        var flow = Enumerable.Range(0, 10)
            .ToFlow()
            .SelectAsync((i, _) => Task.FromResult(i));

        Func<Task> act = () => flow.Should().UseAtMostConcurrency(4).RunAsync();

        var thrown = await act.Should().ThrowAsync<InvalidOperationException>();
        thrown.Which.Message.Should().Contain("CaptureStatistics");
    }

    [Fact]
    public async Task Pipeline_fault_propagates_as_the_same_exception_instance()
    {
        var boom = new InvalidOperationException("boom");

        var flow = Enumerable.Range(0, 10)
            .ToFlow()
            .SelectAsync((i, _) => Task.FromException<int>(boom));

        Func<Task> act = () => flow.Should().PreserveOrder().RunAsync();

        var thrown = await act.Should().ThrowExactlyAsync<InvalidOperationException>();
        thrown.Which.Should().BeSameAs(boom);
    }

    [Fact]
    public async Task Timeout_guard_is_a_hard_bound_even_when_a_stage_ignores_cancellation()
    {
        // The stage never observes its token: the timeout must still return within
        // timeout + grace instead of waiting forever on the stuck pipeline.
        var stuck = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var flow = InfiniteSequence()
            .ToFlow()
            .SelectAsync(async (i, _) =>
            {
                await stuck.Task;
                return i;
            });

        Func<Task> act = () => flow.Should().WithTimeout(TimeSpan.FromMilliseconds(200)).RunAsync();

        var thrown = await act.Should()
            .ThrowAsync<CaudalAssertionException>()
            .WaitAsync(TimeSpan.FromSeconds(15));
        thrown.Which.Message.Should().Contain("ignored cancellation");

        stuck.TrySetResult();
    }

    [Fact]
    public async Task The_callers_cancellation_still_surfaces_as_an_OperationCanceledException()
    {
        // Known deviation, pinned: the OCE's CancellationToken property references
        // the assertions' internal linked token, but the TYPE contract holds — the
        // caller's cancellation always surfaces as OperationCanceledException.
        using var cts = new CancellationTokenSource();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var flow = InfiniteSequence()
            .ToFlow()
            .SelectAsync(async (i, ct) =>
            {
                entered.TrySetResult();
                await Task.Delay(Timeout.Infinite, ct);
                return i;
            });

        var run = flow.Should().RunAsync(cts.Token);
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(10));
        cts.Cancel();

        await FluentActions.Awaiting(() => run.WaitAsync(TimeSpan.FromSeconds(10)))
            .Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact]
    public async Task Timeout_guard_tears_the_pipeline_down_and_fails_promptly()
    {
        var neverCompletes = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var flow = InfiniteSequence()
            .ToFlow()
            .SelectAsync(async (i, ct) =>
            {
                await neverCompletes.Task.WaitAsync(ct);
                return i;
            });

        Func<Task> act = () => flow.Should().WithTimeout(TimeSpan.FromMilliseconds(200)).RunAsync();

        var thrownTask = act.Should().ThrowAsync<CaudalAssertionException>();
        var thrown = await thrownTask.WaitAsync(TimeSpan.FromSeconds(10));
        thrown.Which.Message.Should().Contain("did not complete within");
    }

    private static IEnumerable<int> InfiniteSequence()
    {
        var i = 0;
        while (true)
        {
            yield return i++;
        }
    }
}
