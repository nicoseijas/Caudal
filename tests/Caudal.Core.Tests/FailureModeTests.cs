using Caudal.Core.Tests.Support;
using FluentAssertions;
using Xunit;

namespace Caudal.Core.Tests;

public class FailureModeTests
{
    [Fact]
    public async Task Skip_drops_failed_items_and_the_pipeline_continues()
    {
        var results = await Enumerable.Range(0, 100)
            .ToFlow(capacity: 16)
            .SelectAsync(
                async (i, ct) =>
                {
                    await Task.Yield();
                    return i % 10 == 0 ? throw new InvalidOperationException($"item {i}") : i;
                },
                concurrency: 8,
                failureMode: FlowFailureMode.Skip)
            .ToListAsync();

        results.Should().HaveCount(90);
        results.Should().BeEquivalentTo(Enumerable.Range(0, 100).Where(i => i % 10 != 0));
    }

    [Fact]
    public async Task Skip_with_preserveOrder_keeps_surviving_items_in_source_order()
    {
        var results = await Enumerable.Range(0, 50)
            .ToFlow(capacity: 16)
            .SelectAsync(
                async (i, ct) =>
                {
                    await Task.Yield();
                    return i % 2 == 1 ? throw new InvalidOperationException($"item {i}") : i;
                },
                concurrency: 8,
                preserveOrder: true,
                failureMode: FlowFailureMode.Skip)
            .ToListAsync();

        results.Should().Equal(Enumerable.Range(0, 25).Select(i => i * 2));
    }

    [Fact]
    public async Task Skip_never_absorbs_pipeline_cancellation()
    {
        using var cts = new CancellationTokenSource();
        var firstItemRunning = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var pipeline = TestSources
            .Infinite()
            .ToFlow(capacity: 8)
            .SelectAsync(
                async (i, ct) =>
                {
                    firstItemRunning.TrySetResult();
                    await Task.Delay(Timeout.Infinite, ct);
                    return i;
                },
                concurrency: 4,
                failureMode: FlowFailureMode.Skip)
            .ConsumeAsync(cts.Token);

        await firstItemRunning.Task;
        cts.Cancel();

        // If Skip treated the workers' OperationCanceledException as a failure to
        // drop, the pipeline would keep consuming the infinite source forever.
        await FluentActions.Awaiting(() => pipeline.WaitAsync(TimeSpan.FromSeconds(10)))
            .Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact]
    public async Task A_storm_of_failures_under_skip_completes_without_deadlock_or_orphans()
    {
        var produced = 0;

        var results = await TestSources
            .Range(500)
            .ToFlow(capacity: 8)
            .SelectAsync<int, int>(
                async (i, ct) =>
                {
                    Interlocked.Increment(ref produced);
                    await Task.Yield();
                    throw new InvalidOperationException($"item {i}");
                },
                concurrency: 8,
                failureMode: FlowFailureMode.Skip)
            .ToListAsync()
            .WaitAsync(TimeSpan.FromSeconds(30));

        results.Should().BeEmpty("every item failed and Skip drops failures");
        produced.Should().Be(500, "Skip must keep consuming the whole source");
    }

    [Fact]
    public async Task Stop_is_the_default_failure_mode()
    {
        var boom = new InvalidOperationException("boom");

        var act = () => Enumerable.Range(0, 100)
            .ToFlow()
            .SelectAsync(
                async (i, ct) =>
                {
                    await Task.Yield();
                    return i == 3 ? throw boom : i;
                },
                concurrency: 4)
            .ToListAsync();

        var thrown = await act.Should().ThrowAsync<InvalidOperationException>();
        thrown.Which.Should().BeSameAs(boom);
    }

    [Fact]
    public async Task Capture_turns_failures_into_results_without_faulting()
    {
        var failures = new Dictionary<int, Exception>();

        var results = await Enumerable.Range(0, 100)
            .ToFlow(capacity: 16)
            .SelectResultAsync(
                async (i, ct) =>
                {
                    await Task.Yield();
                    if (i % 10 == 0)
                    {
                        var ex = new InvalidOperationException($"item {i}");
                        lock (failures)
                        {
                            failures[i] = ex;
                        }

                        throw ex;
                    }

                    return i * 2;
                },
                concurrency: 8,
                preserveOrder: true)
            .ToListAsync();

        results.Should().HaveCount(100, "Capture loses nothing: every item yields a result");

        for (var i = 0; i < 100; i++)
        {
            if (i % 10 == 0)
            {
                results[i].IsSuccess.Should().BeFalse();
                results[i].Exception.Should().BeSameAs(failures[i], "the captured exception is the original instance");
            }
            else
            {
                results[i].IsSuccess.Should().BeTrue();
                results[i].Value.Should().Be(i * 2);
                results[i].Exception.Should().BeNull();
            }
        }
    }

    [Fact]
    public async Task Capture_never_captures_pipeline_cancellation()
    {
        using var cts = new CancellationTokenSource();
        var firstItemRunning = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var pipeline = TestSources
            .Infinite()
            .ToFlow(capacity: 8)
            .SelectResultAsync(
                async (i, ct) =>
                {
                    firstItemRunning.TrySetResult();
                    await Task.Delay(Timeout.Infinite, ct);
                    return i;
                },
                concurrency: 4)
            .ConsumeAsync(cts.Token);

        await firstItemRunning.Task;
        cts.Cancel();

        await FluentActions.Awaiting(() => pipeline.WaitAsync(TimeSpan.FromSeconds(10)))
            .Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact]
    public async Task Capture_captures_a_selectors_internal_timeout_as_a_failure()
    {
        // The selector derives its own linked token and cancels it — the classic
        // per-item timeout. That OperationCanceledException does not belong to the
        // pipeline and must be captured as this item's failure, never swallowed.
        var results = await Enumerable.Range(0, 10)
            .ToFlow(capacity: 8)
            .SelectResultAsync(
                async (i, ct) =>
                {
                    if (i == 5)
                    {
                        using var internalTimeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
                        internalTimeout.Cancel();
                        await Task.Delay(Timeout.Infinite, internalTimeout.Token);
                    }

                    await Task.Yield();
                    return i;
                },
                concurrency: 4,
                preserveOrder: true)
            .ToListAsync()
            .WaitAsync(TimeSpan.FromSeconds(10));

        results.Should().HaveCount(10);
        results[5].IsSuccess.Should().BeFalse();
        results[5].Exception.Should().BeAssignableTo<OperationCanceledException>();
        results.Where((r, i) => i != 5).Should().OnlyContain(r => r.IsSuccess);
    }

    [Fact]
    public async Task Concurrent_failures_under_skip_leave_no_running_work_behind()
    {
        var active = 0;
        var maxActive = 0;

        await Enumerable.Range(0, 200)
            .ToFlow(capacity: 16)
            .SelectAsync<int, int>(
                async (i, ct) =>
                {
                    InterlockedMax.Update(ref maxActive, Interlocked.Increment(ref active));
                    try
                    {
                        await Task.Yield();
                        throw new InvalidOperationException($"item {i}");
                    }
                    finally
                    {
                        Interlocked.Decrement(ref active);
                    }
                },
                concurrency: 8,
                failureMode: FlowFailureMode.Skip)
            .ConsumeAsync();

        Volatile.Read(ref active).Should().Be(0);
        maxActive.Should().BeLessThanOrEqualTo(8, "failure handling must not leak extra concurrency");
    }

    [Fact]
    public async Task Skip_treats_a_foreign_cancellation_exception_as_an_ordinary_failure()
    {
        // An OperationCanceledException from a token that is NOT the pipeline's —
        // e.g. a selector's internal timeout — is a per-item failure, not pipeline
        // cancellation, and under Skip it must be dropped like any other failure.
        using var foreign = new CancellationTokenSource();
        foreign.Cancel();

        var results = await Enumerable.Range(0, 20)
            .ToFlow(capacity: 8)
            .SelectAsync(
                async (i, ct) =>
                {
                    await Task.Yield();
                    return i % 5 == 0 ? throw new OperationCanceledException(foreign.Token) : i;
                },
                concurrency: 4,
                failureMode: FlowFailureMode.Skip)
            .ToListAsync()
            .WaitAsync(TimeSpan.FromSeconds(10));

        results.Should().BeEquivalentTo(Enumerable.Range(0, 20).Where(i => i % 5 != 0));
    }

    [Fact]
    public async Task Capture_captures_a_foreign_cancellation_exception_as_a_failure()
    {
        using var foreign = new CancellationTokenSource();
        foreign.Cancel();

        var results = await Enumerable.Range(0, 10)
            .ToFlow(capacity: 8)
            .SelectResultAsync(
                async (i, ct) =>
                {
                    await Task.Yield();
                    return i == 5 ? throw new OperationCanceledException(foreign.Token) : i;
                },
                concurrency: 4,
                preserveOrder: true)
            .ToListAsync()
            .WaitAsync(TimeSpan.FromSeconds(10));

        results.Should().HaveCount(10);
        results[5].IsSuccess.Should().BeFalse();
        results[5].Exception.Should().BeOfType<OperationCanceledException>();
    }

    [Fact]
    public async Task A_skip_storm_in_ordered_mode_survives_cancellation_teardown()
    {
        var active = 0;
        using var cts = new CancellationTokenSource();
        var pastTheStorm = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var pipeline = TestSources
            .Infinite()
            .ToFlow(capacity: 8)
            .SelectAsync<int, int>(
                async (i, ct) =>
                {
                    Interlocked.Increment(ref active);
                    try
                    {
                        await Task.Yield();
                        if (i < 500)
                        {
                            throw new InvalidOperationException($"item {i}");
                        }

                        pastTheStorm.TrySetResult();
                        await Task.Delay(Timeout.Infinite, ct);
                        return i;
                    }
                    finally
                    {
                        Interlocked.Decrement(ref active);
                    }
                },
                concurrency: 8,
                preserveOrder: true,
                failureMode: FlowFailureMode.Skip)
            .ConsumeAsync(cts.Token);

        await pastTheStorm.Task;
        cts.Cancel();

        await FluentActions.Awaiting(() => pipeline.WaitAsync(TimeSpan.FromSeconds(10)))
            .Should().ThrowAsync<OperationCanceledException>();

        Volatile.Read(ref active).Should().Be(0, "ordered teardown must leave no running selector behind");
    }

    [Fact]
    public void SelectAsync_rejects_capture_mode()
    {
        var flow = Enumerable.Range(0, 10).ToFlow();
        var act = () => flow.SelectAsync(
            (i, _) => Task.FromResult(i),
            failureMode: FlowFailureMode.Capture);

        act.Should().Throw<ArgumentException>().WithMessage("*SelectResultAsync*");
    }

    [Fact]
    public async Task WhereAsync_skip_drops_items_whose_predicate_fails()
    {
        var results = await Enumerable.Range(0, 100)
            .ToFlow(capacity: 16)
            .WhereAsync(
                async (i, ct) =>
                {
                    await Task.Yield();
                    return i % 10 == 0
                        ? throw new InvalidOperationException($"item {i}")
                        : i % 2 == 0;
                },
                concurrency: 4,
                failureMode: FlowFailureMode.Skip)
            .ToListAsync();

        // Evens survive, except multiples of 10 whose predicate threw.
        results.Should().Equal(Enumerable.Range(0, 100).Where(i => i % 2 == 0 && i % 10 != 0));
    }

    [Fact]
    public void WhereAsync_rejects_capture_mode()
    {
        var flow = Enumerable.Range(0, 10).ToFlow();
        var act = () => flow.WhereAsync(
            (i, _) => Task.FromResult(true),
            failureMode: FlowFailureMode.Capture);

        act.Should().Throw<ArgumentException>();
    }

    [Fact]
    public void FlowResult_factories_produce_consistent_shapes()
    {
        var success = FlowResult.Success(42);
        success.IsSuccess.Should().BeTrue();
        success.Value.Should().Be(42);
        success.Exception.Should().BeNull();

        var boom = new InvalidOperationException("boom");
        var failure = FlowResult.Failure<int>(boom);
        failure.IsSuccess.Should().BeFalse();
        failure.Value.Should().Be(0);
        failure.Exception.Should().BeSameAs(boom);

        var nullFactory = () => FlowResult.Failure<int>(null!);
        nullFactory.Should().Throw<ArgumentNullException>();
    }
}
