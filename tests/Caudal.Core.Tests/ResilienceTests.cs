using System.Collections.Concurrent;
using Caudal.Core.Tests.Support;
using FluentAssertions;
using Polly;
using Polly.CircuitBreaker;
using Polly.Retry;
using Polly.Timeout;
using Xunit;

namespace Caudal.Core.Tests;

public class ResilienceTests
{
    private static readonly Func<int, CancellationToken, Task<int>> AlwaysFails =
        (i, _) => Task.FromException<int>(new InvalidOperationException($"item {i}"));

    [Fact]
    public async Task Retry_recovers_transient_failures()
    {
        var attempts = new ConcurrentDictionary<int, int>();
        var pipeline = new ResiliencePipelineBuilder()
            .AddRetry(new RetryStrategyOptions
            {
                MaxRetryAttempts = 3,
                Delay = TimeSpan.Zero,
            })
            .Build();

        var results = await Enumerable.Range(0, 20)
            .ToFlow(capacity: 16)
            .SelectAsync(
                async (i, ct) =>
                {
                    var attempt = attempts.AddOrUpdate(i, 1, (_, count) => count + 1);
                    await Task.Yield();
                    if (attempt <= 2)
                    {
                        throw new InvalidOperationException($"item {i} attempt {attempt}");
                    }

                    return i;
                },
                pipeline,
                concurrency: 4)
            .ToListAsync()
            .WaitAsync(TimeSpan.FromSeconds(10));

        results.Should().HaveCount(20);
        attempts.Should().HaveCount(20);
        attempts.Values.Should().OnlyContain(count => count == 3);
    }

    [Fact]
    public async Task Exhausted_retries_respect_failure_modes()
    {
        var pipeline = new ResiliencePipelineBuilder()
            .AddRetry(new RetryStrategyOptions
            {
                MaxRetryAttempts = 2,
                Delay = TimeSpan.Zero,
            })
            .Build();

        var skipped = await Enumerable.Range(0, 10)
            .ToFlow(capacity: 8)
            .SelectAsync(AlwaysFails, pipeline, concurrency: 4, failureMode: FlowFailureMode.Skip)
            .ToListAsync()
            .WaitAsync(TimeSpan.FromSeconds(10));

        skipped.Should().BeEmpty();

        var captured = await Enumerable.Range(0, 10)
            .ToFlow(capacity: 8)
            .SelectResultAsync(AlwaysFails, pipeline, concurrency: 4)
            .ToListAsync()
            .WaitAsync(TimeSpan.FromSeconds(10));

        captured.Should().HaveCount(10);
        captured.Should().OnlyContain(r => !r.IsSuccess);
        captured.Should().OnlyContain(r => r.Exception is InvalidOperationException);
    }

    [Fact]
    public async Task Polly_timeout_cancels_the_slow_attempt()
    {
        var pipeline = new ResiliencePipelineBuilder()
            .AddTimeout(TimeSpan.FromMilliseconds(50))
            .Build();

        var results = await Enumerable.Range(0, 5)
            .ToFlow(capacity: 8)
            .SelectResultAsync(
                async (i, ct) =>
                {
                    if (i == 2)
                    {
                        await Task.Delay(Timeout.Infinite, ct);
                    }

                    await Task.Yield();
                    return i;
                },
                pipeline,
                concurrency: 4,
                preserveOrder: true)
            .ToListAsync()
            .WaitAsync(TimeSpan.FromSeconds(10));

        results.Should().HaveCount(5);
        results[2].IsSuccess.Should().BeFalse();
        results[2].Exception.Should().BeOfType<TimeoutRejectedException>();
        results.Where((_, index) => index != 2).Should().OnlyContain(r => r.IsSuccess);
    }

    [Fact]
    public async Task Circuit_breaker_opens_after_repeated_failures()
    {
        var pipeline = new ResiliencePipelineBuilder()
            .AddCircuitBreaker(new CircuitBreakerStrategyOptions
            {
                FailureRatio = 1.0,
                MinimumThroughput = 2,
                // Both durations are load-bearing for determinism: they must
                // comfortably exceed the test's real wall-clock runtime so the
                // sampling window never expires and the breaker never half-opens
                // mid-test. Do not "clean them up" into shorter values.
                SamplingDuration = TimeSpan.FromSeconds(30),
                BreakDuration = TimeSpan.FromSeconds(30),
            })
            .Build();

        var results = await Enumerable.Range(0, 10)
            .ToFlow(capacity: 8)
            .SelectResultAsync(AlwaysFails, pipeline, concurrency: 1, preserveOrder: true)
            .ToListAsync()
            .WaitAsync(TimeSpan.FromSeconds(10));

        results.Should().HaveCount(10);
        results[0].Exception.Should().BeOfType<InvalidOperationException>();
        results.Should().Contain(r => r.Exception is BrokenCircuitException);
    }

    [Fact]
    public async Task Pipeline_cancellation_is_not_retried()
    {
        var retryCount = 0;
        var pipeline = new ResiliencePipelineBuilder()
            .AddRetry(new RetryStrategyOptions
            {
                MaxRetryAttempts = 5,
                Delay = TimeSpan.Zero,
                OnRetry = _ =>
                {
                    Interlocked.Increment(ref retryCount);
                    return default;
                },
            })
            .Build();

        using var cts = new CancellationTokenSource();
        var firstItemRunning = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var pipelineTask = TestSources
            .Infinite()
            .ToFlow(capacity: 8)
            .SelectAsync(
                async (i, ct) =>
                {
                    firstItemRunning.TrySetResult();
                    await Task.Delay(Timeout.Infinite, ct);
                    return i;
                },
                pipeline,
                concurrency: 4)
            .ConsumeAsync(cts.Token);

        await firstItemRunning.Task;
        cts.Cancel();

        await FluentActions.Awaiting(() => pipelineTask.WaitAsync(TimeSpan.FromSeconds(10)))
            .Should().ThrowAsync<OperationCanceledException>();

        Volatile.Read(ref retryCount).Should().Be(0);
    }

    [Fact]
    public async Task Pipeline_cancellation_through_a_timeout_strategy_is_never_captured_as_an_item_failure()
    {
        // A Timeout strategy makes Polly substitute its own linked token per
        // attempt, so the OCE raised by cancelling the flow arrives carrying a
        // foreign token. The wrapper must re-anchor it: cancellation is
        // cancellation, never the in-flight item's captured failure.
        var pipeline = new ResiliencePipelineBuilder()
            .AddTimeout(TimeSpan.FromSeconds(30))
            .Build();

        using var cts = new CancellationTokenSource();
        var firstItemRunning = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var captured = new List<FlowResult<int>>();

        var pipelineTask = TestSources
            .Infinite()
            .ToFlow(capacity: 8)
            .SelectResultAsync(
                async (i, ct) =>
                {
                    firstItemRunning.TrySetResult();
                    await Task.Delay(Timeout.Infinite, ct);
                    return i;
                },
                pipeline,
                concurrency: 2)
            .ForEachAsync(
                (result, _) =>
                {
                    lock (captured)
                    {
                        captured.Add(result);
                    }

                    return Task.CompletedTask;
                },
                cts.Token);

        await firstItemRunning.Task;
        cts.Cancel();

        await FluentActions.Awaiting(() => pipelineTask.WaitAsync(TimeSpan.FromSeconds(10)))
            .Should().ThrowAsync<OperationCanceledException>();

        lock (captured)
        {
            captured.Should().NotContain(
                r => !r.IsSuccess && r.Exception is OperationCanceledException,
                "pipeline cancellation must never be misfiled as an item's captured failure");
        }
    }

    [Fact]
    public async Task Skip_with_a_timeout_strategy_does_not_absorb_pipeline_cancellation()
    {
        var pipeline = new ResiliencePipelineBuilder()
            .AddTimeout(TimeSpan.FromSeconds(30))
            .Build();

        using var cts = new CancellationTokenSource();
        var firstItemRunning = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var pipelineTask = TestSources
            .Infinite()
            .ToFlow(capacity: 8)
            .SelectAsync(
                async (i, ct) =>
                {
                    firstItemRunning.TrySetResult();
                    await Task.Delay(Timeout.Infinite, ct);
                    return i;
                },
                pipeline,
                concurrency: 2,
                failureMode: FlowFailureMode.Skip)
            .ConsumeAsync(cts.Token);

        await firstItemRunning.Task;
        cts.Cancel();

        // If Skip absorbed the foreign-token OCE as an item failure, the pipeline
        // would keep consuming the infinite source instead of terminating.
        await FluentActions.Awaiting(() => pipelineTask.WaitAsync(TimeSpan.FromSeconds(10)))
            .Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact]
    public async Task WithResiliencePipeline_fluent_shape_matches_the_direct_overload()
    {
        var pipeline = new ResiliencePipelineBuilder()
            .AddRetry(new RetryStrategyOptions
            {
                MaxRetryAttempts = 3,
                Delay = TimeSpan.Zero,
            })
            .Build();

        var attempts = new ConcurrentDictionary<int, int>();

        var results = await Enumerable.Range(0, 10)
            .ToFlow(capacity: 8)
            .WithResiliencePipeline(pipeline)
            .SelectAsync(
                async (i, ct) =>
                {
                    var attempt = attempts.AddOrUpdate(i, 1, (_, count) => count + 1);
                    await Task.Yield();
                    if (attempt <= 1)
                    {
                        throw new InvalidOperationException($"item {i}");
                    }

                    return i;
                },
                concurrency: 4)
            .ToListAsync()
            .WaitAsync(TimeSpan.FromSeconds(10));

        results.Should().HaveCount(10);
        results.Should().BeEquivalentTo(Enumerable.Range(0, 10));
    }
}
