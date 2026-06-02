using Caudal.Core.Tests.Support;
using FluentAssertions;
using Xunit;

namespace Caudal.Core.Tests;

public class ErrorHandlingTests
{
    [Fact]
    public async Task The_original_exception_instance_reaches_the_consumer()
    {
        var boom = new InvalidOperationException("boom");

        var act = () => Enumerable.Range(0, 100)
            .ToFlow(capacity: 8)
            .SelectAsync(
                async (i, ct) =>
                {
                    await Task.Yield();
                    return i == 25 ? throw boom : i;
                },
                concurrency: 4)
            .ToListAsync();

        var thrown = await act.Should().ThrowAsync<InvalidOperationException>();
        thrown.Which.Should().BeSameAs(boom, "the pipeline must rethrow the original exception, not a wrapper");
    }

    [Fact]
    public async Task The_original_exception_reaches_the_consumer_in_ordered_mode()
    {
        var boom = new InvalidOperationException("boom");

        var act = () => Enumerable.Range(0, 100)
            .ToFlow(capacity: 8)
            .SelectAsync(
                async (i, ct) =>
                {
                    await Task.Yield();
                    return i == 25 ? throw boom : i;
                },
                concurrency: 4,
                preserveOrder: true)
            .ToListAsync();

        var thrown = await act.Should().ThrowAsync<InvalidOperationException>();
        thrown.Which.Should().BeSameAs(boom);
    }

    [Fact]
    public async Task A_failure_stops_the_source()
    {
        var produced = 0;

        var act = () => TestSources
            .Infinite(onYield: _ => Interlocked.Increment(ref produced))
            .ToFlow(capacity: 4)
            .SelectAsync(
                async (i, ct) =>
                {
                    await Task.Yield();
                    return i == 10 ? throw new InvalidOperationException("boom") : i;
                },
                concurrency: 2)
            .ConsumeAsync();

        await act.Should().ThrowAsync<InvalidOperationException>();

        // The sink task has completed, so every internal task is done: production stops.
        var afterFault = Volatile.Read(ref produced);
        await Task.Delay(200);
        Volatile.Read(ref produced).Should().Be(afterFault, "no orphaned producer may keep running after the fault");
        afterFault.Should().BeLessThan(100, "the source must be cancelled promptly, not drained");
    }

    [Fact]
    public async Task Simultaneous_failures_across_workers_do_not_deadlock_and_one_original_exception_wins()
    {
        var failures = new[]
        {
            new InvalidOperationException("first"),
            new InvalidOperationException("second"),
        };

        var act = () => Enumerable.Range(0, 100)
            .ToFlow(capacity: 8)
            .SelectAsync<int, int>(
                async (i, ct) =>
                {
                    await Task.Yield();
                    throw failures[i % 2];
                },
                concurrency: 8)
            .ConsumeAsync();

        var thrown = await act.Should()
            .ThrowAsync<InvalidOperationException>()
            .WaitAsync(TimeSpan.FromSeconds(10));

        failures.Should().Contain(thrown.Which, "the winner must be one of the original exceptions");
    }

    [Fact]
    public async Task An_exception_thrown_by_the_sink_stops_the_whole_pipeline()
    {
        var produced = 0;
        var activeWorkers = 0;
        var boom = new InvalidOperationException("sink failed");

        var act = () => TestSources
            .Infinite(onYield: _ => Interlocked.Increment(ref produced))
            .ToFlow(capacity: 8)
            .SelectAsync(
                async (i, ct) =>
                {
                    Interlocked.Increment(ref activeWorkers);
                    try
                    {
                        await Task.Yield();
                        return i;
                    }
                    finally
                    {
                        Interlocked.Decrement(ref activeWorkers);
                    }
                },
                concurrency: 4)
            .ForEachAsync((i, _) => i >= 5 ? throw boom : Task.CompletedTask);

        var thrown = await act.Should().ThrowAsync<InvalidOperationException>();
        thrown.Which.Should().BeSameAs(boom);

        // The sink task completed, so upstream teardown must have finished too.
        Volatile.Read(ref activeWorkers).Should().Be(0, "no worker may survive a sink failure");

        var afterFault = Volatile.Read(ref produced);
        await Task.Delay(200);
        Volatile.Read(ref produced).Should().Be(afterFault, "the producer must stop when the sink faults");
    }

    [Fact]
    public async Task An_exception_thrown_by_the_source_reaches_the_consumer()
    {
        var boom = new InvalidOperationException("source failed");

        var act = () => Failing(boom)
            .ToFlow(capacity: 4)
            .SelectAsync((i, _) => Task.FromResult(i), concurrency: 2)
            .ToListAsync();

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
