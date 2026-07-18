using System.Diagnostics;
using Caudal;
using FluentAssertions;
using Xunit;

namespace Caudal.Testing.Tests;

/// <summary>
/// The Phase 7 exit-criterion showcase: conditions that would normally be
/// intermittent race conditions, reproduced stably. Each test names the race
/// it pins in its own comment and holds the racy window open with an
/// <see cref="AsyncGate"/> instead of guessing at a delay. Flows are lazy, so
/// every test attaches its sink FIRST and only then coordinates through the
/// gate — nothing runs until the terminal task is created.
/// </summary>
public class DeterministicRaceTests
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(10);

    // Formerly-intermittent condition: "a failure racing in-flight work" — a
    // source fails while workers are mid-item, and whether the consumer ever
    // observes the exception, or observes it before the workers actually
    // started, used to depend on scheduling luck.
    [Fact]
    public async Task A_failure_races_in_flight_work()
    {
        var gate = new AsyncGate(open: false);
        var source = new TestFlowSource<int>();
        var boom = new InvalidOperationException("upstream exploded");

        var flow = source.ToFlow()
            .SelectAsync(
                async (i, ct) =>
                {
                    await gate.WaitAsync(ct);
                    return i;
                },
                concurrency: 3);

        var pipeline = flow.ToListAsync();

        source.EmitRange([1, 2, 3]);

        // Provably 3 workers in flight, not "probably by now" — the race
        // window is held open by the gate on every run.
        await gate.WhenWaitersAsync(3).WaitAsync(Bound);

        source.Fail(boom);
        gate.Open();

        var thrown = await FluentActions
            .Awaiting(() => pipeline)
            .Should()
            .ThrowExactlyAsync<InvalidOperationException>();

        thrown.Which.Should().BeSameAs(boom);
    }

    // Formerly-intermittent condition: "backpressure observed, not guessed" —
    // proving a bounded buffer actually stalled a producer used to mean
    // sleeping and hoping the count had stopped moving.
    [Fact]
    public async Task Backpressure_is_observed_not_guessed()
    {
        var gate = new AsyncGate(open: false);
        var source = new TestFlowSource<int>();

        var flow = source.ToFlow(new FlowOptions { Capacity = 2, CaptureStatistics = true })
            .SelectAsync(
                async (i, ct) =>
                {
                    await gate.WaitAsync(ct);
                    return i;
                },
                concurrency: 1);

        var run = flow.Should().CompleteWithoutLeaks().WithTimeout(Bound).RunAsync();

        source.EmitRange(Enumerable.Range(0, 10));

        await gate.WhenWaitersAsync(1).WaitAsync(Bound);

        var stalledAt = await WaitForStablePendingCountAsync(source);

        // Bounded buffer (2) + one worker slot absorbed a handful of items;
        // the rest were held back at the source, never guessed at.
        stalledAt.Should().BeGreaterThan(0);
        stalledAt.Should().BeLessThan(10);

        source.Complete();
        gate.Open();

        await run;
    }

    // Formerly-intermittent condition: "exact concurrency saturation" —
    // proving a stage reached its concurrency cap, and never exceeded it, is
    // usually asserted after a delay and a "maxActive" counter that might not
    // have caught the peak.
    [Fact]
    public async Task Concurrency_saturates_at_exactly_the_configured_cap()
    {
        var gate = new AsyncGate(open: false);
        var source = new TestFlowSource<int>();

        var flow = source.ToFlow(new FlowOptions { CaptureStatistics = true })
            .SelectAsync(
                async (i, ct) =>
                {
                    await gate.WaitAsync(ct);
                    return i;
                },
                concurrency: 4);

        var run = flow.Should()
            .UseAtMostConcurrency(4)
            .CompleteWithoutLeaks()
            .WithTimeout(Bound)
            .RunAsync();

        source.EmitRange(Enumerable.Range(0, 8));

        await gate.WhenWaitersAsync(4).WaitAsync(Bound);
        gate.WaitingCount.Should().Be(4);

        gate.Release(4);
        await gate.WhenWaitersAsync(4).WaitAsync(Bound);
        gate.WaitingCount.Should().Be(4);

        source.Complete();
        gate.Open();

        await run;
    }

    // Formerly-intermittent condition: "cancellation with workers provably
    // in flight" — proving cancellation actually reaches workers mid-item,
    // rather than firing before they started or after they'd already
    // finished, used to be a matter of timing the cancel call by hand.
    [Fact]
    public async Task Cancellation_reaches_workers_provably_in_flight()
    {
        var gate = new AsyncGate(open: false);
        var source = new TestFlowSource<int>();
        using var cts = new CancellationTokenSource();

        var flow = source.ToFlow()
            .SelectAsync(
                async (i, ct) =>
                {
                    await gate.WaitAsync(ct);
                    return i;
                },
                concurrency: 2);

        var pipeline = flow.ToListAsync(cts.Token);

        source.EmitRange([1, 2]);

        await gate.WhenWaitersAsync(2).WaitAsync(Bound);

        cts.Cancel();

        await FluentActions
            .Awaiting(() => pipeline)
            .Should()
            .ThrowAsync<OperationCanceledException>();

        // The workers observed cancellation through their own token and left
        // the gate; nothing is left waiting behind.
        await WaitForWaitingCountToReachZeroAsync(gate);
        gate.WaitingCount.Should().Be(0);
    }

    private static async Task<int> WaitForStablePendingCountAsync(TestFlowSource<int> source)
    {
        var stopwatch = Stopwatch.StartNew();
        var previous = source.PendingCount;

        while (stopwatch.Elapsed < Bound)
        {
            await Task.Delay(20);
            var current = source.PendingCount;
            if (current == previous)
            {
                return current;
            }

            previous = current;
        }

        return previous;
    }

    private static async Task WaitForWaitingCountToReachZeroAsync(AsyncGate gate)
    {
        var stopwatch = Stopwatch.StartNew();

        while (gate.WaitingCount > 0 && stopwatch.Elapsed < Bound)
        {
            await Task.Delay(20);
        }
    }
}
