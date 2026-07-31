using Caudal.Internal;
using FluentAssertions;
using Xunit;

namespace Caudal.Core.Tests;

/// <summary>
/// Tests for the per-stage <see cref="StageStats"/> instrumentation seam: counters
/// only exist when <see cref="FlowOptions.CaptureStatistics"/> is set, and telemetry
/// must never change what a pipeline emits — only what it reports about itself.
/// </summary>
public class StageStatsTests
{
    [Fact]
    public async Task Received_and_completed_counts_match_item_flow_through_source_and_select()
    {
        var options = new FlowOptions { CaptureStatistics = true };
        var source = Enumerable.Range(0, 20).ToFlow(options);
        var selected = source.SelectAsync((i, _) => Task.FromResult(i * 2));

        var results = await selected.ToListAsync().WaitAsync(TimeSpan.FromSeconds(10));

        results.Should().HaveCount(20);

        var sourceStats = source.Node.Stats;
        var selectStats = selected.Node.Stats;

        sourceStats.Should().NotBeNull();
        selectStats.Should().NotBeNull();

        sourceStats!.Received.Should().Be(20);
        sourceStats.Completed.Should().Be(20);
        selectStats!.Received.Should().Be(20);
        selectStats.Completed.Should().Be(20);
    }

    [Fact]
    public async Task Skip_mode_failures_count_as_failed_but_whereAsync_filter_misses_do_not()
    {
        var options = new FlowOptions { CaptureStatistics = true };

        var failingSource = Enumerable.Range(0, 10).ToFlow(options);
        var withFailures = failingSource.SelectAsync(
            (i, _) => i % 3 == 0
                ? throw new InvalidOperationException("boom")
                : Task.FromResult(i),
            failureMode: FlowFailureMode.Skip);

        var survivingResults = await withFailures.ToListAsync().WaitAsync(TimeSpan.FromSeconds(10));

        // 0, 3, 6, 9 throw; the other 6 of the 10 items survive.
        survivingResults.Should().HaveCount(6);

        var failureStats = withFailures.Node.Stats;
        failureStats.Should().NotBeNull();
        failureStats!.Failed.Should().Be(4);

        var filterSource = Enumerable.Range(0, 10).ToFlow(options);
        var filtered = filterSource.WhereAsync((i, _) => Task.FromResult(i % 2 == 0));

        var filteredResults = await filtered.ToListAsync().WaitAsync(TimeSpan.FromSeconds(10));

        filteredResults.Should().HaveCount(5);

        var filterStats = filtered.Node.Stats;
        filterStats.Should().NotBeNull();
        filterStats!.Failed.Should().Be(0, "a predicate returning false is a filter miss, not a failure");
    }

    [Fact]
    public async Task Active_workers_return_to_zero_and_processing_time_is_recorded()
    {
        var options = new FlowOptions { CaptureStatistics = true };
        var source = Enumerable.Range(0, 6).ToFlow(options);
        var selected = source.SelectAsync(
            async (i, ct) =>
            {
                await Task.Delay(5, ct);
                return i;
            },
            concurrency: 3);

        await selected.ToListAsync().WaitAsync(TimeSpan.FromSeconds(10));

        var stats = selected.Node.Stats;
        stats.Should().NotBeNull();
        stats!.Active.Should().Be(0);
        stats.AverageProcessingTime.Should().BeGreaterThan(TimeSpan.Zero);
    }

    [Fact]
    public async Task Stats_is_null_on_every_node_when_capture_statistics_is_off_by_default()
    {
        var source = Enumerable.Range(0, 5).ToFlow();
        var selected = source.SelectAsync((i, _) => Task.FromResult(i));

        await selected.ToListAsync().WaitAsync(TimeSpan.FromSeconds(10));

        source.Node.Stats.Should().BeNull();
        selected.Node.Stats.Should().BeNull();
    }

    [Fact]
    public async Task LatestByKey_reports_pending_queue_length_while_the_consumer_is_blocked()
    {
        var options = new FlowOptions { CaptureStatistics = true, Capacity = 32 };
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var firstItemReceived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var consumed = 0;

        var source = Enumerable.Range(0, 20).ToFlow(options);
        var conflated = source.LatestByKey(i => i % 5);
        var stats = conflated.Node.Stats;
        stats.Should().NotBeNull();

        var pipeline = conflated.ForEachAsync(async (_, ct) =>
        {
            if (Interlocked.Increment(ref consumed) == 1)
            {
                firstItemReceived.TrySetResult();
                await release.Task.WaitAsync(TimeSpan.FromSeconds(10), ct);
            }
        });

        await firstItemReceived.Task.WaitAsync(TimeSpan.FromSeconds(10));

        // The consumer is stuck on the first item; give the pump time to push the
        // rest of the finite source into the conflation dictionary before asserting.
        var deadline = System.Diagnostics.Stopwatch.StartNew();
        while (stats!.Received < 20 && deadline.Elapsed < TimeSpan.FromSeconds(10))
        {
            await Task.Delay(10);
        }

        stats!.Received.Should().Be(20);
        stats.Completed.Should().Be(1, "the consumer has only been handed the first item so far");
        stats.QueueLength.Should().BeGreaterThan(0, "other keys are still pending while the consumer is blocked");

        release.TrySetResult();
        await pipeline.WaitAsync(TimeSpan.FromSeconds(10));

        stats.Received.Should().Be(20);
        stats.Completed.Should().BeLessThanOrEqualTo(5, "only 5 distinct keys exist, so conflation bounds completions");
    }
}
