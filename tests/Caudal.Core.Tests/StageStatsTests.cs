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
    public async Task Inputs_received_and_outputs_emitted_match_item_flow_through_source_and_select()
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

        // Source and SelectAsync are both 1:1 stages: every input becomes one
        // output. That equality is a per-operator contract, not a global rule —
        // it does not hold for cardinality-changing stages like Batch or Where.
        sourceStats!.InputsReceived.Should().Be(20);
        sourceStats.OutputsEmitted.Should().Be(20);
        selectStats!.InputsReceived.Should().Be(20);
        selectStats.OutputsEmitted.Should().Be(20);
    }

    [Fact]
    public async Task Skip_mode_failures_count_as_input_failed_but_whereAsync_filter_misses_count_as_input_filtered()
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
        failureStats!.InputsFailed.Should().Be(4);
        failureStats.InputsFiltered.Should().Be(0, "a Skip failure is a failure, not a filter miss");

        var filterSource = Enumerable.Range(0, 10).ToFlow(options);
        var filtered = filterSource.WhereAsync((i, _) => Task.FromResult(i % 2 == 0));

        var filteredResults = await filtered.ToListAsync().WaitAsync(TimeSpan.FromSeconds(10));

        filteredResults.Should().HaveCount(5);

        var filterStats = filtered.Node.Stats;
        filterStats.Should().NotBeNull();
        filterStats!.InputsFailed.Should().Be(0, "a predicate returning false is a filter miss, not a failure");
        filterStats.InputsFiltered.Should().Be(5, "the 5 odd inputs were rejected by the predicate");
    }

    [Fact]
    public async Task Batch_reports_batches_emitted_and_the_logical_item_count_separately()
    {
        var options = new FlowOptions { CaptureStatistics = true };
        var source = Enumerable.Range(0, 10).ToFlow(options);
        var batched = source.Batch(maximumSize: 4, maximumDelay: TimeSpan.FromSeconds(10));

        var batches = await batched.ToListAsync().WaitAsync(TimeSpan.FromSeconds(10));

        // 10 items at maximumSize 4 -> batches of 4, 4, 2 (the final partial batch
        // is always emitted on source completion).
        batches.Should().HaveCount(3);

        var batchStats = batched.Node.Stats;
        batchStats.Should().NotBeNull();

        // outputs.emitted counts batches (what the stage actually handed
        // downstream) — it is not, and is never meant to be, item count.
        batchStats!.InputsReceived.Should().Be(10);
        batchStats.OutputsEmitted.Should().Be(3);
        batchStats.BatchItemsIncluded.Should().Be(10, "batch.items.included carries the item-level count that outputs.emitted deliberately does not");
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
        var conflated = source.LatestByKey(i => i % 5, maximumKeys: 5);
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
        while (stats!.InputsReceived < 20 && deadline.Elapsed < TimeSpan.FromSeconds(10))
        {
            await Task.Delay(10);
        }

        stats!.InputsReceived.Should().Be(20);
        stats.OutputsEmitted.Should().Be(1, "the consumer has only been handed the first item so far");
        stats.QueueLength.Should().BeGreaterThan(0, "other keys are still pending while the consumer is blocked");

        release.TrySetResult();
        await pipeline.WaitAsync(TimeSpan.FromSeconds(10));

        stats.InputsReceived.Should().Be(20);
        stats.OutputsEmitted.Should().BeLessThanOrEqualTo(5, "only 5 distinct keys exist, so conflation bounds emissions");
    }
}
