using System.Diagnostics.Metrics;
using System.Globalization;
using Caudal;
using FluentAssertions;
using Xunit;

namespace Caudal.Core.Tests;

/// <summary>
/// Golden and end-to-end tests for <c>Caudal.Diagnostics</c>: the textual pipeline
/// view, the aggregate properties on <see cref="FlowSnapshot"/>, and the metrics
/// published through <see cref="System.Diagnostics.Metrics"/>.
/// </summary>
public class DiagnosticsTests
{
    // This is the project's first golden/"visual" test: the expected string is kept
    // as a readable const so a future format change is a deliberate, visible diff.
    private const string TwoStageExpectedRender =
        "diagnostics-demo\n" +
        "├─ LatestByKey\n" +
        "│  received: 100,000\n" +
        "│  completed: 81,588\n" +
        "│  replaced: 18,412\n" +
        "│  queued: 32/128\n" +
        "└─ SelectAsync\n" +
        "   received: 81,588\n" +
        "   completed: 81,563\n" +
        "   failed: 25\n" +
        "   active: 8/8\n" +
        "   avg queue: 2.5 ms\n" +
        "   avg processing: 14.3 ms";

    private const string SingleStageUnnamedExpectedRender =
        "(unnamed)\n" +
        "└─ Source\n" +
        "   received: 10\n" +
        "   completed: 10";

    [Fact]
    public void Render_is_culture_invariant()
    {
        // es-AR uses '.' as the thousands separator and ',' as the decimal
        // separator; the golden strings must survive it untouched.
        var original = CultureInfo.CurrentCulture;
        try
        {
            CultureInfo.CurrentCulture = new CultureInfo("es-AR");
            BuildSingleStageUnnamedSnapshot().Render().Should().Be(SingleStageUnnamedExpectedRender);
            BuildTwoStageSnapshot().Render().Should().Be(TwoStageExpectedRender);
        }
        finally
        {
            CultureInfo.CurrentCulture = original;
        }
    }

    [Fact]
    public async Task Received_never_trails_completed_in_a_mid_flight_snapshot()
    {
        // Regression for the counter-ordering race: received must be recorded
        // before an item becomes visible downstream, so no snapshot at any
        // instant may observe completed > received on any stage.
        using var cts = new CancellationTokenSource();

        var flow = Support.TestSources
            .Infinite()
            .ToFlow(new FlowOptions { Capacity = 8, Name = "invariant", CaptureStatistics = true })
            .Batch(maximumSize: 4, maximumDelay: TimeSpan.FromMilliseconds(1));

        var pipeline = flow.ForEachAsync(async (_, ct) => await Task.Delay(2, ct), cts.Token);

        for (var i = 0; i < 50; i++)
        {
            var snapshot = flow.GetSnapshot();
            foreach (var stage in snapshot.Stages)
            {
                stage.Received.Should().BeGreaterThanOrEqualTo(
                    stage.Completed,
                    $"stage {stage.Operator} must never report more completions than receipts");
            }

            await Task.Delay(5);
        }

        cts.Cancel();
        await FluentActions.Awaiting(() => pipeline.WaitAsync(TimeSpan.FromSeconds(10)))
            .Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact]
    public void Render_prints_every_detail_line_in_order_and_suppresses_zero_counters()
        => BuildTwoStageSnapshot().Render().Should().Be(TwoStageExpectedRender);

    [Fact]
    public void Render_uses_the_unnamed_placeholder_and_a_single_terminal_branch()
        => BuildSingleStageUnnamedSnapshot().Render().Should().Be(SingleStageUnnamedExpectedRender);

    private static FlowSnapshot BuildTwoStageSnapshot()
    {
        var latestByKey = new StageSnapshot(
            Operator: "LatestByKey",
            Received: 100_000,
            Completed: 81_588,
            Failed: 0,
            Dropped: 0,
            Replaced: 18_412,
            Queued: 32,
            QueueCapacity: 128,
            Active: 1,
            ConfiguredConcurrency: 1,
            QueueTimeSamples: 0,
            ProcessingTimeSamples: 0,
            AverageQueueTime: TimeSpan.Zero,
            AverageProcessingTime: TimeSpan.Zero);

        var selectAsync = new StageSnapshot(
            Operator: "SelectAsync",
            Received: 81_588,
            Completed: 81_563,
            Failed: 25,
            Dropped: 0,
            Replaced: 0,
            Queued: 0,
            QueueCapacity: 0,
            Active: 8,
            ConfiguredConcurrency: 8,
            QueueTimeSamples: 81_588,
            ProcessingTimeSamples: 81_563,
            AverageQueueTime: TimeSpan.FromMilliseconds(2.5),
            AverageProcessingTime: TimeSpan.FromMilliseconds(14.3));

        return new FlowSnapshot(
            "diagnostics-demo", new[] { latestByKey, selectAsync }, TimeSpan.FromSeconds(5));
    }

    private static FlowSnapshot BuildSingleStageUnnamedSnapshot()
    {
        var source = new StageSnapshot(
            Operator: "Source",
            Received: 10,
            Completed: 10,
            Failed: 0,
            Dropped: 0,
            Replaced: 0,
            Queued: 0,
            QueueCapacity: 0,
            Active: 0,
            ConfiguredConcurrency: 1,
            QueueTimeSamples: 0,
            ProcessingTimeSamples: 0,
            AverageQueueTime: TimeSpan.Zero,
            AverageProcessingTime: TimeSpan.Zero);

        return new FlowSnapshot(null, new[] { source }, TimeSpan.FromSeconds(1));
    }

    [Fact]
    public void Aggregate_properties_combine_stages_with_sample_count_weighting()
    {
        var stageA = new StageSnapshot(
            Operator: "A",
            Received: 500,
            Completed: 480,
            Failed: 1,
            Dropped: 0,
            Replaced: 0,
            Queued: 5,
            QueueCapacity: 16,
            Active: 1,
            ConfiguredConcurrency: 1,
            QueueTimeSamples: 10,
            ProcessingTimeSamples: 10,
            AverageQueueTime: TimeSpan.FromMilliseconds(4),
            AverageProcessingTime: TimeSpan.FromMilliseconds(2));

        var stageB = new StageSnapshot(
            Operator: "B",
            Received: 480,
            Completed: 470,
            Failed: 0,
            Dropped: 2,
            Replaced: 1,
            Queued: 2,
            QueueCapacity: 16,
            Active: 3,
            ConfiguredConcurrency: 4,
            QueueTimeSamples: 0,
            ProcessingTimeSamples: 20,
            AverageQueueTime: TimeSpan.Zero,
            AverageProcessingTime: TimeSpan.FromMilliseconds(8));

        var stageC = new StageSnapshot(
            Operator: "C",
            Received: 470,
            Completed: 460,
            Failed: 0,
            Dropped: 0,
            Replaced: 0,
            Queued: 0,
            QueueCapacity: 0,
            Active: 0,
            ConfiguredConcurrency: 1,
            QueueTimeSamples: 30,
            ProcessingTimeSamples: 0,
            AverageQueueTime: TimeSpan.FromMilliseconds(1),
            AverageProcessingTime: TimeSpan.Zero);

        var snapshot = new FlowSnapshot("aggregate-demo", new[] { stageA, stageB, stageC }, TimeSpan.FromSeconds(9));

        snapshot.Received.Should().Be(500);
        snapshot.Completed.Should().Be(460);
        snapshot.Failed.Should().Be(1);
        snapshot.Dropped.Should().Be(2);
        snapshot.Replaced.Should().Be(1);
        snapshot.Queued.Should().Be(7);
        snapshot.Active.Should().Be(4);

        // Weighted over 10 (@4ms) + 30 (@1ms) samples = 70ms of samples / 40 samples.
        snapshot.AverageQueueTime.Should().Be(TimeSpan.FromMilliseconds(1.75));

        // Weighted over 10 (@2ms) + 20 (@8ms) samples = 180ms of samples / 30 samples.
        snapshot.AverageProcessingTime.Should().Be(TimeSpan.FromMilliseconds(6));
    }

    [Fact]
    public void Aggregate_properties_are_zero_or_empty_for_a_flow_with_no_stages()
    {
        var snapshot = new FlowSnapshot("empty", Array.Empty<StageSnapshot>(), TimeSpan.Zero);

        snapshot.Received.Should().Be(0);
        snapshot.Completed.Should().Be(0);
        snapshot.AverageQueueTime.Should().Be(TimeSpan.Zero);
        snapshot.AverageProcessingTime.Should().Be(TimeSpan.Zero);
    }

    [Fact]
    public async Task GetSnapshot_walks_the_real_pipeline_from_source_to_terminal_stage()
    {
        var options = new FlowOptions { Capacity = 16, Name = "diag", CaptureStatistics = true };

        var flow = Enumerable.Range(0, 100)
            .ToFlow(options)
            .SelectAsync((i, _) => Task.FromResult(i), concurrency: 4);

        await flow.ToListAsync().WaitAsync(TimeSpan.FromSeconds(10));

        var snapshot = flow.GetSnapshot();

        snapshot.Name.Should().Be("diag");
        snapshot.Stages.Select(stage => stage.Operator).Should().Equal("Source", "SelectAsync");
        snapshot.Received.Should().Be(100);
        snapshot.Completed.Should().Be(100);
        snapshot.Active.Should().Be(0);
        snapshot.PipelineDuration.Should().BeGreaterThan(TimeSpan.Zero);
    }

    [Fact]
    public void GetSnapshot_throws_when_the_flow_was_built_without_CaptureStatistics()
    {
        var flow = Enumerable.Range(0, 10).ToFlow(new FlowOptions { CaptureStatistics = false });

        var act = () => flow.GetSnapshot();

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*CaptureStatistics*");
    }

    [Fact]
    public async Task PublishMetrics_exposes_live_completed_counts_and_stops_after_disposal()
    {
        var options = new FlowOptions { Capacity = 16, Name = "diag-metrics", CaptureStatistics = true };

        var flow = Enumerable.Range(0, 100)
            .ToFlow(options)
            .SelectAsync((i, _) => Task.FromResult(i), concurrency: 4);

        var subscription = flow.PublishMetrics();
        try
        {
            long? completedValue = null;
            string? completedOperator = null;

            using var listener = new MeterListener();
            listener.InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Meter.Name == CaudalDiagnostics.MeterName)
                {
                    l.EnableMeasurementEvents(instrument);
                }
            };
            listener.SetMeasurementEventCallback<long>((instrument, measurement, tags, _) =>
            {
                if (instrument.Name != "caudal.items.completed")
                {
                    return;
                }

                foreach (var tag in tags)
                {
                    if (tag.Key == "operator" && Equals(tag.Value, "SelectAsync"))
                    {
                        completedValue = measurement;
                        completedOperator = (string?)tag.Value;
                    }
                }
            });
            listener.Start();

            await flow.ToListAsync().WaitAsync(TimeSpan.FromSeconds(10));

            listener.RecordObservableInstruments();

            completedValue.Should().Be(100);
            completedOperator.Should().Be("SelectAsync");
        }
        finally
        {
            var dispose = () => subscription.Dispose();
            dispose.Should().NotThrow();
        }
    }
}
