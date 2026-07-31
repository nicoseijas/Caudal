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
        "│  inputs: 100,000\n" +
        "│  outputs: 81,588\n" +
        "│  replaced: 18,412\n" +
        "│  queued: 32/128\n" +
        "└─ SelectAsync\n" +
        "   inputs: 81,588\n" +
        "   outputs: 81,563\n" +
        "   failed: 25\n" +
        "   active: 8/8\n" +
        "   avg queue: 2.5 ms\n" +
        "   avg processing: 14.3 ms";

    private const string SingleStageUnnamedExpectedRender =
        "(unnamed)\n" +
        "└─ Source\n" +
        "   inputs: 10\n" +
        "   outputs: 10";

    private const string BatchStageExpectedRender =
        "batch-demo\n" +
        "└─ Batch\n" +
        "   inputs: 10\n" +
        "   outputs: 3\n" +
        "   batch.items.included: 10";

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
            BuildBatchStageSnapshot().Render().Should().Be(BatchStageExpectedRender);
        }
        finally
        {
            CultureInfo.CurrentCulture = original;
        }
    }

    [Fact]
    public async Task Outputs_never_exceed_inputs_in_a_mid_flight_snapshot_on_a_1to1_source_stage()
    {
        // Regression for the counter-ordering race: a receipt must be recorded
        // before an item becomes visible downstream, so no snapshot at any
        // instant may observe outputs > inputs on the source stage. This is a
        // per-operator contract (Source is 1:1) — it is not a global invariant:
        // cardinality-changing stages like Batch downstream have no such
        // relationship between their own inputs and outputs.
        using var cts = new CancellationTokenSource();

        var flow = Support.TestSources
            .Infinite()
            .ToFlow(new FlowOptions { Capacity = 8, Name = "invariant", CaptureStatistics = true })
            .Batch(maximumSize: 4, maximumDelay: TimeSpan.FromMilliseconds(1));

        var pipeline = flow.ForEachAsync(async (_, ct) => await Task.Delay(2, ct), cts.Token);

        for (var i = 0; i < 50; i++)
        {
            var snapshot = flow.GetSnapshot();
            var sourceStage = snapshot.Stages[0];
            sourceStage.InputsReceived.Should().BeGreaterThanOrEqualTo(
                sourceStage.OutputsEmitted,
                "the Source stage is 1:1: it must never report more emissions than receipts");

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

    [Fact]
    public void Render_prints_operator_counters_after_the_generic_counters()
        => BuildBatchStageSnapshot().Render().Should().Be(BatchStageExpectedRender);

    private static FlowSnapshot BuildTwoStageSnapshot()
    {
        var latestByKey = new StageSnapshot(
            Operator: "LatestByKey",
            InputsReceived: 100_000,
            OutputsEmitted: 81_588,
            InputsFailed: 0,
            InputsDropped: 0,
            InputsReplaced: 18_412,
            InputsFiltered: 0,
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
            InputsReceived: 81_588,
            OutputsEmitted: 81_563,
            InputsFailed: 25,
            InputsDropped: 0,
            InputsReplaced: 0,
            InputsFiltered: 0,
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
            InputsReceived: 10,
            OutputsEmitted: 10,
            InputsFailed: 0,
            InputsDropped: 0,
            InputsReplaced: 0,
            InputsFiltered: 0,
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

    private static FlowSnapshot BuildBatchStageSnapshot()
    {
        var batch = new StageSnapshot(
            Operator: "Batch",
            InputsReceived: 10,
            OutputsEmitted: 3,
            InputsFailed: 0,
            InputsDropped: 0,
            InputsReplaced: 0,
            InputsFiltered: 0,
            Queued: 0,
            QueueCapacity: 0,
            Active: 0,
            ConfiguredConcurrency: 1,
            QueueTimeSamples: 0,
            ProcessingTimeSamples: 0,
            AverageQueueTime: TimeSpan.Zero,
            AverageProcessingTime: TimeSpan.Zero,
            OperatorCounters: new Dictionary<string, long> { ["batch.items.included"] = 10 });

        return new FlowSnapshot("batch-demo", new[] { batch }, TimeSpan.FromSeconds(1));
    }

    [Fact]
    public void Aggregate_properties_combine_stages_with_sample_count_weighting()
    {
        var stageA = new StageSnapshot(
            Operator: "A",
            InputsReceived: 500,
            OutputsEmitted: 480,
            InputsFailed: 1,
            InputsDropped: 0,
            InputsReplaced: 0,
            InputsFiltered: 0,
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
            InputsReceived: 480,
            OutputsEmitted: 470,
            InputsFailed: 0,
            InputsDropped: 2,
            InputsReplaced: 1,
            InputsFiltered: 0,
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
            InputsReceived: 470,
            OutputsEmitted: 460,
            InputsFailed: 0,
            InputsDropped: 0,
            InputsReplaced: 0,
            InputsFiltered: 0,
            Queued: 0,
            QueueCapacity: 0,
            Active: 0,
            ConfiguredConcurrency: 1,
            QueueTimeSamples: 30,
            ProcessingTimeSamples: 0,
            AverageQueueTime: TimeSpan.FromMilliseconds(1),
            AverageProcessingTime: TimeSpan.Zero);

        var snapshot = new FlowSnapshot("aggregate-demo", new[] { stageA, stageB, stageC }, TimeSpan.FromSeconds(9));

        snapshot.InputsReceived.Should().Be(500);
        snapshot.OutputsEmitted.Should().Be(460);
        snapshot.InputsFailed.Should().Be(1);
        snapshot.InputsDropped.Should().Be(2);
        snapshot.InputsReplaced.Should().Be(1);
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

        snapshot.InputsReceived.Should().Be(0);
        snapshot.OutputsEmitted.Should().Be(0);
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
        snapshot.InputsReceived.Should().Be(100);
        snapshot.OutputsEmitted.Should().Be(100);
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
    public async Task PublishMetrics_exposes_live_emitted_counts_and_stops_after_disposal()
    {
        var options = new FlowOptions { Capacity = 16, Name = "diag-metrics", CaptureStatistics = true };

        var flow = Enumerable.Range(0, 100)
            .ToFlow(options)
            .SelectAsync((i, _) => Task.FromResult(i), concurrency: 4);

        var subscription = flow.PublishMetrics();
        try
        {
            long? emittedValue = null;
            string? emittedOperator = null;

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
                if (instrument.Name != "caudal.outputs.emitted")
                {
                    return;
                }

                foreach (var tag in tags)
                {
                    if (tag.Key == "operator" && Equals(tag.Value, "SelectAsync"))
                    {
                        emittedValue = measurement;
                        emittedOperator = (string?)tag.Value;
                    }
                }
            });
            listener.Start();

            await flow.ToListAsync().WaitAsync(TimeSpan.FromSeconds(10));

            listener.RecordObservableInstruments();

            emittedValue.Should().Be(100);
            emittedOperator.Should().Be("SelectAsync");
        }
        finally
        {
            var dispose = () => subscription.Dispose();
            dispose.Should().NotThrow();
        }
    }
}
