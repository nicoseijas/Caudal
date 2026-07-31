using System.Diagnostics.Metrics;
using System.Globalization;
using System.Text;
using Caudal.Internal;

namespace Caudal;

/// <summary>
/// Extensions that expose a Caudal flow's internal statistics: point-in-time
/// snapshots, a textual pipeline view, and live metrics published through
/// <see cref="System.Diagnostics.Metrics"/>.
/// </summary>
public static class FlowDiagnosticsExtensions
{
    /// <summary>
    /// Takes a point-in-time snapshot of every stage in <paramref name="flow"/>, ordered
    /// from its source to its terminal stage.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// <see cref="FlowOptions.CaptureStatistics"/> was not set to <see langword="true"/>
    /// when the flow was built.
    /// </exception>
    public static FlowSnapshot GetSnapshot<T>(this Flow<T> flow)
    {
        ArgumentNullException.ThrowIfNull(flow);

        var nodes = WalkChain(flow);
        var stages = new StageSnapshot[nodes.Count];
        for (var i = 0; i < nodes.Count; i++)
        {
            stages[i] = ToStageSnapshot(nodes[i]);
        }

        var pipelineDuration = nodes.Count == 0 ? TimeSpan.Zero : nodes[0].Stats!.Elapsed;
        return new FlowSnapshot(flow.Name, stages, pipelineDuration);
    }

    /// <summary>
    /// Renders a snapshot as the textual pipeline view described in the project
    /// roadmap: a header line with the pipeline's name, then one branch per stage
    /// with its non-zero counters and timings indented beneath it.
    /// </summary>
    public static string Render(this FlowSnapshot snapshot)
    {
        ArgumentNullException.ThrowIfNull(snapshot);

        var builder = new StringBuilder();
        builder.Append(snapshot.Name ?? "(unnamed)");

        for (var i = 0; i < snapshot.Stages.Count; i++)
        {
            var stage = snapshot.Stages[i];
            var isLast = i == snapshot.Stages.Count - 1;
            var branch = isLast ? "└─ " : "├─ ";
            var continuation = isLast ? "   " : "│  ";

            builder.Append('\n').Append(branch).Append(stage.Operator);

            foreach (var line in DetailLines(stage))
            {
                builder.Append('\n').Append(continuation).Append(line);
            }
        }

        return builder.ToString();
    }

    /// <summary>
    /// Publishes <paramref name="flow"/>'s statistics as observable metrics on a
    /// <see cref="Meter"/> named <see cref="CaudalDiagnostics.MeterName"/>. Every
    /// instrument reads the live, underlying <c>StageStats</c> at scrape time rather
    /// than polling on a timer, so publishing has no cost beyond a listener's own
    /// collection interval. Dispose the returned value to stop publishing.
    /// <see cref="StageSnapshot.OperatorCounters"/> (for example
    /// <c>batch.items.included</c>) is snapshot-only for now — it is not published
    /// as a metric instrument by this method.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// <see cref="FlowOptions.CaptureStatistics"/> was not set to <see langword="true"/>
    /// when the flow was built.
    /// </exception>
    public static IDisposable PublishMetrics<T>(this Flow<T> flow)
    {
        ArgumentNullException.ThrowIfNull(flow);

        var nodes = WalkChain(flow);
        var pipelineName = flow.Name ?? "unnamed";
        var meter = new Meter(CaudalDiagnostics.MeterName);

        meter.CreateObservableCounter(
            "caudal.inputs.received", () => MeasureLong(nodes, pipelineName, static s => s.InputsReceived));
        meter.CreateObservableCounter(
            "caudal.outputs.emitted", () => MeasureLong(nodes, pipelineName, static s => s.OutputsEmitted));
        meter.CreateObservableCounter(
            "caudal.inputs.failed", () => MeasureLong(nodes, pipelineName, static s => s.InputsFailed));
        meter.CreateObservableCounter(
            "caudal.inputs.dropped", () => MeasureLong(nodes, pipelineName, static s => s.InputsDropped));
        meter.CreateObservableCounter(
            "caudal.inputs.replaced", () => MeasureLong(nodes, pipelineName, static s => s.InputsReplaced));
        meter.CreateObservableCounter(
            "caudal.inputs.filtered", () => MeasureLong(nodes, pipelineName, static s => s.InputsFiltered));

        meter.CreateObservableGauge(
            "caudal.queue.length", () => MeasureInt(nodes, pipelineName, static s => s.QueueLength));
        meter.CreateObservableGauge(
            "caudal.queue.capacity", () => MeasureInt(nodes, pipelineName, static s => s.QueueCapacity));
        meter.CreateObservableGauge(
            "caudal.workers.active", () => MeasureInt(nodes, pipelineName, static s => s.Active));

        meter.CreateObservableGauge(
            "caudal.queue.duration.avg",
            () => MeasureDouble(nodes, pipelineName, static s => s.AverageQueueTime.TotalMilliseconds),
            unit: "ms");
        meter.CreateObservableGauge(
            "caudal.processing.duration.avg",
            () => MeasureDouble(nodes, pipelineName, static s => s.AverageProcessingTime.TotalMilliseconds),
            unit: "ms");

        meter.CreateObservableGauge(
            "caudal.pipeline.duration",
            () => MeasurePipelineDuration(nodes, pipelineName),
            unit: "s");

        return meter;
    }

    private static List<FlowNode> WalkChain<T>(Flow<T> flow)
    {
        ArgumentNullException.ThrowIfNull(flow);

        var chain = new List<FlowNode>();
        for (FlowNode? current = flow.Node; current is not null; current = current.UpstreamNode)
        {
            chain.Add(current);
        }

        chain.Reverse();

        if (chain.Exists(static node => node.Stats is null))
        {
            throw new InvalidOperationException(
                "Statistics are not enabled for this flow. Set FlowOptions.CaptureStatistics = true when building it.");
        }

        return chain;
    }

    private static StageSnapshot ToStageSnapshot(FlowNode node)
    {
        var stats = node.Stats!;
        return new StageSnapshot(
            stats.OperatorName,
            stats.InputsReceived,
            stats.OutputsEmitted,
            stats.InputsFailed,
            stats.InputsDropped,
            stats.InputsReplaced,
            stats.InputsFiltered,
            stats.QueueLength,
            stats.QueueCapacity,
            stats.Active,
            stats.ConfiguredConcurrency,
            stats.QueueTimeSampleCount,
            stats.ProcessingTimeSampleCount,
            stats.AverageQueueTime,
            stats.AverageProcessingTime,
            BuildOperatorCounters(stats));
    }

    private static IReadOnlyDictionary<string, long> BuildOperatorCounters(StageStats stats)
    {
        if (stats.BatchItemsIncluded <= 0)
        {
            return EmptyOperatorCounters;
        }

        return new Dictionary<string, long>
        {
            ["batch.items.included"] = stats.BatchItemsIncluded,
        };
    }

    private static readonly IReadOnlyDictionary<string, long> EmptyOperatorCounters = new Dictionary<string, long>();

    private static IEnumerable<string> DetailLines(StageSnapshot stage)
    {
        yield return string.Create(CultureInfo.InvariantCulture, $"inputs: {stage.InputsReceived:N0}");
        yield return string.Create(CultureInfo.InvariantCulture, $"outputs: {stage.OutputsEmitted:N0}");

        if (stage.InputsFailed > 0)
        {
            yield return string.Create(CultureInfo.InvariantCulture, $"failed: {stage.InputsFailed:N0}");
        }

        if (stage.InputsDropped > 0)
        {
            yield return string.Create(CultureInfo.InvariantCulture, $"dropped: {stage.InputsDropped:N0}");
        }

        if (stage.InputsReplaced > 0)
        {
            yield return string.Create(CultureInfo.InvariantCulture, $"replaced: {stage.InputsReplaced:N0}");
        }

        if (stage.InputsFiltered > 0)
        {
            yield return string.Create(CultureInfo.InvariantCulture, $"filtered: {stage.InputsFiltered:N0}");
        }

        foreach (var (key, value) in stage.OperatorCounters)
        {
            yield return string.Create(CultureInfo.InvariantCulture, $"{key}: {value:N0}");
        }

        if (stage.QueueCapacity > 0)
        {
            yield return string.Create(
                CultureInfo.InvariantCulture, $"queued: {stage.Queued:N0}/{stage.QueueCapacity:N0}");
        }

        if (stage.ConfiguredConcurrency > 1)
        {
            yield return string.Create(
                CultureInfo.InvariantCulture, $"active: {stage.Active}/{stage.ConfiguredConcurrency}");
        }

        if (stage.QueueTimeSamples > 0)
        {
            yield return string.Create(
                CultureInfo.InvariantCulture, $"avg queue: {stage.AverageQueueTime.TotalMilliseconds:0.0} ms");
        }

        if (stage.ProcessingTimeSamples > 0)
        {
            yield return string.Create(
                CultureInfo.InvariantCulture, $"avg processing: {stage.AverageProcessingTime.TotalMilliseconds:0.0} ms");
        }
    }

    private static IEnumerable<Measurement<long>> MeasureLong(
        List<FlowNode> nodes, string pipelineName, Func<StageStats, long> selector)
    {
        for (var i = 0; i < nodes.Count; i++)
        {
            var stats = nodes[i].Stats!;
            yield return new Measurement<long>(
                selector(stats),
                new KeyValuePair<string, object?>("pipeline", pipelineName),
                new KeyValuePair<string, object?>("operator", stats.OperatorName),
                new KeyValuePair<string, object?>("stage", i));
        }
    }

    private static IEnumerable<Measurement<int>> MeasureInt(
        List<FlowNode> nodes, string pipelineName, Func<StageStats, int> selector)
    {
        for (var i = 0; i < nodes.Count; i++)
        {
            var stats = nodes[i].Stats!;
            yield return new Measurement<int>(
                selector(stats),
                new KeyValuePair<string, object?>("pipeline", pipelineName),
                new KeyValuePair<string, object?>("operator", stats.OperatorName),
                new KeyValuePair<string, object?>("stage", i));
        }
    }

    private static IEnumerable<Measurement<double>> MeasureDouble(
        List<FlowNode> nodes, string pipelineName, Func<StageStats, double> selector)
    {
        for (var i = 0; i < nodes.Count; i++)
        {
            var stats = nodes[i].Stats!;
            yield return new Measurement<double>(
                selector(stats),
                new KeyValuePair<string, object?>("pipeline", pipelineName),
                new KeyValuePair<string, object?>("operator", stats.OperatorName),
                new KeyValuePair<string, object?>("stage", i));
        }
    }

    private static IEnumerable<Measurement<double>> MeasurePipelineDuration(
        List<FlowNode> nodes, string pipelineName)
    {
        if (nodes.Count == 0)
        {
            yield break;
        }

        yield return new Measurement<double>(
            nodes[0].Stats!.Elapsed.TotalSeconds,
            new KeyValuePair<string, object?>("pipeline", pipelineName));
    }
}
