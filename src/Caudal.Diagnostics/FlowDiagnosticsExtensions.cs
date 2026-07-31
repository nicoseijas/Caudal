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
            "caudal.items.received", () => MeasureLong(nodes, pipelineName, static s => s.Received));
        meter.CreateObservableCounter(
            "caudal.items.completed", () => MeasureLong(nodes, pipelineName, static s => s.Completed));
        meter.CreateObservableCounter(
            "caudal.items.failed", () => MeasureLong(nodes, pipelineName, static s => s.Failed));
        meter.CreateObservableCounter(
            "caudal.items.dropped", () => MeasureLong(nodes, pipelineName, static s => s.Dropped));
        meter.CreateObservableCounter(
            "caudal.items.replaced", () => MeasureLong(nodes, pipelineName, static s => s.Replaced));

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
            stats.Received,
            stats.Completed,
            stats.Failed,
            stats.Dropped,
            stats.Replaced,
            stats.QueueLength,
            stats.QueueCapacity,
            stats.Active,
            stats.ConfiguredConcurrency,
            stats.QueueTimeSampleCount,
            stats.ProcessingTimeSampleCount,
            stats.AverageQueueTime,
            stats.AverageProcessingTime);
    }

    private static IEnumerable<string> DetailLines(StageSnapshot stage)
    {
        yield return string.Create(CultureInfo.InvariantCulture, $"received: {stage.Received:N0}");
        yield return string.Create(CultureInfo.InvariantCulture, $"completed: {stage.Completed:N0}");

        if (stage.Failed > 0)
        {
            yield return string.Create(CultureInfo.InvariantCulture, $"failed: {stage.Failed:N0}");
        }

        if (stage.Dropped > 0)
        {
            yield return string.Create(CultureInfo.InvariantCulture, $"dropped: {stage.Dropped:N0}");
        }

        if (stage.Replaced > 0)
        {
            yield return string.Create(CultureInfo.InvariantCulture, $"replaced: {stage.Replaced:N0}");
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
