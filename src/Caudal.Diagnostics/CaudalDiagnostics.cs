namespace Caudal;

/// <summary>Shared constants for Caudal's diagnostics.</summary>
public static class CaudalDiagnostics
{
    /// <summary>
    /// The name of the <see cref="System.Diagnostics.Metrics.Meter"/> that
    /// <see cref="FlowDiagnosticsExtensions.PublishMetrics{T}"/> registers instruments
    /// under. Listen for this name to consume Caudal's metrics, for example through
    /// an OpenTelemetry <c>MeterProvider</c>.
    /// </summary>
    public const string MeterName = "Caudal";
}
