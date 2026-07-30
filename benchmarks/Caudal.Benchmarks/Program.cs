using BenchmarkDotNet.Running;
using Caudal.Benchmarks;

if (args.Contains("--conflation-report", StringComparer.OrdinalIgnoreCase))
{
    await ConflationReport.RunAsync().ConfigureAwait(false);
    return;
}

BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);

/// <summary>Anchor type for the benchmark switcher.</summary>
public partial class Program
{
}
