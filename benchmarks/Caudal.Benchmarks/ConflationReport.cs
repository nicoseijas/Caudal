using System.Globalization;

namespace Caudal.Benchmarks;

/// <summary>
/// Prints the LatestByKey scenario's conflation numbers directly. BenchmarkDotNet
/// discards benchmark return values, so <c>processedItems</c> — the number that
/// actually carries the conflation argument — is surfaced through this mode
/// instead: <c>dotnet run -c Release -- --conflation-report</c>. Timing here is
/// unmeasured and irrelevant; only the counts matter.
/// </summary>
internal static class ConflationReport
{
    public static async Task RunAsync()
    {
        var benchmarks = new LatestByKeyBenchmarks();

        Console.WriteLine("conflation report: 100,000 updates over 100 keys, slow consumer");
        Console.WriteLine();

        var conflated = await benchmarks.CaudalLatestByKey().ConfigureAwait(false);
        var unconflated = await benchmarks.CaudalNoConflation().ConfigureAwait(false);
        var manual = await benchmarks.ManualDictionaryConflation().ConfigureAwait(false);

        Print("CaudalLatestByKey", conflated, unconflated);
        Print("CaudalNoConflation (baseline)", unconflated, unconflated);
        Print("ManualDictionaryConflation", manual, unconflated);

        Console.WriteLine();
        Console.WriteLine("lower processedItems = more work avoided by conflation, for identical input.");
    }

    private static void Print(string name, long processed, long baseline)
    {
        var ratio = baseline == 0 ? 0 : (double)processed / baseline;
        Console.WriteLine(string.Create(
            CultureInfo.InvariantCulture,
            $"  {name,-32} processedItems: {processed,8:N0}   ({ratio,6:P1} of baseline)"));
    }
}
