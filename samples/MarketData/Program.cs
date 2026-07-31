// Pilot A — market data. A producer emits price updates far faster than the
// pipeline can process them; LatestByKey conflates per symbol so the dashboard
// always works on fresh prices, and Batch coalesces dashboard updates.

using System.Runtime.CompilerServices;
using Caudal;

var symbols = Enumerable.Range(0, 100).Select(i => $"SYM{i:D3}").ToArray();
long produced = 0;
long processed = 0;
long batches = 0;

using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    cts.Cancel();
};

Console.WriteLine("market-data: 100 symbols, 10 seconds, indicator calculation ~2 ms, concurrency 8");

var flowOptions = new FlowOptions
{
    Capacity = 1_024,
    Name = "market-data",
    CaptureStatistics = true,
};

// Held in a variable, before the sink is attached, so it can be snapshotted
// while the pipeline is running.
var flow = GeneratePriceUpdates(symbols, cts.Token)
    .ToFlow(flowOptions)
    .LatestByKey(update => update.Symbol, maximumKeys: 128)
    .SelectAsync(CalculateIndicatorsAsync, concurrency: 8)
    .Batch(maximumSize: 100, maximumDelay: TimeSpan.FromMilliseconds(50));

var sinkTask = flow.ForEachAsync(UpdateDashboardAsync, cts.Token);

while (!sinkTask.IsCompleted)
{
    await Task.Delay(2_500, CancellationToken.None);

    if (!sinkTask.IsCompleted)
    {
        Console.WriteLine(flow.GetSnapshot().Render());
    }
}

try
{
    await sinkTask;
}
catch (OperationCanceledException)
{
    // The 10-second run (or Ctrl+C) ended the pipeline; this is the normal exit.
}

Console.WriteLine();
Console.WriteLine("final snapshot:");
Console.WriteLine(flow.GetSnapshot().Render());

Console.WriteLine();
Console.WriteLine($"produced:  {Interlocked.Read(ref produced):N0} price updates");
Console.WriteLine($"processed: {Interlocked.Read(ref processed):N0} indicator calculations in {Interlocked.Read(ref batches):N0} dashboard batches");
Console.WriteLine($"conflated: {Interlocked.Read(ref produced) - Interlocked.Read(ref processed):N0} stale updates were replaced before processing");

async IAsyncEnumerable<PriceUpdate> GeneratePriceUpdates(
    IReadOnlyList<string> allSymbols,
    [EnumeratorCancellation] CancellationToken cancellationToken = default)
{
    var random = new Random();
    var prices = allSymbols.ToDictionary(s => s, _ => 100.0);

    while (!cancellationToken.IsCancellationRequested)
    {
        var symbol = allSymbols[random.Next(allSymbols.Count)];
        prices[symbol] *= 1 + ((random.NextDouble() - 0.5) * 0.001);
        Interlocked.Increment(ref produced);
        yield return new PriceUpdate(symbol, prices[symbol], DateTime.UtcNow);

        if (Interlocked.Read(ref produced) % 64 == 0)
        {
            await Task.Yield();
        }
    }
}

async Task<Indicators> CalculateIndicatorsAsync(PriceUpdate update, CancellationToken cancellationToken)
{
    await Task.Delay(2, cancellationToken);
    Interlocked.Increment(ref processed);
    return new Indicators(update.Symbol, update.Price, MovingAverage: update.Price * 0.98);
}

async Task UpdateDashboardAsync(IReadOnlyList<Indicators> batch, CancellationToken cancellationToken)
{
    await Task.Delay(1, cancellationToken);
    var count = Interlocked.Increment(ref batches);
    if (count % 20 == 0)
    {
        Console.WriteLine(
            $"  [{count,4}] batch of {batch.Count,3} | produced {Interlocked.Read(ref produced),9:N0} | processed {Interlocked.Read(ref processed),7:N0}");
    }
}

internal sealed record PriceUpdate(string Symbol, double Price, DateTime Timestamp);

internal sealed record Indicators(string Symbol, double Price, double MovingAverage);
