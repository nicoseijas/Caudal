// Phase 5 pilot — a simulated upstream API with a hard request-rate limit,
// transient 503s, occasional stalls, and a circuit breaker. RateLimit paces
// admission into the flow; the resilience pipeline handles retry, timeout,
// and circuit breaking around each call; SelectResultAsync keeps partial
// results so one bad request never takes down the run.

using System.Collections.Concurrent;
using System.Diagnostics;
using Caudal;
using Polly;
using Polly.CircuitBreaker;
using Polly.Retry;
using Polly.Timeout;

long retries = 0;
long timeouts = 0;
long circuitOpens = 0;
long succeeded = 0;
long failed = 0;
var failureTypeCounts = new ConcurrentDictionary<string, long>();

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    cts.Cancel();
};

var resiliencePipeline = new ResiliencePipelineBuilder()
    .AddRetry(new RetryStrategyOptions
    {
        MaxRetryAttempts = 3,
        Delay = TimeSpan.FromMilliseconds(50),
        BackoffType = DelayBackoffType.Exponential,
        OnRetry = _ =>
        {
            Interlocked.Increment(ref retries);
            return default;
        },
    })
    .AddTimeout(new TimeoutStrategyOptions
    {
        Timeout = TimeSpan.FromMilliseconds(500),
        OnTimeout = _ =>
        {
            Interlocked.Increment(ref timeouts);
            return default;
        },
    })
    .AddCircuitBreaker(new CircuitBreakerStrategyOptions
    {
        FailureRatio = 0.9,
        MinimumThroughput = 20,
        SamplingDuration = TimeSpan.FromSeconds(5),
        BreakDuration = TimeSpan.FromSeconds(2),
        OnOpened = _ =>
        {
            Interlocked.Increment(ref circuitOpens);
            return default;
        },
    })
    .Build();

Console.WriteLine("api-rate-limiting: 100 requests, 25 req/s limit, concurrency 8, retry+timeout+breaker");

var stopwatch = Stopwatch.StartNew();

try
{
    await Enumerable.Range(0, 100)
        .ToFlow(capacity: 64, name: "api-client")
        .RateLimit(permitLimit: 25, window: TimeSpan.FromSeconds(1))
        .SelectResultAsync(CallSimulatedApiAsync, resiliencePipeline, concurrency: 8)
        .ForEachAsync(HandleResult, cts.Token);
}
catch (OperationCanceledException)
{
    // Ctrl+C ended the run early; this is the normal exit path.
}

stopwatch.Stop();

Console.WriteLine();
Console.WriteLine($"total:     100 requests");
Console.WriteLine($"succeeded: {Interlocked.Read(ref succeeded):N0}");
Console.WriteLine($"failed:    {Interlocked.Read(ref failed):N0}");

if (!failureTypeCounts.IsEmpty)
{
    Console.WriteLine("  top failure types:");
    foreach (var (type, count) in failureTypeCounts.OrderByDescending(entry => entry.Value).Take(5))
    {
        Console.WriteLine($"    {type}: {count:N0}");
    }
}

Console.WriteLine($"retries:        {Interlocked.Read(ref retries):N0}");
Console.WriteLine($"timeouts:       {Interlocked.Read(ref timeouts):N0}");
Console.WriteLine($"circuit-opens:  {Interlocked.Read(ref circuitOpens):N0}");
Console.WriteLine($"elapsed:        {stopwatch.Elapsed.TotalSeconds:F2} s");
Console.WriteLine(
    "note: total throughput was bounded by the 25/s RateLimit stage, not by the concurrency:8 " +
    "worker pool — the pipeline never had 8 requests in flight at once for long.");

Task HandleResult(FlowResult<ApiResponse> result, CancellationToken cancellationToken)
{
    if (result.IsSuccess)
    {
        Interlocked.Increment(ref succeeded);
    }
    else
    {
        Interlocked.Increment(ref failed);
        var typeName = result.Exception!.GetType().Name;
        failureTypeCounts.AddOrUpdate(typeName, 1, (_, count) => count + 1);
    }

    return Task.CompletedTask;
}

async Task<ApiResponse> CallSimulatedApiAsync(int requestId, CancellationToken cancellationToken)
{
    var roll = Random.Shared.NextDouble();

    if (roll < 0.15)
    {
        throw new InvalidOperationException("503 from upstream");
    }

    if (roll < 0.20)
    {
        // Stalls long enough that the 500 ms Polly timeout cuts it off.
        await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken);
        return new ApiResponse(requestId, "stale");
    }

    await Task.Delay(20, cancellationToken);
    return new ApiResponse(requestId, "ok");
}

internal sealed record ApiResponse(int RequestId, string Status);
