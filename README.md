<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/assets/banner-dark.svg">
    <img src="docs/assets/banner-light.svg" alt="Caudal — bounded, observable async pipelines for .NET" width="560">
  </picture>
</p>

<p align="center">
  <a href="https://github.com/nicoseijas/Caudal/actions/workflows/ci.yml"><img src="https://github.com/nicoseijas/Caudal/actions/workflows/ci.yml/badge.svg" alt="ci"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="MIT license"></a>
</p>

> *Caudal* (Spanish): the volume of water flowing through a channel per unit of time.

Caudal is a .NET library for building bounded, cancellable, observable async pipelines. It is aimed at the code most services end up writing by hand — a `SemaphoreSlim` to limit concurrency, `Task.WhenAll` to fan out, a `Channel<T>` to connect stages — and at the four problems that code keeps getting wrong:

1. **Bounded concurrency** — never more in-flight work than you asked for.
2. **Real backpressure** — a fast producer blocks instead of filling memory.
3. **Correct cancellation and shutdown** — no orphaned tasks, no swallowed exceptions.
4. **Operational diagnostics** — you can explain a slow pipeline without adding logs to your own code.

> **Status: pre-release.** The full 0.1–0.3 API surface from the roadmap is implemented and tested (146 tests, all packages build warning-clean), but nothing is published to NuGet yet and the contracts in [`docs/SEMANTICS.md`](docs/SEMANTICS.md) stay open to change until `1.0`. The build order and exit criteria are in [`ROADMAP.md`](ROADMAP.md).

## What it looks like

```csharp
await source
    .ToFlow(capacity: 128)          // bounded buffer: producer waits when full
    .SelectAsync(
        ProcessAsync,
        concurrency: 8)             // at most 8 concurrent invocations, always explicit
    .ForEachAsync(
        SaveAsync,
        cancellationToken);         // one token stops producer, workers, and sink
```

Pipelines are built on `IAsyncEnumerable<T>` at the edges and `Channel<T>` internally. `Channel<T>` is an implementation detail and is not part of the public API.

The operator that best shows why Caudal exists is `LatestByKey`, for real-time feeds where stale items should be replaced rather than queued:

```csharp
priceUpdates
    .ToFlow(capacity: 1_024)
    .LatestByKey(x => x.Symbol, maximumKeys: 1_000)  // at most one pending item per key, bounded to 1,000 keys; newer replaces older
    .SelectAsync(CalculateIndicatorsAsync, concurrency: 8)
    .Batch(maximumSize: 100, maximumDelay: TimeSpan.FromMilliseconds(50))
    .ForEachAsync(UpdateDashboardAsync, ct);
```

## Design principles

The full contract is in [`docs/SEMANTICS.md`](docs/SEMANTICS.md). The short version:

- No buffer is unbounded by default.
- Every operation accepts a `CancellationToken`.
- Concurrency is always explicit — there is no default parallelism.
- Ordering is never preserved by accident; you opt in with `PreserveOrder`.
- An exception cannot disappear silently. Error handling is a per-stage policy: `Stop`, `Skip`, or `Capture` (failures become `FlowResult<T>` values).
- Completing a pipeline means every internal task has finished or been cancelled.
- Telemetry never changes semantics.
- Each operator documents its behavior under saturation.

Time-based operators (`Debounce`, `Throttle`, `Sample`, `IdleTimeout`) depend on `TimeProvider`, so they are testable with a fake clock and no real delays. Resilience is an integration with `Microsoft.Extensions.Resilience`, not a reimplementation of Polly.

## When not to use Caudal

Caudal is deliberately small: the goal is around ten operators with precise semantics, not a partial Rx. It is the wrong tool when:

- the work is small, sequential, CPU-bound loops — a plain loop wins;
- the collection is small and already materialized — `Task.WhenAll` is fine;
- you need concurrency but no backpressure, batching, or per-key semantics — `Parallel.ForEachAsync` is enough;
- you need a full reactive event system — use Rx.

The benchmark suite under [`benchmarks/Caudal.Benchmarks`](benchmarks/Caudal.Benchmarks) measures the cost of the abstraction against all of these; see [`docs/when-not-to-use.md`](docs/when-not-to-use.md) for the full reasoning and [`docs/benchmarks.md`](docs/benchmarks.md) for how to run the suite and read its results honestly.

## Packages

| Package | Contents |
|---|---|
| `Caudal.Core` | Sources, operators, sinks, error model |
| `Caudal.Diagnostics` | OpenTelemetry metrics, `FlowSnapshot`, pipeline visualization |
| `Caudal.Resilience` | Polly v8 / `Microsoft.Extensions.Resilience` integration |
| `Caudal.RateLimiting` | `RateLimit` / `RateLimitBy` over `System.Threading.RateLimiting` |
| `Caudal.Testing` | Controlled sources, `AsyncGate`, virtual time, pipeline assertions |

## Documentation

- [`docs/SEMANTICS.md`](docs/SEMANTICS.md) — the behavioral contract: backpressure, errors, cancellation, completion, ordering.
- [`ROADMAP.md`](ROADMAP.md) — phases, exit criteria, and what ships in each version.

## License

[MIT](LICENSE)
