# Caudal Roadmap

The priority is not operator count. It is demonstrating that Caudal solves four problems better than `SemaphoreSlim + Task.WhenAll + Channel<T>`:

1. Bounded concurrency.
2. Real backpressure.
3. Correct cancellation and shutdown.
4. Operational diagnostics.

The first public version must be small, hard to misuse, and useful enough for production. Caudal wins if it ships ten operators with impeccable semantics — not if it partially replicates Rx.

---

## Phase 0 — Define the contracts

Before implementing any operator, write down the decisions that govern the whole library.

The principles, architectural decisions, and the first public contract (`Flow<T>`, `Flow.From`, `FlowOptions`) live in [`docs/SEMANTICS.md`](docs/SEMANTICS.md).

**Exit criterion:** `docs/SEMANTICS.md` answers, unambiguously:

- what happens when a stage fills up;
- how errors propagate;
- how the pipeline is cancelled;
- how it completes;
- what guarantees ordering;
- what happens to in-flight items.

---

## Phase 1 — Minimal vertical slice

Build one complete pipeline: a bounded source, a single transforming operator, and a sink.

### Target API

```csharp
await source
    .ToFlow(capacity: 128)
    .SelectAsync(
        ProcessAsync,
        concurrency: 8)
    .ForEachAsync(
        SaveAsync,
        cancellationToken);
```

### Scope

| Component | Members |
|---|---|
| Sources | `IEnumerable<T>.ToFlow()`, `IAsyncEnumerable<T>.ToFlow()`, `ChannelReader<T>.ToFlow()` |
| Operators | `SelectAsync`, `WhereAsync` |
| Sinks | `ForEachAsync`, `ToListAsync`, `ConsumeAsync` |
| Options | `FlowOptions { Capacity = 128, Name }`, `SelectAsyncOptions { Concurrency = 1, PreserveOrder }` |

### Behaviors that must be proven by tests

- The producer blocks when the buffer is full.
- There are never more active work items than `Concurrency`.
- One exception terminates the pipeline, and the original exception reaches the consumer.
- Cancellation stops the producer, the workers, and the sink.
- No orphaned tasks remain.
- An infinite source does not accumulate memory.
- `PreserveOrder = true` delivers results in source order; `PreserveOrder = false` delivers by completion order.

**Exit criterion:** a demo processes one million items with stable memory and verifiable maximum concurrency.

---

## Phase 2 — Explicit error model

No retries yet. First, define what an error *is*.

### Failure modes

```csharp
public enum FlowFailureMode
{
    Stop,    // first error cancels the rest of the pipeline (default)
    Skip,    // the item fails, is reported, and the pipeline continues
    Capture  // the failure becomes data: Flow<FlowResult<T>>
}
```

`Capture` allows processing files or experiments without losing the successful results:

```csharp
var results = await files
    .ToFlow()
    .SelectResultAsync(ParseFileAsync, concurrency: 8)
    .ToListAsync(ct);
```

Deliberately excluded: global error callbacks, hard-to-interpret aggregate exceptions, continue-on-any-error as a default, static error events.

**Exit criterion:** tests that trigger simultaneous failures across multiple workers and prove there are no deadlocks, no lost exceptions, no tasks left running, and that the selected policy is respected.

---

## Phase 3 — Operators that justify the library

The operators that separate Caudal from a wrapper around `Parallel.ForEachAsync`.

### 3.1 `LatestByKey`

The most important operator for real-time applications.

```csharp
priceUpdates
    .ToFlow()
    .LatestByKey(x => x.Symbol, maximumKeys: 1_000)
    .SelectAsync(CalculateIndicators, concurrency: 8);
```

Semantics: at most one pending item per key; a new item replaces the pending one. The operator reports how many items were replaced.

This shipped as **two** operators, because one of them could not keep the contract originally written here. `LatestByKey` conflates until it *emits*: it is a separate stage from the selector, so it cannot know whether the previous value for a key is still being processed, and with a concurrent stage after it two values for one key can run at once. `SelectLatestByKeyAsync` owns the selector, which is what lets it guarantee "an executing item is not interrupted; when it finishes, the latest value received during its execution is processed next" — see [`docs/SEMANTICS.md`](docs/SEMANTICS.md).

### 3.2 `Batch`

```csharp
flow.Batch(maximumSize: 100, maximumDelay: TimeSpan.FromMilliseconds(50));
```

A batch is emitted on whichever comes first: maximum size reached, maximum delay elapsed, or source completion.

### 3.3 `Buffer`

```csharp
flow.Buffer(capacity: 256);
```

With explicit full-buffer policies: `Wait`, `DropNewest`, `DropOldest`, `Reject`.

### 3.4 `Merge`

```csharp
Flow.Merge(exchangeA, exchangeB, exchangeC);
```

### 3.5 `SelectManyAsync`

Turns one server, file, or symbol into several work items.

**Exit criterion:** two working demos — market data with `LatestByKey`, and file processing with bounded concurrency and partial results.

---

## Phase 4 — Time and rate control

Every time-based operator depends on `TimeProvider`, enabling instant, deterministic tests.

Operators: `Debounce`, `Throttle`, `Sample`, `BatchEvery`, `IdleTimeout`, `DelayEach`.

`Throttle`, `Debounce`, and `Sample` have ambiguous names across libraries, so the documentation must include marble-style timing diagrams:

```text
Input:     A--B-C-------D---E
Debounce:       C-------D---E
Sample:    ----C----C----D----
```

**Exit criterion:** every time-based test uses `FakeTimeProvider` — no real `Task.Delay`, no slow tests.

---

## Phase 5 — Resilience and rate limiting

Resilience is an integration with `Microsoft.Extensions.Resilience`, not another Polly reimplementation.

```csharp
flow.WithResiliencePipeline(resiliencePipeline);
flow.SelectAsync(ProcessAsync, resiliencePipeline, concurrency: 8);
```

Supported primitives: retry, timeout, circuit breaker, fallback. Rate limiting ships as `Caudal.RateLimiting` (or in Core, only if the dependency stays light):

```csharp
flow.RateLimit(permitLimit: 10, window: TimeSpan.FromSeconds(1));
flow.RateLimitBy(keySelector: order => order.Exchange, permitLimit: 10, window: TimeSpan.FromSeconds(1));
```

Decisions already made (see [`docs/SEMANTICS.md`](docs/SEMANTICS.md)):

- queue time and execution time are measured separately;
- `IdleTimeout` applies to execution, not to queue wait;
- a retry releases its worker slot during the delay;
- external cancellation is never classified as a transient error.

**Exit criterion:** a demo consuming a simulated API with request limits, transient errors, timeouts, a circuit breaker, and retry metrics.

---

## Phase 6 — Diagnostics

One of the strongest differentiators. Ships as `Caudal.Diagnostics`.

Per pipeline and per stage: `items.received`, `items.completed`, `items.failed`, `items.dropped`, `items.replaced`, `items.retried`, `queue.length`, `queue.capacity`, `workers.active`, `queue.duration`, `processing.duration`, `pipeline.duration`.

OpenTelemetry integration plus a local snapshot API:

```csharp
FlowSnapshot snapshot = pipeline.GetSnapshot();
```

And a textual pipeline view:

```text
market-data
├─ LatestByKey
│  queued: 32
│  replaced: 18,412
└─ SelectAsync
   active: 8/8
   completed: 243,812
   p95 processing: 14.3 ms
```

**Exit criterion:** a slow pipeline can be explained without adding manual logs to user code.

---

## Phase 7 — Deterministic testing

Ships as `Caudal.Testing`: a controlled source (`TestFlowSource<T>`), a controlled worker gate (`AsyncGate`), virtual time via `FakeTimeProvider`, and pipeline assertions:

```csharp
await flow.Should()
    .UseAtMostConcurrency(4)
    .PreserveOrder()
    .CompleteWithoutLeaks();
```

No full model checker in the first version — just utilities that control input, time, completion order, cancellation, failures, and buffer pressure.

**Exit criterion:** conditions that would normally be intermittent race conditions can be reproduced stably.

---

## Phase 8 — Honest benchmarks

Benchmarks measure the cost of the abstraction and when it pays off, not just "Caudal processes more items per second".

**Baselines:** sequential loop, `Parallel.ForEachAsync`, `SemaphoreSlim`, `Task.WhenAll`, manual `Channel<T>`, TPL Dataflow, Caudal ordered, Caudal unordered, Caudal with diagnostics on and off.

**Scenarios:** near-empty work (pure overhead), short I/O, variable-latency I/O, backpressure (producer much faster than consumer), infinite stream (stable memory over a long run), `LatestByKey` (100,000 updates across 100 symbols with a slow consumer).

**Metrics:** throughput, p50/p95/p99 latency, allocations, peak memory, peak task count, cancellation time, shutdown time, items lost or replaced.

**Editorial rule:** the documentation includes a "when Caudal loses" section — small sequential CPU work, small materialized collections, pipelines that need no backpressure, cases where `Parallel.ForEachAsync` is enough.

---

## Phase 9 — First public release

Packages: `Caudal.Core`, `Caudal.Diagnostics`, `Caudal.Resilience`, `Caudal.Testing`. No further splitting until there are real users.

### Repository layout

```text
Caudal/
├─ src/
│  ├─ Caudal.Core/
│  ├─ Caudal.Diagnostics/
│  ├─ Caudal.Resilience/
│  └─ Caudal.Testing/
├─ tests/
│  ├─ Caudal.Core.Tests/
│  ├─ Caudal.StressTests/
│  └─ Caudal.Testing.Tests/
├─ benchmarks/
│  └─ Caudal.Benchmarks/
├─ samples/
│  ├─ MarketData/
│  ├─ FileProcessing/
│  └─ ApiRateLimiting/
├─ docs/
│  ├─ SEMANTICS.md
│  ├─ backpressure.md
│  ├─ cancellation.md
│  ├─ ordering.md
│  └─ when-not-to-use.md
└─ README.md
```

### Version scope

| Version | API |
|---|---|
| `0.1` | `ToFlow`, `SelectAsync`, `WhereAsync`, `SelectManyAsync`, `SelectLatestByKeyAsync`, `ForEachAsync`, `ToListAsync`, `Buffer`, `Batch`, `Merge`, `LatestByKey` |
| `0.2` | `Debounce`, `Throttle`, `Sample`, `IdleTimeout`, `RateLimit`, `RateLimitBy`, `WithResiliencePipeline` |
| `0.3` | OpenTelemetry, snapshots, testing utilities, stress testing |

### Conditions for `1.0`

`1.0` does not ship until:

- cancellation semantics are frozen;
- the error model is validated;
- stress tests show zero orphaned tasks;
- the API has been used in at least two real applications;
- compatibility with ASP.NET Core and WPF is proven;
- diagnostics have controlled cardinality;
- a compatibility policy is published.

---

## Pilot applications

Caudal is not developed in isolation. Two pilots exercise it from the start.

### Pilot A — Market data

```csharp
priceUpdates
    .ToFlow(capacity: 1_024)
    .LatestByKey(x => x.Symbol, maximumKeys: 1_000)
    .SelectAsync(CalculateIndicatorsAsync, concurrency: 8)
    .Batch(maximumSize: 100, maximumDelay: TimeSpan.FromMilliseconds(50))
    .ForEachAsync(UpdateDashboardAsync, ct);
```

Validates: stale data, per-key replacement, batching, high frequency, UI integration, metrics.

### Pilot B — File processing

```csharp
files
    .ToFlow(capacity: 64)
    .SelectResultAsync(ParseAsync, concurrency: 8)
    .ForEachAsync(StoreResultAsync, ct);
```

Validates: partial errors, progress, ordering, cancellation, shutdown, finite work, memory pressure.

Together these two cases cover almost every important decision without touching distributed infrastructure.

---

## Build order

1. Write `docs/SEMANTICS.md`.
2. Implement `ToFlow → SelectAsync → ForEachAsync`.
3. Write concurrency, cancellation, and backpressure tests.
4. Add ordered and unordered processing.
5. Implement the error model.
6. Build `LatestByKey`.
7. Integrate it into a market-data demo.
8. Add `Batch`, `Buffer`, and `Merge`.
9. Adopt `TimeProvider` and the time-based operators.
10. Integrate resilience and rate limiting.
11. Add metrics and snapshots.
12. Build the testing utilities.
13. Run long-duration stress tests.
14. Publish `0.1-preview`.
15. Use it in two real applications before widening the API.
