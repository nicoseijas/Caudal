# Caudal — Semantics and Contracts

This document defines the behavioral contract of Caudal before any operator is implemented. Every operator, option, and package must conform to it. If an implementation and this document disagree, one of them is a bug — and it is usually the implementation.

Status: draft. These contracts freeze at `1.0`; until then, changes are allowed but must be recorded here first.

## Principles

1. **No buffer is unbounded by default.** Every stage has a finite capacity, and the default (`128`) is deliberately small.
2. **Every operation accepts a `CancellationToken`.**
3. **Concurrency is always explicit.** No operator parallelizes unless the caller passes a concurrency value. The default is `1`.
4. **Ordering is never preserved accidentally.** Ordered delivery is an explicit, paid-for option (`PreserveOrder = true`), so an implementation change can never silently remove an ordering guarantee users depended on.
5. **An exception cannot disappear silently.** Every failure either terminates the pipeline, is reported, or becomes data — per the stage's failure mode.
6. **A pipeline has a single lifecycle.** One start, one terminal state, one owner.
7. **Completing means waiting for or cancelling all internal work.** When the terminal awaitable finishes, no pipeline task is still running.
8. **Retries must not hold capacity unnecessarily.** The goal is to release a worker slot while a retry waits out its delay; see *Time, queues, and retries* for the 0.x status of this principle.
9. **Telemetry never changes semantics.** Enabling or disabling diagnostics must not alter ordering, timing contracts, or error behavior — only cost.
10. **Every operator documents its behavior under saturation.** "What happens when this stage is full" is part of each operator's public contract, not an implementation detail.

## Architectural decisions

- **`IAsyncEnumerable<T>`** for sources and results. It is the .NET-native representation of an async sequence and composes with `await foreach` and cancellation.
- **`Channel<T>`** as the internal stage mechanism. It is not exposed publicly in the MVP: it is an implementation, not the conceptual model of the API.
- **`ValueTask`** where measurement justifies it, not by default.
- **`TimeProvider`** for all time-based behavior, so tests run against a fake clock with no real delays.
- **`System.Diagnostics.Metrics`** and **`ActivitySource`** for telemetry, consumable through OpenTelemetry.
- **`Microsoft.Extensions.Resilience`** for retry, timeout, circuit breaker, and fallback. Caudal integrates resilience; it does not reimplement Polly.

## First public contract

```csharp
public interface IFlow<out T>
{
    string? Name { get; }
}

public static class Flow
{
    public static IFlow<T> From<T>(
        IAsyncEnumerable<T> source,
        FlowOptions? options = null);
}
```

```csharp
public sealed record FlowOptions
{
    public int Capacity { get; init; } = 128;
    public string? Name { get; init; }
}

public sealed record SelectAsyncOptions
{
    public int Concurrency { get; init; } = 1;
    public bool PreserveOrder { get; init; }
}
```

---

## The six questions

### 1. What happens when a stage fills up?

The upstream producer **waits**. Backpressure propagates stage by stage back to the source: a full sink slows the workers, full workers slow the buffer, a full buffer suspends the source enumerator. A pipeline reading an infinite source therefore runs at the speed of its slowest stage with bounded memory.

`Wait` is the only default. Dropping is always an explicit choice, made through `Buffer` with one of four policies:

```csharp
public enum BufferFullMode
{
    Wait,       // default: producer suspends until space frees up
    DropNewest, // the incoming item is discarded
    DropOldest, // the oldest buffered item is discarded to make room
    Reject      // the write fails visibly
}
```

Dropped items are counted (`items.dropped`) — a drop is never silent. `LatestByKey` is the other sanctioned form of shedding: replacement per key, counted as `items.replaced`.

### 2. How do errors propagate?

Per stage, governed by an explicit failure mode:

```csharp
public enum FlowFailureMode
{
    Stop,    // default
    Skip,
    Capture
}
```

- **`Stop`** — the first exception cancels the rest of the pipeline: the source stops being read, in-flight work is cancelled, and the **original exception** (not a wrapper, not an aggregate) is rethrown at the terminal awaitable. If multiple workers fail concurrently, the first observed failure wins; the others are cancelled and their exceptions are discarded once the primary failure is chosen. When every failure must be retained, `Capture` is the mode for that.
- **`Skip`** — the failed item is discarded, the failure is reported (counted as `items.failed`, visible to diagnostics), and the pipeline continues. Skip is never the default.
- **`Capture`** — the failure becomes data. The stage produces `IFlow<FlowResult<T>>`:

```csharp
public readonly record struct FlowResult<T>(
    T? Value,
    Exception? Exception,
    bool IsSuccess);
```

This is the mode for batch work where successful results must survive individual failures (file processing, experiments).

Deliberately excluded: global error callbacks, static error events, aggregate exceptions as the primary error surface, and continue-on-any-error as a default.

`OperationCanceledException` caused by the pipeline's own token is cancellation, not failure. It does not count as `items.failed` and is never retried. The classification is strict: the exception must carry the pipeline's token *and* that token must actually be cancelled. An `OperationCanceledException` that merely forwards the token while nothing was cancelled — or that comes from user code's internal timeout — is an ordinary failure.

### 3. How is the pipeline cancelled?

One token, passed at the terminal operation, governs the whole pipeline:

- the source enumerator is disposed;
- pending buffered items are abandoned;
- in-flight worker invocations receive the cancellation through the token they were given;
- the sink stops.

Cancellation is cooperative but bounded in intent: after the terminal awaitable completes (by throwing `OperationCanceledException`), no pipeline task is still running. "No orphaned tasks" is a tested invariant, not an aspiration.

External cancellation is never classified as a transient error: it must not trigger retries or trip a circuit breaker.

### 4. How does the pipeline complete?

Completion flows forward. When the source ends:

- each stage finishes processing every item it has already accepted (buffered and in-flight);
- `Batch` emits its final partial batch;
- the completion signal reaches the sink only after all upstream work has drained;
- the terminal awaitable then completes.

There is exactly one terminal state per run: completed, faulted (with the original exception), or cancelled. Awaiting the terminal operation is sufficient to know that all internal work is finished — there is nothing else to join.

### 5. What guarantees ordering?

Nothing, unless requested. With `Concurrency > 1` and `PreserveOrder = false` (the default), results are delivered in **completion order**.

`PreserveOrder = true` delivers results in **source order**: a result is held until all earlier items have been delivered. This costs reordering-buffer memory and head-of-line latency — a slow item delays everything behind it — which is why it is opt-in.

With `Concurrency = 1`, delivery order equals source order as a consequence, but only `PreserveOrder = true` is a contract.

Operators that intentionally break ordering or completeness (`LatestByKey`, `Merge`, drop-mode `Buffer`) say so in their own contract; `Merge` provides no inter-source ordering guarantee.

### 6. What happens to in-flight items?

By terminal cause:

- **Completion** — in-flight items are drained: processed and delivered before the pipeline completes.
- **Failure (`Stop`)** — in-flight items are cancelled via their token; their results are discarded. Buffered, not-yet-started items are abandoned.
- **Cancellation** — same as failure: cancel in-flight work, abandon buffered items, terminate.

For `LatestByKey`: an executing item is never interrupted by a newer arrival. The newer item replaces the *pending* slot for that key; when the current execution finishes, the latest value received during it is processed next.

---

## Time, queues, and retries

These decisions bind Phase 4 (time operators) and Phase 5 (resilience):

- **Queue time and execution time are measured separately** (`queue.duration` vs `processing.duration`). A "slow" item that spent 4.9 s in a queue and 100 ms executing is a capacity problem, not a latency problem, and the metrics must be able to say which.
- **`TimeoutEach` applies to execution only.** Time spent waiting in a queue does not count against an item's timeout.
- **A retry releasing its worker slot during its delay remains the design goal (principle 8).** In 0.x, the resilience integration executes the whole Polly `ResiliencePipeline` — including every retry attempt and the delay between them — inside the item's worker slot, so a backoff currently *does* hold capacity that other items could otherwise use. Releasing the slot during a wait would require Caudal to own the retry loop itself (re-queueing an item with its attempt state and resuming it later), which contradicts "Caudal integrates resilience; it does not reimplement Polly." This gap is documented in [`docs/resilience.md`](resilience.md) and will be revisited before `1.0`.
- **Pipeline cancellation is distinguishable from timeout.** An item cancelled because the pipeline is shutting down is not a timed-out item and must not be recorded or retried as one.
- All of the above use `TimeProvider`, never `DateTime.UtcNow` or raw `Task.Delay`, so every temporal behavior is deterministic under test.

## Saturation contracts per operator (0.1 surface)

| Operator | When its downstream is full | Items it may discard |
|---|---|---|
| `ToFlow` | Suspends the source enumerator | None |
| `SelectAsync` / `WhereAsync` / `SelectManyAsync` | Workers hold their results; upstream intake pauses | None |
| `Buffer(Wait)` | Producer waits | None |
| `Buffer(DropNewest/DropOldest)` | Accepts and discards per policy | Counted as `items.dropped` |
| `Buffer(Reject)` | Write fails visibly | None (failure is surfaced) |
| `Batch` | Holds the forming batch; upstream backpressure applies | None; final partial batch always emitted |
| `Merge` | Backpressure propagates to every source | None |
| `LatestByKey` | Replaces the pending item per key | Counted as `items.replaced` |
| `ForEachAsync` / `ToListAsync` / `ConsumeAsync` | N/A (terminal) | None |
