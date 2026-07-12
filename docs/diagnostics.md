# Diagnostics

`Caudal.Diagnostics` answers one question: *why is this pipeline slow, right
now, without adding a single `Console.WriteLine` to your own code?* That is
the Phase 6 exit criterion, and this page is organized around it — enabling
capture, reading a snapshot, reading a snapshot *usefully*, and wiring
metrics into OpenTelemetry.

## Enabling capture

Statistics are off by default and cost nothing when off. Turn them on per
pipeline:

```csharp
var options = new FlowOptions
{
    Capacity = 1_024,
    Name = "market-data",
    CaptureStatistics = true,
};

var flow = source.ToFlow(options).LatestByKey(x => x.Symbol);
```

Per principle 9 in [`SEMANTICS.md`](SEMANTICS.md) — *telemetry never changes
semantics* — `CaptureStatistics` cannot change ordering, timing contracts, or
error behavior. It only changes cost. When it's `false`, every counter
increment and timestamp capture is skipped entirely: no interlocked
operations, no allocations. When it's `true`, each stage maintains its
counters with `Interlocked` operations (no locks, no contention beyond what
the CPU already serializes) and, for concurrent stages, stamps each item with
a small timestamp envelope on entry and on dequeue so queue time and
processing time can be told apart. That envelope is the only per-item
allocation capture adds; nothing else in the hot path changes shape.

Calling `GetSnapshot()` on a flow built without `CaptureStatistics = true`
throws `InvalidOperationException` — a snapshot silently returning zeros
would be worse than an explicit failure, since zeros are indistinguishable
from "nothing happened."

## Taking a snapshot

`GetSnapshot()` works on any `IFlow<T>` in the chain — call it on the final
composed flow to see every stage, or on an intermediate one to see only the
stages upstream of it:

```csharp
using Caudal;

var flow = priceUpdates
    .ToFlow(new FlowOptions { Capacity = 1_024, Name = "market-data", CaptureStatistics = true })
    .LatestByKey(update => update.Symbol)
    .SelectAsync(CalculateIndicatorsAsync, concurrency: 8);

// ... pipeline is running elsewhere ...

FlowSnapshot snapshot = flow.GetSnapshot();
Console.WriteLine(snapshot.Render());
```

```text
market-data
├─ Source
│  received: 46,856,728
│  completed: 243,812
│  queued: 32/1,024
├─ LatestByKey
│  received: 243,844
│  completed: 243,812
│  replaced: 18,412
└─ SelectAsync
   received: 243,812
   completed: 243,812
   active: 8/8
   avg queue: 0.4 ms
   avg processing: 14.3 ms
```

A snapshot is a point-in-time copy — counters keep moving after you take it —
so calling `GetSnapshot()` repeatedly in a loop while the pipeline runs is the
normal way to watch it, not an error.

## Reading a snapshot

This is the part that actually explains a slow pipeline. Each row in a
`StageSnapshot` means something specific; read them together, not in
isolation, because the diagnosis usually lives in the *relationship* between
adjacent stages.

| Symptom | Diagnosis |
|---|---|
| A stage's `queued` is pinned at `QueueCapacity`, and the **next** stage's `active` is pinned at its configured concurrency with a high `AverageProcessingTime` | That next stage is the bottleneck. Its workers are all busy for a long time each, so nothing drains the queue in front of it. Raise its concurrency or speed up the selector. |
| A stage's `queued` is at capacity, but its own `active` (or the next stage's) is low | Workers are starved somewhere downstream — the backpressure is coming from further down the chain, not from this stage. Keep walking toward the sink. |
| `Received` is far greater than `Completed` on `LatestByKey`, `Sample`, or `Debounce` | Healthy conflation, not loss. These operators are designed to shed stale items; check `Replaced` — it should roughly account for the gap. If `Replaced + Completed` still falls short of `Received`, something else is wrong, but the gap by itself is the intended behavior. |
| `Failed` is growing on a stage running under `FlowFailureMode.Skip` | The failure policy is quietly eating errors that would otherwise have stopped the pipeline. `Skip` is doing its job, but a rising `Failed` counter means it's worth looking at *why* — check the exceptions being skipped, don't just watch the counter. |
| The source stage's `Received` is flat (barely increasing between two snapshots) | The source itself is the bottleneck — it isn't producing items fast enough to saturate anything downstream. Look at what's upstream of the flow, not inside it. |
| `AverageQueueTime` is high but `AverageProcessingTime` is low | Items spend their time waiting for a free worker, not being processed. This is a capacity problem (raise concurrency, or reduce arrival rate), not a per-item latency problem. |
| `AverageQueueTime` is low but `AverageProcessingTime` is high | The opposite: workers get items quickly but each one takes a long time. The selector itself is slow — profile it, or reduce what it does per item. |

The general method: find the stage where `queued` is saturated, then look at
its immediate downstream neighbor. If the neighbor is also saturated (or its
workers are all `active` and slow), the problem is there — repeat the walk
one stage further down until you find a stage whose workers are *not* all
busy. That stage's upstream neighbor is the actual bottleneck.

## OpenTelemetry

`PublishMetrics<T>()` registers observable instruments on the `Caudal` meter
for the lifetime of the returned `IDisposable`:

```csharp
using Caudal;

var registration = flow.PublishMetrics();

// elsewhere, at application startup:
services.AddOpenTelemetry()
    .WithMetrics(m => m
        .AddMeter(CaudalDiagnostics.MeterName) // "Caudal"
        .AddOtlpExporter());

// when the pipeline ends:
registration.Dispose();
```

The instruments are observable gauges and counters — they are read at scrape
time, when your OpenTelemetry exporter asks for a value, not polled on a
timer. There is no background thread walking the flow between scrapes; the
cost of publishing metrics is paid once per scrape, not once per interval.

| Instrument | Kind | Meaning |
|---|---|---|
| `caudal.items.received` | counter | items a stage has accepted |
| `caudal.items.completed` | counter | items a stage finished successfully |
| `caudal.items.failed` | counter | items that failed under `Skip`/`Capture` |
| `caudal.items.dropped` | counter | items shed by a `Buffer` drop policy |
| `caudal.items.replaced` | counter | items superseded by conflation (`LatestByKey`, `Sample`, `Debounce`) |
| `caudal.queue.length` | gauge | items currently buffered at a stage |
| `caudal.queue.capacity` | gauge | the stage's configured buffer capacity |
| `caudal.workers.active` | gauge | concurrently executing workers |
| `caudal.queue.duration.avg` | gauge (ms) | average time an item waits for a worker |
| `caudal.processing.duration.avg` | gauge (ms) | average time a worker spends per item |
| `caudal.pipeline.duration` | gauge (s) | wall-clock time since the pipeline started |

Every instrument carries `pipeline` (the flow's `Name`), `operator` (e.g.
`LatestByKey`, `SelectAsync`), and `stage` (its index in the chain) as tags,
so a dashboard can break down by stage without guessing which operator
produced which series.

Dispose the registration when the pipeline ends — it unhooks the observable
callbacks from the meter so a finished flow doesn't keep reporting stale
numbers (or, worse, get scraped after its internal state has been collected).

## Honest limitations

- `items.retried` is not emitted yet. Retries happen inside Polly, which has
  its own telemetry; a Caudal-side retry counter arrives with the resilience
  telemetry integration, not in this phase.
- Under `FlowFailureMode.Capture`, a failed item still counts as `completed`,
  not `failed` — the failure became data (the `FlowResult` carries it), and
  from the stage's point of view an item that produced a `FlowResult` is an
  item it finished processing.
- `Merge`'s snapshot walks only its first source's chain; the other sources'
  upstream stages are not included in the tree.
- `Batch`'s `Completed` counts batches emitted, not items inside them — check
  the upstream stage for item-level counts.
- Queue time is only measured in unordered concurrent stages; a stage with
  `Concurrency = 1` or `PreserveOrder = true` does not currently report
  `AverageQueueTime`.
- There are no percentile latencies yet. `AverageQueueTime` and
  `AverageProcessingTime` are means; p95/p99 need histograms, which are
  deferred past this phase.
