# Benchmarks

This page explains what `benchmarks/Caudal.Benchmarks` measures, how to run it,
and — more importantly — how to read the results honestly. Phase 8 of the
[roadmap](../ROADMAP.md) exists to answer a question the rest of the docs
don't: what does the abstraction cost, and when does it pay for itself?

No benchmark numbers are published in this repository. They depend on the
machine, the OS scheduler, thermal throttling, background load, and the .NET
version installed — a number pasted into a doc goes stale the moment any of
that changes, and a stale performance number is worse than no number, because
it looks authoritative. Run the suite yourself; see below.

## Methodology

- **BenchmarkDotNet** drives every scenario. It handles warmup, iteration
  counts, and statistical noise reduction itself — no scenario hand-rolls its
  own timing loop.
- **`[MemoryDiagnoser]`** is enabled wherever allocations matter, which is
  every scenario. Mean time tells you speed; `Allocated` tells you GC
  pressure, and for some scenarios (`BackpressureBenchmarks`) `Allocated` is
  the entire point, not a side note.
- **Deterministic workloads.** No `Random`. Where a scenario needs a skewed
  pattern (every 10th item slow, one key hot), the skew is a fixed, seeded
  pattern baked into the benchmark, not sampled at run time. A benchmark whose
  workload changes shape between runs cannot be compared to itself, let alone
  to another baseline.
- **Server GC.** The benchmark project runs with
  `<ServerGarbageCollection>true</ServerGarbageCollection>` — the standard
  choice for throughput-oriented pipeline workloads, and the configuration a
  service consuming Caudal would most likely run under. Workstation GC numbers
  will differ, particularly on allocation-heavy scenarios.
- **Fair baselines.** Every hand-rolled comparison — the `SequentialLoop`,
  `SemaphoreSlimWhenAll`, `ManualChannel`, `TplDataflowBounded`, and so on — is
  written the way a competent engineer would actually write it, not a
  deliberately weakened strawman. Same degree-of-parallelism, same buffer
  capacity, same total work, across every baseline in a given scenario. If a
  Caudal shape has `Capacity = 64`, so does the hand-rolled `Channel<T>` next
  to it in the same class. Comparing a bounded Caudal pipeline to an unbounded
  baseline would make Caudal look better for the wrong reason; comparing it to
  an equally-bounded hand-rolled pipeline is the only comparison worth
  publishing.

## How to run it

Full suite, every scenario:

```bash
dotnet run -c Release --project benchmarks/Caudal.Benchmarks -- --filter '*'
```

Benchmarks only build and run meaningfully in `Release`; a Debug build's
timings are not representative of anything and BenchmarkDotNet will warn
about it.

For a quick local sanity check while iterating on a change — far fewer
iterations, much faster, not the numbers to trust for a real comparison — add
the short job:

```bash
dotnet run -c Release --project benchmarks/Caudal.Benchmarks -- --filter '*' --job short
```

Filter to one scenario class while working on it:

```bash
dotnet run -c Release --project benchmarks/Caudal.Benchmarks -- --filter '*NearEmptyWork*'
```

## Scenario-by-scenario guide

### `NearEmptyWorkBenchmarks` — pure overhead, and Caudal is meant to lose

The workload is `x => x + 1` over 10,000 ints. Baselines: `SequentialLoop`
(the actual baseline — a plain `for` loop), `ParallelForEachAsync`,
`SemaphoreSlimWhenAll`, `ManualChannel`, `TplDataflow`, against
`CaudalSequential`, `CaudalUnordered`, `CaudalOrdered`, and
`CaudalWithStatistics`.

**Read this scenario expecting Caudal to lose badly, because it does, and
that's correct.** A channel hop — a write, a read, a continuation resumption —
costs far more than an integer increment. Every concurrent shape in this list
pays that hop per item; the plain loop pays nothing. This scenario isn't a bug
to fix or a number to explain away; it's the sharpest illustration of the
first entry in [`when-not-to-use.md`](when-not-to-use.md): if the work per
item is smaller than the cost of moving the item, no pipeline abstraction —
Caudal's or anyone else's — will beat a loop. `CaudalWithStatistics` will
generally be slightly slower and allocate slightly more than the plain Caudal
shapes; that delta is the diagnostics tax, which is the number to look at when
deciding whether to leave metrics on in a hot, low-value path.

### `ShortIoBenchmarks` — everything converges, because I/O dominates

`Task.Delay(1)` per item, 200 items, degree-of-parallelism 8 across every
shape. Expect `SequentialLoop` and every concurrent shape to land close
together on `Mean`: the delay swamps whatever overhead each shape adds, so
this scenario mostly measures `Task.Delay`'s own granularity, not the
abstractions wrapping it. On Windows, timer resolution means a "1 ms" delay
routinely takes closer to ~15 ms in practice — and it does so identically for
`SequentialLoop`, `ManualChannel`, and every Caudal shape, since none of them
control the OS timer. Don't read a difference here as one shape being faster
at I/O; read the *lack* of a meaningful difference as the honest result: once
real I/O dominates, the pipeline's own overhead stops being visible. What's
left to compare honestly is `Allocated`, where per-item overhead still shows
up even when it's invisible in `Mean`.

### `VariableIoBenchmarks` — where ordering has a cost

Every 10th item takes 10 ms; the rest yield immediately (`Task.Yield`). The
datum this isolates is head-of-line blocking under skew, not throughput:
compare `PreserveOrder = true` against `PreserveOrder = false` from the same
`SEMANTICS.md` contract. Unordered delivery lets the 9 fast items behind a
slow one flow through as soon as they finish; ordered delivery holds them
until the slow item clears, exactly as documented in
[`SEMANTICS.md`](SEMANTICS.md#5-what-guarantees-ordering). Read the gap
between the ordered and unordered variants as the price of the ordering
guarantee under this specific skew pattern — it is not a defect in either
mode, it is the contract working as specified.

### `BackpressureBenchmarks` — the interesting column is `Allocated`

Fast producer, slow consumer, bounded capacity 64 everywhere:
`ManualChannelBounded`, `TplDataflowBounded`, `CaudalBounded`. All three cap
in-flight items at the same capacity and pay a similar backpressure tax, so
their `Mean` values should land close together — that convergence is the
result, not a disappointment. `UnboundedListBuffering` is the anti-pattern
contrast: a producer that materializes everything into a `List<T>` before the
consumer even starts, with no capacity limit at all. Its `Mean` is not the
point; its `Allocated` is. That column is what a team pays, invisibly, the
day they reach for a `List<T>` instead of a bounded queue because a fast
producer "worked fine in testing" against a small input.

### `LatestByKeyBenchmarks` — the headline number is `processedItems`, not `Mean`

100,000 updates across 100 keys, feeding a slow consumer. `CaudalNoConflation`
is the baseline: it processes every update, so it's slow by construction —
that's what "no conflation" means, not a flaw. `CaudalLatestByKey` processes
only the freshest value per key by the time the consumer is ready for it, per
the `LatestByKey` contract in `SEMANTICS.md`. `ManualDictionaryConflation` is
the honest hand-rolled equivalent — a dictionary keyed by symbol, overwritten
in place, read by the consumer loop — written the way a competent engineer
would build it without reaching for Caudal.

The number that carries the argument for `LatestByKey` is `processedItems`:
how many of the 100,000 updates were actually handed to the slow consumer.
Lower is better — it means more staleness was correctly discarded before
doing expensive work. Do not read this scenario's `Mean` as the headline; a
benchmark that does less work finishes faster almost by definition, and
that's circular.

BenchmarkDotNet discards benchmark return values, so `processedItems` never
appears in its tables. To see it, run the dedicated report mode:

```
dotnet run -c Release --project benchmarks/Caudal.Benchmarks -- --conflation-report
```

which executes the three scenarios once (untimed — only the counts matter)
and prints each one's `processedItems` and its ratio against the
no-conflation baseline. That ratio, compared against how well
`ManualDictionaryConflation` does the same job, is what tells you whether
the operator earns its keep over rolling your own dictionary-based
conflation.

### `LifecycleBenchmarks` — the fixed cost of the guarantees

Two scenarios:

- **`CancellationLatency`** — an infinite, warm pipeline (source never
  completes on its own) is torn down via its `CancellationToken`, and the
  benchmark measures the time from cancellation to the terminal awaitable
  actually completing. Pipeline construction and warm-up happen in
  `IterationSetup`, outside the measured window, so the reported `Mean`
  genuinely is the cancel-to-complete latency and nothing else. This is the cost of "no orphaned tasks" as a *tested
  invariant* rather than an aspiration (`SEMANTICS.md`, question 3): every
  in-flight worker, every buffered item, every stage has to notice
  cancellation and unwind before the pipeline can report itself done. That
  unwind is not instantaneous, and this benchmark is where its latency is
  actually measured instead of assumed.
- **`GracefulShutdown`** — a finite pipeline draining normally to completion,
  measuring the fixed lifecycle overhead: channel teardown, final drain,
  terminal-state bookkeeping. This is the number relevant to
  [`when-not-to-use.md`](when-not-to-use.md)'s point about single-item,
  ultra-low-latency paths — this fixed cost is paid once per pipeline
  lifetime, not per item, so it only matters when the pipeline itself is
  short-lived relative to it.

## What's not measured yet

BenchmarkDotNet's `MemoryDiagnoser` reports allocations, and every scenario's
`Mean`/`StdDev` come from BenchmarkDotNet's own statistics — that's what's
available today. Not yet instrumented, and called out here so it isn't
silently assumed to exist:

- **Peak task count.** The roadmap's Phase 8 metrics list includes it; the
  current suite reports timing and allocations, not a live count of
  outstanding `Task` objects during a run.
- **Peak working set.** Allocations (via `MemoryDiagnoser`) are not the same
  measurement as process-level peak memory, which needs its own
  instrumentation (e.g. sampling `Process.WorkingSet64` around each
  benchmark) to report honestly.
- **p95/p99 percentile configuration.** BenchmarkDotNet can report additional
  percentile columns with explicit configuration; the suite doesn't turn
  that on yet, so today's output is Mean/StdDev, not a latency-distribution
  view.
- **CI regression tracking.** Nothing currently fails a build when a change
  regresses a benchmark. Wiring the suite into CI with a stored baseline and
  a regression threshold is future work, not a claim this document is making
  about the current state.

## Adding a new scenario

If you add a benchmark class, follow the pattern already in
`BackpressureBenchmarks.cs`: document in the class-level XML comment what the
scenario isolates and what the honest reading of its numbers is, keep every
baseline in the class fair (same concurrency, same capacity, same total
work), and don't publish absolute numbers in this file — describe what the
scenario measures and how to interpret it, and let whoever runs it read their
own machine's numbers.
