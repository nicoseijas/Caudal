# When Not to Use Caudal

This is the page the [roadmap](../ROADMAP.md) calls for at Phase 8: a "when
Caudal loses" section that stays current as the benchmark suite
(`benchmarks/Caudal.Benchmarks`, see [`docs/benchmarks.md`](benchmarks.md))
grows. It expands the short list in the [README](../README.md#when-not-to-use-caudal)
with the reasoning behind each entry and points to the specific benchmark
that backs it up.

Caudal is deliberately small — around ten operators with precise semantics,
not a partial reimplementation of Rx or TPL Dataflow. That means there are
whole categories of problems it isn't trying to be the answer to. Reaching
for it there doesn't just fail to help; it adds a channel hop, a bounded
buffer, and a cancellation contract you didn't need, and asks you to pay for
all three.

## Small, sequential, CPU-bound work

If the transformation is `x => x + 1` — or anything else where the per-item
work is smaller than the cost of moving the item between stages — a plain
loop wins by orders of magnitude, not by a rounding error.
`NearEmptyWorkBenchmarks` (see [`benchmarks.md`](benchmarks.md#nearemptyworkbenchmarks--pure-overhead-and-caudal-is-meant-to-lose))
exists specifically to make this loss visible: every concurrent shape in that
scenario, Caudal's included, pays a channel write, a channel read, and a
continuation resumption per item, and none of that is free. An integer
increment is free. No pipeline abstraction closes that gap, because the gap
isn't in the abstraction's implementation — it's structural: you cannot make
moving an item between stages cheaper than not moving it at all.

**Use instead:** a `for` or `foreach` loop. If the work is CPU-bound and large
enough to actually benefit from parallelism, `Parallel.For`/`Parallel.ForEach`
with `System.Threading.Tasks.Parallel` is the right tool — it's built for
CPU-bound fan-out and doesn't carry an I/O-shaped bounded-buffer contract that
this workload never needed.

## Small, already-materialized collections

If you have a `List<T>` with a few dozen or a few hundred items already in
memory and you want to run an async operation over each of them, you don't
have a backpressure problem: nothing is streaming, nothing needs to be
bounded, there's no producer to slow down. `Task.WhenAll` over
`list.Select(ProcessAsync)` says exactly what's happening and nothing more.

**Use instead:** `Task.WhenAll(items.Select(ProcessAsync))`, optionally gated
by a `SemaphoreSlim` if you need a concurrency cap. That's two lines and no
new vocabulary for the next person reading the code.

## Concurrency without backpressure, batching, or per-key semantics

If all you need is "run these N things with at most K concurrent," and you
don't need a bounded producer, `Batch`, `Merge`, or `LatestByKey`'s
per-key conflation, `Parallel.ForEachAsync` already does that — and it does
it with less ceremony and fewer allocations than standing up a Caudal
pipeline for one stage. `BackpressureBenchmarks`
(see [`benchmarks.md`](benchmarks.md#backpressurebenchmarks--the-interesting-column-is-allocated))
shows `Mean` converging across bounded shapes precisely because, once
concurrency is capped the same way everywhere, there's no meaningful
difference left to buy with a heavier abstraction.

**Use instead:** `Parallel.ForEachAsync(source, new ParallelOptions {
MaxDegreeOfParallelism = k }, ProcessAsync)`. Reach for Caudal only once you
also need one of: a bounded producer that must slow down instead of filling
memory, ordered-vs-unordered delivery as an explicit choice, per-key
conflation, or the diagnostics to explain a slow run without adding your own
logging.

## True event systems and rich operator composition

If the problem is genuinely reactive — combining, windowing, and composing
many independent event streams with operators like `CombineLatest`, `Zip`,
`Scan`, or custom operator chains — that is Rx's problem to solve, and it has
decades of prior art doing it. Caudal's `Debounce`, `Throttle`, and `Sample`
(see [`docs/time-operators.md`](time-operators.md)) cover the common
single-stream rate-shaping cases, but Caudal is not trying to replace Rx's
composition model, and stretching it to do so produces worse code than just
using Rx.

**Use instead:** `System.Reactive` (Rx.NET). If your actual need is "one
stream, rate-shaped, with bounded memory and cancellation that actually
works," that's Caudal's normal case — but if you need to combine and
transform several independent streams together, that's a different design
problem and Rx already has the vocabulary for it.

## Ultra-low-latency, single-item paths

Every Caudal pipeline has a fixed lifecycle cost: channel setup and teardown,
worker startup, drain-and-complete bookkeeping. `LifecycleBenchmarks`'s
`GracefulShutdown` scenario
(see [`benchmarks.md`](benchmarks.md#lifecyclebenchmarks--the-fixed-cost-of-the-guarantees))
measures exactly that fixed overhead. For a pipeline processing millions of
items over its lifetime, that fixed cost is invisible, amortized away. For a
single sub-millisecond call — the kind of path where every microsecond is
counted — the fixed lifecycle overhead can dwarf the actual work, the same
way opening a database connection per row would dwarf the row's own update.

**Use instead:** call the operation directly, or use a lighter-weight
primitive purpose-built for the hot path (a hand-tuned `Channel<T>` with no
lifecycle ceremony, or no queue at all if there's truly one item). Caudal is
built for pipelines that live long enough, and process enough items, for the
fixed lifecycle cost to be amortized.

## An existing, correct, hand-rolled `Channel<T>` pipeline

If a team already has a working `Channel<T>`-based pipeline — bounded
correctly, cancels cleanly, and nobody is losing sleep over debugging it —
rewriting it onto Caudal is a cost with no matching benefit. Caudal earns its
keep on *semantics and diagnostics*: a documented saturation contract per
operator, tested cancellation with no orphaned tasks, `LatestByKey`'s
per-key conflation, and a snapshot API that explains a slow pipeline without
added logging. If none of that is currently a pain point — if the existing
code is already correct and nobody needs to explain *why* it's slow — the
migration cost buys nothing. Caudal is worth adopting for new work, or for
pipelines whose hand-rolled version is quietly wrong (unbounded buffers,
swallowed cancellation, no visibility into where time goes), not as a
blanket replacement for working code.

**Use instead:** the existing pipeline. Revisit this if the team starts
hitting the kind of bug Caudal is built to make impossible — an orphaned
task after a cancelled request, a `List<T>` standing in for a bounded queue
because nobody set a capacity (the exact anti-pattern
`UnboundedListBuffering` demonstrates in `BackpressureBenchmarks`), or a slow
pipeline nobody can explain without adding print statements.

---

## Where Caudal does pay for itself

The inverse list, stated as facts about the semantics, not as marketing:

- **Bounded concurrency with an explicit, non-default limit** — every
  operator requires a concurrency value; there is no implicit fan-out to
  discover in production under load.
- **Real backpressure, not a buffer that silently grows** — a full stage
  suspends its producer; the only ways to shed instead of wait are the
  explicit `Buffer` drop policies and `LatestByKey`'s per-key replacement,
  and both count what they discard (`inputs.dropped`, `inputs.replaced`).
- **Cancellation and shutdown as a tested invariant** — "no orphaned tasks"
  is verified by tests, not assumed; `CancellationLatency` in
  `LifecycleBenchmarks` measures the actual cost of that guarantee rather
  than asserting it's free.
- **Per-key conflation for stale-data-replacement workloads** —
  `LatestByKey` processes the freshest value per key instead of every update,
  with the discard rate (`inputs.replaced`) as a first-class, counted number,
  not a side effect you have to infer, and bounded to an explicit
  `maximumKeys` rather than growing with key cardinality.
- **Diagnostics without instrumenting your own code** — `queue.duration` vs.
  `processing.duration`, worker utilization, and drop/replace counts are
  available per stage without adding logging calls to the selector itself.

These are the problems Caudal was built to solve well
(see [`ROADMAP.md`](../ROADMAP.md)'s opening framing against
`SemaphoreSlim + Task.WhenAll + Channel<T>`). Everything above this line is
what it deliberately does not try to be better at.
