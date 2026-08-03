# Time Operators

`Debounce`, `Throttle`, and `Sample` mean different things in different libraries.
This page is the authoritative definition of what they mean in Caudal. Every
diagram uses one dash per time unit; letters are items, `|` is source completion.

All of these operators take an optional `TimeProvider` (system clock by default),
so every behavior below is testable with `FakeTimeProvider` — no real delays.

## Debounce(period)

Emit an item only after `period` of **silence** follows it. A new arrival within
the period replaces the pending item and restarts the timer. Completion flushes
the pending item immediately. Replaced items are counted (`inputs.replaced`).

```text
period = 3
Input:     A-B--C----------D--|
Output:    --------C----------D
                   ^ C emitted 3 units after the last arrival of its burst
```

Use it for: search-as-you-type, config reloads, anything where only the final
value of a burst matters.

## Throttle(period)

**Leading edge** rate limit: emit the first item immediately, then drop every
arrival during the following `period`. Dropped items are counted
(`inputs.dropped`) — they are shed, not delayed.

```text
period = 4
Input:     A-B-C---D---E-F--|
Output:    A-------D---E-----|
           ^ A opens a window; B and C fall inside it and are dropped
```

Use it for: rate-limiting button clicks or notifications where the *first*
event matters and repeats are noise.

## Sample(interval)

A clock ticks every `interval`. At each tick, emit the **latest** item received
since the previous emission; a tick with nothing new emits nothing. Items
overwritten between ticks are counted (`inputs.replaced`). Completion flushes the
last unsampled item.

```text
interval = 4  (ticks at t=4, 8, 12, …)
Input:     A-B-C----D-------|
Output:    ----C---C?--D----      ← in Caudal:  ----C-------D----
```

Note the difference from some libraries: Caudal never re-emits an unchanged
value on a tick (`C?` above does not happen).

A gap spanning several intervals — a large clock jump, a GC pause, a stalled
consumer — **coalesces to a single tick** of the latest value; Caudal does not
replay one tick per boundary crossed. The next tick is anchored at the moment
the coalesced tick fired, not at wall-clock-aligned boundaries.

```text
interval = 4, consumer stalls for 12 units
Input:     A-B----(stall)----C--
Output:    ----B----------B?----     ← in Caudal: one B, not three
```

Use it for: dashboards and UIs that want the freshest value at a fixed cadence,
regardless of how fast the source produces.

## Debounce vs Sample vs LatestByKey

| | Emits when | Drops/replaces |
|---|---|---|
| `Debounce` | after silence | all but the last of a burst |
| `Sample` | on a fixed cadence | all but the latest per tick |
| `LatestByKey` | as fast as downstream can take, per key | all but the latest per key, until emission |
| `SelectLatestByKeyAsync` | when the key's previous execution ends | all but the latest per key, until execution |

## BatchEvery(interval, maximumSize)

Sugar over `Batch(maximumSize, maximumDelay: interval)`: one batch emission per
interval containing everything received while the batch was open. The window
is anchored at the batch's **first item**, not aligned to wall-clock
boundaries, and an empty window emits nothing.

`maximumSize` is a required argument, not an optional one with an unbounded
default: no buffer in Caudal is allowed to grow without limit, so a batch that
never closes on time (a stalled downstream, a clock that never ticks) must
still have a hard cap on how much it accumulates.

```text
interval = 4
Input:     A-B-C---D-E------|
Output:    ----[ABC]----[DE]|
```

## IdleTimeout(timeout)

Bounds the **silence between consecutive items**: if no item arrives within
`timeout`, the pipeline faults with `TimeoutException`.

This operator was renamed from `TimeoutEach`: the old name suggested it bounded
each item's processing time, but it has only ever measured the gap between
upstream arrivals. `TimeoutEach` is reserved for a future operator that times
individual executions via a `Caudal.Resilience` strategy.

```text
timeout = 5
Input:     A--B---------✗
Output:    A--B----X          X = TimeoutException 5 units after B
```

This times *upstream production*. It deliberately does not time an individual
item's processing — per-item execution timeouts belong to a resilience strategy
attached to the processing stage (`Caudal.Resilience`, phase 5), where queue
time and execution time can be separated correctly.

One consequence worth knowing: while already-arrived items are being drained to
a slow consumer, no silence is being measured, so the clock for a gap that
starts *right after a burst* only starts once the drain finishes. Detection
latency for that gap is `(time to drain the burst) + timeout`, not `timeout`.
If your own processing is occasionally slower than the timeout, "the feed died"
is flagged later than the raw timeout suggests — the timeout still fires, it is
never masked indefinitely.

## DelayEach(delay)

Waits `delay` before emitting each item: pacing with natural backpressure.
Throughput is bounded to one item per `delay`; upstream fills its bounded
buffers and then waits.

```text
delay = 2
Input:     ABC------|
Output:    --A-B-C--|
```
