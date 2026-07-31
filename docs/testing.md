# Testing

Every concurrency bug Caudal exists to prevent has a matching failure mode in
the tests that verify it: "wait 200 ms and hope the workers got there,"
"sleep and check a counter," "run it 50 times and see if it flakes." That
style of test is not a minor inconvenience — it is actively dishonest. It
does not prove the race is fixed; it proves the race did not happen to fire
in the time you happened to wait. `Caudal.Testing` exists so a test can
assert "3 workers are in flight right now" instead of "3 workers are
probably in flight after 200 ms," and get the same answer every run.

There are four things an intermittent concurrency test is guessing about,
and `Caudal.Testing` gives you explicit control of each one:

| Guessed at | Controlled by |
|---|---|
| What arrives, and when | `TestFlowSource<T>` |
| When execution proceeds | `AsyncGate` |
| How much time has passed | `FakeTimeProvider` ([`time-operators.md`](time-operators.md)) |
| Whether the pipeline behaved | `Should()` / `FlowAssertions<T>` |

## `TestFlowSource<T>` — controlling input

A `TestFlowSource<T>` is a source you drive by hand: emit items, complete it,
or fail it, on your own schedule, from test code.

```csharp
public sealed class TestFlowSource<T>
{
    void Emit(T item);
    void EmitRange(IEnumerable<T> items);
    void Complete();
    void Fail(Exception exception);
    int PendingCount { get; }
    Flow<T> ToFlow(FlowOptions? options = null);
}
```

- `Emit`/`EmitRange` never block — the source is backed by an unbounded
  channel on the test side. Backpressure still applies downstream: it comes
  from `FlowOptions.Capacity` on the flow built by `ToFlow`, exactly as it
  would for any other source.
- `Fail(exception)` delivers *that exact instance*, unwrapped, to whatever
  observes the pipeline's completion — same contract as every other failure
  path in Caudal (`SEMANTICS.md`, "how errors propagate").
- `PendingCount` is the number of items emitted but not yet pulled into the
  flow's own buffer. It is how a test observes that a bounded stage actually
  stalled, instead of assuming it did.
- `ToFlow` may be called once. A `TestFlowSource<T>` feeds a single flow; if
  you need two pipelines under test, use two sources.

```csharp
var source = new TestFlowSource<int>();
var flow = source.ToFlow(new FlowOptions { Capacity = 4 });

source.EmitRange(Enumerable.Range(0, 10));
source.Complete();

var results = await flow.ToListAsync();
```

## `AsyncGate` — controlling execution

`AsyncGate` is a manually-operated door: closed, callers awaiting it block;
open, they pass. It is the one primitive in this package that lets a test
*know* the pipeline reached a particular state, instead of assuming it did
after some delay.

```csharp
public sealed class AsyncGate
{
    AsyncGate(bool open = false);
    Task WaitAsync(CancellationToken ct = default);
    void Release(int count = 1);
    void Open();
    void Close();
    int WaitingCount { get; }
    Task WhenWaitersAsync(int count, CancellationToken ct = default);
}
```

- A closed gate blocks every `WaitAsync` caller until permits are available.
- `Release(count)` hands out `count` permits. A permit released before anyone
  is waiting is *banked* — the next `count` callers to arrive pass through
  immediately without blocking. This matters for tests where the gate is
  released slightly before a worker calls `WaitAsync`; the release is not
  lost.
- `Open()` puts the gate into a state where every current and future waiter
  passes immediately, without consuming banked permits one at a time.
- `Close()` puts it back into blocking mode and clears any banked permits —
  a fresh `WaitAsync` after `Close()` blocks even if permits had accumulated
  before.
- `WaitingCount` is the number of callers currently blocked inside
  `WaitAsync`.
- `WhenWaitersAsync(n)` is the primitive that makes race conditions
  deterministic: it completes exactly when `n` callers are simultaneously
  blocked on the gate. Awaiting it is how a test proves the system reached a
  specific state of concurrent execution, rather than asserting it happened
  to reach that state before a timeout expired.

```csharp
var gate = new AsyncGate(open: false);

var worker = Task.Run(async () =>
{
    await gate.WaitAsync();
    return 42;
});

await gate.WhenWaitersAsync(1); // now provably blocked, not "probably by now"
gate.Release();
var result = await worker; // 42
```

## Virtual time

Time-based operators (`Debounce`, `Throttle`, `Sample`, `IdleTimeout`,
`BatchEvery`, `DelayEach`) already take a `TimeProvider`, so `Caudal.Testing`
does not need to introduce its own clock abstraction — it relies on
`Microsoft.Extensions.Time.Testing.FakeTimeProvider`, referenced transitively
by `Caudal.Testing`. Pass a `FakeTimeProvider` into `FlowOptions` (or the
operator directly, per operator) and advance it explicitly instead of
sleeping:

```csharp
var time = new FakeTimeProvider();
var flow = source.ToFlow().Debounce(TimeSpan.FromSeconds(3), time);

source.Emit(1);
time.Advance(TimeSpan.FromSeconds(3));
```

See [`time-operators.md`](time-operators.md) for what each operator does
with the clock — this page only points at it, so the two docs don't drift
out of sync on operator semantics.

## Reproducing a race, stably

The recipe behind every deterministic race test in this package is the same
four steps:

1. **Gate the workers.** Give the selector a closed `AsyncGate` to block on.
2. **Prove the racy state.** Await `gate.WhenWaitersAsync(n)` — now `n`
   workers are known to be in flight, not guessed to be.
3. **Act.** Fail the source, cancel the token, emit more items — whatever
   triggers the condition you're pinning, while the workers are held open.
4. **Release and assert.** Open the gate, let the workers unblock into
   whatever they were going to observe (the failure, the cancellation), and
   assert on the outcome.

This is exactly the ad-hoc pattern `BackpressureTests.cs` and
`ConcurrencyTests.cs` built by hand with `TaskCompletionSource` and
`Interlocked` counters — `TestFlowSource<T>` and `AsyncGate` are that pattern
made reusable and named.

```csharp
[Fact]
public async Task Concurrent_failure_while_workers_are_in_flight()
{
    var gate = new AsyncGate(open: false);
    var source = new TestFlowSource<int>();
    var boom = new InvalidOperationException("upstream exploded");

    var flow = source.ToFlow()
        .SelectAsync(async (i, ct) =>
        {
            await gate.WaitAsync(ct);
            return i;
        }, concurrency: 3);

    source.EmitRange([1, 2, 3]);

    await gate.WhenWaitersAsync(3).WaitAsync(TimeSpan.FromSeconds(10));
    // 3 workers are provably blocked mid-item — this is the race window a
    // sleep-based test could only ever hope to land inside.

    source.Fail(boom);
    gate.Open();

    var thrown = await FluentActions
        .Awaiting(() => flow.ToListAsync())
        .Should()
        .ThrowExactlyAsync<InvalidOperationException>();

    thrown.Which.Should().BeSameAs(boom);
}
```

Every run holds the same window open — the workers cannot race ahead past
the gate, and the failure cannot be missed by arriving before the workers
started. There is no timing-dependent branch left in the test.

## The assertions API

```csharp
public static class FlowTestingExtensions
{
    FlowAssertions<T> Should<T>(this Flow<T> flow);
}

public sealed class FlowAssertions<T>
{
    FlowAssertions<T> UseAtMostConcurrency(int max);
    FlowAssertions<T> PreserveOrder(IComparer<T>? comparer = null);
    FlowAssertions<T> CompleteWithoutLeaks();
    FlowAssertions<T> WithTimeout(TimeSpan timeout);
    Task RunAsync(CancellationToken ct = default);
    TaskAwaiter GetAwaiter();
}
```

`Should()` builds a fluent set of expectations; nothing runs until you await
it (or call `RunAsync` explicitly). Awaiting `FlowAssertions<T>` *is* running
the pipeline — the assertions object becomes the sink. That's why there is
exactly one `Should()` per flow: it's a terminal operation, the same as
`ToListAsync` or `ForEachAsync`, and attaching a second sink to the same flow
isn't meaningful any more than it would be for those.

```csharp
await flow.Should()
    .UseAtMostConcurrency(4)
    .PreserveOrder()
    .CompleteWithoutLeaks()
    .WithTimeout(TimeSpan.FromSeconds(5));
```

- **`UseAtMostConcurrency(max)`** and **`CompleteWithoutLeaks()`** read the
  flow's own statistics, so they require `FlowOptions.CaptureStatistics =
  true` on the flow being asserted on. Without it there is nothing to check
  against — the same reasoning as `GetSnapshot()` in
  [`diagnostics.md`](diagnostics.md): a silent zero would be worse than an
  explicit failure, so build the flow with statistics on when you intend to
  assert on them.
- **`PreserveOrder(comparer)`** asserts that delivered items are
  non-decreasing under `Comparer<T>.Default` (or the comparer you pass) — see
  the limitation below on what that does and does not prove.
- **`WithTimeout(timeout)`** bounds the whole run: if the pipeline hasn't
  completed (successfully, by fault, or by cancellation) within `timeout`,
  the assertion run fails with a timeout, rather than hanging the test suite
  on a pipeline that deadlocked.
- A pipeline fault propagates **unwrapped**. If the selector throws, the same
  exception instance is what the awaited `Should()` throws — no
  `AggregateException`, no wrapper type to unwrap in the test.

### A failing assertion prints the pipeline

When an assertion is violated, `Should()` throws `CaudalAssertionException`
with a message that includes the diagnostics tree, provided
`CaptureStatistics` was on — you get the same rendering `GetSnapshot().
Render()` produces in [`diagnostics.md`](diagnostics.md), attached to the
failure that needs it, not a separate step you have to remember to run:

```text
Caudal.Testing.CaudalAssertionException : Expected concurrency to never exceed
4, but observed 6.

race-repro
└─ SelectAsync
   received: 8
   completed: 8
   active: 0/4
   max observed active: 6
```

That's the point of pairing this package with `Caudal.Diagnostics`: a
concurrency-cap violation doesn't just fail, it fails with the stage's own
counters attached, so you don't have to reproduce it a second time under a
debugger to see what actually happened.

### The timeout guard

`WithTimeout` exists because a deadlocked pipeline under test would otherwise
hang forever — the same class of bug the assertion suite is trying to catch
can also freeze the test that's checking for it. Set it generously relative
to the gates you're using (gate releases are instant; only real `Task.Delay`
or a stuck consumer should ever approach the timeout) and treat a timeout
failure as itself a finding, not test flakiness to retry away.

The timeout is a **hard bound on the run itself**: when it fires, the guard
cancels the pipeline and waits a short teardown grace period (5 s). A
cooperative pipeline unwinds within it; a stage that ignores its cancellation
token entirely still cannot hang the test — the run fails at roughly
`timeout + grace` with a message saying the pipeline ignored cancellation,
which is itself the finding.

One small deviation from the pipeline's normal contract: the assertions
enumerate through an internal linked token (so the timeout can tear the run
down), which means the `CancellationToken` **property** on a propagated
`OperationCanceledException` references that internal token, not your own.
The type contract is unaffected — cancelling your token still surfaces as
`OperationCanceledException` — and that is what tests should assert on.

## Honest limitations

- **Not a model checker.** `Caudal.Testing` does not explore schedules for
  you. It gives you exact control at the points you choose to gate — if a
  race can also occur at an interleaving you didn't think to hold open with
  an `AsyncGate`, this package will not find it for you. It replaces "hope
  the scheduler visits the bad interleaving" with "pin the interleaving you
  already suspect," which is a narrower but much more reliable guarantee.
- **`PreserveOrder` proves non-decreasing delivery, not source order.** It
  asserts the sequence delivered downstream is non-decreasing under the
  comparer, which is exactly equal to "delivered in source order" only when
  the source itself is monotonic (e.g. `0, 1, 2, …`). If your source emits
  `[3, 1, 2]` and the assertion sees `[1, 2, 3]` delivered, that passes
  `PreserveOrder` even though it isn't the source's order — because
  `PreserveOrder` as a flow guarantee (`SEMANTICS.md`) is about not
  reordering relative to arrival for equal-comparing items, and the
  assertion checks the property it can check generically, not your specific
  fixture's arrival order. Use values whose natural order matches emission
  order (indices, timestamps) when the test's intent is "arrived in the
  order I sent them."
- **One `Should()` per flow.** It is a terminal sink; it consumes the flow
  the way any other sink does. If you need to observe results *and* assert
  concurrency, capture what you need inside the selector (or via
  `CaptureStatistics` and a snapshot) rather than trying to attach two sinks.
