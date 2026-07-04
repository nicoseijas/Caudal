# Resilience and Rate Limiting

`Caudal.Resilience` and `Caudal.RateLimiting` add two things to a flow: the
ability to run a Polly v8 `ResiliencePipeline` around each selector
invocation, and the ability to pace a flow against a permit budget. Neither
package reimplements anything. Retry, timeout, circuit breaker, and fallback
are Polly's; rate limiting is `System.Threading.RateLimiting`'s. Caudal's job
is to wire these into the flow's worker slots and queues without breaking the
contracts in [`SEMANTICS.md`](SEMANTICS.md).

## The integration model

`Caudal.Resilience` does not have its own retry loop, its own timeout clock,
or its own circuit breaker state machine. It takes a `ResiliencePipeline` —
built however you already build one — and executes it around the selector
for each item:

```csharp
using Polly;
using Caudal;

var pipeline = new ResiliencePipelineBuilder()
    .AddRetry(new RetryStrategyOptions { MaxRetryAttempts = 3 })
    .AddTimeout(TimeSpan.FromSeconds(2))
    .Build();

await orders
    .ToFlow(capacity: 128)
    .SelectAsync(CallApiAsync, pipeline, concurrency: 8)
    .ForEachAsync(SaveAsync, ct);
```

Two entry points exist:

```csharp
IFlow<TResult> SelectAsync<TSource, TResult>(
    Func<TSource, CancellationToken, Task<TResult>> selector,
    ResiliencePipeline resiliencePipeline,
    int concurrency = 1,
    bool preserveOrder = false,
    FlowFailureMode failureMode = FlowFailureMode.Stop);

IFlow<FlowResult<TResult>> SelectResultAsync<TSource, TResult>(
    Func<TSource, CancellationToken, Task<TResult>> selector,
    ResiliencePipeline resiliencePipeline,
    int concurrency = 1,
    bool preserveOrder = false);
```

and one fluent form for a pipeline used across several stages:

```csharp
var resilient = orders.ToFlow(capacity: 128).WithResiliencePipeline(pipeline);
await resilient.SelectAsync(CallApiAsync, concurrency: 8).ForEachAsync(SaveAsync, ct);
```

`ResilientFlow<T>` just carries the flow and the pipeline together; its
`SelectAsync`/`SelectResultAsync` are the same operators with the pipeline
already bound.

It does not matter whether the `ResiliencePipeline` came from a
`ResiliencePipelineBuilder` used directly, or from
`Microsoft.Extensions.Resilience`'s `AddResiliencePipeline` registered in DI
and resolved through `ResiliencePipelineProvider<string>`. Both produce a
`ResiliencePipeline`, and Caudal only ever sees that type. If your app
already standardizes strategies through DI, keep doing that and hand the
resolved pipeline to `SelectAsync`.

## What each strategy means inside a stage

The pipeline wraps a single call to the selector for a single item, inside
that item's worker slot. What each strategy does with that placement:

- **Retry.** All attempts for one item happen inside that item's slot. The
  slot is occupied from the first attempt until the retry policy gives up or
  succeeds. See the Limitation section below — this is the one place where
  Caudal's integration falls short of the stated goal in `SEMANTICS.md`.
- **Timeout.** A Polly timeout strategy bounds one attempt's *execution*,
  the same way `processing.duration` is measured separately from
  `queue.duration` everywhere else in Caudal. Time the item spent waiting for
  a free worker slot is never counted against the timeout — the clock starts
  when the selector is actually invoked. This is a different guarantee from
  `TimeoutEach` (see [`time-operators.md`](time-operators.md)): `TimeoutEach`
  bounds silence *upstream*, between items arriving at a stage; a Polly
  timeout here bounds one item's own execution. Use `TimeoutEach` to detect a
  dead source, and a resilience timeout to bound a slow call.
- **Circuit breaker.** An open circuit means the pipeline throws
  `BrokenCircuitException` from the selector call, exactly like any other
  exception the selector could throw. It then follows the stage's ordinary
  `FlowFailureMode`: `Stop` faults the whole pipeline, `Skip` drops the item
  and counts it as `items.failed`, `Capture` records it in the item's
  `FlowResult`. Caudal adds no circuit-breaker-specific failure mode — an open
  circuit is just another exception to classify.
- **Fallback.** Runs inside the same call; if it succeeds, the selector
  effectively succeeded and the item proceeds normally.

## Cancellation is never a transient error

Polly's default `ShouldHandle` predicate does not retry
`OperationCanceledException`, and Caudal does not override that. On top of
that, Caudal's own cancellation classification (`SEMANTICS.md`, "How do
errors propagate") applies here too: an `OperationCanceledException` only
counts as the pipeline's own cancellation when it carries the pipeline's
token *and* that token was actually cancelled. Anything else — a
`TaskCanceledException` from an unrelated timeout, a cancellation your own
code triggers internally for reasons unrelated to pipeline shutdown — is an
ordinary failure and is free to be retried or to trip the breaker like any
other exception.

The practical effect: shutting down the flow's `CancellationToken` never
looks like a transient fault to the resilience pipeline. It does not
increment a retry counter, does not count toward `MinimumThroughput` for the
circuit breaker, and does not get wrapped or reclassified. It is cancellation,
full stop.

## Limitation: a retry holds its worker slot during backoff

`SEMANTICS.md` states the design goal plainly: a worker slot should be
released while a retry waits out its delay, so other items can use that
capacity. In 0.x, this is not what happens. The Polly pipeline executes
*inside* the item's worker slot from the first attempt through the last, and
`Task.Delay` between attempts runs there too. A `concurrency: 8` stage with a
3-attempt retry and a slow-to-recover dependency can end up with all 8 slots
parked in backoff, doing nothing, while queued items wait behind them.

This is a direct consequence of "integrate, don't reimplement." Releasing
the slot during backoff means Caudal would have to own the retry loop
itself — track attempt count and delay per item, re-queue the item after the
delay elapses, and resume it later — which is exactly the kind of
Polly-shaped logic this package exists to avoid rebuilding. Polly's
`ResiliencePipeline.ExecuteAsync` does not expose "call me back after the
delay instead of awaiting it yourself"; it owns the whole attempt loop as one
call. Handing back the slot mid-pipeline is not currently possible without
either forking Polly's execution model or duplicating it.

This is tracked as a known 0.x gap, not silently accepted. It will be
revisited before `1.0`, either by finding a supported extension point in
Polly or by accepting the re-queueing complexity if there is no other way to
keep the "integrate, don't reimplement" principle intact.

Until then:

- **Keep total retry time small relative to item latency.** If a call
  normally takes 50 ms, `MaxRetryAttempts: 3` with exponential backoff off a
  50 ms base delay costs at most a few hundred milliseconds of a held slot —
  tolerable. `MaxRetryAttempts: 5` with a 5-second base delay against a
  `concurrency: 8` stage can stall the whole stage for tens of seconds; don't
  do that.
- **For genuinely long backoffs, use `Capture` and re-enqueue externally.**
  `SelectResultAsync` with a pipeline that fails fast (few or no retries)
  turns a failure into a `FlowResult` instead of holding a slot. Your own code
  can inspect the failure and push the item onto a separate retry queue with
  its own schedule, outside the flow's worker pool entirely.

## Rate limiting

`Caudal.RateLimiting` adds a pacing stage built on
`System.Threading.RateLimiting`:

```csharp
using System.Threading.RateLimiting;
using Caudal;

IFlow<T> RateLimit<T>(int permitLimit, TimeSpan window);
IFlow<T> RateLimit<T>(Func<RateLimiter> limiterFactory);
IFlow<T> RateLimitBy<T, TKey>(Func<T, TKey> keySelector, int permitLimit, TimeSpan window)
    where TKey : notnull;
```

```csharp
await orders
    .ToFlow(capacity: 128)
    .RateLimit(permitLimit: 25, window: TimeSpan.FromSeconds(1))
    .SelectAsync(CallApiAsync, pipeline, concurrency: 8)
    .ForEachAsync(SaveAsync, ct);
```

The sugar overload (`permitLimit`, `window`) builds a `FixedWindowRateLimiter`
underneath. Pass a `limiterFactory` to use `SlidingWindowRateLimiter`,
`TokenBucketRateLimiter`, or any other `RateLimiter`; the flow owns the
disposal of whatever the factory returns, so you don't have to track its
lifetime yourself.

One limiter type deliberately does not fit here: `ConcurrencyLimiter`. The
stage acquires permits sequentially — at most one lease outstanding — and
releases each lease when the next item is requested, not when this item's
downstream processing finishes. A limiter whose permits are released by lease
disposal therefore degrades to a near no-op: it can never cap concurrent
downstream work from this position. To cap concurrent work, use the processing
stage's own `concurrency` parameter; to hold a permit for the duration of an
item's processing, acquire and dispose it inside the selector itself (the same
placement the retry section below recommends). Time-released limiters — fixed
window, sliding window, token bucket — behave exactly as expected.

`RateLimit` is a pacing stage, not a buffer. An item waiting for a permit is
exactly the kind of backpressure described in `SEMANTICS.md`'s answer to
"what happens when a stage fills up": the wait propagates upstream through
the flow's bounded buffers until the source itself suspends. There is no
separate internal queue inside `RateLimit` holding items past the flow's
existing capacity — it paces admission into the rest of the pipeline, it does
not add a place for items to pile up.

`RateLimitBy` partitions the limiter by key: each key gets its own permit
budget under the same `permitLimit`/`window`, useful for "25 requests/second
per exchange" rather than one shared budget across all exchanges.

## Choosing where to put the limiter

Put `RateLimit` *before* the stage it's protecting when the limit is "N
admissions per window," which is the common case — you want at most N items
entering the expensive stage per second, regardless of what happens to them
once they're in:

```csharp
.RateLimit(permitLimit: 25, window: TimeSpan.FromSeconds(1))
.SelectAsync(CallApiAsync, pipeline, concurrency: 8)
```

A permit here is spent on the *item*, not on each attempt Polly makes for
that item. If `CallApiAsync` fails and the resilience pipeline retries it
twice, those two extra attempts do not acquire two more permits — the item
already paid for admission once.

If the real constraint is "N requests per second including retries" (an
upstream API that counts every HTTP call, successful or not, against its own
rate limit), rate limiting before `SelectAsync` under-counts. In that case
acquire a permit from inside the selector itself, around each attempt, rather
than relying on the flow-level `RateLimit` stage:

```csharp
async Task<Response> CallApiAsync(Request request, CancellationToken ct)
{
    using var lease = await limiter.AcquireAsync(1, ct);
    if (!lease.IsAcquired)
    {
        throw new RateLimitLeaseException();
    }

    return await httpClient.PostAsJsonAsync(request, ct);
}
```

This moves the limiter into user code, but it's the only way to make retries
themselves count against the same budget the flow-level `RateLimit` stage
would otherwise apply only once per item.
