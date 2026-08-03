using System.Collections.Concurrent;
using System.Threading.Channels;
using Caudal.Internal;
using FluentAssertions;
using Xunit;

namespace Caudal.Core.Tests;

public class SelectLatestByKeyTests
{
    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(10);

    /// <summary>The values of key A that may have started while A@1 was still executing.</summary>
    private static readonly int[] OnlyTheFirstValue = [1];

    /// <summary>The values of key A that must have executed once the stage drains.</summary>
    private static readonly int[] TheFirstAndTheLatestValue = [1, 4];

    [Fact]
    public async Task A_key_never_executes_twice_at_once_and_resumes_with_the_latest_value()
    {
        var source = Channel.CreateUnbounded<(string Key, int Value)>();
        var started = new ConcurrentQueue<int>();
        var firstStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var active = 0;
        var maximumActive = 0;

        var flow = source.Reader
            .ToFlow(capacity: 16)
            .SelectLatestByKeyAsync(
                x => x.Key,
                async (x, ct) =>
                {
                    RecordMaximum(ref maximumActive, Interlocked.Increment(ref active));
                    started.Enqueue(x.Value);
                    firstStarted.TrySetResult();
                    if (x.Value == 1)
                    {
                        await gate.Task.WaitAsync(Patience, ct);
                    }

                    Interlocked.Decrement(ref active);
                    return x.Value;
                },
                concurrency: 4,
                maximumKeys: 8);

        var delivered = new List<int>();
        var pipeline = flow.ForEachAsync((value, _) =>
        {
            delivered.Add(value);
            return Task.CompletedTask;
        });

        source.Writer.TryWrite(("A", 1));
        await firstStarted.Task.WaitAsync(Patience);

        // A@1 is stuck in the selector. These three all target the same key, so they
        // must conflate into one waiting value instead of starting alongside it —
        // even though the stage has three idle workers and would happily run them.
        source.Writer.TryWrite(("A", 2));
        source.Writer.TryWrite(("A", 3));
        source.Writer.TryWrite(("A", 4));

        var stage = (SelectLatestByKeyFlow<(string Key, int Value), int, string>)flow.Node;
        while (stage.ReplacedCount < 2)
        {
            await Task.Delay(10);
        }

        // All three have been admitted and A@1 is still running: if the stage let a key
        // overlap with itself, a second execution would already have begun here.
        started.Should().Equal(OnlyTheFirstValue, "no later value for A may start while A@1 is executing");

        // Completing upstream while A is mid-execution with a successor waiting: the
        // stage must still run that successor before it ends.
        source.Writer.Complete();
        gate.TrySetResult();
        await pipeline.WaitAsync(Patience);

        started.Should().Equal(TheFirstAndTheLatestValue, "A@2 and A@3 were replaced by A@4 before the key freed up");
        delivered.Should().Equal(1, 4);
        maximumActive.Should().Be(1, "one key was in play, so nothing may have run in parallel");
        stage.ReplacedCount.Should().Be(2);
    }

    [Fact]
    public async Task Distinct_keys_execute_in_parallel_up_to_the_concurrency_bound()
    {
        const int concurrency = 4;
        var allInFlight = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var entered = 0;

        // Every selector blocks until `concurrency` of them are inside at the same
        // time. Serializing by key must not serialize across keys: if fewer than four
        // could run at once, nothing would ever release and this would time out.
        var results = await Enumerable.Range(0, concurrency)
            .ToFlow(capacity: 8)
            .SelectLatestByKeyAsync(
                i => i,
                async (i, ct) =>
                {
                    if (Interlocked.Increment(ref entered) == concurrency)
                    {
                        allInFlight.TrySetResult();
                    }

                    await allInFlight.Task.WaitAsync(Patience, ct);
                    return i;
                },
                concurrency: concurrency,
                maximumKeys: concurrency)
            .ToListAsync()
            .WaitAsync(Patience);

        results.Should().BeEquivalentTo(Enumerable.Range(0, concurrency));
    }

    [Fact]
    public async Task Replacements_of_an_executing_key_never_count_against_the_limit()
    {
        var source = Channel.CreateUnbounded<int>();
        var firstStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var flow = source.Reader
            .ToFlow(capacity: 16)
            .SelectLatestByKeyAsync(
                _ => 0,
                async (value, ct) =>
                {
                    firstStarted.TrySetResult();
                    if (value == 1)
                    {
                        await gate.Task.WaitAsync(Patience, ct);
                    }

                    return value;
                },
                concurrency: 2,
                maximumKeys: 1);

        var delivered = new List<int>();
        var pipeline = flow.ForEachAsync((value, _) =>
        {
            delivered.Add(value);
            return Task.CompletedTask;
        });

        source.Writer.TryWrite(1);
        await firstStarted.Task.WaitAsync(Patience);

        // The one tracked key is executing, so every one of these lands in its pending
        // slot. None is a new key, so none may overflow a limit of exactly one key.
        for (var i = 2; i <= 50; i++)
        {
            source.Writer.TryWrite(i);
        }

        var stage = (SelectLatestByKeyFlow<int, int, int>)flow.Node;
        while (stage.ReplacedCount < 48)
        {
            await Task.Delay(10);
        }

        source.Writer.Complete();
        gate.TrySetResult();
        await pipeline.WaitAsync(Patience);

        delivered.Should().Equal(1, 50);
        stage.ReplacedCount.Should().Be(48);
    }

    [Fact]
    public async Task A_new_key_past_the_limit_faults_the_pipeline_with_FlowKeyCapacityException()
    {
        // Every item is its own key and the selector is far slower than the pump, so
        // the tracked set must hit the cap and reject the next new key.
        var act = () => Enumerable.Range(0, 1_000)
            .ToFlow(capacity: 64)
            .SelectLatestByKeyAsync(
                i => i,
                async (i, ct) =>
                {
                    await Task.Delay(20, ct);
                    return i;
                },
                concurrency: 2,
                maximumKeys: 8)
            .ConsumeAsync();

        await act.Should().ThrowAsync<FlowKeyCapacityException>().WaitAsync(Patience);
    }

    [Fact]
    public async Task A_source_failure_reaches_the_consumer_unchanged()
    {
        var boom = new InvalidOperationException("source failed");

        var act = () => Failing(boom)
            .ToFlow(capacity: 8)
            .SelectLatestByKeyAsync(i => i % 3, (i, _) => Task.FromResult(i), concurrency: 2, maximumKeys: 4)
            .ConsumeAsync();

        var thrown = await act.Should().ThrowAsync<InvalidOperationException>();
        thrown.Which.Should().BeSameAs(boom);
    }

    [Fact]
    public async Task A_selector_failure_under_Stop_faults_with_the_original_exception()
    {
        var boom = new InvalidOperationException("selector failed");

        var act = () => Enumerable.Range(0, 100)
            .ToFlow(capacity: 8)
            .SelectLatestByKeyAsync(
                i => i,
                (i, _) => i == 7 ? throw boom : Task.FromResult(i),
                concurrency: 1,
                maximumKeys: 16)
            .ConsumeAsync();

        var thrown = await act.Should().ThrowAsync<InvalidOperationException>();
        thrown.Which.Should().BeSameAs(boom);
    }

    [Fact]
    public async Task A_key_selector_failure_faults_the_pipeline_with_the_original_exception()
    {
        var boom = new InvalidOperationException("bad key");

        var act = () => Enumerable.Range(0, 100)
            .ToFlow(capacity: 8)
            .SelectLatestByKeyAsync(
                i => i == 50 ? throw boom : i,
                (i, _) => Task.FromResult(i),
                concurrency: 2,
                // Above the item count on purpose: every item is its own key, so a
                // tighter bound would fault on capacity before reaching the bad key.
                maximumKeys: 128)
            .ConsumeAsync();

        var thrown = await act.Should().ThrowAsync<InvalidOperationException>();
        thrown.Which.Should().BeSameAs(boom);
    }

    [Fact]
    public async Task Every_replacement_is_counted_and_never_reported_as_an_emission()
    {
        var source = Channel.CreateUnbounded<int>();
        var options = new FlowOptions { CaptureStatistics = true, Capacity = 16 };
        var firstStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var flow = source.Reader
            .ToFlow(options)
            .SelectLatestByKeyAsync(
                _ => 0,
                async (value, ct) =>
                {
                    firstStarted.TrySetResult();
                    if (value == 1)
                    {
                        await gate.Task.WaitAsync(Patience, ct);
                    }

                    return value;
                },
                concurrency: 2,
                maximumKeys: 4);

        var pipeline = flow.ConsumeAsync();

        source.Writer.TryWrite(1);
        await firstStarted.Task.WaitAsync(Patience);
        for (var i = 2; i <= 20; i++)
        {
            source.Writer.TryWrite(i);
        }

        var stage = (SelectLatestByKeyFlow<int, int, int>)flow.Node;
        while (stage.ReplacedCount < 18)
        {
            await Task.Delay(10);
        }

        source.Writer.Complete();
        gate.TrySetResult();
        await pipeline.WaitAsync(Patience);

        var stats = flow.Node.Stats;
        stats.Should().NotBeNull();

        // A conflating stage has no in-equals-out invariant: what the gap must be
        // accounted for by is the replacement count, never silent loss.
        stats!.InputsReceived.Should().Be(20);
        stats.OutputsEmitted.Should().Be(2, "only value 1 and the latest survivor executed");
        stats.InputsReplaced.Should().Be(18);
        (stats.OutputsEmitted + stats.InputsReplaced).Should().Be(stats.InputsReceived);
    }

    [Fact]
    public async Task A_selector_failure_under_Skip_releases_the_key_for_its_next_value()
    {
        var source = Channel.CreateUnbounded<int>();
        var failed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var flow = source.Reader
            .ToFlow(capacity: 8)
            .SelectLatestByKeyAsync(
                _ => "the-only-key",
                (value, _) =>
                {
                    if (value != 1)
                    {
                        return Task.FromResult(value);
                    }

                    failed.TrySetResult();
                    throw new InvalidOperationException("selector failed");
                },
                concurrency: 1,
                maximumKeys: 1,
                failureMode: FlowFailureMode.Skip);

        var pipeline = flow.ToListAsync();

        source.Writer.TryWrite(1);
        await failed.Task.WaitAsync(Patience);

        // The dropped item must not strand its key: the next value for it still runs.
        source.Writer.TryWrite(2);
        source.Writer.Complete();

        var delivered = await pipeline.WaitAsync(Patience);
        delivered.Should().Equal(2);
    }

    [Fact]
    public async Task Cancellation_stops_the_stage_without_orphaning_its_workers()
    {
        using var cts = new CancellationTokenSource();
        var running = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var observedCancellation = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var act = () => Enumerable.Range(0, 1_000)
            .ToFlow(capacity: 8)
            .SelectLatestByKeyAsync(
                i => i,
                async (i, ct) =>
                {
                    running.TrySetResult();
                    try
                    {
                        await Task.Delay(Timeout.Infinite, ct);
                    }
                    catch (OperationCanceledException)
                    {
                        observedCancellation.TrySetResult();
                        throw;
                    }

                    return i;
                },
                concurrency: 2,
                maximumKeys: 16)
            .ConsumeAsync(cts.Token);

        var pipeline = act.Should().ThrowAsync<OperationCanceledException>();
        await running.Task.WaitAsync(Patience);
        await cts.CancelAsync();

        await pipeline.WaitAsync(Patience);
        await observedCancellation.Task.WaitAsync(Patience);
    }

    [Theory]
    [InlineData(0, 4)]
    [InlineData(-1, 4)]
    [InlineData(2, 0)]
    [InlineData(2, -1)]
    public void Concurrency_and_maximumKeys_below_one_are_rejected(int concurrency, int maximumKeys)
    {
        var flow = Enumerable.Range(0, 10).ToFlow(capacity: 8);

        var act = () => flow.SelectLatestByKeyAsync(
            i => i, (i, _) => Task.FromResult(i), concurrency, maximumKeys);

        act.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Fact]
    public void Capture_is_rejected_because_this_operator_does_not_express_it()
    {
        var flow = Enumerable.Range(0, 10).ToFlow(capacity: 8);

        var act = () => flow.SelectLatestByKeyAsync(
            i => i, (i, _) => Task.FromResult(i), concurrency: 2, maximumKeys: 4, failureMode: FlowFailureMode.Capture);

        act.Should().Throw<ArgumentException>();
    }


    private static void RecordMaximum(ref int maximum, int candidate)
    {
        var observed = Volatile.Read(ref maximum);
        while (candidate > observed)
        {
            var previous = Interlocked.CompareExchange(ref maximum, candidate, observed);
            if (previous == observed)
            {
                return;
            }

            observed = previous;
        }
    }

    private static async IAsyncEnumerable<int> Failing(Exception exception)
    {
        yield return 1;
        yield return 2;
        await Task.Yield();
        throw exception;
    }
}
