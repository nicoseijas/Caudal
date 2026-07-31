using System.Threading.Channels;
using Caudal.Internal;
using FluentAssertions;
using Microsoft.Extensions.Time.Testing;
using Xunit;

namespace Caudal.Core.Tests;

public class TimeOperatorTests
{
    private static readonly TimeSpan Step = TimeSpan.FromMilliseconds(100);
    private static readonly int[] TwoItems = [1, 2];

    [Fact]
    public async Task Debounce_collapses_a_burst_to_its_last_item()
    {
        var time = new FakeTimeProvider();
        var source = Channel.CreateUnbounded<string>();
        var delivered = new List<string>();

        var debounced = source.Reader
            .ToFlow(capacity: 8)
            .Debounce(Step, time);
        var stage = (DebounceFlow<string>)debounced.Node;

        var pipeline = debounced.ForEachAsync((item, _) =>
        {
            lock (delivered)
            {
                delivered.Add(item);
            }

            return Task.CompletedTask;
        });

        source.Writer.TryWrite("A");
        source.Writer.TryWrite("B");
        source.Writer.TryWrite("C");
        while (stage.ReplacedCount < 2)
        {
            await Task.Delay(10);
        }

        lock (delivered)
        {
            delivered.Should().BeEmpty("the clock is frozen, so the silence period cannot elapse");
        }

        while (CountOf(delivered) < 1)
        {
            time.Advance(Step);
            await Task.Delay(10);
        }

        lock (delivered)
        {
            // The burst collapses to its last item.
            delivered.Should().Equal("C");
        }

        // Completion flushes a pending item without needing the clock.
        source.Writer.TryWrite("D");
        source.Writer.Complete();
        await pipeline.WaitAsync(TimeSpan.FromSeconds(10));

        delivered.Should().Equal("C", "D");
        stage.ReplacedCount.Should().Be(2, "A and B were replaced within the burst");
    }

    [Fact]
    public async Task Throttle_emits_the_leading_item_and_drops_the_window()
    {
        var time = new FakeTimeProvider();
        var source = Channel.CreateUnbounded<int>();
        var delivered = new List<int>();

        var throttled = source.Reader
            .ToFlow(capacity: 8)
            .Throttle(Step, time);
        var stage = (ThrottleFlow<int>)throttled.Node;

        var pipeline = throttled.ForEachAsync((item, _) =>
        {
            lock (delivered)
            {
                delivered.Add(item);
            }

            return Task.CompletedTask;
        });

        for (var i = 1; i <= 5; i++)
        {
            source.Writer.TryWrite(i);
        }

        while (stage.DroppedCount < 4)
        {
            await Task.Delay(10);
        }

        lock (delivered)
        {
            delivered.Should().Equal(1);
        }

        time.Advance(Step);
        source.Writer.TryWrite(6);
        while (CountOf(delivered) < 2)
        {
            await Task.Delay(10);
        }

        source.Writer.Complete();
        await pipeline.WaitAsync(TimeSpan.FromSeconds(10));

        delivered.Should().Equal(1, 6);
        stage.DroppedCount.Should().Be(4);
    }

    [Fact]
    public async Task Sample_emits_the_latest_value_per_tick_and_flushes_on_completion()
    {
        var time = new FakeTimeProvider();
        var source = Channel.CreateUnbounded<string>();
        var delivered = new List<string>();

        var sampled = source.Reader
            .ToFlow(capacity: 8)
            .Sample(Step, time);
        var stage = (SampleFlow<string>)sampled.Node;

        var pipeline = sampled.ForEachAsync((item, _) =>
        {
            lock (delivered)
            {
                delivered.Add(item);
            }

            return Task.CompletedTask;
        });

        source.Writer.TryWrite("A");
        source.Writer.TryWrite("B");
        while (stage.ReplacedCount < 1)
        {
            await Task.Delay(10);
        }

        lock (delivered)
        {
            delivered.Should().BeEmpty("no tick has fired yet");
        }

        while (CountOf(delivered) < 1)
        {
            time.Advance(Step);
            await Task.Delay(10);
        }

        lock (delivered)
        {
            // The tick samples the latest value, replacing A.
            delivered.Should().Equal("B");
        }

        source.Writer.TryWrite("C");
        while (CountOf(delivered) < 2)
        {
            time.Advance(Step);
            await Task.Delay(10);
        }

        // Completion flushes the last unsampled value without a tick.
        source.Writer.TryWrite("D");
        source.Writer.Complete();
        await pipeline.WaitAsync(TimeSpan.FromSeconds(10));

        delivered.Should().Equal("B", "C", "D");
    }

    [Fact]
    public async Task TimeoutEach_faults_when_the_source_goes_silent()
    {
        var time = new FakeTimeProvider();
        var source = Channel.CreateUnbounded<int>();
        var delivered = new List<int>();

        var pipeline = source.Reader
            .ToFlow(capacity: 8, name: "quotes")
            .TimeoutEach(Step, time)
            .ForEachAsync((item, _) =>
            {
                lock (delivered)
                {
                    delivered.Add(item);
                }

                return Task.CompletedTask;
            });

        source.Writer.TryWrite(1);
        while (CountOf(delivered) < 1)
        {
            await Task.Delay(10);
        }

        // Source goes silent; advancing the clock past the timeout must fault.
        var advancing = Task.Run(async () =>
        {
            while (!pipeline.IsCompleted)
            {
                time.Advance(Step);
                await Task.Delay(10);
            }
        });

        var thrown = await FluentActions.Awaiting(() => pipeline)
            .Should().ThrowAsync<TimeoutException>()
            .WaitAsync(TimeSpan.FromSeconds(10));
        thrown.Which.Message.Should().Contain("quotes");

        await advancing;
        delivered.Should().Equal(1);
    }

    [Fact]
    public async Task TimeoutEach_lets_a_lively_source_flow_and_complete()
    {
        var time = new FakeTimeProvider();

        var results = await Enumerable.Range(0, 100)
            .ToFlow(capacity: 8)
            .TimeoutEach(Step, time)
            .ToListAsync()
            .WaitAsync(TimeSpan.FromSeconds(10));

        results.Should().Equal(Enumerable.Range(0, 100), "a frozen clock can never time out a flowing source");
    }

    [Fact]
    public async Task DelayEach_holds_items_until_the_clock_advances()
    {
        var time = new FakeTimeProvider();
        var delivered = new List<int>();

        var pipeline = TwoItems
            .ToFlow(capacity: 4)
            .DelayEach(Step, time)
            .ForEachAsync((item, _) =>
            {
                lock (delivered)
                {
                    delivered.Add(item);
                }

                return Task.CompletedTask;
            });

        // Delivery is impossible without advancing the fake clock.
        await Task.Delay(100);
        lock (delivered)
        {
            delivered.Should().BeEmpty();
        }

        while (!pipeline.IsCompleted)
        {
            time.Advance(Step);
            await Task.Delay(10);
        }

        await pipeline;
        delivered.Should().Equal(1, 2);
    }

    [Fact]
    public async Task BatchEvery_groups_whatever_arrived_in_the_window()
    {
        var time = new FakeTimeProvider();
        var source = Channel.CreateUnbounded<int>();
        var batches = new List<IReadOnlyList<int>>();

        var pipeline = source.Reader
            .ToFlow(capacity: 8)
            .BatchEvery(Step, timeProvider: time)
            .ForEachAsync((batch, _) =>
            {
                lock (batches)
                {
                    batches.Add(batch);
                }

                return Task.CompletedTask;
            });

        source.Writer.TryWrite(1);
        source.Writer.TryWrite(2);
        source.Writer.TryWrite(3);

        while (CountOf(batches) < 1)
        {
            time.Advance(Step);
            await Task.Delay(10);
        }

        source.Writer.TryWrite(4);
        source.Writer.Complete();
        await pipeline.WaitAsync(TimeSpan.FromSeconds(10));

        batches.SelectMany(b => b).Should().Equal(1, 2, 3, 4);
        batches[^1].Should().EndWith(4);
    }

    [Fact]
    public async Task Sample_coalesces_a_multi_interval_jump_into_a_single_tick()
    {
        var time = new FakeTimeProvider();
        var source = Channel.CreateUnbounded<string>();
        var delivered = new List<string>();

        var sampled = source.Reader
            .ToFlow(capacity: 8)
            .Sample(Step, time);
        var stage = (SampleFlow<string>)sampled.Node;

        var pipeline = sampled.ForEachAsync((item, _) =>
        {
            lock (delivered)
            {
                delivered.Add(item);
            }

            return Task.CompletedTask;
        });

        source.Writer.TryWrite("A");
        source.Writer.TryWrite("B");
        while (stage.ReplacedCount < 1)
        {
            await Task.Delay(10);
        }

        // One jump spanning ten intervals: exactly one tick, not ten replays.
        while (CountOf(delivered) < 1)
        {
            time.Advance(10 * Step);
            await Task.Delay(10);
        }

        await Task.Delay(100);
        lock (delivered)
        {
            delivered.Should().Equal("B");
        }

        source.Writer.Complete();
        await pipeline.WaitAsync(TimeSpan.FromSeconds(10));
        delivered.Should().Equal("B");
    }

    [Fact]
    public async Task TimeoutEach_still_fires_after_a_slow_drain_followed_by_silence()
    {
        var time = new FakeTimeProvider();
        var source = Channel.CreateUnbounded<int>();
        var delivered = new List<int>();
        var gate = new SemaphoreSlim(0);
        var firstEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var pipeline = source.Reader
            .ToFlow(capacity: 8)
            .TimeoutEach(Step, time)
            .ForEachAsync(async (item, ct) =>
            {
                firstEntered.TrySetResult();
                await gate.WaitAsync(ct);
                lock (delivered)
                {
                    delivered.Add(item);
                }
            });

        // A burst arrives, then the source goes silent forever.
        source.Writer.TryWrite(1);
        source.Writer.TryWrite(2);
        source.Writer.TryWrite(3);
        await firstEntered.Task;

        // The consumer is stalled mid-drain; advancing far past the timeout must
        // NOT fault (no silence is being measured while data is queued)...
        time.Advance(10 * Step);
        await Task.Delay(100);
        pipeline.IsCompleted.Should().BeFalse("a stalled drain is not upstream silence");

        // ...but once the drain finishes and real silence follows, the timeout
        // still fires — late, never masked.
        gate.Release(10);
        while (CountOf(delivered) < 3)
        {
            await Task.Delay(10);
        }

        var advancing = Task.Run(async () =>
        {
            while (!pipeline.IsCompleted)
            {
                time.Advance(Step);
                await Task.Delay(10);
            }
        });

        await FluentActions.Awaiting(() => pipeline)
            .Should().ThrowAsync<TimeoutException>()
            .WaitAsync(TimeSpan.FromSeconds(10));
        await advancing;

        delivered.Should().Equal(1, 2, 3);
    }

    [Fact]
    public async Task A_large_window_batch_grows_past_the_preallocation_cap()
    {
        var batches = await Enumerable.Range(0, 2_000)
            .ToFlow(capacity: 64)
            .BatchEvery(TimeSpan.FromHours(1))
            .ToListAsync()
            .WaitAsync(TimeSpan.FromSeconds(10));

        batches.Should().ContainSingle("the clock never ticks, so completion flushes one batch");
        batches[0].Should().HaveCount(2_000).And.BeInAscendingOrder();
    }

    [Fact]
    public async Task Cancellation_tears_down_throttle_and_delay_stages()
    {
        var produced = 0;
        using var cts = new CancellationTokenSource();
        var firstDelivered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var pipeline = Support.TestSources
            .Infinite(onYield: _ => Interlocked.Increment(ref produced))
            .ToFlow(capacity: 8)
            .Throttle(TimeSpan.FromTicks(1))
            .DelayEach(TimeSpan.FromTicks(1))
            .ForEachAsync(
                (_, _) =>
                {
                    firstDelivered.TrySetResult();
                    return Task.CompletedTask;
                },
                cts.Token);

        await firstDelivered.Task;
        cts.Cancel();

        await FluentActions.Awaiting(() => pipeline.WaitAsync(TimeSpan.FromSeconds(10)))
            .Should().ThrowAsync<OperationCanceledException>();

        var afterCancel = Volatile.Read(ref produced);
        await Task.Delay(200);
        Volatile.Read(ref produced).Should().Be(afterCancel, "the producer must stop through pass-through stages too");
    }

    [Fact]
    public void BatchEvery_rejects_invalid_arguments()
    {
        var flow = Enumerable.Range(0, 10).ToFlow();

        FluentActions.Invoking(() => flow.BatchEvery(TimeSpan.Zero)).Should().Throw<ArgumentOutOfRangeException>();
        FluentActions.Invoking(() => flow.BatchEvery(TimeSpan.FromSeconds(1), maximumSize: 0))
            .Should().Throw<ArgumentOutOfRangeException>();
    }

    [Fact]
    public async Task An_upstream_failure_passes_through_a_time_stage_unwrapped()
    {
        var time = new FakeTimeProvider();
        var boom = new InvalidOperationException("source failed");

        var act = () => Failing(boom)
            .ToFlow(capacity: 8)
            .Debounce(Step, time)
            .ToListAsync();

        var thrown = await act.Should()
            .ThrowAsync<InvalidOperationException>()
            .WaitAsync(TimeSpan.FromSeconds(10));
        thrown.Which.Should().BeSameAs(boom);
    }

    [Fact]
    public void Time_operators_reject_non_positive_durations()
    {
        var flow = Enumerable.Range(0, 10).ToFlow();

        FluentActions.Invoking(() => flow.Debounce(TimeSpan.Zero)).Should().Throw<ArgumentOutOfRangeException>();
        FluentActions.Invoking(() => flow.Throttle(TimeSpan.FromSeconds(-1))).Should().Throw<ArgumentOutOfRangeException>();
        FluentActions.Invoking(() => flow.Sample(TimeSpan.Zero)).Should().Throw<ArgumentOutOfRangeException>();
        FluentActions.Invoking(() => flow.TimeoutEach(TimeSpan.Zero)).Should().Throw<ArgumentOutOfRangeException>();
        FluentActions.Invoking(() => flow.DelayEach(TimeSpan.Zero)).Should().Throw<ArgumentOutOfRangeException>();
    }

    private static int CountOf<T>(List<T> list)
    {
        lock (list)
        {
            return list.Count;
        }
    }

    private static async IAsyncEnumerable<int> Failing(Exception exception)
    {
        yield return 1;
        await Task.Yield();
        throw exception;
    }
}
