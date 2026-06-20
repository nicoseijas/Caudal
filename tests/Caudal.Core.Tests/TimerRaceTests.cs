using System.Threading.Channels;
using Caudal.Internal;
using FluentAssertions;
using Microsoft.Extensions.Time.Testing;
using Xunit;

namespace Caudal.Core.Tests;

/// <summary>
/// TimerRace is the shared primitive behind Debounce, Sample, Batch, and
/// TimeoutEach; its contract is pinned down here directly, not only through them.
/// </summary>
public class TimerRaceTests
{
    private static readonly TimeSpan Delay = TimeSpan.FromMilliseconds(100);

    [Fact]
    public async Task Returns_Readable_immediately_when_an_item_is_available()
    {
        var time = new FakeTimeProvider();
        var channel = Channel.CreateUnbounded<int>();
        channel.Writer.TryWrite(42);

        var outcome = await TimerRace
            .WaitToReadOrTimeoutAsync(channel.Reader, Delay, time, CancellationToken.None)
            .WaitAsync(TimeSpan.FromSeconds(10));

        outcome.Should().Be(TimedWaitOutcome.Readable, "no clock advance is needed when data is ready");
    }

    [Fact]
    public async Task Returns_Completed_when_the_channel_is_done()
    {
        var time = new FakeTimeProvider();
        var channel = Channel.CreateUnbounded<int>();
        channel.Writer.Complete();

        var outcome = await TimerRace
            .WaitToReadOrTimeoutAsync(channel.Reader, Delay, time, CancellationToken.None)
            .WaitAsync(TimeSpan.FromSeconds(10));

        outcome.Should().Be(TimedWaitOutcome.Completed);
    }

    [Fact]
    public async Task Returns_TimerElapsed_when_the_clock_passes_the_deadline_first()
    {
        var time = new FakeTimeProvider();
        var channel = Channel.CreateUnbounded<int>();

        var race = TimerRace.WaitToReadOrTimeoutAsync(channel.Reader, Delay, time, CancellationToken.None);
        while (!race.IsCompleted)
        {
            time.Advance(Delay);
            await Task.Delay(10);
        }

        (await race).Should().Be(TimedWaitOutcome.TimerElapsed);
    }

    [Fact]
    public async Task A_channel_fault_rethrows_the_original_exception()
    {
        var time = new FakeTimeProvider();
        var boom = new InvalidOperationException("upstream failed");
        var channel = Channel.CreateUnbounded<int>();
        channel.Writer.TryComplete(boom);

        var act = () => TimerRace
            .WaitToReadOrTimeoutAsync(channel.Reader, Delay, time, CancellationToken.None)
            .WaitAsync(TimeSpan.FromSeconds(10));

        var thrown = await act.Should().ThrowAsync<InvalidOperationException>();
        thrown.Which.Should().BeSameAs(boom);
    }

    [Fact]
    public async Task External_cancellation_throws_with_the_callers_token_not_a_timer_tick()
    {
        var time = new FakeTimeProvider();
        var channel = Channel.CreateUnbounded<int>();
        using var cts = new CancellationTokenSource();

        var race = TimerRace.WaitToReadOrTimeoutAsync(channel.Reader, Delay, time, cts.Token);
        cts.Cancel();

        var thrown = await FluentActions.Awaiting(() => race.WaitAsync(TimeSpan.FromSeconds(10)))
            .Should().ThrowAsync<OperationCanceledException>();
        thrown.Which.CancellationToken.Should().Be(cts.Token);
    }
}
