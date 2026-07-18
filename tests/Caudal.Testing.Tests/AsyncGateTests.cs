using FluentAssertions;
using Xunit;

namespace Caudal.Testing.Tests;

public class AsyncGateTests
{
    private static readonly TimeSpan ShortWait = TimeSpan.FromMilliseconds(50);
    private static readonly TimeSpan BoundedWait = TimeSpan.FromSeconds(10);

    [Fact]
    public async Task A_closed_gate_blocks_a_waiter()
    {
        var gate = new AsyncGate();

        var waiting = gate.WaitAsync();
        await Task.Delay(ShortWait);

        waiting.IsCompleted.Should().BeFalse();
    }

    [Fact]
    public async Task Release_completes_exactly_one_waiter_fifo()
    {
        var gate = new AsyncGate();
        var first = gate.WaitAsync();
        var second = gate.WaitAsync();
        await gate.WhenWaitersAsync(2).WaitAsync(BoundedWait);

        gate.Release(1);

        await first.WaitAsync(BoundedWait);
        await Task.Delay(ShortWait);
        second.IsCompleted.Should().BeFalse();
    }

    [Fact]
    public async Task Release_with_surplus_banks_permits_for_future_waiters()
    {
        var gate = new AsyncGate();
        var first = gate.WaitAsync();
        await gate.WhenWaitersAsync(1).WaitAsync(BoundedWait);

        gate.Release(3);
        await first.WaitAsync(BoundedWait);

        // Two banked permits let the next two WaitAsync calls complete without a
        // further Release.
        await gate.WaitAsync().WaitAsync(BoundedWait);
        await gate.WaitAsync().WaitAsync(BoundedWait);

        var thirdAfterPermits = gate.WaitAsync();
        await Task.Delay(ShortWait);
        thirdAfterPermits.IsCompleted.Should().BeFalse();
    }

    [Fact]
    public async Task Open_releases_all_queued_and_future_waiters()
    {
        var gate = new AsyncGate();
        var queued = gate.WaitAsync();
        await gate.WhenWaitersAsync(1).WaitAsync(BoundedWait);

        gate.Open();

        await queued.WaitAsync(BoundedWait);
        await gate.WaitAsync().WaitAsync(BoundedWait);
    }

    [Fact]
    public async Task Close_after_open_blocks_again_and_clears_banked_permits()
    {
        var gate = new AsyncGate();
        gate.Open();
        gate.Release(5);

        gate.Close();

        var waiting = gate.WaitAsync();
        await Task.Delay(ShortWait);
        waiting.IsCompleted.Should().BeFalse();
    }

    [Fact]
    public async Task WaitingCount_tracks_currently_blocked_waiters()
    {
        var gate = new AsyncGate();

        gate.WaitingCount.Should().Be(0);

        var first = gate.WaitAsync();
        var second = gate.WaitAsync();
        await gate.WhenWaitersAsync(2).WaitAsync(BoundedWait);

        gate.WaitingCount.Should().Be(2);

        gate.Release(2);
        await Task.WhenAll(first, second).WaitAsync(BoundedWait);

        gate.WaitingCount.Should().Be(0);
    }

    [Fact]
    public async Task WhenWaitersAsync_completes_only_once_the_threshold_is_reached()
    {
        var gate = new AsyncGate();

        _ = gate.WaitAsync();
        _ = gate.WaitAsync();
        var threshold = gate.WhenWaitersAsync(3);

        await Task.Delay(ShortWait);
        threshold.IsCompleted.Should().BeFalse();

        _ = gate.WaitAsync();

        await threshold.WaitAsync(BoundedWait);
    }

    [Fact]
    public async Task WhenWaitersAsync_completes_immediately_when_already_satisfied()
    {
        var gate = new AsyncGate();
        _ = gate.WaitAsync();
        await gate.WhenWaitersAsync(1).WaitAsync(BoundedWait);

        var alreadySatisfied = gate.WhenWaitersAsync(1);

        alreadySatisfied.IsCompleted.Should().BeTrue();
    }

    [Fact]
    public async Task A_cancelled_waiter_leaves_the_queue()
    {
        var gate = new AsyncGate();
        using var cts = new CancellationTokenSource();

        var waiting = gate.WaitAsync(cts.Token);
        await gate.WhenWaitersAsync(1).WaitAsync(BoundedWait);

        cts.Cancel();

        var act = () => waiting.WaitAsync(BoundedWait);
        var thrown = await act.Should().ThrowAsync<OperationCanceledException>();
        thrown.Which.CancellationToken.Should().Be(cts.Token);

        await Task.Delay(ShortWait);
        gate.WaitingCount.Should().Be(0);
    }
}
