using FluentAssertions;
using Xunit;

namespace Caudal.Testing.Tests;

public class TestFlowSourceTests
{
    private static readonly int[] ThreeItems = [1, 2, 3];

    [Fact]
    public async Task Emitted_items_flow_end_to_end_in_emission_order()
    {
        var source = new TestFlowSource<int>();
        var flow = source.ToFlow();

        var pending = flow.ToListAsync();

        source.EmitRange(ThreeItems);
        source.Complete();

        var results = await pending.WaitAsync(TimeSpan.FromSeconds(10));

        results.Should().Equal(ThreeItems);
    }

    [Fact]
    public async Task Fail_delivers_the_same_exception_instance_to_the_consumer()
    {
        var source = new TestFlowSource<int>();
        var boom = new InvalidOperationException("boom");
        var flow = source.ToFlow();

        var pending = flow.ToListAsync();

        source.Emit(1);
        source.Fail(boom);

        var act = () => pending.WaitAsync(TimeSpan.FromSeconds(10));

        var thrown = await act.Should().ThrowAsync<InvalidOperationException>();
        thrown.Which.Should().BeSameAs(boom);
    }

    [Fact]
    public void Emit_after_complete_throws()
    {
        var source = new TestFlowSource<int>();
        source.Complete();

        var act = () => source.Emit(1);

        act.Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public void Emit_after_fail_throws()
    {
        var source = new TestFlowSource<int>();
        source.Fail(new InvalidOperationException("boom"));

        var act = () => source.Emit(1);

        act.Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public void Complete_after_complete_throws()
    {
        var source = new TestFlowSource<int>();
        source.Complete();

        var act = () => source.Complete();

        act.Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public void A_second_call_to_ToFlow_throws()
    {
        var source = new TestFlowSource<int>();
        source.ToFlow();

        var act = () => source.ToFlow();

        act.Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public void PendingCount_reflects_items_emitted_before_any_sink_attaches()
    {
        var source = new TestFlowSource<int>();

        source.EmitRange(ThreeItems);

        source.PendingCount.Should().Be(3);
    }

    [Fact]
    public async Task PendingCount_drains_to_zero_once_the_flow_is_fully_consumed()
    {
        var source = new TestFlowSource<int>();
        source.EmitRange(ThreeItems);
        source.Complete();

        await source.ToFlow().ToListAsync().WaitAsync(TimeSpan.FromSeconds(10));

        source.PendingCount.Should().Be(0);
    }
}
