using FluentAssertions;
using Xunit;

namespace Caudal.Core.Tests;

public class ValidationTests
{
    [Fact]
    public void ToFlow_rejects_a_non_positive_capacity()
    {
        var act = () => Enumerable.Range(0, 10).ToFlow(capacity: 0);
        act.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Fact]
    public void SelectAsync_rejects_a_non_positive_concurrency()
    {
        var flow = Enumerable.Range(0, 10).ToFlow();
        var act = () => flow.SelectAsync((i, _) => Task.FromResult(i), concurrency: 0);
        act.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Fact]
    public void WhereAsync_rejects_a_non_positive_concurrency()
    {
        var flow = Enumerable.Range(0, 10).ToFlow();
        var act = () => flow.WhereAsync((i, _) => Task.FromResult(true), concurrency: -1);
        act.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Fact]
    public void Null_sources_and_selectors_are_rejected()
    {
        var fromNull = () => Flow.From<int>(null!);
        fromNull.Should().Throw<ArgumentNullException>();

        var flow = Enumerable.Range(0, 10).ToFlow();
        var nullSelector = () => flow.SelectAsync((Func<int, CancellationToken, Task<int>>)null!);
        nullSelector.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void Foreign_IFlow_implementations_are_rejected()
    {
        var foreign = new ForeignFlow();
        var act = () => foreign.SelectAsync((i, _) => Task.FromResult(i));
        act.Should().Throw<ArgumentException>().WithMessage("*not created by Caudal*");
    }

    private sealed class ForeignFlow : IFlow<int>
    {
        public string? Name => null;
    }
}
