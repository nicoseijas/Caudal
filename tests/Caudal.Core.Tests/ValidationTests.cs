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

    // A foreign Flow<T> implementation can no longer be written: Flow<T> is sealed
    // with an internal constructor, so the type system — not a runtime check —
    // excludes external implementations.
}
