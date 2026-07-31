using System.Runtime.CompilerServices;
using Caudal.Core.Tests.Support;
using Caudal.Internal;
using FluentAssertions;
using Xunit;

namespace Caudal.Core.Tests;

public class MergeAndSelectManyTests
{
    private static readonly int[] MergedWithSlowSource = [0, 1, 2, 3, 4, 1000];

    [Fact]
    public async Task Merge_delivers_every_item_from_every_source()
    {
        var merged = Flow.Merge(
            Enumerable.Range(0, 100).ToFlow(capacity: 8),
            Enumerable.Range(100, 100).ToFlow(capacity: 8),
            Enumerable.Range(200, 100).ToFlow(capacity: 8));

        var results = await merged.ToListAsync();

        results.Should().HaveCount(300);
        results.Should().BeEquivalentTo(Enumerable.Range(0, 300));
    }

    [Fact]
    public async Task Merge_completes_only_when_all_sources_complete()
    {
        var slowRelease = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var merged = Flow.Merge(
            Enumerable.Range(0, 5).ToFlow(capacity: 4),
            Gated(slowRelease.Task).ToFlow(capacity: 4));

        var pipeline = merged.ToListAsync();
        await Task.Delay(100);
        pipeline.IsCompleted.Should().BeFalse("one source has not completed yet");

        slowRelease.TrySetResult();
        var results = await pipeline;
        results.Should().BeEquivalentTo(MergedWithSlowSource);
    }

    [Fact]
    public async Task Merge_first_source_failure_stops_the_rest_with_the_original_exception()
    {
        var boom = new InvalidOperationException("exchange down");
        var produced = 0;

        var act = () => Flow.Merge(
                TestSources.Infinite(onYield: _ => Interlocked.Increment(ref produced)).ToFlow(capacity: 8),
                Failing(boom).ToFlow(capacity: 8))
            .ConsumeAsync();

        var thrown = await act.Should()
            .ThrowAsync<InvalidOperationException>()
            .WaitAsync(TimeSpan.FromSeconds(10));
        thrown.Which.Should().BeSameAs(boom);

        var afterFault = Volatile.Read(ref produced);
        await Task.Delay(200);
        Volatile.Read(ref produced).Should().Be(afterFault, "the healthy source must be cancelled by the failure");
    }

    [Fact]
    public async Task Merge_does_not_hang_when_a_source_throws_a_spurious_cancellation()
    {
        // The rogue source throws an OperationCanceledException that FORWARDS the
        // token it was given, without any cancellation having happened. That is a
        // source failure, not teardown: the merge must reach a terminal state and
        // stop its healthy infinite sibling instead of hanging forever.
        var produced = 0;

        var act = () => Flow.Merge(
                TestSources.Infinite(onYield: _ => Interlocked.Increment(ref produced)).ToFlow(capacity: 8),
                new Flow<int>(new SpuriousCancellationFlow()))
            .ConsumeAsync();

        await act.Should()
            .ThrowAsync<OperationCanceledException>("the forwarded OCE is the original failure")
            .WaitAsync(TimeSpan.FromSeconds(10));

        var afterFault = Volatile.Read(ref produced);
        await Task.Delay(200);
        Volatile.Read(ref produced).Should().Be(afterFault, "the healthy source must be stopped by the failure");
    }

    [Fact]
    public async Task Merge_simultaneous_failures_terminate_with_one_original_exception()
    {
        var failures = new[]
        {
            new InvalidOperationException("exchange A down"),
            new InvalidOperationException("exchange B down"),
        };

        var act = () => Flow.Merge(
                Failing(failures[0]).ToFlow(capacity: 4),
                Failing(failures[1]).ToFlow(capacity: 4))
            .ConsumeAsync();

        var thrown = await act.Should()
            .ThrowAsync<InvalidOperationException>()
            .WaitAsync(TimeSpan.FromSeconds(10));

        failures.Should().Contain(thrown.Which, "the winner must be one of the original exceptions");
    }

    [Fact]
    public void Merge_requires_at_least_one_flow()
    {
        var act = () => Flow.Merge<int>();
        act.Should().Throw<ArgumentException>();
    }

    [Fact]
    public async Task SelectManyAsync_flattens_sub_sequences_in_source_order()
    {
        var results = await Enumerable.Range(1, 3)
            .ToFlow(capacity: 8)
            .SelectManyAsync((i, _) => Expand(i))
            .ToListAsync();

        results.Should().Equal(1, 10, 2, 20, 3, 30);
    }

    [Fact]
    public async Task SelectManyAsync_supports_materialized_sequences()
    {
        var results = await Enumerable.Range(1, 3)
            .ToFlow(capacity: 8)
            .SelectManyAsync(async (i, ct) =>
            {
                await Task.Yield();
                return Enumerable.Repeat(i, i);
            })
            .ToListAsync();

        results.Should().Equal(1, 2, 2, 3, 3, 3);
    }

    [Fact]
    public async Task SelectManyAsync_propagates_a_sub_sequence_failure()
    {
        var boom = new InvalidOperationException("expansion failed");

        var act = () => Enumerable.Range(1, 5)
            .ToFlow(capacity: 8)
            .SelectManyAsync((i, _) => i == 3 ? Failing(boom) : Expand(i))
            .ToListAsync();

        var thrown = await act.Should().ThrowAsync<InvalidOperationException>();
        thrown.Which.Should().BeSameAs(boom);
    }

    private static async IAsyncEnumerable<int> Expand(int i)
    {
        yield return i;
        await Task.Yield();
        yield return i * 10;
    }

    private static async IAsyncEnumerable<int> Failing(Exception exception)
    {
        yield return 1;
        await Task.Yield();
        throw exception;
    }

    private static async IAsyncEnumerable<int> Gated(Task release)
    {
        await release;
        yield return 1000;
    }

    private sealed class SpuriousCancellationFlow : FlowBase<int>
    {
        public SpuriousCancellationFlow()
            : base(null, "SpuriousSource", new FlowOptions())
        {
        }

        public override async IAsyncEnumerable<int> Enumerate(
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            yield return 1;
            await Task.Yield();
            throw new OperationCanceledException("spurious", null, cancellationToken);
        }
    }
}
