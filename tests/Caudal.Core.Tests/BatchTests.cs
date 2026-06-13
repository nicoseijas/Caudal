using System.Threading.Channels;
using FluentAssertions;
using Microsoft.Extensions.Time.Testing;
using Xunit;

namespace Caudal.Core.Tests;

public class BatchTests
{
    [Fact]
    public async Task A_batch_is_emitted_when_it_reaches_maximum_size()
    {
        var batches = await Enumerable.Range(0, 10)
            .ToFlow(capacity: 16)
            .Batch(maximumSize: 3, maximumDelay: TimeSpan.FromHours(1))
            .ToListAsync();

        batches.Select(b => b.Count).Should().Equal(3, 3, 3, 1);
        batches.SelectMany(b => b).Should().Equal(Enumerable.Range(0, 10), "batching must not lose or reorder items");
    }

    [Fact]
    public async Task A_batch_is_emitted_when_its_maximum_delay_elapses()
    {
        var time = new FakeTimeProvider();
        var source = Channel.CreateUnbounded<int>();
        var batches = new List<IReadOnlyList<int>>();
        var firstBatch = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var pipeline = source.Reader
            .ToFlow(capacity: 8)
            .Batch(maximumSize: 10, maximumDelay: TimeSpan.FromMilliseconds(100), time)
            .ForEachAsync((batch, _) =>
            {
                batches.Add(batch);
                firstBatch.TrySetResult();
                return Task.CompletedTask;
            });

        source.Writer.TryWrite(1);
        source.Writer.TryWrite(2);

        // Advance the fake clock until the open batch's timer fires. The polling
        // loop only exists to let the batch stage observe the items and arm its
        // timer; the emission itself is driven purely by the fake clock.
        while (!firstBatch.Task.IsCompleted)
        {
            time.Advance(TimeSpan.FromMilliseconds(100));
            await Task.Delay(10);
        }

        batches.Should().ContainSingle().Which.Should().Equal(1, 2);

        source.Writer.Complete();
        await pipeline;
    }

    [Fact]
    public async Task Source_completion_flushes_the_final_partial_batch()
    {
        var source = Channel.CreateUnbounded<int>();
        source.Writer.TryWrite(7);
        source.Writer.TryWrite(8);
        source.Writer.Complete();

        var batches = await source.Reader
            .ToFlow(capacity: 8)
            .Batch(maximumSize: 100, maximumDelay: TimeSpan.FromHours(1))
            .ToListAsync();

        batches.Should().ContainSingle().Which.Should().Equal(7, 8);
    }

    [Fact]
    public async Task Batching_a_large_stream_loses_nothing()
    {
        var batches = await Enumerable.Range(0, 5_000)
            .ToFlow(capacity: 64)
            .Batch(maximumSize: 50, maximumDelay: TimeSpan.FromSeconds(10))
            .ToListAsync();

        batches.SelectMany(b => b).Should().Equal(Enumerable.Range(0, 5_000));
        batches.Should().OnlyContain(b => b.Count <= 50);
    }

    [Fact]
    public async Task An_upstream_failure_reaches_the_consumer_through_the_batch_stage()
    {
        var boom = new InvalidOperationException("source failed");

        var act = () => Failing(boom)
            .ToFlow(capacity: 8)
            .Batch(maximumSize: 10, maximumDelay: TimeSpan.FromHours(1))
            .ToListAsync();

        var thrown = await act.Should().ThrowAsync<InvalidOperationException>();
        thrown.Which.Should().BeSameAs(boom);
    }

    [Fact]
    public void Batch_rejects_invalid_arguments()
    {
        var flow = Enumerable.Range(0, 10).ToFlow();

        var badSize = () => flow.Batch(maximumSize: 0, maximumDelay: TimeSpan.FromSeconds(1));
        badSize.Should().Throw<ArgumentOutOfRangeException>();

        var badDelay = () => flow.Batch(maximumSize: 10, maximumDelay: TimeSpan.Zero);
        badDelay.Should().Throw<ArgumentOutOfRangeException>();
    }

    private static async IAsyncEnumerable<int> Failing(Exception exception)
    {
        yield return 1;
        await Task.Yield();
        throw exception;
    }
}
