using FluentAssertions;
using Xunit;

namespace Caudal.Core.Tests;

public class SinkAndOperatorTests
{
    [Fact]
    public async Task ForEachAsync_invokes_the_action_sequentially_in_order()
    {
        var seen = new List<int>();
        var active = 0;

        await Enumerable.Range(0, 50)
            .ToFlow(capacity: 8)
            .ForEachAsync(async (i, _) =>
            {
                Interlocked.Increment(ref active).Should().Be(1, "the sink is sequential");
                seen.Add(i);
                await Task.Yield();
                Interlocked.Decrement(ref active);
            });

        seen.Should().Equal(Enumerable.Range(0, 50));
    }

    [Fact]
    public async Task ToListAsync_returns_every_item()
    {
        var results = await Enumerable.Range(0, 1000)
            .ToFlow(capacity: 16)
            .ToListAsync();

        results.Should().Equal(Enumerable.Range(0, 1000));
    }

    [Fact]
    public async Task ConsumeAsync_drains_the_whole_flow()
    {
        var processed = 0;

        await Enumerable.Range(0, 500)
            .ToFlow(capacity: 16)
            .SelectAsync(
                (i, _) =>
                {
                    Interlocked.Increment(ref processed);
                    return Task.FromResult(i);
                },
                concurrency: 4)
            .ConsumeAsync();

        processed.Should().Be(500);
    }

    [Fact]
    public async Task WhereAsync_filters_and_keeps_relative_order()
    {
        var results = await Enumerable.Range(0, 100)
            .ToFlow(capacity: 16)
            .WhereAsync(
                async (i, ct) =>
                {
                    await Task.Yield();
                    return i % 2 == 0;
                },
                concurrency: 4)
            .ToListAsync();

        results.Should().Equal(Enumerable.Range(0, 50).Select(i => i * 2));
    }

    [Fact]
    public async Task ChannelReader_source_flows_end_to_end()
    {
        var channel = System.Threading.Channels.Channel.CreateUnbounded<int>();
        for (var i = 0; i < 10; i++)
        {
            channel.Writer.TryWrite(i);
        }

        channel.Writer.Complete();

        var results = await channel.Reader
            .ToFlow(capacity: 4)
            .SelectAsync((i, _) => Task.FromResult(i + 100))
            .ToListAsync();

        results.Should().Equal(Enumerable.Range(100, 10));
    }

    [Fact]
    public async Task Flow_name_is_exposed()
    {
        var flow = Enumerable.Range(0, 1)
            .ToFlow(capacity: 4, name: "market-data");

        flow.Name.Should().Be("market-data");
        await flow.ConsumeAsync();
    }
}
