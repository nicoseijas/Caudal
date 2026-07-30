using System.Threading.Channels;
using System.Threading.Tasks.Dataflow;
using BenchmarkDotNet.Attributes;

namespace Caudal.Benchmarks;

/// <summary>
/// Isolates a producer much faster than its consumer, comparing shapes that hold
/// backpressure at a fixed capacity against the anti-pattern of materializing
/// everything up front. The interesting scenario here is not raw speed: a bounded
/// Channel, TPL Dataflow, and Caudal all cap in-flight items at the same capacity
/// and pay a similar backpressure tax, so their Mean values should land close
/// together. <see cref="UnboundedListBuffering"/> is the honest counterexample —
/// it does the same total work with no capacity limit, so read its Allocated
/// column, not its Mean, to see the memory cost teams take on by accident when
/// they reach for a List instead of a bounded queue.
/// </summary>
[MemoryDiagnoser]
public class BackpressureBenchmarks
{
    private const int Capacity = 64;
    private const int YieldsPerItem = 16;

    [Params(2_000)]
    public int ItemCount { get; set; }

    private static async Task ConsumeAsync(int _)
    {
        for (var i = 0; i < YieldsPerItem; i++)
        {
            await Task.Yield();
        }
    }

    /// <summary>Hand-rolled baseline: a bounded Channel with one slow consumer.</summary>
    [Benchmark(Baseline = true)]
    public async Task<long> ManualChannelBounded()
    {
        var channel = Channel.CreateBounded<int>(Capacity);
        var producer = Task.Run(async () =>
        {
            for (var i = 0; i < ItemCount; i++)
            {
                await channel.Writer.WriteAsync(i).ConfigureAwait(false);
            }

            channel.Writer.Complete();
        });

        long checksum = 0;
        await foreach (var item in channel.Reader.ReadAllAsync().ConfigureAwait(false))
        {
            await ConsumeAsync(item).ConfigureAwait(false);
            checksum += item;
        }

        await producer.ConfigureAwait(false);
        return checksum;
    }

    /// <summary>TPL Dataflow with an equivalent bounded capacity, backpressure via SendAsync.</summary>
    [Benchmark]
    public async Task<long> TplDataflowBounded()
    {
        long checksum = 0;
        var block = new ActionBlock<int>(
            async item =>
            {
                await ConsumeAsync(item).ConfigureAwait(false);
                Interlocked.Add(ref checksum, item);
            },
            new ExecutionDataflowBlockOptions { BoundedCapacity = Capacity, MaxDegreeOfParallelism = 1 });

        for (var i = 0; i < ItemCount; i++)
        {
            await block.SendAsync(i).ConfigureAwait(false);
        }

        block.Complete();
        await block.Completion.ConfigureAwait(false);
        return checksum;
    }

    /// <summary>Caudal's bounded flow: same capacity, same slow consumer.</summary>
    [Benchmark]
    public async Task<long> CaudalBounded()
    {
        long checksum = 0;
        await Enumerable.Range(0, ItemCount)
            .ToFlow(capacity: Capacity)
            .ForEachAsync(async (item, _) =>
            {
                await ConsumeAsync(item).ConfigureAwait(false);
                checksum += item;
            })
            .ConfigureAwait(false);
        return checksum;
    }

    /// <summary>
    /// The anti-pattern datum: the producer materializes everything into a List
    /// before the consumer starts, so there is no backpressure at all. Same total
    /// work as the bounded variants above — the point is the Allocated column, not
    /// Mean. This is the shape teams fall into accidentally when a fast producer
    /// outruns a slow consumer and nobody puts a capacity on the queue.
    /// </summary>
    [Benchmark]
    public async Task<long> UnboundedListBuffering()
    {
        var buffered = new List<int>(ItemCount);
        for (var i = 0; i < ItemCount; i++)
        {
            buffered.Add(i);
        }

        long checksum = 0;
        foreach (var item in buffered)
        {
            await ConsumeAsync(item).ConfigureAwait(false);
            checksum += item;
        }

        return checksum;
    }
}
