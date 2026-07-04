using System.Threading.RateLimiting;
using FluentAssertions;
using Xunit;

namespace Caudal.Core.Tests;

public class RateLimitingTests
{
    private static readonly int[] SixItemsInOrder = [0, 1, 2, 3, 4, 5];

    [Fact]
    public async Task RateLimit_paces_deliveries_to_the_permit_window()
    {
        // Window must be short: TryReplenish is a no-op until a full window has
        // elapsed, even with AutoReplenishment disabled. Delivery is still
        // impossible without an explicit TryReplenish call, which keeps the
        // pacing assertions deterministic.
        var limiter = new FixedWindowRateLimiter(new FixedWindowRateLimiterOptions
        {
            PermitLimit = 2,
            Window = TimeSpan.FromMilliseconds(50),
            QueueLimit = 1,
            QueueProcessingOrder = QueueProcessingOrder.OldestFirst,
            AutoReplenishment = false,
        });
        var delivered = new List<int>();

        var pipeline = Enumerable.Range(0, 6)
            .ToFlow()
            .RateLimit(() => limiter)
            .ForEachAsync((item, _) =>
            {
                lock (delivered)
                {
                    delivered.Add(item);
                }

                return Task.CompletedTask;
            });

        await WaitUntilCountAsync(delivered, 2);
        await Task.Delay(100).WaitAsync(TimeSpan.FromSeconds(10));
        CountOf(delivered).Should().Be(2);

        limiter.TryReplenish();
        await WaitUntilCountAsync(delivered, 4);

        // Let another window elapse so the next TryReplenish actually replenishes.
        await Task.Delay(100).WaitAsync(TimeSpan.FromSeconds(10));
        limiter.TryReplenish();
        await WaitUntilCountAsync(delivered, 6);

        await pipeline.WaitAsync(TimeSpan.FromSeconds(10));

        delivered.Should().Equal(SixItemsInOrder);
    }

    [Fact]
    public async Task RateLimit_disposes_the_limiter_when_the_pipeline_completes()
    {
        var limiter = new FixedWindowRateLimiter(new FixedWindowRateLimiterOptions
        {
            PermitLimit = 100,
            Window = TimeSpan.FromSeconds(1),
            QueueLimit = 10,
            QueueProcessingOrder = QueueProcessingOrder.OldestFirst,
            AutoReplenishment = true,
        });

        await Enumerable.Range(0, 5)
            .ToFlow()
            .RateLimit(() => limiter)
            .ToListAsync()
            .WaitAsync(TimeSpan.FromSeconds(10));

        FluentActions.Invoking(() => limiter.AttemptAcquire(1)).Should().Throw<ObjectDisposedException>();
    }

    [Fact]
    public async Task RateLimitBy_isolates_permits_per_key()
    {
        var items = new[] { "A1", "B1", "A2", "B2", "A3", "B3" };
        var delivered = new List<string>();
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        await items
            .ToFlow()
            .RateLimitBy(
                key => key[..1],
                () => PartitionedRateLimiter.Create<string, string>(key => RateLimitPartition.GetFixedWindowLimiter(
                    key,
                    _ => new FixedWindowRateLimiterOptions
                    {
                        PermitLimit = 1,
                        Window = TimeSpan.FromMilliseconds(50),
                        QueueLimit = 1,
                        QueueProcessingOrder = QueueProcessingOrder.OldestFirst,
                        AutoReplenishment = true,
                    })))
            .ForEachAsync(item =>
            {
                lock (delivered)
                {
                    delivered.Add(item);
                }

                return Task.CompletedTask;
            })
            .WaitAsync(TimeSpan.FromSeconds(10));

        var elapsed = stopwatch.Elapsed;

        delivered.Should().HaveCount(6);
        delivered.Where(item => item.StartsWith('A')).Should().Equal("A1", "A2", "A3");
        delivered.Where(item => item.StartsWith('B')).Should().Equal("B1", "B2", "B3");
        (elapsed >= TimeSpan.FromMilliseconds(100)).Should().BeTrue();
    }

    [Fact]
    public async Task Cancellation_while_waiting_for_a_permit_throws_operation_canceled()
    {
        var limiter = new FixedWindowRateLimiter(new FixedWindowRateLimiterOptions
        {
            PermitLimit = 1,
            Window = TimeSpan.FromHours(1),
            QueueLimit = 1,
            QueueProcessingOrder = QueueProcessingOrder.OldestFirst,
            AutoReplenishment = false,
        });
        using var cts = new CancellationTokenSource();
        var delivered = new List<int>();

        var pipeline = Enumerable.Range(0, 3)
            .ToFlow()
            .RateLimit(() => limiter)
            .ForEachAsync(
                (item, _) =>
                {
                    lock (delivered)
                    {
                        delivered.Add(item);
                    }

                    return Task.CompletedTask;
                },
                cts.Token);

        await WaitUntilCountAsync(delivered, 1);
        cts.Cancel();

        await FluentActions.Awaiting(() => pipeline)
            .Should().ThrowAsync<OperationCanceledException>()
            .WaitAsync(TimeSpan.FromSeconds(10));
    }

    [Fact]
    public void RateLimit_rejects_invalid_arguments()
    {
        var flow = Enumerable.Range(0, 10).ToFlow();

        FluentActions.Invoking(() => flow.RateLimit(0, TimeSpan.FromSeconds(1)))
            .Should().Throw<ArgumentOutOfRangeException>();
        FluentActions.Invoking(() => flow.RateLimit(1, TimeSpan.Zero))
            .Should().Throw<ArgumentOutOfRangeException>();
        FluentActions.Invoking(() => flow.RateLimitBy<int, string>(null!, 1, TimeSpan.FromSeconds(1)))
            .Should().Throw<ArgumentNullException>();
    }

    private static int CountOf<T>(List<T> list)
    {
        lock (list)
        {
            return list.Count;
        }
    }

    private static async Task WaitUntilCountAsync<T>(List<T> list, int count)
    {
        var deadline = System.Diagnostics.Stopwatch.StartNew();
        while (CountOf(list) < count)
        {
            if (deadline.Elapsed > TimeSpan.FromSeconds(5))
            {
                throw new TimeoutException($"Expected at least {count} delivered items, but only {CountOf(list)} arrived.");
            }

            await Task.Delay(10).WaitAsync(TimeSpan.FromSeconds(10));
        }
    }
}
