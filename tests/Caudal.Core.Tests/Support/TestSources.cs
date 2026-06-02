using System.Runtime.CompilerServices;

namespace Caudal.Core.Tests.Support;

internal static class TestSources
{
    /// <summary>
    /// An endless source that reports every yielded item. Yields cooperatively so a
    /// blocked consumer can exert backpressure without starving the scheduler.
    /// </summary>
    public static async IAsyncEnumerable<int> Infinite(
        Action<int>? onYield = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        for (var i = 0; ; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            onYield?.Invoke(i);
            yield return i;
            await Task.Yield();
        }
    }

    public static async IAsyncEnumerable<int> Range(
        int count,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        for (var i = 0; i < count; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return i;

            if (i % 1024 == 0)
            {
                await Task.Yield();
            }
        }
    }
}

internal static class Quiesce
{
    /// <summary>
    /// Polls a counter until it stops changing across a full window, so assertions
    /// about "the producer stalled" don't depend on a single fixed sleep.
    /// </summary>
    public static async Task<int> WaitForStableAsync(
        Func<int> read,
        TimeSpan? window = null,
        TimeSpan? timeout = null)
    {
        var pollWindow = window ?? TimeSpan.FromMilliseconds(150);
        var deadline = System.Diagnostics.Stopwatch.StartNew();
        var limit = timeout ?? TimeSpan.FromSeconds(10);

        var last = read();
        while (deadline.Elapsed < limit)
        {
            await Task.Delay(pollWindow);
            var current = read();
            if (current == last)
            {
                return current;
            }

            last = current;
        }

        throw new TimeoutException("The counter never stabilized.");
    }
}

internal static class InterlockedMax
{
    public static void Update(ref int location, int value)
    {
        int current;
        while (value > (current = Volatile.Read(ref location)))
        {
            if (Interlocked.CompareExchange(ref location, value, current) == current)
            {
                break;
            }
        }
    }
}
