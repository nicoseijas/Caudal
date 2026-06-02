using Caudal.Internal;

namespace Caudal;

/// <summary>
/// Terminal operations. Awaiting a sink runs the pipeline; when the returned task
/// completes — successfully, faulted, or cancelled — no internal task is still running.
/// </summary>
public static class FlowSinkExtensions
{
    /// <summary>Runs the pipeline, invoking an async action for each item sequentially.</summary>
    public static async Task ForEachAsync<T>(
        this IFlow<T> flow,
        Func<T, CancellationToken, Task> action,
        CancellationToken cancellationToken = default)
    {
        var upstream = FlowBase<T>.FromFlow(flow, nameof(flow));
        ArgumentNullException.ThrowIfNull(action);

        await foreach (var item in upstream.Enumerate(cancellationToken).ConfigureAwait(false))
        {
            await action(item, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>Runs the pipeline, invoking an async action that does not observe cancellation.</summary>
    public static Task ForEachAsync<T>(
        this IFlow<T> flow,
        Func<T, Task> action,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(action);
        return flow.ForEachAsync((item, _) => action(item), cancellationToken);
    }

    /// <summary>Runs the pipeline and collects every result into a list.</summary>
    public static async Task<List<T>> ToListAsync<T>(
        this IFlow<T> flow,
        CancellationToken cancellationToken = default)
    {
        var upstream = FlowBase<T>.FromFlow(flow, nameof(flow));

        var results = new List<T>();
        await foreach (var item in upstream.Enumerate(cancellationToken).ConfigureAwait(false))
        {
            results.Add(item);
        }

        return results;
    }

    /// <summary>Runs the pipeline, discarding results. Useful when stages have side effects.</summary>
    public static async Task ConsumeAsync<T>(
        this IFlow<T> flow,
        CancellationToken cancellationToken = default)
    {
        var upstream = FlowBase<T>.FromFlow(flow, nameof(flow));

        await foreach (var _ in upstream.Enumerate(cancellationToken).ConfigureAwait(false))
        {
        }
    }
}
