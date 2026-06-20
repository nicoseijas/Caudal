using System.Threading.Channels;

namespace Caudal.Internal;

/// <summary>
/// The standard stage pump: enumerate the upstream into a channel, completing the
/// channel normally on end-of-source and with the original exception on failure so
/// it reaches the consumer unwrapped.
/// </summary>
internal static class FlowPump
{
    public static async Task RunAsync<T>(
        FlowBase<T> upstream,
        ChannelWriter<T> writer,
        CancellationToken cancellationToken)
    {
        try
        {
            await foreach (var item in upstream.Enumerate(cancellationToken).ConfigureAwait(false))
            {
                await writer.WriteAsync(item, cancellationToken).ConfigureAwait(false);
            }

            writer.Complete();
        }
        catch (Exception ex)
        {
            writer.TryComplete(ex);
        }
    }
}
