using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace Caudal.Internal;

/// <summary>
/// A conflating stage: at most one pending item per key. A new arrival replaces the
/// pending item for its key instead of queueing behind it, so a slow consumer sees
/// the latest value per key rather than a growing backlog. This is the one stage
/// that intentionally absorbs upstream pressure — replacement, not waiting, is its
/// contract. Memory is bounded by the number of distinct keys, and every
/// replacement is counted.
/// </summary>
internal sealed class LatestByKeyFlow<T, TKey> : FlowBase<T>
    where TKey : notnull
{
    private readonly FlowBase<T> _upstream;
    private readonly Func<T, TKey> _keySelector;
    private readonly IEqualityComparer<TKey> _comparer;
    private long _replaced;

    internal LatestByKeyFlow(
        FlowBase<T> upstream,
        Func<T, TKey> keySelector,
        IEqualityComparer<TKey>? comparer)
        : base(upstream.Options)
    {
        _upstream = upstream;
        _keySelector = keySelector;
        _comparer = comparer ?? EqualityComparer<TKey>.Default;
    }

    /// <summary>How many pending items have been replaced by a newer arrival.</summary>
    internal long ReplacedCount => Interlocked.Read(ref _replaced);

    public override async IAsyncEnumerable<T> Enumerate(
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        // Invariant: a key is in this channel if and only if it has an entry in
        // `pending`, so the channel holds each key at most once and its effective
        // bound is the number of distinct keys.
        var readyKeys = Channel.CreateUnbounded<TKey>(new UnboundedChannelOptions
        {
            SingleWriter = true,
            SingleReader = true,
        });
        var pending = new Dictionary<TKey, T>(_comparer);
        var stateLock = new object();

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var pump = PumpAsync(readyKeys.Writer, pending, stateLock, cts.Token);

        try
        {
            await foreach (var key in readyKeys.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
            {
                T value;
                lock (stateLock)
                {
                    value = pending[key];
                    pending.Remove(key);
                }

                yield return value;
            }
        }
        finally
        {
            cts.Cancel();
            await TaskHelpers.IgnoreErrorsAsync(pump).ConfigureAwait(false);
        }
    }

    private async Task PumpAsync(
        ChannelWriter<TKey> writer,
        Dictionary<TKey, T> pending,
        object stateLock,
        CancellationToken cancellationToken)
    {
        try
        {
            await foreach (var item in _upstream.Enumerate(cancellationToken).ConfigureAwait(false))
            {
                var key = _keySelector(item);
                lock (stateLock)
                {
                    if (pending.ContainsKey(key))
                    {
                        pending[key] = item;
                        Interlocked.Increment(ref _replaced);
                    }
                    else
                    {
                        pending[key] = item;
                        writer.TryWrite(key);
                    }
                }
            }

            writer.Complete();
        }
        catch (Exception ex)
        {
            writer.TryComplete(ex);
        }
    }
}
