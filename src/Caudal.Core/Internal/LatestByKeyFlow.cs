using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace Caudal.Internal;

/// <summary>
/// A conflating stage: at most one pending item per key. A new arrival replaces the
/// pending item for its key instead of queueing behind it, so a slow consumer sees
/// the latest value per key rather than a growing backlog. This is the one stage
/// that intentionally absorbs upstream pressure — replacement, not waiting, is its
/// contract. Memory is bounded by <c>maximumKeys</c> distinct pending keys, and
/// every replacement is counted. A new key arriving once that bound is reached
/// faults the pipeline with <see cref="FlowKeyCapacityException"/> instead of
/// growing without limit.
///
/// The conflation window ends at emission: the key leaves <c>pending</c> when this
/// stage yields its value, not when a downstream consumer finishes with it. A stage
/// separate from the selector cannot know the latter, which is what
/// <see cref="SelectLatestByKeyFlow{TSource, TResult, TKey}"/> exists to provide.
/// </summary>
internal sealed class LatestByKeyFlow<T, TKey> : FlowBase<T>
    where TKey : notnull
{
    private readonly FlowBase<T> _upstream;
    private readonly Func<T, TKey> _keySelector;
    private readonly int _maximumKeys;
    private readonly KeyOverflowMode _overflowMode;
    private readonly IEqualityComparer<TKey> _comparer;
    private long _replaced;
    private int _pendingCount;

    internal LatestByKeyFlow(
        FlowBase<T> upstream,
        Func<T, TKey> keySelector,
        int maximumKeys,
        KeyOverflowMode overflowMode,
        IEqualityComparer<TKey>? comparer)
        : base(upstream, "LatestByKey", upstream.Options)
    {
        _upstream = upstream;
        _keySelector = keySelector;
        _maximumKeys = maximumKeys;
        _overflowMode = overflowMode;
        _comparer = comparer ?? EqualityComparer<TKey>.Default;
    }

    /// <summary>How many pending items have been replaced by a newer arrival.</summary>
    internal long ReplacedCount => Interlocked.Read(ref _replaced);

    public override async IAsyncEnumerable<T> Enumerate(
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        Stats?.MarkStarted();

        // Invariant: a key is in this channel if and only if it has an entry in
        // `pending`, so the channel holds each key at most once and its effective
        // bound is the number of distinct keys. That bound is now structural, not
        // just documented: the channel's capacity is `_maximumKeys`, and the pump
        // below refuses a new key past that limit while holding `stateLock`, so
        // `writer.TryWrite` below can never fail for lack of capacity.
        var readyKeys = Channel.CreateBounded<TKey>(new BoundedChannelOptions(_maximumKeys)
        {
            SingleWriter = true,
            SingleReader = true,
        });
        var pending = new Dictionary<TKey, T>(_comparer);
        var stateLock = new object();

        if (Stats is { } stats)
        {
            stats.QueueCapacity = _maximumKeys;
            stats.QueueLengthProbe = () => Volatile.Read(ref _pendingCount);
        }

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
                    Interlocked.Decrement(ref _pendingCount);
                }

                Stats?.OutputEmitted();
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
                Stats?.InputReceived();
                var key = _keySelector(item);
                lock (stateLock)
                {
                    if (pending.ContainsKey(key))
                    {
                        pending[key] = item;
                        Interlocked.Increment(ref _replaced);
                        Stats?.InputReplaced();
                    }
                    else
                    {
                        if (pending.Count >= _maximumKeys)
                        {
                            // Reject is the only overflow policy today; _overflowMode is
                            // stored so a future policy (e.g. evicting the oldest key) has
                            // somewhere to branch without changing the stage's shape.
                            Debug.Assert(_overflowMode == KeyOverflowMode.Reject, "Reject is the only supported KeyOverflowMode.");
                            throw new FlowKeyCapacityException(
                                $"LatestByKey is tracking {_maximumKeys} pending keys and received a new one. Raise maximumKeys, reduce key cardinality, or drain faster.");
                        }

                        pending[key] = item;
                        Interlocked.Increment(ref _pendingCount);
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
