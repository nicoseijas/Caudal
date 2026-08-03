using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace Caudal.Internal;

/// <summary>
/// Latest-wins execution per key: at most one selector invocation runs for a given
/// key at a time, at most one replaceable value waits behind it, and a global
/// concurrency bound applies across all keys. When an execution finishes, the latest
/// value that arrived for that key while it ran is what executes next — everything
/// received in between is replaced, never queued.
///
/// This stage owns the selector on purpose. <see cref="LatestByKeyFlow{T, TKey}"/>
/// conflates only until it hands a value downstream, so it cannot know whether the
/// previous value for a key is still being processed; a stage that schedules the work
/// itself can. That is the whole reason this operator exists as its own stage rather
/// than as a <c>LatestByKey</c> followed by a <c>SelectAsync</c>.
///
/// Like <c>LatestByKey</c>, it intentionally absorbs upstream pressure by replacement
/// rather than by waiting, and its memory is bounded by <c>maximumKeys</c> tracked
/// keys — executing or waiting. A genuinely new key arriving once that bound is
/// reached faults the pipeline with <see cref="FlowKeyCapacityException"/>.
/// </summary>
internal sealed class SelectLatestByKeyFlow<TSource, TResult, TKey> : FlowBase<TResult>
    where TKey : notnull
{
    private readonly FlowBase<TSource> _upstream;
    private readonly Func<TSource, TKey> _keySelector;
    private readonly Func<TSource, CancellationToken, Task<StageResult<TResult>>> _selector;
    private readonly int _concurrency;
    private readonly int _maximumKeys;
    private readonly IEqualityComparer<TKey> _comparer;
    private volatile KeyTable? _table;

    internal SelectLatestByKeyFlow(
        FlowBase<TSource> upstream,
        Func<TSource, TKey> keySelector,
        Func<TSource, CancellationToken, Task<StageResult<TResult>>> selector,
        int concurrency,
        int maximumKeys,
        IEqualityComparer<TKey>? comparer)
        : base(upstream, "SelectLatestByKeyAsync", upstream.Options)
    {
        _upstream = upstream;
        _keySelector = keySelector;
        _selector = selector;
        _concurrency = concurrency;
        _maximumKeys = maximumKeys;
        _comparer = comparer ?? EqualityComparer<TKey>.Default;
    }

    /// <summary>How many values were replaced by a newer arrival before executing.</summary>
    internal long ReplacedCount => _table?.ReplacedCount ?? 0;

    /// <summary>
    /// One tracked key. <c>Active</c> means a selector invocation for it is running (or
    /// its result is still being handed downstream); <c>HasPending</c> means a value is
    /// waiting to run next for it. An entry exists only while at least one holds, so
    /// the three reachable states are executing, waiting, and executing-with-a-successor.
    /// </summary>
    private readonly record struct KeyState(bool Active, bool HasPending, TSource? Pending);

    public override async IAsyncEnumerable<TResult> Enumerate(
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        Stats?.MarkStarted();

        // Invariant: a key is in `readyKeys` if and only if it has a pending value and
        // no active execution. So the channel holds each key at most once, its
        // effective bound is the number of tracked keys, and — since `KeyTable` refuses
        // a new key past `_maximumKeys` while holding its lock — a write to it can
        // never be refused for lack of capacity.
        var readyKeys = Channel.CreateBounded<TKey>(new BoundedChannelOptions(_maximumKeys));
        var output = Channel.CreateBounded<TResult>(new BoundedChannelOptions(Options.Capacity)
        {
            SingleReader = true,
        });

        var table = new KeyTable(readyKeys.Writer, _maximumKeys, _comparer, Stats);
        _table = table;

        if (Stats is { } stats)
        {
            stats.ConfiguredConcurrency = _concurrency;
            stats.QueueCapacity = _maximumKeys;

            // Values waiting for a worker. A key that is executing with nothing behind
            // it holds a slot against `maximumKeys` but is not queued work, so it is
            // deliberately not counted here.
            stats.QueueLengthProbe = () => table.PendingCount;
        }

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        var pump = PumpAsync(table, cts.Token);
        var workers = new Task[_concurrency];
        for (var i = 0; i < workers.Length; i++)
        {
            workers[i] = WorkerAsync(readyKeys.Reader, output.Writer, table, cts);
        }

        var completion = CompleteWhenDrainedAsync(workers, output.Writer);

        try
        {
            await foreach (var result in output.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
            {
                yield return result;
            }
        }
        finally
        {
            cts.Cancel();
            await TaskHelpers.IgnoreErrorsAsync(pump).ConfigureAwait(false);
            await TaskHelpers.IgnoreErrorsAsync(completion).ConfigureAwait(false);
        }
    }

    private async Task PumpAsync(KeyTable table, CancellationToken cancellationToken)
    {
        try
        {
            await foreach (var item in _upstream.Enumerate(cancellationToken).ConfigureAwait(false))
            {
                Stats?.InputReceived();

                // Never blocks: this stage sheds by replacement, so upstream is bounded
                // by the source's own capacity, never by how slow the selector is.
                table.Offer(_keySelector(item), item);
            }

            table.CompleteInput();
        }
        catch (Exception ex)
        {
            table.Fault(ex);
        }
    }

    private async Task WorkerAsync(
        ChannelReader<TKey> readyKeys,
        ChannelWriter<TResult> writer,
        KeyTable table,
        CancellationTokenSource cts)
    {
        try
        {
            await foreach (var key in readyKeys.ReadAllAsync(cts.Token).ConfigureAwait(false))
            {
                var item = table.Take(key);
                try
                {
                    await ExecuteAsync(item, writer, cts.Token).ConfigureAwait(false);
                }
                finally
                {
                    // Unconditional, including on failure and teardown: a key left
                    // marked active would strand its slot against `maximumKeys` and
                    // block every later value for it forever.
                    table.Release(key);
                }
            }
        }
        catch (OperationCanceledException oce)
            when (oce.CancellationToken == cts.Token && cts.Token.IsCancellationRequested)
        {
            // The stage is tearing down; its own cancellation is not a failure and must
            // not race to become the pipeline's terminal exception. Matching on the
            // exception's token keeps a foreign OperationCanceledException — a
            // selector's internal timeout, say — classified as an ordinary failure.
            throw;
        }
        catch (Exception ex)
        {
            // First failure wins: it becomes the pipeline's terminal exception and
            // promptly cancels the source, the sibling workers, and their in-flight work.
            Stats?.InputFailed();
            if (writer.TryComplete(ex))
            {
                cts.Cancel();
            }

            throw;
        }
    }

    private async Task ExecuteAsync(TSource item, ChannelWriter<TResult> writer, CancellationToken cancellationToken)
    {
        Stats?.WorkerStarted();
        var t0 = Stats is null ? 0 : Stopwatch.GetTimestamp();
        StageResult<TResult> result;
        try
        {
            result = await _selector(item, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            if (t0 != 0)
            {
                Stats?.RecordProcessingTicks(Stopwatch.GetTimestamp() - t0);
            }

            Stats?.WorkerFinished();
        }

        if (result.Emit)
        {
            // Delivered before the caller releases the key, so a key's results reach
            // the consumer in the order they executed, and a value arriving while this
            // one is still being handed downstream conflates instead of overtaking it.
            await writer.WriteAsync(result.Value!, cancellationToken).ConfigureAwait(false);
            Stats?.OutputEmitted();
        }
        else if (result.Failed)
        {
            Stats?.InputFailed();
        }
        else
        {
            // Emit=false, Failed=false: a predicate rejected this input — a filter
            // miss, never counted as failure.
            Stats?.InputFiltered();
        }
    }

    private static async Task CompleteWhenDrainedAsync(Task[] workers, ChannelWriter<TResult> writer)
    {
        try
        {
            await Task.WhenAll(workers).ConfigureAwait(false);
            writer.TryComplete();
        }
        catch (Exception ex)
        {
            writer.TryComplete(ex);
        }
    }

    /// <summary>
    /// The per-key scheduling state: which keys are executing, which have a value
    /// waiting, and which are ready to run. Every transition happens under one lock and
    /// nothing is awaited while holding it. The serialization guarantee lives entirely
    /// here: a key reaches the ready channel only from <see cref="Offer"/> (when it was
    /// untracked) or from <see cref="Release"/> (when its execution just ended), so it
    /// can never be handed to two workers at once.
    /// </summary>
    private sealed class KeyTable
    {
        private readonly Dictionary<TKey, KeyState> _keys;
        private readonly ChannelWriter<TKey> _ready;
        private readonly int _maximumKeys;
        private readonly StageStats? _stats;
        private readonly object _gate = new();
        private bool _inputCompleted;
        private bool _readyClosed;
        private long _replaced;
        private int _pending;

        internal KeyTable(
            ChannelWriter<TKey> ready,
            int maximumKeys,
            IEqualityComparer<TKey> comparer,
            StageStats? stats)
        {
            _ready = ready;
            _maximumKeys = maximumKeys;
            _stats = stats;
            _keys = new Dictionary<TKey, KeyState>(comparer);
        }

        internal long ReplacedCount => Interlocked.Read(ref _replaced);

        /// <summary>Values waiting for a worker; excludes the values currently executing.</summary>
        internal int PendingCount => Volatile.Read(ref _pending);

        /// <summary>
        /// Admits one upstream value. Called only by the single pump task, and never
        /// after <see cref="CompleteInput"/> — <see cref="Enqueue"/>'s tolerance of an
        /// already-closed channel depends on that ordering, so a second producer here
        /// would turn a dropped key into silently lost work.
        ///
        /// A key that is already executing keeps its slot and
        /// the value lands in its single pending slot; a key that already has a value
        /// waiting has that value replaced outright. Only a genuinely untracked key can
        /// overflow <c>maximumKeys</c>, so a hot key is never a capacity problem.
        /// </summary>
        internal void Offer(TKey key, TSource value)
        {
            lock (_gate)
            {
                if (_keys.TryGetValue(key, out var state))
                {
                    Debug.Assert(state.Active || state.HasPending, "A tracked key is always executing, waiting, or both.");

                    if (state.HasPending)
                    {
                        Interlocked.Increment(ref _replaced);
                        _stats?.InputReplaced();
                    }
                    else
                    {
                        Interlocked.Increment(ref _pending);
                    }

                    // No enqueue in either case. A waiting key is already in the ready
                    // channel, and an executing key is claimed by the worker that will
                    // release it — enqueueing here is exactly how a key would end up
                    // running twice.
                    _keys[key] = state with { HasPending = true, Pending = value };
                    return;
                }

                if (_keys.Count >= _maximumKeys)
                {
                    throw new FlowKeyCapacityException(
                        $"SelectLatestByKeyAsync is tracking {_maximumKeys} keys and received a new one. Raise maximumKeys, reduce key cardinality, or drain faster.");
                }

                _keys[key] = new KeyState(Active: false, HasPending: true, Pending: value);
                Interlocked.Increment(ref _pending);
                Enqueue(key);
            }
        }

        /// <summary>Claims the waiting value of a key a worker just dequeued, and marks it executing.</summary>
        internal TSource Take(TKey key)
        {
            lock (_gate)
            {
                var state = _keys[key];
                Debug.Assert(
                    state is { Active: false, HasPending: true },
                    "Only a waiting, non-executing key is ever in the ready channel.");

                _keys[key] = new KeyState(Active: true, HasPending: false, Pending: default);
                Interlocked.Decrement(ref _pending);
                return state.Pending!;
            }
        }

        /// <summary>
        /// Ends one execution. A value that arrived while it ran becomes the next
        /// execution for that key — that requeue, and nothing else, is what serializes
        /// a key. A key with nothing waiting stops being tracked, which is what frees
        /// its slot against <c>maximumKeys</c>.
        /// </summary>
        internal void Release(TKey key)
        {
            lock (_gate)
            {
                var state = _keys[key];
                if (state.HasPending)
                {
                    _keys[key] = state with { Active = false };
                    Enqueue(key);
                    return;
                }

                _keys.Remove(key);
                CompleteIfDrained();
            }
        }

        /// <summary>
        /// Signals that upstream is done. The stage cannot end here: keys may still be
        /// executing, and each of those may still have a successor to run.
        /// </summary>
        internal void CompleteInput()
        {
            lock (_gate)
            {
                _inputCompleted = true;
                CompleteIfDrained();
            }
        }

        /// <summary>Ends the stage with an upstream or key-selector failure.</summary>
        internal void Fault(Exception exception)
        {
            lock (_gate)
            {
                _readyClosed = true;
                _ready.TryComplete(exception);
            }
        }

        private void CompleteIfDrained()
        {
            if (!_inputCompleted || _keys.Count != 0)
            {
                return;
            }

            _readyClosed = true;
            _ready.TryComplete();
        }

        private void Enqueue(TKey key)
        {
            if (_readyClosed)
            {
                // Already ending, by drain or by fault. Silently dropping the key is
                // correct only here: nothing will run it, and throwing from a teardown
                // path would mask the failure that started the teardown.
                return;
            }

            if (!_ready.TryWrite(key))
            {
                // Unreachable: capacity is `maximumKeys`, a key is in the channel at
                // most once, and the channel is open. A refusal would mean the
                // invariant above is broken, which must not degrade into a key whose
                // pending value never runs.
                throw new InvalidOperationException(
                    "SelectLatestByKeyAsync scheduling invariant violated: the ready-key channel refused a key.");
            }
        }
    }
}
