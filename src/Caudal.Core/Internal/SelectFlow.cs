using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace Caudal.Internal;

/// <summary>
/// A concurrent async transformation stage. Unordered mode uses a worker pool over a
/// shared input channel; ordered mode queues one task per item so results are
/// delivered in source order while at most <c>Concurrency</c> items execute.
/// The selector returns a <see cref="StageResult{T}"/>, which lets filtering and
/// failure policies decide per item whether anything is emitted. An exception that
/// escapes the selector wrapper completes the output channel with the original
/// exception and cancels the rest of the stage (Stop semantics).
/// </summary>
internal sealed class SelectFlow<TSource, TResult> : FlowBase<TResult>
{
    private readonly FlowBase<TSource> _upstream;
    private readonly Func<TSource, CancellationToken, Task<StageResult<TResult>>> _selector;
    private readonly SelectAsyncOptions _selectOptions;

    internal SelectFlow(
        FlowBase<TSource> upstream,
        string operatorName,
        Func<TSource, CancellationToken, Task<StageResult<TResult>>> selector,
        SelectAsyncOptions selectOptions)
        : base(upstream, operatorName, upstream.Options)
    {
        _upstream = upstream;
        _selector = selector;
        _selectOptions = selectOptions;
    }

    /// <summary>An item waiting in the unordered path's input channel, stamped with the time it was enqueued.</summary>
    private readonly record struct InputItem(TSource Item, long EnqueuedTimestamp);

    public override IAsyncEnumerable<TResult> Enumerate(CancellationToken cancellationToken)
    {
        if (Stats is { } stats)
        {
            stats.ConfiguredConcurrency = _selectOptions.Concurrency;
            stats.QueueCapacity = Options.Capacity;
        }

        return _selectOptions.PreserveOrder
            ? EnumerateOrdered(cancellationToken)
            : EnumerateUnordered(cancellationToken);
    }

    private async IAsyncEnumerable<TResult> EnumerateUnordered(
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        Stats?.MarkStarted();

        var input = Channel.CreateBounded<InputItem>(new BoundedChannelOptions(1)
        {
            SingleWriter = true,
        });
        var output = Channel.CreateBounded<TResult>(new BoundedChannelOptions(Options.Capacity)
        {
            SingleReader = true,
        });

        if (Stats is { } stats)
        {
            stats.QueueLengthProbe = () => output.Reader.Count;
        }

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        var pump = PumpUnorderedAsync(input.Writer, cts.Token);
        var workers = new Task[_selectOptions.Concurrency];
        for (var i = 0; i < workers.Length; i++)
        {
            workers[i] = WorkerAsync(input.Reader, output.Writer, cts);
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

    private async Task PumpUnorderedAsync(
        ChannelWriter<InputItem> writer,
        CancellationToken cancellationToken)
    {
        try
        {
            await foreach (var item in _upstream.Enumerate(cancellationToken).ConfigureAwait(false))
            {
                var timestamp = Stats is null ? 0 : Stopwatch.GetTimestamp();
                await writer.WriteAsync(new InputItem(item, timestamp), cancellationToken).ConfigureAwait(false);
            }

            writer.Complete();
        }
        catch (Exception ex)
        {
            writer.TryComplete(ex);
        }
    }

    private async Task WorkerAsync(
        ChannelReader<InputItem> reader,
        ChannelWriter<TResult> writer,
        CancellationTokenSource cts)
    {
        try
        {
            await foreach (var entry in reader.ReadAllAsync(cts.Token).ConfigureAwait(false))
            {
                Stats?.ItemReceived();
                if (entry.EnqueuedTimestamp != 0)
                {
                    Stats?.RecordQueueTicks(Stopwatch.GetTimestamp() - entry.EnqueuedTimestamp);
                }

                Stats?.WorkerStarted();
                var t0 = Stats is null ? 0 : Stopwatch.GetTimestamp();
                StageResult<TResult> result;
                try
                {
                    result = await _selector(entry.Item, cts.Token).ConfigureAwait(false);
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
                    await writer.WriteAsync(result.Value!, cts.Token).ConfigureAwait(false);
                    Stats?.ItemCompleted();
                }
                else if (result.Failed)
                {
                    Stats?.ItemFailed();
                }
            }
        }
        catch (OperationCanceledException oce)
            when (oce.CancellationToken == cts.Token && cts.Token.IsCancellationRequested)
        {
            // The stage is tearing down; its own cancellation is not a failure and
            // must not race to become the pipeline's terminal exception. Matching on
            // the exception's token (not the ambient flag) keeps a foreign
            // OperationCanceledException — e.g. a selector's internal timeout —
            // classified as an ordinary failure even mid-teardown.
            throw;
        }
        catch (Exception ex)
        {
            // First failure wins: it becomes the pipeline's terminal exception and
            // promptly cancels the source, the sibling workers, and their in-flight work.
            Stats?.ItemFailed();
            if (writer.TryComplete(ex))
            {
                cts.Cancel();
            }

            throw;
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

    private async IAsyncEnumerable<TResult> EnumerateOrdered(
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        Stats?.MarkStarted();

        var tasks = Channel.CreateBounded<Task<StageResult<TResult>>>(new BoundedChannelOptions(Options.Capacity)
        {
            SingleWriter = true,
            SingleReader = true,
        });

        if (Stats is { } stats)
        {
            stats.QueueLengthProbe = () => tasks.Reader.Count;
        }

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        using var gate = new SemaphoreSlim(_selectOptions.Concurrency);
        var pump = PumpOrderedAsync(tasks.Writer, gate, cts.Token);

        try
        {
            // Note: the task-queue occupancy time above is not item queue time (that
            // would require stamping each task at creation and reading it back out
            // here) — ordered mode does not record AverageQueueTime.
            await foreach (var task in tasks.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
            {
                var result = await task.ConfigureAwait(false);
                if (result.Emit)
                {
                    Stats?.ItemCompleted();
                    yield return result.Value!;
                }
                else if (result.Failed)
                {
                    Stats?.ItemFailed();
                }
            }
        }
        finally
        {
            cts.Cancel();

            // Await the pump first so no further tasks can enter the channel, then
            // drain the channel so every started task is observed before the gate
            // is disposed.
            await TaskHelpers.IgnoreErrorsAsync(pump).ConfigureAwait(false);
            while (tasks.Reader.TryRead(out var task))
            {
                await TaskHelpers.IgnoreErrorsAsync(task).ConfigureAwait(false);
            }
        }
    }

    private async Task PumpOrderedAsync(
        ChannelWriter<Task<StageResult<TResult>>> writer,
        SemaphoreSlim gate,
        CancellationToken cancellationToken)
    {
        Task<StageResult<TResult>>? pending = null;
        try
        {
            await foreach (var item in _upstream.Enumerate(cancellationToken).ConfigureAwait(false))
            {
                Stats?.ItemReceived();
                await gate.WaitAsync(cancellationToken).ConfigureAwait(false);
                pending = RunAsync(item, gate, cancellationToken);
                await writer.WriteAsync(pending, cancellationToken).ConfigureAwait(false);
                pending = null;
            }

            writer.Complete();
        }
        catch (Exception ex)
        {
            writer.TryComplete(ex);

            // A task created but never handed to the consumer must still be observed.
            if (pending is not null)
            {
                await TaskHelpers.IgnoreErrorsAsync(pending).ConfigureAwait(false);
            }
        }
    }

    private async Task<StageResult<TResult>> RunAsync(TSource item, SemaphoreSlim gate, CancellationToken cancellationToken)
    {
        try
        {
            Stats?.WorkerStarted();
            var t0 = Stats is null ? 0 : Stopwatch.GetTimestamp();
            try
            {
                return await _selector(item, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                if (t0 != 0)
                {
                    Stats?.RecordProcessingTicks(Stopwatch.GetTimestamp() - t0);
                }

                Stats?.WorkerFinished();
            }
        }
        finally
        {
            gate.Release();
        }
    }
}
