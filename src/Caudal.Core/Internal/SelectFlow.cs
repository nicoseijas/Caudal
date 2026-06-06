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
        Func<TSource, CancellationToken, Task<StageResult<TResult>>> selector,
        SelectAsyncOptions selectOptions)
        : base(upstream.Options)
    {
        _upstream = upstream;
        _selector = selector;
        _selectOptions = selectOptions;
    }

    public override IAsyncEnumerable<TResult> Enumerate(CancellationToken cancellationToken)
        => _selectOptions.PreserveOrder
            ? EnumerateOrdered(cancellationToken)
            : EnumerateUnordered(cancellationToken);

    private async IAsyncEnumerable<TResult> EnumerateUnordered(
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var input = Channel.CreateBounded<TSource>(new BoundedChannelOptions(1)
        {
            SingleWriter = true,
        });
        var output = Channel.CreateBounded<TResult>(new BoundedChannelOptions(Options.Capacity)
        {
            SingleReader = true,
        });

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        var pump = PumpAsync(input.Writer, cts.Token);
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

    private async Task PumpAsync(ChannelWriter<TSource> writer, CancellationToken cancellationToken)
    {
        try
        {
            await foreach (var item in _upstream.Enumerate(cancellationToken).ConfigureAwait(false))
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

    private async Task WorkerAsync(
        ChannelReader<TSource> reader,
        ChannelWriter<TResult> writer,
        CancellationTokenSource cts)
    {
        try
        {
            await foreach (var item in reader.ReadAllAsync(cts.Token).ConfigureAwait(false))
            {
                var result = await _selector(item, cts.Token).ConfigureAwait(false);
                if (result.Emit)
                {
                    await writer.WriteAsync(result.Value!, cts.Token).ConfigureAwait(false);
                }
            }
        }
        catch (OperationCanceledException oce) when (oce.CancellationToken == cts.Token)
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
        var tasks = Channel.CreateBounded<Task<StageResult<TResult>>>(new BoundedChannelOptions(Options.Capacity)
        {
            SingleWriter = true,
            SingleReader = true,
        });

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        using var gate = new SemaphoreSlim(_selectOptions.Concurrency);
        var pump = PumpOrderedAsync(tasks.Writer, gate, cts.Token);

        try
        {
            await foreach (var task in tasks.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
            {
                var result = await task.ConfigureAwait(false);
                if (result.Emit)
                {
                    yield return result.Value!;
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
            return await _selector(item, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            gate.Release();
        }
    }
}
