using System.Threading.Channels;

namespace Caudal.Testing;

/// <summary>
/// A test-controlled source that feeds a single <see cref="IFlow{T}"/>. Tests push
/// items, completion, or failure at will, from any thread, while the pipeline under
/// test drains them through <see cref="ToFlow"/>.
/// </summary>
/// <remarks>
/// The source itself is backed by an unbounded channel: the test side must never
/// block on <see cref="Emit"/>. Backpressure is still meaningful for the flow built
/// from it — it is governed by the <see cref="FlowOptions.Capacity"/> passed to
/// <see cref="ToFlow"/>, which applies to the buffer downstream of this source.
/// </remarks>
/// <typeparam name="T">The type of the items the source produces.</typeparam>
public sealed class TestFlowSource<T>
{
    private readonly Channel<T> _channel = Channel.CreateUnbounded<T>();
    private readonly object _gate = new();
    private bool _completed;
    private bool _flowCreated;

    /// <summary>Emits an item to the source. Never blocks.</summary>
    /// <param name="item">The item to emit.</param>
    /// <exception cref="InvalidOperationException">The source is already completed or failed.</exception>
    public void Emit(T item)
    {
        lock (_gate)
        {
            ThrowIfCompleted();
        }

        if (!_channel.Writer.TryWrite(item))
        {
            throw new InvalidOperationException("the source is completed");
        }
    }

    /// <summary>Emits a sequence of items, in order, to the source. Never blocks.</summary>
    /// <param name="items">The items to emit.</param>
    /// <exception cref="InvalidOperationException">The source is already completed or failed.</exception>
    public void EmitRange(IEnumerable<T> items)
    {
        ArgumentNullException.ThrowIfNull(items);

        foreach (var item in items)
        {
            Emit(item);
        }
    }

    /// <summary>
    /// Completes the source successfully. The flow built from it finishes draining
    /// already-emitted items, then completes. Calling this more than once throws.
    /// </summary>
    /// <exception cref="InvalidOperationException">The source is already completed or failed.</exception>
    public void Complete()
    {
        lock (_gate)
        {
            ThrowIfCompleted();
            _completed = true;
        }

        _channel.Writer.Complete();
    }

    /// <summary>
    /// Completes the source with a failure. The exact <paramref name="exception"/>
    /// instance — never wrapped — reaches the consumer of the flow built from this
    /// source. Calling this more than once throws.
    /// </summary>
    /// <param name="exception">The exception to deliver to the consumer.</param>
    /// <exception cref="InvalidOperationException">The source is already completed or failed.</exception>
    public void Fail(Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);

        lock (_gate)
        {
            ThrowIfCompleted();
            _completed = true;
        }

        _channel.Writer.TryComplete(exception);
    }

    /// <summary>The number of items emitted but not yet pulled by the flow.</summary>
    public int PendingCount => _channel.Reader.Count;

    /// <summary>
    /// Builds the single <see cref="IFlow{T}"/> fed by this source. May be called
    /// only once: a <see cref="TestFlowSource{T}"/> feeds a single flow.
    /// </summary>
    /// <param name="options">Buffer capacity and pipeline name for the resulting flow.</param>
    /// <exception cref="InvalidOperationException"><see cref="ToFlow"/> was already called.</exception>
    public IFlow<T> ToFlow(FlowOptions? options = null)
    {
        lock (_gate)
        {
            if (_flowCreated)
            {
                throw new InvalidOperationException("a TestFlowSource feeds a single flow");
            }

            _flowCreated = true;
        }

        return Flow.From(_channel.Reader.ReadAllAsync(), options);
    }

    private void ThrowIfCompleted()
    {
        if (_completed)
        {
            throw new InvalidOperationException("the source is completed");
        }
    }
}
