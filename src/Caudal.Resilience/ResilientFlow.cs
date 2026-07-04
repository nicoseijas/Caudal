using Polly;

namespace Caudal;

/// <summary>
/// A flow bound to a <see cref="Polly.ResiliencePipeline"/>: every selector passed to
/// <see cref="SelectAsync{TResult}"/> or <see cref="SelectResultAsync{TResult}"/> runs
/// through <see cref="Pipeline"/> on each invocation. Produced by
/// <see cref="ResilienceFlowExtensions.WithResiliencePipeline{T}"/> to give the fluent
/// shape <c>flow.WithResiliencePipeline(pipeline).SelectAsync(selector, concurrency: 8)</c>.
/// </summary>
/// <typeparam name="T">The type of items in the underlying flow.</typeparam>
public sealed class ResilientFlow<T>
{
    internal ResilientFlow(IFlow<T> source, ResiliencePipeline pipeline)
    {
        Source = source;
        Pipeline = pipeline;
    }

    /// <summary>The underlying flow whose items will be projected.</summary>
    /// <remarks>
    /// Calling <c>Source.SelectAsync(...)</c> uses the plain Core operator and runs
    /// WITHOUT the resilience pipeline — call <see cref="SelectAsync{TResult}"/> on
    /// this instance instead. <c>Source</c> exists as the escape hatch back to the
    /// full Core operator set (Buffer, Batch, time operators, …).
    /// </remarks>
    public IFlow<T> Source { get; }

    /// <summary>The resilience pipeline every selector invocation runs through.</summary>
    public ResiliencePipeline Pipeline { get; }

    /// <summary>
    /// Projects each item through <paramref name="selector"/>, running every
    /// invocation through <see cref="Pipeline"/>. See
    /// <see cref="ResilienceFlowExtensions.SelectAsync{TSource, TResult}(IFlow{TSource}, Func{TSource, CancellationToken, Task{TResult}}, ResiliencePipeline, int, bool, FlowFailureMode)"/>
    /// for the full contract.
    /// </summary>
    public IFlow<TResult> SelectAsync<TResult>(
        Func<T, CancellationToken, Task<TResult>> selector,
        int concurrency = 1,
        bool preserveOrder = false,
        FlowFailureMode failureMode = FlowFailureMode.Stop)
        => Source.SelectAsync(selector, Pipeline, concurrency, preserveOrder, failureMode);

    /// <summary>
    /// Projects each item through <paramref name="selector"/> under
    /// <see cref="FlowFailureMode.Capture"/>, running every invocation through
    /// <see cref="Pipeline"/>. See
    /// <see cref="ResilienceFlowExtensions.SelectResultAsync{TSource, TResult}(IFlow{TSource}, Func{TSource, CancellationToken, Task{TResult}}, ResiliencePipeline, int, bool)"/>
    /// for the full contract.
    /// </summary>
    public IFlow<FlowResult<TResult>> SelectResultAsync<TResult>(
        Func<T, CancellationToken, Task<TResult>> selector,
        int concurrency = 1,
        bool preserveOrder = false)
        => Source.SelectResultAsync(selector, Pipeline, concurrency, preserveOrder);
}
