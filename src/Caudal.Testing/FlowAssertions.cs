using System.Globalization;
using System.Runtime.CompilerServices;
using Caudal.Internal;

namespace Caudal.Testing;

/// <summary>
/// A fluent set of checks against a Caudal flow's runtime behavior. Configuring an
/// assertion does nothing by itself: awaiting the instance (or calling
/// <see cref="RunAsync"/>) is the terminal sink that runs the pipeline to completion
/// and then evaluates every configured check, throwing <see cref="CaudalAssertionException"/>
/// on the first violation. The flow must not already be attached to another sink —
/// a Caudal pipeline has a single lifecycle.
/// </summary>
/// <typeparam name="T">The type of the items the flow produces.</typeparam>
public sealed class FlowAssertions<T>
{
    private static readonly TimeSpan DefaultTimeout = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan TeardownGracePeriod = TimeSpan.FromSeconds(5);

    private readonly Flow<T> _flow;
    private readonly FlowBase<T> _flowNode;

    private int? _maxConcurrency;
    private bool _preserveOrder;
    private IComparer<T> _orderComparer = Comparer<T>.Default;
    private bool _completeWithoutLeaks;
    private TimeSpan _timeout = DefaultTimeout;

    internal FlowAssertions(Flow<T> flow, FlowBase<T> flowNode)
    {
        _flow = flow;
        _flowNode = flowNode;
    }

    /// <summary>
    /// Asserts that no stage in the pipeline ever ran more than <paramref name="maximum"/>
    /// workers concurrently. Requires <see cref="FlowOptions.CaptureStatistics"/> to have
    /// been set on every stage; calling this a second time replaces the earlier limit.
    /// </summary>
    /// <param name="maximum">The highest number of concurrently active workers allowed, per stage.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="maximum"/> is less than 1.</exception>
    public FlowAssertions<T> UseAtMostConcurrency(int maximum)
    {
        if (maximum < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maximum), maximum, "maximum must be at least 1.");
        }

        _maxConcurrency = maximum;
        return this;
    }

    /// <summary>
    /// Asserts that items are delivered from the flow in non-decreasing order according
    /// to <paramref name="comparer"/> (<see cref="Comparer{T}.Default"/> when
    /// <see langword="null"/>). For a monotonic test sequence this is equivalent to
    /// asserting delivery matches source order. The check streams: items are not
    /// buffered, and it does not require <see cref="FlowOptions.CaptureStatistics"/>.
    /// Calling this a second time replaces the earlier comparer.
    /// </summary>
    /// <param name="comparer">The comparer used to compare consecutively delivered items.</param>
    public FlowAssertions<T> PreserveOrder(IComparer<T>? comparer = null)
    {
        _preserveOrder = true;
        _orderComparer = comparer ?? Comparer<T>.Default;
        return this;
    }

    /// <summary>
    /// Asserts that after the pipeline completes, every stage has zero active workers
    /// and an empty queue — no work left stranded. Requires
    /// <see cref="FlowOptions.CaptureStatistics"/> to have been set on every stage.
    /// </summary>
    public FlowAssertions<T> CompleteWithoutLeaks()
    {
        _completeWithoutLeaks = true;
        return this;
    }

    /// <summary>
    /// Bounds how long the pipeline is allowed to run before the assertions give up,
    /// cancel it, and fail. Defaults to 30 seconds. Calling this a second time replaces
    /// the earlier timeout.
    /// </summary>
    /// <param name="timeout">The maximum time to let the pipeline run.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="timeout"/> is not positive.</exception>
    public FlowAssertions<T> WithTimeout(TimeSpan timeout)
    {
        if (timeout <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(timeout), timeout, "timeout must be positive.");
        }

        _timeout = timeout;
        return this;
    }

    /// <summary>
    /// Runs the pipeline to completion and evaluates every configured assertion,
    /// throwing <see cref="CaudalAssertionException"/> on the first violation found.
    /// A pipeline fault (any exception the flow itself raises, including cancellation
    /// observed through <paramref name="cancellationToken"/>) propagates unwrapped —
    /// assertions never mask a pipeline error. If the pipeline does not complete
    /// within the configured timeout, it is torn down and a
    /// <see cref="CaudalAssertionException"/> is thrown instead.
    /// </summary>
    /// <param name="cancellationToken">A token the caller can use to cancel the run.</param>
    /// <exception cref="CaudalAssertionException">A configured assertion was violated, or the pipeline timed out.</exception>
    /// <exception cref="InvalidOperationException">
    /// A stats-requiring assertion (<see cref="UseAtMostConcurrency"/> or
    /// <see cref="CompleteWithoutLeaks"/>) was configured but
    /// <see cref="FlowOptions.CaptureStatistics"/> was not set on every stage.
    /// </exception>
    public async Task RunAsync(CancellationToken cancellationToken = default)
    {
        using var timeoutSource = new CancellationTokenSource();
        using var linkedSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutSource.Token);

        var hasPrevious = false;
        T previous = default!;
        var itemIndex = 0;
        (int Index, T Previous, T Current)? orderViolation = null;

        async Task ConsumeAsync()
        {
            await foreach (var item in _flowNode.Enumerate(linkedSource.Token).ConfigureAwait(false))
            {
                if (_preserveOrder && orderViolation is null && hasPrevious
                    && _orderComparer.Compare(previous, item) > 0)
                {
                    orderViolation = (itemIndex, previous, item);
                }

                previous = item;
                hasPrevious = true;
                itemIndex++;
            }
        }

        var consumeTask = ConsumeAsync();

        using var delaySource = new CancellationTokenSource();
        var timeoutTask = Task.Delay(_timeout, delaySource.Token);

        var completedTask = await Task.WhenAny(consumeTask, timeoutTask).ConfigureAwait(false);

        if (completedTask == timeoutTask)
        {
            timeoutSource.Cancel();
            var ignoredCancellation = false;
            try
            {
                // Grace-bounded: a pipeline that ignores cancellation entirely (a
                // stuck, non-cooperative stage — exactly what this guard exists to
                // catch) must not turn the timeout into an infinite wait. RunAsync
                // is a hard bound of roughly _timeout + grace.
                await consumeTask.WaitAsync(TeardownGracePeriod, CancellationToken.None).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // The pipeline was torn down deliberately by the timeout guard; its
                // own cancellation is expected here, not a fault to report.
            }
            catch (TimeoutException)
            {
                ignoredCancellation = true;
            }

            var message = ignoredCancellation
                ? $"the pipeline did not complete within {_timeout} and ignored cancellation " +
                  $"for another {TeardownGracePeriod} — a stage is not observing its token"
                : $"the pipeline did not complete within {_timeout}";
            throw new CaudalAssertionException(AppendSnapshotIfAvailable(message));
        }

        delaySource.Cancel();

        // Propagates any pipeline fault — including the caller's own cancellation —
        // unwrapped, exactly as raised.
        await consumeTask.ConfigureAwait(false);

        Evaluate(orderViolation);
    }

    /// <summary>
    /// Awaits <see cref="RunAsync"/>, so <c>await flow.Should()...</c> reads as the
    /// terminal sink that it is.
    /// </summary>
    public TaskAwaiter GetAwaiter() => RunAsync().GetAwaiter();

    private void Evaluate((int Index, T Previous, T Current)? orderViolation)
    {
        if (_maxConcurrency is { } maxConcurrency)
        {
            var chain = WalkChain();
            EnsureStatsAvailable(chain);

            foreach (var node in chain)
            {
                var maxActive = node.Stats!.MaxActive;
                if (maxActive > maxConcurrency)
                {
                    throw new CaudalAssertionException(AppendSnapshotIfAvailable(string.Create(
                        CultureInfo.InvariantCulture,
                        $"stage '{node.OperatorName}' exceeded UseAtMostConcurrency({maxConcurrency}): observed max concurrency {maxActive}")));
                }
            }
        }

        if (_preserveOrder && orderViolation is { } violation)
        {
            throw new CaudalAssertionException(AppendSnapshotIfAvailable(string.Create(
                CultureInfo.InvariantCulture,
                $"PreserveOrder violated at index {violation.Index}: previous={violation.Previous}, current={violation.Current}")));
        }

        if (_completeWithoutLeaks)
        {
            var chain = WalkChain();
            EnsureStatsAvailable(chain);

            foreach (var node in chain)
            {
                var stats = node.Stats!;
                if (stats.Active != 0 || stats.QueueLength != 0)
                {
                    throw new CaudalAssertionException(AppendSnapshotIfAvailable(string.Create(
                        CultureInfo.InvariantCulture,
                        $"stage '{node.OperatorName}' leaked: Active={stats.Active}, QueueLength={stats.QueueLength}")));
                }
            }
        }
    }

    private List<FlowNode> WalkChain()
    {
        var chain = new List<FlowNode>();
        for (var current = (FlowNode?)_flowNode; current is not null; current = current.UpstreamNode)
        {
            chain.Add(current);
        }

        chain.Reverse();
        return chain;
    }

    private static void EnsureStatsAvailable(List<FlowNode> chain)
    {
        if (chain.Exists(static node => node.Stats is null))
        {
            throw new InvalidOperationException(
                "Statistics are not enabled for this flow. Set FlowOptions.CaptureStatistics = true when building it.");
        }
    }

    private string AppendSnapshotIfAvailable(string message)
    {
        var chain = WalkChain();
        if (chain.TrueForAll(static node => node.Stats is not null))
        {
            return message + "\n\npipeline snapshot:\n" + _flow.GetSnapshot().Render();
        }

        return message;
    }
}
