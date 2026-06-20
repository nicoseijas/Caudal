using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace Caudal.Internal;

/// <summary>
/// Bounds the silence between consecutive upstream items: if no item arrives within
/// the timeout, the pipeline faults with <see cref="TimeoutException"/>. The clock
/// measures upstream production, not downstream queue wait; timing out per-item
/// processing belongs to a resilience strategy on the processing stage instead.
/// </summary>
internal sealed class TimeoutEachFlow<T> : FlowBase<T>
{
    private readonly FlowBase<T> _upstream;
    private readonly TimeSpan _timeout;
    private readonly TimeProvider _timeProvider;

    internal TimeoutEachFlow(FlowBase<T> upstream, TimeSpan timeout, TimeProvider timeProvider)
        : base(upstream.Options)
    {
        _upstream = upstream;
        _timeout = timeout;
        _timeProvider = timeProvider;
    }

    public override async IAsyncEnumerable<T> Enumerate(
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var input = Channel.CreateBounded<T>(new BoundedChannelOptions(Options.Capacity)
        {
            SingleWriter = true,
            SingleReader = true,
        });

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var pump = FlowPump.RunAsync(_upstream, input.Writer, cts.Token);

        try
        {
            var reader = input.Reader;
            while (true)
            {
                var outcome = await TimerRace
                    .WaitToReadOrTimeoutAsync(reader, _timeout, _timeProvider, cancellationToken)
                    .ConfigureAwait(false);

                if (outcome == TimedWaitOutcome.TimerElapsed)
                {
                    throw new TimeoutException(
                        $"No item arrived within {_timeout} in flow '{Name ?? "unnamed"}'.");
                }

                if (outcome == TimedWaitOutcome.Completed)
                {
                    break;
                }

                while (reader.TryRead(out var item))
                {
                    yield return item;
                }
            }
        }
        finally
        {
            cts.Cancel();
            await TaskHelpers.IgnoreErrorsAsync(pump).ConfigureAwait(false);
        }
    }
}
