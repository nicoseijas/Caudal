using System.Threading.Channels;

namespace Caudal.Internal;

internal enum TimedWaitOutcome
{
    /// <summary>The delay elapsed before anything became readable.</summary>
    TimerElapsed,

    /// <summary>At least one item is readable.</summary>
    Readable,

    /// <summary>The channel completed without more items.</summary>
    Completed,
}

/// <summary>
/// The one implementation of "wait for the next item or a deadline, whichever comes
/// first" shared by every time-based stage. The subtle parts live here once: the
/// loser of the race is always observed before returning or throwing, external
/// cancellation is rethrown with the caller's token (never misreported as a timer
/// tick), and a channel fault on a winning read rethrows the original exception.
/// Note: timer completions may run synchronously on the thread that advances the
/// clock (FakeTimeProvider.Advance, or a real timer callback), so the continuation
/// — potentially the whole stage loop up to a sink callback — can execute nested
/// inside that call. Callers must not advance a fake clock while holding a lock a
/// downstream callback also takes.
/// </summary>
internal static class TimerRace
{
    public static async Task<TimedWaitOutcome> WaitToReadOrTimeoutAsync<T>(
        ChannelReader<T> reader,
        TimeSpan delay,
        TimeProvider timeProvider,
        CancellationToken cancellationToken)
    {
        using var raceCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var waitTask = reader.WaitToReadAsync(raceCts.Token).AsTask();
        var timerTask = Task.Delay(delay, timeProvider, raceCts.Token);

        var winner = await Task.WhenAny(waitTask, timerTask).ConfigureAwait(false);
        raceCts.Cancel();

        // Observe the loser before any throw below: a faulted task left behind
        // would surface as an UnobservedTaskException instead of flowing to the
        // consumer. A fault swallowed here is sticky on the channel and resurfaces
        // on the next wait.
        await TaskHelpers.IgnoreErrorsAsync(winner == timerTask ? waitTask : timerTask).ConfigureAwait(false);

        // If the external token fired, the "winner" may just be a cancelled task;
        // report cancellation with the caller's token, never as a timer tick.
        cancellationToken.ThrowIfCancellationRequested();

        if (winner == timerTask)
        {
            return TimedWaitOutcome.TimerElapsed;
        }

        return await waitTask.ConfigureAwait(false)
            ? TimedWaitOutcome.Readable
            : TimedWaitOutcome.Completed;
    }
}
