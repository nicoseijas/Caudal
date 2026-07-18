namespace Caudal.Testing;

/// <summary>
/// A manually controlled gate for orchestrating deterministic races in tests: hold a
/// set of concurrent operations at a specific point, then release them in a chosen
/// order or all at once.
/// </summary>
/// <remarks>
/// <para>
/// While closed, <see cref="WaitAsync"/> returns an uncompleted task that queues its
/// caller FIFO. <see cref="Release"/> completes queued waiters in that order; if it is
/// asked to release more waiters than are currently queued, the surplus is banked as
/// permits and silently consumed by future <see cref="WaitAsync"/> calls — the gate
/// behaves like a semaphore once permits are banked.
/// </para>
/// <para>
/// <see cref="Open"/> makes the gate permanently open: every queued waiter completes
/// immediately, and every future <see cref="WaitAsync"/> call completes synchronously,
/// until <see cref="Close"/> is called. <see cref="Close"/> also clears any banked
/// permits — a closed gate is always a clean slate, never a hangover from a prior
/// <see cref="Release"/>.
/// </para>
/// <para>
/// <see cref="WhenWaitersAsync"/> is the tool for setting up a deterministic race: it
/// completes as soon as <see cref="WaitingCount"/> reaches the requested threshold,
/// so a test can wait until exactly N operations are blocked on the gate before
/// acting. It completes immediately if the threshold is already met, and multiple
/// concurrent calls with different thresholds are all honored independently.
/// </para>
/// <para>
/// A waiter whose token is cancelled is removed from the queue — <see cref="WaitingCount"/>
/// drops accordingly — and its task transitions to cancelled with that token.
/// <see cref="WhenWaitersAsync"/> waiters honor their own token the same way.
/// </para>
/// </remarks>
public sealed class AsyncGate
{
    private readonly object _sync = new();
    private readonly Queue<Waiter> _waiters = new();
    private readonly List<Observer> _observers = [];
    private int _permits;
    private bool _open;

    /// <summary>Creates a gate, closed by default.</summary>
    /// <param name="open">When <see langword="true"/>, the gate starts open.</param>
    public AsyncGate(bool open = false)
    {
        _open = open;
    }

    /// <summary>The number of callers currently blocked in <see cref="WaitAsync"/>.</summary>
    public int WaitingCount
    {
        get
        {
            lock (_sync)
            {
                return _waiters.Count;
            }
        }
    }

    /// <summary>
    /// Waits for the gate to open, a banked permit, or an explicit <see cref="Release"/>.
    /// Completes synchronously if the gate is already open or a permit is available.
    /// </summary>
    /// <param name="cancellationToken">
    /// Cancels the wait. A cancelled waiter is removed from the queue and its task
    /// transitions to cancelled with this token; it does not count against a
    /// subsequent <see cref="Release"/>.
    /// </param>
    public Task WaitAsync(CancellationToken cancellationToken = default)
    {
        if (cancellationToken.IsCancellationRequested)
        {
            return Task.FromCanceled(cancellationToken);
        }

        lock (_sync)
        {
            if (_open)
            {
                return Task.CompletedTask;
            }

            if (_permits > 0)
            {
                _permits--;
                return Task.CompletedTask;
            }

            var waiter = new Waiter(new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));
            _waiters.Enqueue(waiter);

            if (cancellationToken.CanBeCanceled)
            {
                waiter.Registration = cancellationToken.Register(static state =>
                {
                    var (gate, waiter, token) = ((AsyncGate, Waiter, CancellationToken))state!;
                    gate.CancelWaiter(waiter, token);
                }, (this, waiter, cancellationToken));
            }

            CheckObservers();
            return waiter.Tcs.Task;
        }
    }

    /// <summary>
    /// Completes up to <paramref name="count"/> queued waiters, in FIFO order. Any
    /// surplus beyond the number currently queued is banked as permits, consumed by
    /// future <see cref="WaitAsync"/> calls.
    /// </summary>
    /// <param name="count">The number of waiters to release. Must be at least 1.</param>
    public void Release(int count = 1)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(count, 1);

        var toComplete = new List<Waiter>();

        lock (_sync)
        {
            var remaining = count;
            while (remaining > 0 && _waiters.Count > 0)
            {
                toComplete.Add(_waiters.Dequeue());
                remaining--;
            }

            if (remaining > 0)
            {
                _permits += remaining;
            }

            CheckObservers();
        }

        foreach (var waiter in toComplete)
        {
            waiter.Registration?.Dispose();
            waiter.Tcs.TrySetResult();
        }
    }

    /// <summary>
    /// Opens the gate permanently: every queued waiter completes now, and every
    /// future <see cref="WaitAsync"/> call completes synchronously, until
    /// <see cref="Close"/> is called.
    /// </summary>
    public void Open()
    {
        List<Waiter> toComplete;

        lock (_sync)
        {
            _open = true;
            toComplete = [.. _waiters];
            _waiters.Clear();
            CheckObservers();
        }

        foreach (var waiter in toComplete)
        {
            waiter.Registration?.Dispose();
            waiter.Tcs.TrySetResult();
        }
    }

    /// <summary>
    /// Closes the gate: future <see cref="WaitAsync"/> calls block again, and any
    /// permits banked by a prior over-<see cref="Release"/> are cleared — a closed
    /// gate is always a clean slate.
    /// </summary>
    public void Close()
    {
        lock (_sync)
        {
            _open = false;
            _permits = 0;
        }
    }

    /// <summary>
    /// Completes as soon as <see cref="WaitingCount"/> reaches <paramref name="count"/>.
    /// Completes immediately if that threshold is already met. Multiple concurrent
    /// calls with different thresholds are all honored independently.
    /// </summary>
    /// <param name="count">The number of blocked waiters to wait for.</param>
    /// <param name="cancellationToken">Cancels this specific wait.</param>
    public Task WhenWaitersAsync(int count, CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(count);

        if (cancellationToken.IsCancellationRequested)
        {
            return Task.FromCanceled(cancellationToken);
        }

        lock (_sync)
        {
            if (_waiters.Count >= count)
            {
                return Task.CompletedTask;
            }

            var observer = new Observer(count, new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));
            _observers.Add(observer);

            if (cancellationToken.CanBeCanceled)
            {
                observer.Registration = cancellationToken.Register(static state =>
                {
                    var (gate, observer, token) = ((AsyncGate, Observer, CancellationToken))state!;
                    gate.CancelObserver(observer, token);
                }, (this, observer, cancellationToken));
            }

            return observer.Tcs.Task;
        }
    }

    /// <summary>Must be called while holding <see cref="_sync"/>.</summary>
    private void CheckObservers()
    {
        if (_observers.Count == 0)
        {
            return;
        }

        var waitingCount = _waiters.Count;
        List<Observer>? satisfied = null;

        foreach (var observer in _observers)
        {
            if (waitingCount >= observer.Threshold)
            {
                (satisfied ??= []).Add(observer);
            }
        }

        if (satisfied is null)
        {
            return;
        }

        foreach (var observer in satisfied)
        {
            _observers.Remove(observer);
        }

        foreach (var observer in satisfied)
        {
            observer.Registration?.Dispose();
            observer.Tcs.TrySetResult();
        }
    }

    private void CancelWaiter(Waiter waiter, CancellationToken token)
    {
        lock (_sync)
        {
            if (!RemoveWaiter(waiter))
            {
                return;
            }

            CheckObservers();
        }

        waiter.Tcs.TrySetCanceled(token);
    }

    private bool RemoveWaiter(Waiter waiter)
    {
        // Queue<T> has no direct remove; rebuild without the cancelled waiter.
        // Cheap in test scenarios, where waiter counts are small.
        if (!_waiters.Contains(waiter))
        {
            return false;
        }

        var remaining = new Queue<Waiter>(_waiters.Count - 1);
        foreach (var candidate in _waiters)
        {
            if (!ReferenceEquals(candidate, waiter))
            {
                remaining.Enqueue(candidate);
            }
        }

        _waiters.Clear();
        foreach (var candidate in remaining)
        {
            _waiters.Enqueue(candidate);
        }

        return true;
    }

    private void CancelObserver(Observer observer, CancellationToken token)
    {
        lock (_sync)
        {
            if (!_observers.Remove(observer))
            {
                return;
            }
        }

        observer.Tcs.TrySetCanceled(token);
    }

    private sealed class Waiter(TaskCompletionSource tcs)
    {
        public TaskCompletionSource Tcs { get; } = tcs;

        public CancellationTokenRegistration? Registration { get; set; }
    }

    private sealed class Observer(int threshold, TaskCompletionSource tcs)
    {
        public int Threshold { get; } = threshold;

        public TaskCompletionSource Tcs { get; } = tcs;

        public CancellationTokenRegistration? Registration { get; set; }
    }
}
