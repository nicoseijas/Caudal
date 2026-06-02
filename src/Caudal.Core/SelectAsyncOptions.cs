namespace Caudal;

/// <summary>Options governing a <c>SelectAsync</c> or <c>WhereAsync</c> stage.</summary>
public sealed record SelectAsyncOptions
{
    /// <summary>
    /// The maximum number of concurrent selector invocations. Concurrency is
    /// always explicit; the default is sequential. Must be at least 1.
    /// </summary>
    public int Concurrency { get; init; } = 1;

    /// <summary>
    /// When <see langword="true"/>, results are delivered in source order, at the
    /// cost of head-of-line latency. When <see langword="false"/> (the default),
    /// results are delivered in completion order.
    /// </summary>
    public bool PreserveOrder { get; init; }

    internal void Validate()
    {
        if (Concurrency < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(Concurrency), Concurrency, "Concurrency must be at least 1.");
        }
    }
}
