namespace Caudal.Internal;

internal static class TaskHelpers
{
    /// <summary>
    /// Awaits a pipeline task during teardown. The task's failure, if any, has
    /// already been routed to the consumer through the stage channel; this await
    /// only guarantees the task is finished before the stage reports completion.
    /// </summary>
    public static async Task IgnoreErrorsAsync(Task task)
    {
        try
        {
            await task.ConfigureAwait(false);
        }
        catch
        {
            // Already surfaced through the stage channel; see summary.
        }
    }
}
