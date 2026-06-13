// Pilot B — file processing. Parses a directory of files with bounded
// concurrency under Capture semantics: malformed files become failed
// FlowResults instead of killing the run, so every good file is still stored.

using System.Globalization;
using Caudal;

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    cts.Cancel();
};

var directory = Directory.CreateTempSubdirectory("caudal-files-");
try
{
    CreateSampleFiles(directory.FullName, count: 200, malformedEvery: 9);
    Console.WriteLine($"file-processing: 200 files in {directory.FullName}, parse concurrency 8");

    var stored = 0;
    var failed = new List<(string File, Exception Error)>();

    await Directory.EnumerateFiles(directory.FullName, "*.txt")
        .ToFlow(capacity: 64, name: "file-processing")
        .SelectResultAsync(ParseAsync, concurrency: 8)
        .ForEachAsync(
            async (result, ct) =>
            {
                if (result.IsSuccess)
                {
                    await StoreAsync(result.Value!, ct);
                    stored++;
                }
                else
                {
                    failed.Add((((ParseException)result.Exception!).File, result.Exception!));
                }
            },
            cts.Token);

    Console.WriteLine();
    Console.WriteLine($"stored: {stored} parsed files");
    Console.WriteLine($"failed: {failed.Count} files (run completed anyway — Capture keeps partial results)");
    foreach (var (file, error) in failed.Take(5))
    {
        Console.WriteLine($"  {Path.GetFileName(file)}: {error.Message}");
    }

    if (failed.Count > 5)
    {
        Console.WriteLine($"  … and {failed.Count - 5} more");
    }
}
finally
{
    directory.Delete(recursive: true);
}

static void CreateSampleFiles(string directory, int count, int malformedEvery)
{
    for (var i = 0; i < count; i++)
    {
        var content = i % malformedEvery == 0
            ? $"not-a-number;{i}"
            : $"{i * 1.5m};{i}";
        File.WriteAllText(Path.Combine(directory, $"record-{i:D4}.txt"), content);
    }
}

static async Task<ParsedRecord> ParseAsync(string path, CancellationToken cancellationToken)
{
    var content = await File.ReadAllTextAsync(path, cancellationToken);
    var parts = content.Split(';');

    if (parts.Length != 2 || !decimal.TryParse(parts[0], CultureInfo.InvariantCulture, out var amount))
    {
        throw new ParseException(path, $"malformed content: '{content}'");
    }

    return new ParsedRecord(path, amount, int.Parse(parts[1], CultureInfo.InvariantCulture));
}

static async Task StoreAsync(ParsedRecord record, CancellationToken cancellationToken)
{
    // Stand-in for a database write.
    await Task.Delay(1, cancellationToken);
}

internal sealed record ParsedRecord(string File, decimal Amount, int Sequence);

internal sealed class ParseException(string file, string message) : FormatException(message)
{
    public string File { get; } = file;
}
