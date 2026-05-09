// Writes the "add partitioning to existing tables" SQL migration scripts to
// docker/clickhouse/migrate_add_partitioning/.
//
// Generates one file per table (plus a 00_drop_mvs.sql prelude) so each step
// can be run, monitored, and verified independently — and disk reclaimed
// between tables by dropping each *_old before the next.
//
// Usage:
//   dotnet run --project tools/MigrationGen
//   dotnet run --project tools/MigrationGen -- /custom/output/dir
using QubicExplorer.Shared;

var outDir = args.Length > 0
    ? args[0]
    : Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..", "..",
                   "docker", "clickhouse", "migrate_add_partitioning");

outDir = Path.GetFullPath(outDir);
Directory.CreateDirectory(outDir);

var files = ClickHouseSchema.GenerateMigrationScriptsPerTable();

foreach (var (fileName, content) in files)
{
    var path = Path.Combine(outDir, fileName);
    File.WriteAllText(path, content);
    Console.WriteLine($"  {fileName}  ({content.Length:N0} chars)");
}

Console.WriteLine();
Console.WriteLine($"Wrote {files.Count} migration files to: {outDir}");
