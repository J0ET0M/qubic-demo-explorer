using System.Text.Json;
using ClickHouse.Client.ADO;
using ClickHouse.Client.Copy;
using Microsoft.Extensions.Options;
using QubicExplorer.Shared.Configuration;

namespace QubicExplorer.Analytics.Services;

/// <summary>
/// Snapshots per-epoch (owner) aggregations into <c>computor_owner_summary</c>.
///
/// Reads <c>computor_revenue</c> (per-computor revenue + DOGE scores) and
/// <c>computor_ownership</c> (identity → owner), groups by owner, computes the
/// same metrics the API used to derive on-the-fly. Run after both prerequisite
/// services have produced data for the epoch.
///
/// Idempotent: same epoch can be re-snapshotted as data evolves (e.g. mid-epoch
/// progression). The table is ReplacingMergeTree on (epoch, owner) with
/// snapshotted_at as the version, so the latest snapshot wins on FINAL reads.
/// </summary>
public class ComputorOwnerSummaryService : IDisposable
{
    private readonly ClickHouseConnection _connection;
    private readonly ILogger<ComputorOwnerSummaryService> _logger;
    private bool _disposed;

    private static readonly JsonSerializerOptions JsonOpts = new()
    {
        PropertyNameCaseInsensitive = true
    };

    public ComputorOwnerSummaryService(
        IOptions<ClickHouseOptions> options,
        ILogger<ComputorOwnerSummaryService> logger)
    {
        _logger = logger;
        _connection = new ClickHouseConnection(options.Value.ConnectionString);
        _connection.Open();
    }

    /// <summary>
    /// Marker owner string used to bucket all computors that fattydoge doesn't
    /// have a label for. Surfaced in the API + frontend as a normal row so
    /// users can see the unattributed footprint at a glance.
    /// </summary>
    private const string UnknownOwner = "Unknown";

    /// <summary>
    /// Snapshot the owner aggregations for one epoch. Returns the number of
    /// owner rows written (0 if there's no revenue data yet).
    /// </summary>
    public async Task<int> RefreshAsync(uint epoch, CancellationToken ct)
    {
        // 1. Per-computor revenue snapshot for the epoch (stored as JSON blob).
        var entries = await LoadComputorEntriesAsync(epoch, ct);
        if (entries == null || entries.Count == 0)
        {
            _logger.LogDebug("OwnerSummary epoch {Epoch}: no revenue data yet, skipping", epoch);
            return 0;
        }

        // 2. Identity → owner map for the epoch. Missing entries fall back to
        // the synthetic "Unknown" bucket below, so we don't require a non-empty
        // map — we'd still produce one Unknown row covering every computor.
        var ownerMap = await LoadOwnershipMapAsync(epoch, ct);

        // 3. Qubic mining: count solution transactions (input_type=2) per
        // to_address for the epoch.
        var qubicSolutionsByAddress = await LoadQubicSolutionsAsync(epoch, ct);

        // 4. Aggregate.
        ulong totalDogePoints = 0;
        ulong totalQubicSolutions = 0;
        var mappedCount = 0;
        var groups = new Dictionary<string, List<ComputorEntry>>(StringComparer.Ordinal);
        foreach (var c in entries)
        {
            totalDogePoints += c.MiningScore;
            if (qubicSolutionsByAddress.TryGetValue(c.Address, out var solCount))
                totalQubicSolutions += solCount;

            var owner = ownerMap.TryGetValue(c.Address, out var o) && !string.IsNullOrEmpty(o)
                ? o
                : UnknownOwner;
            if (owner != UnknownOwner) mappedCount++;

            if (!groups.TryGetValue(owner, out var list))
            {
                list = new List<ComputorEntry>();
                groups[owner] = list;
            }
            list.Add(c);
        }

        long totalAttributed = 0;
        foreach (var list in groups.Values) totalAttributed += list.Sum(c => c.Revenue);
        var unmappedCount = (uint)(entries.Count - mappedCount);
        var now = DateTime.UtcNow;

        // 5. Build rows.
        var rows = new List<object[]>(groups.Count);
        foreach (var (owner, list) in groups)
        {
            ulong ownerDoge = 0;
            ulong ownerQubic = 0;
            foreach (var c in list)
            {
                ownerDoge += c.MiningScore;
                if (qubicSolutionsByAddress.TryGetValue(c.Address, out var s))
                    ownerQubic += s;
            }
            var dogePct = totalDogePoints > 0
                ? 100.0 * ownerDoge / totalDogePoints
                : 0.0;
            var qubicPct = totalQubicSolutions > 0
                ? 100.0 * ownerQubic / totalQubicSolutions
                : 0.0;
            var totalRev = list.Sum(c => c.Revenue);

            rows.Add(new object[]
            {
                epoch,
                owner,
                (uint)list.Count,
                totalRev,
                list.Count > 0 ? (double)totalRev / list.Count : 0.0,
                list.Max(c => c.Revenue),
                list.Min(c => c.Revenue),
                list.Average(c => (double)c.MiningFactor),
                list.Average(c => (double)c.MultiDimDogeRootScaled),
                (uint)list.Count(c => c.MiningFactor > 0),
                ownerDoge,
                dogePct,
                ownerQubic,
                qubicPct,
                list.Select(c => c.ComputorIndex).OrderBy(i => i).ToArray(),
                totalDogePoints,
                totalQubicSolutions,
                (uint)mappedCount,
                unmappedCount,
                totalAttributed,
                now
            });
        }

        using var bulk = new ClickHouseBulkCopy(_connection)
        {
            DestinationTableName = "computor_owner_summary",
            ColumnNames =
            [
                "epoch", "owner", "computor_count",
                "total_revenue", "avg_revenue", "max_revenue", "min_revenue",
                "avg_mining_factor", "avg_doge_root_scaled", "computors_with_doge_mining",
                "doge_points", "doge_participation_percent",
                "qubic_solutions", "qubic_solutions_percent",
                "computor_indices",
                "total_doge_points", "total_qubic_solutions",
                "computors_with_owner", "computors_without_owner",
                "total_attributed_revenue", "snapshotted_at"
            ],
            BatchSize = 1000
        };
        await bulk.InitAsync();
        await bulk.WriteToServerAsync(rows, ct);

        _logger.LogInformation(
            "OwnerSummary epoch {Epoch}: persisted {OwnerCount} owner row(s) covering {Mapped}/{Total} computors ({Unknown} unattributed)",
            epoch, rows.Count, mappedCount, entries.Count, unmappedCount);
        return rows.Count;
    }

    /// <summary>
    /// Per-computor count of mining-solution transactions for the epoch.
    ///
    /// Qubic solution txs are signed by the computor (their public key signs
    /// the tx → <c>from_address</c>) and sent to the burn/zero address. Other
    /// <c>input_type=2</c> traffic — e.g. into smart contracts — isn't a
    /// solution, so we restrict to <c>to_address = BURN</c> to exclude noise.
    /// </summary>
    private async Task<Dictionary<string, ulong>> LoadQubicSolutionsAsync(uint epoch, CancellationToken ct)
    {
        const string burnAddress = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAFXIB";
        var map = new Dictionary<string, ulong>(676, StringComparer.Ordinal);
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = @"
            SELECT from_address, count() AS cnt
            FROM transactions
            WHERE epoch = {epoch:UInt32}
              AND input_type = 2
              AND to_address = {burn:String}
            GROUP BY from_address";
        AddParam(cmd, "epoch", epoch);
        AddParam(cmd, "burn", burnAddress);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            map[reader.GetString(0)] = Convert.ToUInt64(reader.GetValue(1));
        }
        return map;
    }

    /// <summary>
    /// Refresh the most recent N epochs for which we have any computor_revenue
    /// row but the snapshot may be stale. Cheap to call on every analytics pass.
    /// </summary>
    public async Task<int> RefreshRecentAsync(uint upToEpoch, int lookback, CancellationToken ct)
    {
        var total = 0;
        for (int e = (int)upToEpoch; e > 0 && e > (int)upToEpoch - lookback; e--)
        {
            total += await RefreshAsync((uint)e, ct);
        }
        return total;
    }

    private async Task<List<ComputorEntry>?> LoadComputorEntriesAsync(uint epoch, CancellationToken ct)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = "SELECT computors FROM computor_revenue FINAL WHERE epoch = {epoch:UInt32}";
        AddParam(cmd, "epoch", epoch);
        var result = await cmd.ExecuteScalarAsync(ct);
        if (result is not string json || string.IsNullOrEmpty(json))
            return null;
        return JsonSerializer.Deserialize<List<ComputorEntry>>(json, JsonOpts);
    }

    private async Task<Dictionary<string, string>> LoadOwnershipMapAsync(uint epoch, CancellationToken ct)
    {
        var map = new Dictionary<string, string>(676, StringComparer.Ordinal);
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = "SELECT identity, owner FROM computor_ownership FINAL WHERE epoch = {epoch:UInt32}";
        AddParam(cmd, "epoch", epoch);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            map[reader.GetString(0)] = reader.GetString(1);
        }
        return map;
    }

    private static void AddParam(System.Data.Common.DbCommand cmd, string name, object value)
    {
        var p = cmd.CreateParameter();
        p.ParameterName = name;
        p.Value = value;
        cmd.Parameters.Add(p);
    }

    /// <summary>Minimal projection of ComputorRevenueEntryDto we actually need.</summary>
    private sealed class ComputorEntry
    {
        public ushort ComputorIndex { get; set; }
        public string Address { get; set; } = "";
        public long Revenue { get; set; }
        public ulong MiningScore { get; set; }
        public ulong MiningFactor { get; set; }
        public ulong MultiDimDogeRootScaled { get; set; }
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _connection.Dispose();
    }
}
