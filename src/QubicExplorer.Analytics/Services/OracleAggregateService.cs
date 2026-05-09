using ClickHouse.Client.ADO;
using Microsoft.Extensions.Options;
using QubicExplorer.Shared.Configuration;

namespace QubicExplorer.Analytics.Services;

/// <summary>
/// Builds long-term oracle aggregates for completed epochs:
///   - oracle_query_summary  (per query: timing, totals, quorum cutoff)
///   - oracle_computor_summary (per computor: commits/reveals/wins for the epoch)
///
/// Runs once per completed epoch (epoch < currentEpoch). Skips already-aggregated
/// epochs via indexer_state.oracle_aggregates_last_epoch.
///
/// "estimated_points" for a computor in an epoch = the number of queries where
/// the computor's commit_tick was within the in-quorum cutoff (first 451 sorted
/// by tick + ties at the 451st tick). This mirrors the core's revenuePoints rule
/// but without K12/digest validation — so it's an upper bound on actual points.
/// </summary>
public class OracleAggregateService : IDisposable
{
    private readonly ClickHouseConnection _connection;
    private readonly ILogger<OracleAggregateService> _logger;
    private bool _disposed;

    private const string StateKey = "oracle_aggregates_last_epoch";
    private const int Quorum = 451;  // 676 * 2/3 + 1

    public OracleAggregateService(
        IOptions<ClickHouseOptions> options,
        ILogger<OracleAggregateService> logger)
    {
        _logger = logger;
        _connection = new ClickHouseConnection(options.Value.ConnectionString);
        _connection.Open();
    }

    /// <summary>
    /// Aggregate one completed epoch (the next one we haven't done yet).
    /// Returns true if work was done; loop the caller until false to drain catch-up.
    /// </summary>
    public async Task<bool> ProcessNextEpochAsync(uint currentEpoch, CancellationToken ct)
    {
        var lastDone = await GetStateAsync(ct);
        var startEpoch = lastDone.HasValue ? lastDone.Value + 1 : (currentEpoch > 0 ? currentEpoch - 1 : currentEpoch);

        // Only aggregate completed epochs
        if (startEpoch >= currentEpoch) return false;

        // Don't aggregate an epoch with no events at all (saves needless inserts)
        var hasEvents = await EpochHasEventsAsync(startEpoch, ct);
        if (!hasEvents)
        {
            _logger.LogDebug("Oracle aggregates: epoch {Epoch} has no events, marking done", startEpoch);
            await UpdateStateAsync(startEpoch, ct);
            return true;
        }

        await AggregateEpochAsync(startEpoch, ct);
        await UpdateStateAsync(startEpoch, ct);
        return true;
    }

    private async Task<bool> EpochHasEventsAsync(uint epoch, CancellationToken ct)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $"SELECT count() FROM oracle_query_events WHERE epoch = {{epoch:UInt32}} LIMIT 1";
        AddParam(cmd, "epoch", epoch);
        var result = await cmd.ExecuteScalarAsync(ct);
        return Convert.ToInt64(result) > 0;
    }

    private async Task AggregateEpochAsync(uint epoch, CancellationToken ct)
    {
        _logger.LogInformation("Aggregating oracle data for epoch {Epoch}", epoch);

        // Per-query summary. Using a CTE so the sorted commit_ticks array is
        // built once and referenced multiple times — ClickHouse forbids nested
        // aggregate calls inside other aggregates.
        await using (var qSummary = _connection.CreateCommand())
        {
            qSummary.CommandText = $@"
                INSERT INTO oracle_query_summary
                (epoch, query_id, first_commit_tick, last_commit_tick,
                 quorum_cutoff_tick, total_commits, total_reveals,
                 commits_in_quorum, unique_committors, aggregated_at)
                WITH g AS (
                    SELECT
                        epoch,
                        query_id,
                        minIf(tick_number, event_type = 0) AS first_commit_tick,
                        maxIf(tick_number, event_type = 0) AS last_commit_tick,
                        arraySort(groupArrayIf(tick_number, event_type = 0)) AS commit_ticks,
                        countIf(event_type = 0) AS total_commits,
                        countIf(event_type = 1) AS total_reveals,
                        uniqExactIf(computor_index, event_type = 0) AS unique_committors
                    FROM oracle_query_events
                    WHERE epoch = {{epoch:UInt32}}
                    GROUP BY epoch, query_id
                )
                SELECT
                    epoch, query_id, first_commit_tick, last_commit_tick,
                    if(total_commits >= {Quorum},
                       arrayElement(commit_ticks, {Quorum}), 0) AS quorum_cutoff_tick,
                    total_commits, total_reveals,
                    if(total_commits >= {Quorum},
                       arrayCount(t -> t <= arrayElement(commit_ticks, {Quorum}), commit_ticks),
                       total_commits) AS commits_in_quorum,
                    unique_committors,
                    now64(3) AS aggregated_at
                FROM g";
            AddParam(qSummary, "epoch", epoch);
            await qSummary.ExecuteNonQueryAsync(ct);
        }

        // Per-computor summary. estimated_points needs to know each query's cutoff,
        // so we join back to the (just-inserted) summary table.
        await using (var cSummary = _connection.CreateCommand())
        {
            cSummary.CommandText = $@"
                INSERT INTO oracle_computor_summary
                (epoch, computor_index, commit_count, reveal_count,
                 estimated_points, avg_tick_offset, participations, aggregated_at)
                SELECT
                    e.epoch,
                    e.computor_index,
                    countIf(e.event_type = 0) AS commit_count,
                    countIf(e.event_type = 1) AS reveal_count,
                    -- in-quorum wins: commits where tick <= the query's cutoff
                    -- (or all commits if quorum wasn't reached for that query)
                    countIf(e.event_type = 0 AND
                            (s.quorum_cutoff_tick = 0 OR e.tick_number <= s.quorum_cutoff_tick)
                    ) AS estimated_points,
                    -- Average tick offset behind first commit per query (lower = faster).
                    -- avgIf with arrayMap pattern: compute (tick - first_commit_tick) per row
                    avgIf(toFloat32(e.tick_number - s.first_commit_tick), e.event_type = 0) AS avg_tick_offset,
                    uniqExactIf(e.query_id, e.event_type = 0) AS participations,
                    now64(3) AS aggregated_at
                FROM oracle_query_events e
                LEFT JOIN (
                    SELECT epoch, query_id, first_commit_tick, quorum_cutoff_tick
                    FROM oracle_query_summary FINAL
                    WHERE epoch = {{epoch:UInt32}}
                ) AS s
                  ON s.epoch = e.epoch AND s.query_id = e.query_id
                WHERE e.epoch = {{epoch:UInt32}}
                GROUP BY e.epoch, e.computor_index";
            AddParam(cSummary, "epoch", epoch);
            await cSummary.ExecuteNonQueryAsync(ct);
        }

        // Log a quick summary
        await using (var stats = _connection.CreateCommand())
        {
            stats.CommandText = $@"
                SELECT
                    (SELECT count() FROM oracle_query_summary FINAL WHERE epoch = {{epoch:UInt32}}) AS queries,
                    (SELECT count() FROM oracle_computor_summary FINAL WHERE epoch = {{epoch:UInt32}}) AS computors";
            AddParam(stats, "epoch", epoch);
            await using var reader = await stats.ExecuteReaderAsync(ct);
            if (await reader.ReadAsync(ct))
            {
                var queries = Convert.ToInt64(reader.GetValue(0));
                var computors = Convert.ToInt64(reader.GetValue(1));
                _logger.LogInformation(
                    "Oracle aggregates done for epoch {Epoch}: {Queries} queries, {Computors} computors",
                    epoch, queries, computors);
            }
        }
    }

    private async Task<uint?> GetStateAsync(CancellationToken ct)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $"SELECT value FROM indexer_state FINAL WHERE key = '{StateKey}'";
        var result = await cmd.ExecuteScalarAsync(ct);
        if (result == null || result == DBNull.Value) return null;
        return uint.TryParse(result.ToString(), out var val) ? val : null;
    }

    private async Task UpdateStateAsync(uint epoch, CancellationToken ct)
    {
        var now = DateTime.UtcNow;
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            INSERT INTO indexer_state (key, value, updated_at)
            VALUES ('{StateKey}', '{epoch}', '{now:yyyy-MM-dd HH:mm:ss.fff}')";
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static void AddParam(System.Data.Common.DbCommand cmd, string name, object value)
    {
        var p = cmd.CreateParameter();
        p.ParameterName = name;
        p.Value = value;
        cmd.Parameters.Add(p);
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _connection.Dispose();
    }
}
