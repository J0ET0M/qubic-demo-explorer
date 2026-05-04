using ClickHouse.Client.ADO;
using ClickHouse.Client.Copy;
using Microsoft.Extensions.Options;
using QubicExplorer.Shared.Configuration;
using QubicExplorer.Shared.Constants;

namespace QubicExplorer.Analytics.Services;

/// <summary>
/// Computes reward distributions for completed epochs once and persists them
/// to the reward_distributions table. Once an epoch ends its rewards are
/// immutable, so the expensive START/END marker join only needs to run a
/// single time per epoch.
///
/// The API reads from this table for historical epochs and falls back to the
/// live query only for the current (in-progress) epoch.
/// </summary>
public class RewardDistributionPersistenceService : IDisposable
{
    private readonly ClickHouseConnection _connection;
    private readonly ILogger<RewardDistributionPersistenceService> _logger;
    private bool _disposed;

    private const string StateKey = "rewards_last_computed_epoch";

    public RewardDistributionPersistenceService(
        IOptions<ClickHouseOptions> options,
        ILogger<RewardDistributionPersistenceService> logger)
    {
        _logger = logger;
        _connection = new ClickHouseConnection(options.Value.ConnectionString);
        _connection.Open();
    }

    /// <summary>
    /// Compute rewards for one completed epoch (the next one we haven't done yet).
    /// Returns true if work was done, false if there's nothing to do.
    /// Caller should loop until false to fully catch up.
    /// </summary>
    public async Task<bool> ProcessNextEpochAsync(uint currentEpoch, CancellationToken ct)
    {
        var lastComputed = await GetStateAsync(ct);
        var startEpoch = lastComputed.HasValue ? lastComputed.Value + 1 : 1u;

        // Only process completed epochs (anything < currentEpoch is immutable)
        if (startEpoch >= currentEpoch)
        {
            _logger.LogDebug("Reward distributions: caught up (lastComputed={Last}, current={Current})",
                lastComputed, currentEpoch);
            return false;
        }

        await ComputeAndPersistAsync(startEpoch, ct);
        await UpdateStateAsync(startEpoch, ct);
        return true;
    }

    private async Task ComputeAndPersistAsync(uint epoch, CancellationToken ct)
    {
        _logger.LogInformation("Computing reward distributions for epoch {Epoch}", epoch);

        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            WITH start_markers AS (
                SELECT epoch, tick_number, log_id as start_log_id, timestamp
                FROM logs
                PREWHERE log_type = 255
                WHERE epoch = {{epoch:UInt32}}
                  AND JSONExtractUInt(raw_data, 'customMessage') = {{startOp:UInt64}}
            ),
            end_markers AS (
                SELECT tick_number as end_tick_number, log_id as end_log_id
                FROM logs
                PREWHERE log_type = 255
                WHERE epoch = {{epoch:UInt32}}
                  AND JSONExtractUInt(raw_data, 'customMessage') = {{endOp:UInt64}}
            ),
            reward_ranges AS (
                SELECT
                    s.epoch, s.tick_number, s.start_log_id, s.timestamp,
                    min(e.end_log_id) as end_log_id
                FROM start_markers s
                INNER JOIN end_markers e ON e.end_tick_number = s.tick_number
                WHERE e.end_log_id > s.start_log_id
                GROUP BY s.epoch, s.tick_number, s.start_log_id, s.timestamp
            ),
            transfers AS (
                SELECT tick_number as t_tick_number, log_id as t_log_id,
                       source_address, amount
                FROM logs
                PREWHERE log_type = 0
                  AND tick_number IN (SELECT tick_number FROM reward_ranges)
            )
            SELECT
                dr.epoch,
                any(t.source_address) as contract_address,
                dr.tick_number,
                dr.timestamp,
                sum(t.amount) as total_amount,
                count() as transfer_count
            FROM reward_ranges dr
            INNER JOIN transfers t ON t.t_tick_number = dr.tick_number
            WHERE t.t_log_id > dr.start_log_id AND t.t_log_id < dr.end_log_id
            GROUP BY dr.epoch, dr.tick_number, dr.start_log_id, dr.end_log_id, dr.timestamp
            ORDER BY dr.tick_number ASC";
        AddParam(cmd, "epoch", epoch);
        AddParam(cmd, "startOp", LogTypes.CustomMessageOpStartDistributeRewards);
        AddParam(cmd, "endOp", LogTypes.CustomMessageOpEndDistributeRewards);

        var rows = new List<object[]>();
        var now = DateTime.UtcNow;

        await using (var reader = await cmd.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                rows.Add(new object[]
                {
                    reader.GetFieldValue<uint>(0),                          // epoch
                    reader.GetString(1),                                    // contract_address
                    reader.GetFieldValue<ulong>(2),                         // tick_number
                    Convert.ToUInt64(reader.GetValue(4)),                   // total_amount
                    Convert.ToUInt32(reader.GetValue(5)),                   // transfer_count
                    reader.GetDateTime(3),                                  // timestamp
                    now                                                     // computed_at
                });
            }
        }

        if (rows.Count == 0)
        {
            _logger.LogInformation("Epoch {Epoch}: no reward distributions found", epoch);
            return;
        }

        using var bulk = new ClickHouseBulkCopy(_connection)
        {
            DestinationTableName = "reward_distributions",
            ColumnNames = ["epoch", "contract_address", "tick_number", "total_amount",
                           "transfer_count", "timestamp", "computed_at"],
            BatchSize = 10000
        };
        await bulk.InitAsync();
        await bulk.WriteToServerAsync(rows, ct);

        _logger.LogInformation("Epoch {Epoch}: persisted {Count} reward distributions", epoch, rows.Count);
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
