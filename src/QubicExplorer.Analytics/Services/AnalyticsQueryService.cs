using System.Data;
using System.Numerics;
using System.Text;
using ClickHouse.Client.ADO;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using QubicExplorer.Shared.Configuration;
using QubicExplorer.Shared.Constants;
using QubicExplorer.Shared.DTOs;
using QubicExplorer.Shared.Services;

namespace QubicExplorer.Analytics.Services;

public class AnalyticsQueryService : IDisposable
{
    private readonly ClickHouseConnection _connection;
    private readonly AddressLabelService _labelService;
    private readonly ILogger<AnalyticsQueryService> _logger;
    private bool _disposed;

    public AnalyticsQueryService(IOptions<ClickHouseOptions> options, AddressLabelService labelService, ILogger<AnalyticsQueryService> logger)
    {
        _connection = new ClickHouseConnection(options.Value.ConnectionString);
        _labelService = labelService;
        _logger = logger;
        _connection.Open();
    }

    // =====================================================
    // NETWORK STATS (REAL-TIME)
    // =====================================================

    public async Task<NetworkStatsDto> GetNetworkStatsAsync(CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = @"
            SELECT
                (SELECT max(tick_number) FROM ticks) as latest_tick,
                (SELECT max(epoch) FROM ticks) as current_epoch,
                (SELECT count() FROM transactions) as total_txs,
                (SELECT count() FROM logs) as total_logs,
                (SELECT COALESCE(sum(amount), 0) FROM transactions) as total_volume";

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        if (await reader.ReadAsync(ct))
        {
            return new NetworkStatsDto(
                reader.IsDBNull(0) ? 0 : reader.GetFieldValue<ulong>(0),
                reader.IsDBNull(1) ? 0 : reader.GetFieldValue<uint>(1),
                reader.IsDBNull(2) ? 0 : reader.GetFieldValue<ulong>(2),
                reader.IsDBNull(3) ? 0 : reader.GetFieldValue<ulong>(3),
                reader.IsDBNull(4) ? 0 : reader.GetFieldValue<ulong>(4),
                DateTime.UtcNow
            );
        }

        return new NetworkStatsDto(0, 0, 0, 0, 0, DateTime.UtcNow);
    }

    // =====================================================
    // HOLDER DISTRIBUTION (REAL-TIME COMPUTATION)
    // =====================================================

    /// <summary>
    /// Calculates holder distribution from ClickHouse. Uses spectrum snapshot + delta when available,
    /// falls back to transfer-only calculation otherwise.
    /// </summary>
    public async Task<HolderDistributionDto> GetHolderDistributionAsync(CancellationToken ct = default)
    {
        // Check if we have spectrum snapshots available
        var hasSnapshots = await HasBalanceSnapshotsAsync(ct);

        await using var cmd = _connection.CreateCommand();

        if (hasSnapshots)
        {
            // Use spectrum snapshot + delta from transfers since snapshot
            // Formula: Current Balance = Snapshot Balance + (Incoming since snapshot) - (Outgoing since snapshot)
            _logger.LogDebug("Using spectrum snapshot + delta for holder distribution");
            cmd.CommandText = @"
                WITH
                -- Get the latest snapshot epoch and tick
                latest_snapshot AS (
                    SELECT max(epoch) as epoch, max(tick_number) as tick_number
                    FROM spectrum_imports
                ),
                -- Snapshot balances from the latest import
                snapshot_balances AS (
                    SELECT address, balance as snapshot_balance
                    FROM balance_snapshots
                    WHERE epoch = (SELECT epoch FROM latest_snapshot)
                ),
                -- Transfer deltas since the snapshot tick
                transfer_deltas AS (
                    SELECT
                        address,
                        sum(incoming) - sum(outgoing) as delta
                    FROM (
                        SELECT dest_address as address, toInt64(amount) as incoming, 0 as outgoing
                        FROM logs
                        WHERE log_type = 0 AND dest_address != ''
                          AND tick_number > (SELECT tick_number FROM latest_snapshot)
                        UNION ALL
                        SELECT source_address as address, 0 as incoming, toInt64(amount) as outgoing
                        FROM logs
                        WHERE log_type = 0 AND source_address != ''
                          AND tick_number > (SELECT tick_number FROM latest_snapshot)
                    )
                    GROUP BY address
                ),
                -- Combined current balances
                current_balances AS (
                    SELECT
                        coalesce(s.address, d.address) as address,
                        coalesce(s.snapshot_balance, 0) + coalesce(d.delta, 0) as balance
                    FROM snapshot_balances s
                    FULL OUTER JOIN transfer_deltas d ON s.address = d.address
                    HAVING balance > 0
                )
                SELECT
                    countIf(balance >= 100000000000) as whales,
                    countIf(balance >= 20000000000 AND balance < 100000000000) as large,
                    countIf(balance >= 5000000000 AND balance < 20000000000) as medium,
                    countIf(balance >= 500000000 AND balance < 5000000000) as small,
                    countIf(balance < 500000000) as micro,
                    sumIf(balance, balance >= 100000000000) as whale_balance,
                    sumIf(balance, balance >= 20000000000 AND balance < 100000000000) as large_balance,
                    sumIf(balance, balance >= 5000000000 AND balance < 20000000000) as medium_balance,
                    sumIf(balance, balance >= 500000000 AND balance < 5000000000) as small_balance,
                    sumIf(balance, balance < 500000000) as micro_balance,
                    sum(balance) as total_balance,
                    count() as total_holders
                FROM current_balances";
        }
        else
        {
            // Fallback: Calculate balances from all transfer logs
            _logger.LogDebug("No spectrum snapshots available, using transfer-only calculation");
            cmd.CommandText = @"
                WITH balances AS (
                    SELECT
                        address,
                        sum(incoming) - sum(outgoing) as balance
                    FROM (
                        SELECT dest_address as address, toInt64(amount) as incoming, 0 as outgoing
                        FROM logs WHERE log_type = 0 AND dest_address != ''
                        UNION ALL
                        SELECT source_address as address, 0 as incoming, toInt64(amount) as outgoing
                        FROM logs WHERE log_type = 0 AND source_address != ''
                    )
                    GROUP BY address
                    HAVING balance > 0
                )
                SELECT
                    countIf(balance >= 100000000000) as whales,
                    countIf(balance >= 20000000000 AND balance < 100000000000) as large,
                    countIf(balance >= 5000000000 AND balance < 20000000000) as medium,
                    countIf(balance >= 500000000 AND balance < 5000000000) as small,
                    countIf(balance < 500000000) as micro,
                    sumIf(balance, balance >= 100000000000) as whale_balance,
                    sumIf(balance, balance >= 20000000000 AND balance < 100000000000) as large_balance,
                    sumIf(balance, balance >= 5000000000 AND balance < 20000000000) as medium_balance,
                    sumIf(balance, balance >= 500000000 AND balance < 5000000000) as small_balance,
                    sumIf(balance, balance < 500000000) as micro_balance,
                    sum(balance) as total_balance,
                    count() as total_holders
                FROM balances";
        }

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        if (await reader.ReadAsync(ct))
        {
            var totalBalance = Convert.ToDecimal(reader.GetValue(10));

            var result = new HolderDistributionDto(
                new List<HolderBracketDto>
                {
                    new("Whales (≥100B)", Convert.ToUInt64(reader.GetValue(0)),
                        Convert.ToDecimal(reader.GetValue(5)),
                        totalBalance > 0 ? Convert.ToDecimal(reader.GetValue(5)) / totalBalance * 100 : 0),
                    new("Large (20B-100B)", Convert.ToUInt64(reader.GetValue(1)),
                        Convert.ToDecimal(reader.GetValue(6)),
                        totalBalance > 0 ? Convert.ToDecimal(reader.GetValue(6)) / totalBalance * 100 : 0),
                    new("Medium (5B-20B)", Convert.ToUInt64(reader.GetValue(2)),
                        Convert.ToDecimal(reader.GetValue(7)),
                        totalBalance > 0 ? Convert.ToDecimal(reader.GetValue(7)) / totalBalance * 100 : 0),
                    new("Small (500M-5B)", Convert.ToUInt64(reader.GetValue(3)),
                        Convert.ToDecimal(reader.GetValue(8)),
                        totalBalance > 0 ? Convert.ToDecimal(reader.GetValue(8)) / totalBalance * 100 : 0),
                    new("Micro (<500M)", Convert.ToUInt64(reader.GetValue(4)),
                        Convert.ToDecimal(reader.GetValue(9)),
                        totalBalance > 0 ? Convert.ToDecimal(reader.GetValue(9)) / totalBalance * 100 : 0)
                },
                Convert.ToUInt64(reader.GetValue(11)),
                totalBalance
            );

            return result;
        }

        return new HolderDistributionDto(new List<HolderBracketDto>(), 0, 0);
    }

    /// <summary>
    /// Get holder distribution with concentration metrics (top 10/50/100 holders)
    /// </summary>
    public async Task<HolderDistributionDto> GetHolderDistributionWithConcentrationAsync(CancellationToken ct = default)
    {
        // Get base distribution
        var distribution = await GetHolderDistributionAsync(ct);

        // Get concentration metrics
        var concentration = await GetConcentrationMetricsAsync(ct);

        return new HolderDistributionDto(
            distribution.Brackets,
            distribution.TotalHolders,
            distribution.TotalBalance,
            concentration
        );
    }

    /// <summary>
    /// Check if balance snapshots from spectrum imports are available
    /// </summary>
    public async Task<bool> HasBalanceSnapshotsAsync(CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = "SELECT count() FROM spectrum_imports";
        var count = Convert.ToInt64(await cmd.ExecuteScalarAsync(ct));
        return count > 0;
    }

    /// <summary>
    /// Get concentration metrics showing balance held by top holders
    /// </summary>
    public async Task<ConcentrationMetricsDto> GetConcentrationMetricsAsync(CancellationToken ct = default)
    {
        var hasSnapshots = await HasBalanceSnapshotsAsync(ct);

        await using var cmd = _connection.CreateCommand();

        if (hasSnapshots)
        {
            cmd.CommandText = @"
                WITH
                latest_snapshot AS (
                    SELECT max(epoch) as epoch, max(tick_number) as tick_number
                    FROM spectrum_imports
                ),
                snapshot_balances AS (
                    SELECT address, balance as snapshot_balance
                    FROM balance_snapshots
                    WHERE epoch = (SELECT epoch FROM latest_snapshot)
                ),
                transfer_deltas AS (
                    SELECT
                        address,
                        sum(incoming) - sum(outgoing) as delta
                    FROM (
                        SELECT dest_address as address, toInt64(amount) as incoming, 0 as outgoing
                        FROM logs
                        WHERE log_type = 0 AND dest_address != ''
                          AND tick_number > (SELECT tick_number FROM latest_snapshot)
                        UNION ALL
                        SELECT source_address as address, 0 as incoming, toInt64(amount) as outgoing
                        FROM logs
                        WHERE log_type = 0 AND source_address != ''
                          AND tick_number > (SELECT tick_number FROM latest_snapshot)
                    )
                    GROUP BY address
                ),
                current_balances AS (
                    SELECT
                        coalesce(s.address, d.address) as address,
                        coalesce(s.snapshot_balance, 0) + coalesce(d.delta, 0) as balance
                    FROM snapshot_balances s
                    FULL OUTER JOIN transfer_deltas d ON s.address = d.address
                    HAVING balance > 0
                ),
                ranked AS (
                    SELECT balance, row_number() OVER (ORDER BY balance DESC) as rank
                    FROM current_balances
                ),
                totals AS (
                    SELECT sum(balance) as total FROM current_balances
                )
                SELECT
                    sumIf(balance, rank <= 10) as top10,
                    sumIf(balance, rank <= 50) as top50,
                    sumIf(balance, rank <= 100) as top100,
                    (SELECT total FROM totals) as total
                FROM ranked";
        }
        else
        {
            cmd.CommandText = @"
                WITH balances AS (
                    SELECT
                        address,
                        sum(incoming) - sum(outgoing) as balance
                    FROM (
                        SELECT dest_address as address, toInt64(amount) as incoming, 0 as outgoing
                        FROM logs WHERE log_type = 0 AND dest_address != ''
                        UNION ALL
                        SELECT source_address as address, 0 as incoming, toInt64(amount) as outgoing
                        FROM logs WHERE log_type = 0 AND source_address != ''
                    )
                    GROUP BY address
                    HAVING balance > 0
                ),
                ranked AS (
                    SELECT balance, row_number() OVER (ORDER BY balance DESC) as rank
                    FROM balances
                ),
                totals AS (
                    SELECT sum(balance) as total FROM balances
                )
                SELECT
                    sumIf(balance, rank <= 10) as top10,
                    sumIf(balance, rank <= 50) as top50,
                    sumIf(balance, rank <= 100) as top100,
                    (SELECT total FROM totals) as total
                FROM ranked";
        }

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        if (await reader.ReadAsync(ct))
        {
            var top10 = Convert.ToDecimal(reader.GetValue(0));
            var top50 = Convert.ToDecimal(reader.GetValue(1));
            var top100 = Convert.ToDecimal(reader.GetValue(2));
            var total = Convert.ToDecimal(reader.GetValue(3));

            return new ConcentrationMetricsDto(
                top10,
                total > 0 ? top10 / total * 100 : 0,
                top50,
                total > 0 ? top50 / total * 100 : 0,
                top100,
                total > 0 ? top100 / total * 100 : 0
            );
        }

        return new ConcentrationMetricsDto(0, 0, 0, 0, 0, 0);
    }

    // =====================================================
    // SNAPSHOT SAVE OPERATIONS
    // =====================================================

    public async Task SaveHolderDistributionSnapshotAsync(uint epoch, ulong tickStart, ulong tickEnd, CancellationToken ct = default)
    {
        _logger.LogInformation("Saving holder distribution snapshot for epoch {Epoch} (ticks {TickStart}-{TickEnd})",
            epoch, tickStart, tickEnd);

        // Get the timestamp of tick_end to use as snapshot_at
        var tickEndTimestamp = await GetTickTimestampAsync(tickEnd, ct);
        var snapshotAt = tickEndTimestamp ?? DateTime.UtcNow;

        // Get current distribution with concentration
        var distribution = await GetHolderDistributionWithConcentrationAsync(ct);
        var hasSnapshots = await HasBalanceSnapshotsAsync(ct);

        // Extract bracket data
        var brackets = distribution.Brackets;
        var whaleData = brackets.FirstOrDefault(b => b.Name.Contains("Whale"));
        var largeData = brackets.FirstOrDefault(b => b.Name.Contains("Large"));
        var mediumData = brackets.FirstOrDefault(b => b.Name.Contains("Medium"));
        var smallData = brackets.FirstOrDefault(b => b.Name.Contains("Small"));
        var microData = brackets.FirstOrDefault(b => b.Name.Contains("Micro"));

        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            INSERT INTO holder_distribution_history
            (epoch, snapshot_at, tick_start, tick_end, whale_count, large_count, medium_count, small_count, micro_count,
             whale_balance, large_balance, medium_balance, small_balance, micro_balance,
             total_holders, total_balance, top10_balance, top50_balance, top100_balance, data_source)
            VALUES
            ({epoch}, '{snapshotAt:yyyy-MM-dd HH:mm:ss.fff}', {tickStart}, {tickEnd},
             {whaleData?.Count ?? 0}, {largeData?.Count ?? 0}, {mediumData?.Count ?? 0},
             {smallData?.Count ?? 0}, {microData?.Count ?? 0},
             {(ulong)(whaleData?.Balance ?? 0)}, {(ulong)(largeData?.Balance ?? 0)},
             {(ulong)(mediumData?.Balance ?? 0)}, {(ulong)(smallData?.Balance ?? 0)},
             {(ulong)(microData?.Balance ?? 0)},
             {distribution.TotalHolders}, {(ulong)distribution.TotalBalance},
             {(ulong)(distribution.Concentration?.Top10Balance ?? 0)},
             {(ulong)(distribution.Concentration?.Top50Balance ?? 0)},
             {(ulong)(distribution.Concentration?.Top100Balance ?? 0)},
             '{(hasSnapshots ? "spectrum" : "transfers")}')";

        await cmd.ExecuteNonQueryAsync(ct);
        _logger.LogInformation("Saved holder distribution snapshot for epoch {Epoch} (ticks {TickStart}-{TickEnd})",
            epoch, tickStart, tickEnd);
    }

    /// <summary>
    /// Save network stats snapshot for the given epoch and tick range.
    /// If tickStart/tickEnd are 0, calculates for entire epoch.
    /// Otherwise calculates only for the specified tick window.
    /// </summary>
    public async Task SaveNetworkStatsSnapshotAsync(
        uint epoch,
        ulong tickStart = 0,
        ulong tickEnd = 0,
        CancellationToken ct = default)
    {
        var isWindowSnapshot = tickStart > 0 && tickEnd > 0;
        _logger.LogInformation(
            "Saving network stats snapshot for epoch {Epoch} (ticks {TickStart}-{TickEnd})",
            epoch, tickStart, tickEnd);

        // Get the timestamp of tick_end to use as snapshot_at
        var tickEndTimestamp = await GetTickTimestampAsync(tickEnd, ct);
        var snapshotAt = tickEndTimestamp ?? DateTime.UtcNow;

        // Build filter based on tick range or epoch
        string txFilter;
        string logFilter;
        if (isWindowSnapshot)
        {
            txFilter = $"tick_number >= {tickStart} AND tick_number <= {tickEnd}";
            logFilter = $"tick_number >= {tickStart} AND tick_number <= {tickEnd}";
        }
        else
        {
            txFilter = $"epoch = {epoch}";
            logFilter = $"epoch = {epoch}";
        }

        // Get transaction and transfer counts
        await using var txCmd = _connection.CreateCommand();
        txCmd.CommandText = $@"
            SELECT
                count() as tx_count,
                sum(amount) as total_volume
            FROM transactions
            WHERE {txFilter}";

        ulong totalTransactions = 0;
        decimal totalVolume = 0;
        await using (var reader = await txCmd.ExecuteReaderAsync(ct))
        {
            if (await reader.ReadAsync(ct))
            {
                totalTransactions = Convert.ToUInt64(reader.GetValue(0));
                totalVolume = Convert.ToDecimal(reader.GetValue(1));
            }
        }

        // Get transfer count
        await using var transferCmd = _connection.CreateCommand();
        transferCmd.CommandText = $@"
            SELECT count() FROM logs FINAL WHERE {logFilter} AND log_type = 0";
        var totalTransfers = Convert.ToUInt64(await transferCmd.ExecuteScalarAsync(ct));

        // Get active addresses
        await using var activeCmd = _connection.CreateCommand();
        activeCmd.CommandText = $@"
            SELECT
                uniq(from_address) as unique_senders,
                uniq(to_address) as unique_receivers
            FROM transactions
            WHERE {txFilter}";

        ulong uniqueSenders = 0;
        ulong uniqueReceivers = 0;
        await using (var reader = await activeCmd.ExecuteReaderAsync(ct))
        {
            if (await reader.ReadAsync(ct))
            {
                uniqueSenders = Convert.ToUInt64(reader.GetValue(0));
                uniqueReceivers = Convert.ToUInt64(reader.GetValue(1));
            }
        }

        // Get new vs returning addresses for the window
        var newVsReturning = isWindowSnapshot
            ? await GetNewVsReturningForTickRangeAsync(tickStart, tickEnd, ct)
            : await GetNewVsReturningForEpochAsync(epoch, ct);

        // Get exchange flows for the window
        var exchangeFlows = isWindowSnapshot
            ? await GetExchangeFlowsForTickRangeAsync(tickStart, tickEnd, ct)
            : await GetExchangeFlowsForEpochAsync(epoch, ct);

        // Get smart contract usage
        await using var scCmd = _connection.CreateCommand();
        scCmd.CommandText = $@"
            SELECT
                count() as call_count,
                uniq(from_address) as unique_callers
            FROM transactions
            WHERE {txFilter} AND input_type > 0";

        ulong scCallCount = 0;
        ulong scUniqueCallers = 0;
        await using (var reader = await scCmd.ExecuteReaderAsync(ct))
        {
            if (await reader.ReadAsync(ct))
            {
                scCallCount = Convert.ToUInt64(reader.GetValue(0));
                scUniqueCallers = Convert.ToUInt64(reader.GetValue(1));
            }
        }

        // Get average tx size
        await using var sizeCmd = _connection.CreateCommand();
        sizeCmd.CommandText = $@"
            SELECT
                avg(amount) as avg_size,
                median(amount) as median_size
            FROM logs FINAL
            WHERE {logFilter} AND log_type = 0 AND amount > 0";

        double avgTxSize = 0;
        double medianTxSize = 0;
        await using (var reader = await sizeCmd.ExecuteReaderAsync(ct))
        {
            if (await reader.ReadAsync(ct))
            {
                avgTxSize = ToSafeDouble(reader.GetValue(0));
                medianTxSize = ToSafeDouble(reader.GetValue(1));
            }
        }

        // Get new users with high balances for this window
        var newUsersHighBalance = isWindowSnapshot
            ? await GetNewUsersWithHighBalanceForTickRangeAsync(tickStart, tickEnd, ct)
            : await GetNewUsersWithHighBalanceForEpochAsync(epoch, ct);

        // Insert the snapshot
        await using var insertCmd = _connection.CreateCommand();
        insertCmd.CommandText = $@"
            INSERT INTO network_stats_history
            (epoch, snapshot_at, tick_start, tick_end,
             total_transactions, total_transfers, total_volume,
             unique_senders, unique_receivers, total_active_addresses,
             new_addresses, returning_addresses,
             exchange_inflow_volume, exchange_inflow_count,
             exchange_outflow_volume, exchange_outflow_count, exchange_net_flow,
             sc_call_count, sc_unique_callers,
             avg_tx_size, median_tx_size,
             new_users_100m_plus, new_users_1b_plus, new_users_10b_plus)
            VALUES
            ({epoch}, '{snapshotAt:yyyy-MM-dd HH:mm:ss.fff}', {tickStart}, {tickEnd},
             {totalTransactions}, {totalTransfers}, {(ulong)totalVolume},
             {uniqueSenders}, {uniqueReceivers}, {uniqueSenders + uniqueReceivers},
             {newVsReturning.NewAddresses}, {newVsReturning.ReturningAddresses},
             {(ulong)exchangeFlows.InflowVolume}, {exchangeFlows.InflowCount},
             {(ulong)exchangeFlows.OutflowVolume}, {exchangeFlows.OutflowCount},
             {(long)exchangeFlows.InflowVolume - (long)exchangeFlows.OutflowVolume},
             {scCallCount}, {scUniqueCallers},
             {avgTxSize}, {medianTxSize},
             {newUsersHighBalance.Users100MPlus}, {newUsersHighBalance.Users1BPlus}, {newUsersHighBalance.Users10BPlus})";

        await insertCmd.ExecuteNonQueryAsync(ct);
        _logger.LogInformation(
            "Saved network stats snapshot for epoch {Epoch} (ticks {TickStart}-{TickEnd})",
            epoch, tickStart, tickEnd);
    }

    /// <summary>
    /// Save burn stats snapshot for the given epoch and tick range.
    /// Aggregates BURNING (log_type=8), DUST_BURNING (log_type=9),
    /// and direct transfers to burn address (log_type=0, input_type=0).
    /// </summary>
    public async Task SaveBurnStatsSnapshotAsync(
        uint epoch, ulong tickStart, ulong tickEnd, CancellationToken ct = default)
    {
        _logger.LogInformation(
            "Saving burn stats snapshot for epoch {Epoch} (ticks {TickStart}-{TickEnd})",
            epoch, tickStart, tickEnd);

        var tickEndTimestamp = await GetTickTimestampAsync(tickEnd, ct);
        var snapshotAt = tickEndTimestamp ?? DateTime.UtcNow;

        var tickFilter = $"tick_number >= {tickStart} AND tick_number <= {tickEnd}";

        // Query 1: explicit + dust burns
        await using var burnCmd = _connection.CreateCommand();
        burnCmd.CommandText = $@"
            SELECT
                countIf(log_type = 8) as burn_count,
                sumIf(amount, log_type = 8) as burn_amount,
                countIf(log_type = 9) as dust_count,
                sumIf(amount, log_type = 9) as dust_amount,
                max(amount) as max_burn
            FROM logs FINAL
            WHERE log_type IN (8, 9) AND {tickFilter}";

        ulong burnCount = 0, burnAmount = 0, dustBurnCount = 0, dustBurned = 0, maxBurnFromLogs = 0;
        await using (var reader = await burnCmd.ExecuteReaderAsync(ct))
        {
            if (await reader.ReadAsync(ct))
            {
                burnCount = Convert.ToUInt64(reader.GetValue(0));
                burnAmount = Convert.ToUInt64(reader.GetValue(1));
                dustBurnCount = Convert.ToUInt64(reader.GetValue(2));
                dustBurned = Convert.ToUInt64(reader.GetValue(3));
                maxBurnFromLogs = Convert.ToUInt64(reader.GetValue(4));
            }
        }

        // Query 2: direct transfers to burn address (input_type=0)
        // input_type=0 already excludes solution deposits (contract calls have input_type > 0)
        // and IPO bids (also input_type > 0). No need to subtract refunds.
        await using var transferCmd = _connection.CreateCommand();
        transferCmd.CommandText = $@"
            SELECT count() as transfer_burn_count, sum(amount) as transfer_burned, max(amount) as max_transfer_burn
            FROM logs FINAL
            WHERE log_type = 0 AND dest_address = '{AddressLabelService.BurnAddress}' AND input_type = 0
              AND {tickFilter}";

        ulong transferBurnCount = 0, transferBurned = 0, maxTransferBurn = 0;
        await using (var reader = await transferCmd.ExecuteReaderAsync(ct))
        {
            if (await reader.ReadAsync(ct))
            {
                transferBurnCount = Convert.ToUInt64(reader.GetValue(0));
                transferBurned = Convert.ToUInt64(reader.GetValue(1));
                maxTransferBurn = Convert.ToUInt64(reader.GetValue(2));
            }
        }

        // Query 3: unique burners across all types
        await using var uniqueCmd = _connection.CreateCommand();
        uniqueCmd.CommandText = $@"
            SELECT uniq(source_address)
            FROM logs FINAL
            WHERE (log_type IN (8, 9) OR (log_type = 0 AND dest_address = '{AddressLabelService.BurnAddress}' AND input_type = 0))
              AND {tickFilter}";
        var uniqueBurners = Convert.ToUInt64(await uniqueCmd.ExecuteScalarAsync(ct));

        var totalBurned = burnAmount + dustBurned + transferBurned;
        var largestBurn = Math.Max(maxBurnFromLogs, maxTransferBurn);

        // Query 4: cumulative total from prior snapshots
        await using var cumulativeCmd = _connection.CreateCommand();
        cumulativeCmd.CommandText = "SELECT sum(total_burned) FROM burn_stats_history";
        var priorTotal = Convert.ToUInt64(await cumulativeCmd.ExecuteScalarAsync(ct) ?? 0UL);
        var cumulativeBurned = priorTotal + totalBurned;

        // Insert
        await using var insertCmd = _connection.CreateCommand();
        insertCmd.CommandText = $@"
            INSERT INTO burn_stats_history
            (epoch, snapshot_at, tick_start, tick_end,
             total_burned, burn_count, burn_amount,
             dust_burn_count, dust_burned,
             transfer_burn_count, transfer_burned,
             unique_burners, largest_burn, cumulative_burned)
            VALUES
            ({epoch}, '{snapshotAt:yyyy-MM-dd HH:mm:ss.fff}', {tickStart}, {tickEnd},
             {totalBurned}, {burnCount}, {burnAmount},
             {dustBurnCount}, {dustBurned},
             {transferBurnCount}, {transferBurned},
             {uniqueBurners}, {largestBurn}, {cumulativeBurned})";

        await insertCmd.ExecuteNonQueryAsync(ct);
        _logger.LogInformation(
            "Saved burn stats snapshot for epoch {Epoch} (ticks {TickStart}-{TickEnd}): totalBurned={Total}, burns={Burns}, dust={Dust}, transfers={Transfers}",
            epoch, tickStart, tickEnd, totalBurned, burnCount, dustBurnCount, transferBurnCount);
    }

    /// <summary>
    /// Saves miner flow statistics
    /// </summary>
    public async Task SaveMinerFlowStatsAsync(MinerFlowStatsDto stats, CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            INSERT INTO miner_flow_stats (
                epoch, snapshot_at, tick_start, tick_end, emission_epoch,
                total_emission, computor_count, total_outflow, outflow_tx_count,
                flow_to_exchange_direct, flow_to_exchange_1hop, flow_to_exchange_2hop, flow_to_exchange_3plus,
                flow_to_exchange_total, flow_to_exchange_count, flow_to_other, miner_net_position,
                hop_1_volume, hop_2_volume, hop_3_volume, hop_4_plus_volume
            ) VALUES (
                {stats.Epoch},
                '{stats.SnapshotAt:yyyy-MM-dd HH:mm:ss}',
                {stats.TickStart},
                {stats.TickEnd},
                {stats.EmissionEpoch},
                {stats.TotalEmission},
                {stats.ComputorCount},
                {stats.TotalOutflow},
                {stats.OutflowTxCount},
                {stats.FlowToExchangeDirect},
                {stats.FlowToExchange1Hop},
                {stats.FlowToExchange2Hop},
                {stats.FlowToExchange3Plus},
                {stats.FlowToExchangeTotal},
                {stats.FlowToExchangeCount},
                {stats.FlowToOther},
                {stats.MinerNetPosition},
                {stats.Hop1Volume},
                {stats.Hop2Volume},
                {stats.Hop3Volume},
                {stats.Hop4PlusVolume}
            )";

        await cmd.ExecuteNonQueryAsync(ct);
        _logger.LogInformation("Saved miner flow stats for epoch {Epoch}", stats.Epoch);
    }

    // =====================================================
    // SNAPSHOT WINDOWING HELPERS
    // =====================================================

    /// <summary>
    /// Get the last holder distribution snapshot tick_end for the given epoch
    /// </summary>
    public async Task<ulong> GetLastHolderDistributionSnapshotTickEndAsync(uint epoch, CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT max(tick_end)
            FROM holder_distribution_history
            WHERE epoch = {epoch}";

        var result = await cmd.ExecuteScalarAsync(ct);
        if (result == null || result == DBNull.Value)
            return 0;

        return Convert.ToUInt64(result);
    }

    /// <summary>
    /// Get the last network stats snapshot tick end for the given epoch
    /// </summary>
    public async Task<ulong> GetLastNetworkStatsSnapshotTickEndAsync(uint epoch, CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();
        // Only consider snapshots with actual tick ranges (tick_end > 0)
        // This allows us to retry with proper tick ranges if we previously fell back to epoch-based
        cmd.CommandText = $"SELECT max(tick_end) FROM network_stats_history WHERE epoch = {epoch} AND tick_end > 0";
        var result = await cmd.ExecuteScalarAsync(ct);
        if (result == null || result == DBNull.Value)
            return 0;
        return Convert.ToUInt64(result);
    }

    /// <summary>
    /// Get the last burn stats snapshot tick end for the given epoch
    /// </summary>
    public async Task<ulong> GetLastBurnStatsSnapshotTickEndAsync(uint epoch, CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $"SELECT max(tick_end) FROM burn_stats_history WHERE epoch = {epoch} AND tick_end > 0";
        var result = await cmd.ExecuteScalarAsync(ct);
        if (result == null || result == DBNull.Value)
            return 0;
        return Convert.ToUInt64(result);
    }

    /// <summary>
    /// Gets the last miner flow stats snapshot tick_end for catch-up
    /// </summary>
    public async Task<ulong> GetLastMinerFlowSnapshotTickEndAsync(uint epoch, CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT MAX(tick_end)
            FROM miner_flow_stats
            WHERE epoch = {epoch}";

        var result = await cmd.ExecuteScalarAsync(ct);
        if (result == null || result == DBNull.Value)
            return 0;

        return Convert.ToUInt64(result);
    }

    // =====================================================
    // TICK/TIME HELPERS
    // =====================================================

    /// <summary>
    /// Get the current (latest) epoch from the ticks table (real-time source of truth)
    /// </summary>
    public async Task<uint?> GetCurrentEpochAsync(CancellationToken ct = default)
    {
        // Use ticks table as the real-time source of truth
        // (ticks are indexed continuously, epoch_meta lags behind)
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = "SELECT max(epoch) FROM ticks";
        var result = await cmd.ExecuteScalarAsync(ct);
        if (result != null && result != DBNull.Value)
        {
            var epoch = Convert.ToUInt32(result);
            if (epoch > 0)
                return epoch;
        }

        // Fall back to epoch_meta if ticks table is empty
        await using var metaCmd = _connection.CreateCommand();
        metaCmd.CommandText = "SELECT max(epoch) FROM epoch_meta";
        var metaResult = await metaCmd.ExecuteScalarAsync(ct);
        if (metaResult == null || metaResult == DBNull.Value)
            return null;
        return Convert.ToUInt32(metaResult);
    }

    /// <summary>
    /// Get the current (latest) tick number from the ticks table
    /// </summary>
    public async Task<ulong?> GetCurrentTickAsync(CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = "SELECT max(tick_number) FROM ticks";
        var result = await cmd.ExecuteScalarAsync(ct);
        if (result == null || result == DBNull.Value)
            return null;
        return Convert.ToUInt64(result);
    }

    /// <summary>
    /// Get the timestamp of a specific tick
    /// </summary>
    public async Task<DateTime?> GetTickTimestampAsync(ulong tickNumber, CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $"SELECT timestamp FROM ticks WHERE tick_number = {tickNumber}";
        var result = await cmd.ExecuteScalarAsync(ct);
        if (result == null || result == DBNull.Value)
            return null;
        return Convert.ToDateTime(result);
    }

    /// <summary>
    /// Get the next tick after a given tick number (handles gaps in tick numbers)
    /// </summary>
    public async Task<(ulong TickNumber, DateTime Timestamp)?> GetNextTickAfterAsync(ulong tickNumber, CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $"SELECT tick_number, timestamp FROM ticks WHERE tick_number > {tickNumber} ORDER BY tick_number ASC LIMIT 1";
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct))
            return null;
        return (Convert.ToUInt64(reader.GetValue(0)), reader.GetDateTime(1));
    }

    /// <summary>
    /// Get the tick number at or before a specific timestamp
    /// </summary>
    public async Task<ulong?> GetTickAtTimestampAsync(DateTime timestamp, CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT max(tick_number)
            FROM ticks
            WHERE timestamp <= '{timestamp:yyyy-MM-dd HH:mm:ss}'";
        var result = await cmd.ExecuteScalarAsync(ct);
        if (result == null || result == DBNull.Value)
            return null;
        return Convert.ToUInt64(result);
    }

    /// <summary>
    /// Get the first tick (min tick_number) in the database and its timestamp
    /// </summary>
    public async Task<(ulong TickNumber, DateTime Timestamp)?> GetFirstTickAsync(CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = "SELECT tick_number, timestamp FROM ticks WHERE tick_number > 0 ORDER BY tick_number ASC LIMIT 1";
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct))
            return null;
        return (Convert.ToUInt64(reader.GetValue(0)), reader.GetDateTime(1));
    }

    /// <summary>
    /// Get the tick range (min and max tick_number) for a specific epoch from the ticks table.
    /// </summary>
    public async Task<(ulong MinTick, ulong MaxTick)?> GetTickRangeForEpochAsync(uint epoch, CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT min(tick_number), max(tick_number)
            FROM ticks
            WHERE epoch = {epoch}";

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct) || reader.IsDBNull(0) || reader.IsDBNull(1))
            return null;

        return (Convert.ToUInt64(reader.GetValue(0)), Convert.ToUInt64(reader.GetValue(1)));
    }

    // =====================================================
    // PRIVATE COMPUTATION HELPERS
    // =====================================================

    /// <summary>
    /// Get new vs returning addresses for a specific epoch
    /// </summary>
    public async Task<(ulong NewAddresses, ulong ReturningAddresses)> GetNewVsReturningForEpochAsync(
        uint epoch, CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            WITH first_appearance AS (
                SELECT
                    address,
                    min(epoch) as first_epoch
                FROM (
                    SELECT from_address as address, epoch FROM transactions WHERE from_address != ''
                    UNION ALL
                    SELECT to_address as address, epoch FROM transactions WHERE to_address != ''
                )
                GROUP BY address
            ),
            epoch_addresses AS (
                SELECT DISTINCT address
                FROM (
                    SELECT from_address as address FROM transactions WHERE from_address != '' AND epoch = {epoch}
                    UNION ALL
                    SELECT to_address as address FROM transactions WHERE to_address != '' AND epoch = {epoch}
                )
            )
            SELECT
                countIf(fa.first_epoch = {epoch}) as new_addresses,
                countIf(fa.first_epoch < {epoch}) as returning_addresses
            FROM epoch_addresses ea
            JOIN first_appearance fa ON ea.address = fa.address";

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        if (await reader.ReadAsync(ct))
        {
            return (
                Convert.ToUInt64(reader.GetValue(0)),
                Convert.ToUInt64(reader.GetValue(1))
            );
        }

        return (0, 0);
    }

    /// <summary>
    /// Get new vs returning addresses for a specific tick range
    /// </summary>
    public async Task<(ulong NewAddresses, ulong ReturningAddresses)> GetNewVsReturningForTickRangeAsync(
        ulong tickStart, ulong tickEnd, CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            WITH first_appearance AS (
                SELECT
                    address,
                    min(tick_number) as first_tick
                FROM (
                    SELECT from_address as address, tick_number FROM transactions WHERE from_address != ''
                    UNION ALL
                    SELECT to_address as address, tick_number FROM transactions WHERE to_address != ''
                )
                GROUP BY address
            ),
            window_addresses AS (
                SELECT DISTINCT address
                FROM (
                    SELECT from_address as address FROM transactions WHERE from_address != '' AND tick_number >= {tickStart} AND tick_number <= {tickEnd}
                    UNION ALL
                    SELECT to_address as address FROM transactions WHERE to_address != '' AND tick_number >= {tickStart} AND tick_number <= {tickEnd}
                )
            )
            SELECT
                countIf(fa.first_tick >= {tickStart} AND fa.first_tick <= {tickEnd}) as new_addresses,
                countIf(fa.first_tick < {tickStart}) as returning_addresses
            FROM window_addresses wa
            JOIN first_appearance fa ON wa.address = fa.address";

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        if (await reader.ReadAsync(ct))
        {
            return (
                Convert.ToUInt64(reader.GetValue(0)),
                Convert.ToUInt64(reader.GetValue(1))
            );
        }

        return (0, 0);
    }

    /// <summary>
    /// Get exchange flows for a specific epoch
    /// </summary>
    public async Task<(decimal InflowVolume, ulong InflowCount, decimal OutflowVolume, ulong OutflowCount)>
        GetExchangeFlowsForEpochAsync(uint epoch, CancellationToken ct = default)
    {
        await _labelService.EnsureFreshDataAsync();
        var exchangeAddresses = _labelService.GetAddressesByType(AddressType.Exchange);

        if (!exchangeAddresses.Any())
            return (0, 0, 0, 0);

        var addressList = string.Join("','", exchangeAddresses.Select(e => e.Address));

        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT
                sumIf(amount, dest_address IN ('{addressList}')) as inflow_volume,
                countIf(dest_address IN ('{addressList}')) as inflow_count,
                sumIf(amount, source_address IN ('{addressList}')) as outflow_volume,
                countIf(source_address IN ('{addressList}')) as outflow_count
            FROM logs
            WHERE log_type = 0 AND amount > 0 AND epoch = {epoch}";

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        if (await reader.ReadAsync(ct))
        {
            return (
                Convert.ToDecimal(reader.GetValue(0)),
                Convert.ToUInt64(reader.GetValue(1)),
                Convert.ToDecimal(reader.GetValue(2)),
                Convert.ToUInt64(reader.GetValue(3))
            );
        }

        return (0, 0, 0, 0);
    }

    /// <summary>
    /// Get exchange flows for a specific tick range
    /// </summary>
    public async Task<(decimal InflowVolume, ulong InflowCount, decimal OutflowVolume, ulong OutflowCount)>
        GetExchangeFlowsForTickRangeAsync(ulong tickStart, ulong tickEnd, CancellationToken ct = default)
    {
        await _labelService.EnsureFreshDataAsync();
        var exchangeAddresses = _labelService.GetAddressesByType(AddressType.Exchange);

        if (!exchangeAddresses.Any())
            return (0, 0, 0, 0);

        var addressList = string.Join("','", exchangeAddresses.Select(e => e.Address));

        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT
                sumIf(amount, dest_address IN ('{addressList}')) as inflow_volume,
                countIf(dest_address IN ('{addressList}')) as inflow_count,
                sumIf(amount, source_address IN ('{addressList}')) as outflow_volume,
                countIf(source_address IN ('{addressList}')) as outflow_count
            FROM logs
            WHERE log_type = 0 AND amount > 0 AND tick_number >= {tickStart} AND tick_number <= {tickEnd}";

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        if (await reader.ReadAsync(ct))
        {
            return (
                Convert.ToDecimal(reader.GetValue(0)),
                Convert.ToUInt64(reader.GetValue(1)),
                Convert.ToDecimal(reader.GetValue(2)),
                Convert.ToUInt64(reader.GetValue(3))
            );
        }

        return (0, 0, 0, 0);
    }

    /// <summary>
    /// Get new users with high balances for a specific epoch.
    /// Returns count of new addresses (first seen this epoch) that received >=100M, >=1B, >=10B.
    /// </summary>
    public async Task<(ulong Users100MPlus, ulong Users1BPlus, ulong Users10BPlus)>
        GetNewUsersWithHighBalanceForEpochAsync(uint epoch, CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();
        // Find addresses first seen in this epoch that received significant amounts
        cmd.CommandText = $@"
            WITH new_addresses AS (
                -- Addresses first seen in this epoch
                SELECT address
                FROM address_first_seen
                WHERE first_epoch = {epoch}
            ),
            received_amounts AS (
                -- Total amount received by each new address in this epoch
                SELECT
                    dest_address as address,
                    sum(amount) as total_received
                FROM logs
                WHERE log_type = 0 AND epoch = {epoch}
                  AND dest_address IN (SELECT address FROM new_addresses)
                GROUP BY dest_address
            )
            SELECT
                countIf(total_received >= 100000000) as users_100m,     -- >=100M
                countIf(total_received >= 1000000000) as users_1b,      -- >=1B
                countIf(total_received >= 10000000000) as users_10b     -- >=10B
            FROM received_amounts";

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        if (await reader.ReadAsync(ct))
        {
            return (
                Convert.ToUInt64(reader.GetValue(0)),
                Convert.ToUInt64(reader.GetValue(1)),
                Convert.ToUInt64(reader.GetValue(2))
            );
        }

        return (0, 0, 0);
    }

    /// <summary>
    /// Get new users with high balances for a specific tick range.
    /// Returns count of new addresses (first seen in this window) that received >=100M, >=1B, >=10B.
    /// </summary>
    public async Task<(ulong Users100MPlus, ulong Users1BPlus, ulong Users10BPlus)>
        GetNewUsersWithHighBalanceForTickRangeAsync(ulong tickStart, ulong tickEnd, CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();
        // Find addresses first seen in this tick range that received significant amounts
        cmd.CommandText = $@"
            WITH new_addresses AS (
                -- Addresses first seen in this tick range
                SELECT address
                FROM address_first_seen
                WHERE first_tick >= {tickStart} AND first_tick <= {tickEnd}
            ),
            received_amounts AS (
                -- Total amount received by each new address in this tick range
                SELECT
                    dest_address as address,
                    sum(amount) as total_received
                FROM logs
                WHERE log_type = 0
                  AND tick_number >= {tickStart} AND tick_number <= {tickEnd}
                  AND dest_address IN (SELECT address FROM new_addresses)
                GROUP BY dest_address
            )
            SELECT
                countIf(total_received >= 100000000) as users_100m,     -- >=100M
                countIf(total_received >= 1000000000) as users_1b,      -- >=1B
                countIf(total_received >= 10000000000) as users_10b     -- >=10B
            FROM received_amounts";

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        if (await reader.ReadAsync(ct))
        {
            return (
                Convert.ToUInt64(reader.GetValue(0)),
                Convert.ToUInt64(reader.GetValue(1)),
                Convert.ToUInt64(reader.GetValue(2))
            );
        }

        return (0, 0, 0);
    }

    // =====================================================
    // EPOCH METADATA
    // =====================================================

    /// <summary>
    /// Get epoch metadata by epoch number
    /// </summary>
    public async Task<EpochMetaDto?> GetEpochMetaAsync(uint epoch, CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT epoch, initial_tick, end_tick, end_tick_start_log_id, end_tick_end_log_id,
                   is_complete, updated_at
            FROM epoch_meta
            WHERE epoch = {epoch}";

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct))
            return null;

        return new EpochMetaDto(
            reader.GetFieldValue<uint>(0),
            reader.GetFieldValue<ulong>(1),
            reader.GetFieldValue<ulong>(2),
            reader.GetFieldValue<ulong>(3),
            reader.GetFieldValue<ulong>(4),
            reader.GetFieldValue<byte>(5) == 1,
            reader.GetDateTime(6)
        );
    }

    // =====================================================
    // COMPUTOR/MINER FLOW TRACKING METHODS
    // =====================================================

    /// <summary>
    /// Saves computor list for an epoch
    /// </summary>
    public async Task SaveComputorsAsync(uint epoch, List<string> addresses, CancellationToken ct = default)
    {
        if (addresses.Count == 0) return;

        var sb = new StringBuilder();
        sb.AppendLine("INSERT INTO computors (epoch, address, computor_index) VALUES");

        var values = new List<string>();
        for (int i = 0; i < addresses.Count; i++)
        {
            values.Add($"({epoch}, '{EscapeSql(addresses[i])}', {i})");
        }
        sb.AppendLine(string.Join(",\n", values));

        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = sb.ToString();
        await cmd.ExecuteNonQueryAsync(ct);

        // Record import
        await using var importCmd = _connection.CreateCommand();
        importCmd.CommandText = $@"
            INSERT INTO computor_imports (epoch, computor_count)
            VALUES ({epoch}, {addresses.Count})";
        await importCmd.ExecuteNonQueryAsync(ct);

        _logger.LogInformation("Saved {Count} computors for epoch {Epoch}", addresses.Count, epoch);
    }

    /// <summary>
    /// Gets computor list for an epoch
    /// </summary>
    public async Task<ComputorListDto?> GetComputorsAsync(uint epoch, CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT address, computor_index
            FROM computors
            WHERE epoch = {epoch}
            ORDER BY computor_index";

        var computors = new List<ComputorDto>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var address = reader.GetString(0);
            var label = _labelService.GetLabel(address);
            computors.Add(new ComputorDto(
                Epoch: epoch,
                Address: address,
                Index: reader.GetFieldValue<ushort>(1),
                Label: label
            ));
        }

        if (computors.Count == 0) return null;

        // Get import timestamp
        await using var importCmd = _connection.CreateCommand();
        importCmd.CommandText = $"SELECT imported_at FROM computor_imports WHERE epoch = {epoch} LIMIT 1";
        var importedAt = await importCmd.ExecuteScalarAsync(ct);

        return new ComputorListDto(
            Epoch: epoch,
            Computors: computors,
            Count: computors.Count,
            ImportedAt: importedAt != null ? Convert.ToDateTime(importedAt) : null
        );
    }

    /// <summary>
    /// Gets flow hops for visualization
    /// </summary>
    public async Task<List<FlowHopDto>> GetFlowHopsAsync(
        uint epoch,
        ulong tickStart,
        ulong tickEnd,
        int maxDepth,
        CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT
                epoch, tick_number, timestamp, tx_hash,
                source_address, dest_address, amount,
                origin_address, origin_type, hop_level, dest_type, dest_label
            FROM flow_hops
            WHERE epoch = {epoch}
              AND tick_number BETWEEN {tickStart} AND {tickEnd}
              AND hop_level <= {maxDepth}
            ORDER BY hop_level, tick_number";

        var result = new List<FlowHopDto>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var sourceAddr = reader.GetString(4);
            var destAddr = reader.GetString(5);
            var sourceInfo = _labelService.GetAddressInfo(sourceAddr);
            var destInfo = _labelService.GetAddressInfo(destAddr);
            var destLabel = reader.IsDBNull(11) ? null : reader.GetString(11);
            if (string.IsNullOrEmpty(destLabel))
            {
                destLabel = destInfo?.Label;
            }

            result.Add(new FlowHopDto(
                Epoch: reader.GetFieldValue<uint>(0),
                TickNumber: reader.GetFieldValue<ulong>(1),
                Timestamp: reader.GetDateTime(2),
                TxHash: reader.GetString(3),
                SourceAddress: sourceAddr,
                SourceLabel: sourceInfo?.Label,
                SourceType: sourceInfo?.Type.ToString().ToLowerInvariant(),
                DestAddress: destAddr,
                DestLabel: destLabel,
                DestType: reader.IsDBNull(10) ? null : reader.GetString(10),
                Amount: ToBigDecimal(reader.GetValue(6)),
                OriginAddress: reader.GetString(7),
                OriginType: reader.GetString(8),
                HopLevel: reader.GetFieldValue<byte>(9)
            ));
        }

        return result;
    }

    /// <summary>
    /// Gets all flow hops for a specific emission epoch (across all tick windows).
    /// This is used for visualization to show the complete flow from emission through all tracked hops.
    /// </summary>
    public async Task<List<FlowHopDto>> GetFlowHopsByEmissionEpochAsync(
        uint emissionEpoch,
        int maxDepth,
        CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT
                epoch, tick_number, timestamp, tx_hash,
                source_address, dest_address, amount,
                origin_address, origin_type, hop_level, dest_type, dest_label
            FROM flow_hops FINAL
            WHERE emission_epoch = {emissionEpoch}
              AND hop_level <= {maxDepth}
            ORDER BY hop_level, tick_number";

        var result = new List<FlowHopDto>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var sourceAddr = reader.GetString(4);
            var destAddr = reader.GetString(5);
            var sourceInfo = _labelService.GetAddressInfo(sourceAddr);
            var destInfo = _labelService.GetAddressInfo(destAddr);
            var destLabel = reader.IsDBNull(11) ? null : reader.GetString(11);
            if (string.IsNullOrEmpty(destLabel))
            {
                destLabel = destInfo?.Label;
            }

            result.Add(new FlowHopDto(
                Epoch: reader.GetFieldValue<uint>(0),
                TickNumber: reader.GetFieldValue<ulong>(1),
                Timestamp: reader.GetDateTime(2),
                TxHash: reader.GetString(3),
                SourceAddress: sourceAddr,
                SourceLabel: sourceInfo?.Label,
                SourceType: sourceInfo?.Type.ToString().ToLowerInvariant(),
                DestAddress: destAddr,
                DestLabel: destLabel,
                DestType: reader.IsDBNull(10) ? null : reader.GetString(10),
                Amount: ToBigDecimal(reader.GetValue(6)),
                OriginAddress: reader.GetString(7),
                OriginType: reader.GetString(8),
                HopLevel: reader.GetFieldValue<byte>(9)
            ));
        }

        return result;
    }

    /// <summary>
    /// Gets miner flow stats history.
    /// </summary>
    public async Task<List<MinerFlowStatsDto>> GetMinerFlowStatsHistoryAsync(int limit, DateTime? from = null, DateTime? to = null, CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();
        var conditions = new List<string>();
        if (from.HasValue)
            conditions.Add($"snapshot_at >= '{from.Value:yyyy-MM-dd HH:mm:ss}'");
        if (to.HasValue)
            conditions.Add($"snapshot_at <= '{to.Value:yyyy-MM-dd HH:mm:ss}'");
        var whereClause = conditions.Count > 0 ? "WHERE " + string.Join(" AND ", conditions) : "";

        cmd.CommandText = $@"
            SELECT
                epoch, snapshot_at, tick_start, tick_end, emission_epoch,
                total_emission, computor_count, total_outflow, outflow_tx_count,
                flow_to_exchange_direct, flow_to_exchange_1hop, flow_to_exchange_2hop, flow_to_exchange_3plus,
                flow_to_exchange_total, flow_to_exchange_count, flow_to_other, miner_net_position,
                hop_1_volume, hop_2_volume, hop_3_volume, hop_4_plus_volume
            FROM miner_flow_stats
            {whereClause}
            ORDER BY snapshot_at DESC
            LIMIT {limit}";

        var result = new List<MinerFlowStatsDto>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            result.Add(new MinerFlowStatsDto(
                Epoch: reader.GetFieldValue<uint>(0),
                SnapshotAt: reader.GetDateTime(1),
                TickStart: reader.GetFieldValue<ulong>(2),
                TickEnd: reader.GetFieldValue<ulong>(3),
                EmissionEpoch: reader.GetFieldValue<uint>(4),
                TotalEmission: ToBigDecimal(reader.GetValue(5)),
                ComputorCount: reader.GetFieldValue<ushort>(6),
                TotalOutflow: ToBigDecimal(reader.GetValue(7)),
                OutflowTxCount: reader.GetFieldValue<ulong>(8),
                FlowToExchangeDirect: ToBigDecimal(reader.GetValue(9)),
                FlowToExchange1Hop: ToBigDecimal(reader.GetValue(10)),
                FlowToExchange2Hop: ToBigDecimal(reader.GetValue(11)),
                FlowToExchange3Plus: ToBigDecimal(reader.GetValue(12)),
                FlowToExchangeTotal: ToBigDecimal(reader.GetValue(13)),
                FlowToExchangeCount: reader.GetFieldValue<ulong>(14),
                FlowToOther: ToBigDecimal(reader.GetValue(15)),
                MinerNetPosition: ToBigDecimal(reader.GetValue(16)),
                Hop1Volume: ToBigDecimal(reader.GetValue(17)),
                Hop2Volume: ToBigDecimal(reader.GetValue(18)),
                Hop3Volume: ToBigDecimal(reader.GetValue(19)),
                Hop4PlusVolume: ToBigDecimal(reader.GetValue(20))
            ));
        }

        return result;
    }

    /// <summary>
    /// Checks if emissions have been imported for an epoch
    /// </summary>
    public async Task<bool> IsEmissionImportedAsync(uint epoch, CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $"SELECT count() FROM emission_imports WHERE epoch = {epoch}";
        var result = await cmd.ExecuteScalarAsync(ct);
        return Convert.ToInt64(result ?? 0) > 0;
    }

    /// <summary>
    /// Gets emission summary for an epoch
    /// </summary>
    public async Task<EmissionSummaryDto?> GetEmissionSummaryAsync(uint epoch, CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT computor_count, total_emission, emission_tick, imported_at
            FROM emission_imports
            WHERE epoch = {epoch}";

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        if (await reader.ReadAsync(ct))
        {
            return new EmissionSummaryDto(
                Epoch: epoch,
                ComputorCount: reader.GetFieldValue<ushort>(0),
                TotalEmission: ToBigDecimal(reader.GetValue(1)),
                EmissionTick: reader.GetFieldValue<ulong>(2),
                ImportedAt: reader.GetDateTime(3)
            );
        }

        return null;
    }

    /// <summary>
    /// Gets all emissions for an epoch with computor details
    /// </summary>
    public async Task<List<ComputorEmissionDto>> GetEmissionsForEpochAsync(uint epoch, CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT
                epoch, computor_index, address, emission_amount, emission_tick, emission_timestamp
            FROM computor_emissions
            WHERE epoch = {epoch}
            ORDER BY computor_index";

        var result = new List<ComputorEmissionDto>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var address = reader.GetString(2);
            var label = _labelService.GetLabel(address);

            result.Add(new ComputorEmissionDto(
                Epoch: reader.GetFieldValue<uint>(0),
                ComputorIndex: reader.GetFieldValue<ushort>(1),
                Address: address,
                Label: label,
                EmissionAmount: ToBigDecimal(reader.GetValue(3)),
                EmissionTick: reader.GetFieldValue<ulong>(4),
                EmissionTimestamp: reader.GetDateTime(5)
            ));
        }

        return result;
    }

    /// <summary>
    /// Gets emission for a specific computor address in an epoch
    /// </summary>
    public async Task<decimal> GetComputorEmissionAsync(uint epoch, string address, CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT COALESCE(emission_amount, 0)
            FROM computor_emissions
            WHERE epoch = {epoch} AND address = '{EscapeSql(address)}'";

        var result = await cmd.ExecuteScalarAsync(ct);
        return ToBigDecimal(result ?? 0);
    }

    /// <summary>
    /// Captures and saves emissions for computors at the end of an epoch.
    /// Scans the end-epoch tick logs for transfers from zero address to computor addresses.
    /// </summary>
    public async Task<(int ComputorCount, decimal TotalEmission)> CaptureEmissionsForEpochAsync(
        uint epoch,
        ulong endTick,
        HashSet<string> computorAddresses,
        Dictionary<string, int> addressToIndex,
        CancellationToken ct = default)
    {
        // Query transfers from zero address to computor addresses in the end tick
        var addressList = string.Join(",", computorAddresses.Select(a => $"'{EscapeSql(a)}'"));

        _logger.LogDebug("Computorslist for Epoch {Epoch} in tick {Tick}: {List}", epoch, endTick, addressList);

        await using var queryCmd = _connection.CreateCommand();
        queryCmd.CommandText = $@"
            SELECT
                dest_address,
                sum(amount) as emission_amount,
                max(timestamp) as emission_timestamp
            FROM logs
            WHERE log_type = 0
              AND tick_number = {endTick}
              AND source_address = '{AddressLabelService.BurnAddress}'
              AND dest_address IN ({addressList})
            GROUP BY dest_address";

        var emissions = new List<(string Address, int Index, decimal Amount, DateTime Timestamp)>();
        decimal totalEmission = 0;

        await using var reader = await queryCmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var address = reader.GetString(0);
            var amount = ToBigDecimal(reader.GetValue(1));
            var timestamp = reader.GetDateTime(2);

            if (addressToIndex.TryGetValue(address, out var index))
            {
                emissions.Add((address, index, amount, timestamp));
                totalEmission += amount;
            }
        }

        if (emissions.Count == 0)
        {
            _logger.LogWarning("No emissions found for epoch {Epoch} at tick {Tick}", epoch, endTick);
            return (0, 0);
        }

        // Save emissions
        var sb = new StringBuilder();
        sb.AppendLine(@"INSERT INTO computor_emissions (
            epoch, computor_index, address, emission_amount, emission_tick, emission_timestamp
        ) VALUES");

        var values = emissions.Select(e => $@"(
            {epoch},
            {e.Index},
            '{EscapeSql(e.Address)}',
            {e.Amount},
            {endTick},
            '{e.Timestamp:yyyy-MM-dd HH:mm:ss}'
        )");

        sb.AppendLine(string.Join(",\n", values));

        await using var insertCmd = _connection.CreateCommand();
        insertCmd.CommandText = sb.ToString();
        await insertCmd.ExecuteNonQueryAsync(ct);

        // Save import record
        await using var importCmd = _connection.CreateCommand();
        importCmd.CommandText = $@"
            INSERT INTO emission_imports (epoch, computor_count, total_emission, emission_tick)
            VALUES ({epoch}, {emissions.Count}, {totalEmission}, {endTick})";
        await importCmd.ExecuteNonQueryAsync(ct);

        _logger.LogInformation(
            "Captured emissions for epoch {Epoch}: {Count} computors, total {Total}",
            epoch, emissions.Count, totalEmission);

        return (emissions.Count, totalEmission);
    }

    /// <summary>
    /// Recalculates all miner_flow_stats snapshots with correct emission values from emission_imports.
    /// Returns the number of snapshots updated.
    /// </summary>
    public async Task<int> RecalculateMinerFlowStatsEmissionsAsync(CancellationToken ct = default)
    {
        // Get all snapshots with their emission epochs
        await using var cmdSelect = _connection.CreateCommand();
        cmdSelect.CommandText = @"
            SELECT epoch, emission_epoch, snapshot_at, tick_start, tick_end,
                   computor_count, total_outflow, outflow_tx_count,
                   flow_to_exchange_direct, flow_to_exchange_1hop, flow_to_exchange_2hop, flow_to_exchange_3plus,
                   flow_to_exchange_total, flow_to_exchange_count, flow_to_other, miner_net_position,
                   hop1_volume, hop2_volume, hop3_volume, hop4_plus_volume
            FROM miner_flow_stats FINAL
            ORDER BY epoch, tick_start";

        var snapshots = new List<(uint Epoch, uint EmissionEpoch, DateTime SnapshotAt, ulong TickStart, ulong TickEnd,
            ushort ComputorCount, decimal TotalOutflow, ulong OutflowTxCount,
            decimal FlowToExchangeDirect, decimal FlowToExchange1Hop, decimal FlowToExchange2Hop, decimal FlowToExchange3Plus,
            decimal FlowToExchangeTotal, ulong FlowToExchangeCount, decimal FlowToOther, decimal MinerNetPosition,
            decimal Hop1Volume, decimal Hop2Volume, decimal Hop3Volume, decimal Hop4PlusVolume)>();

        await using (var reader = await cmdSelect.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                snapshots.Add((
                    reader.GetFieldValue<uint>(0),
                    reader.GetFieldValue<uint>(1),
                    reader.GetDateTime(2),
                    reader.GetFieldValue<ulong>(3),
                    reader.GetFieldValue<ulong>(4),
                    reader.GetFieldValue<ushort>(5),
                    ToBigDecimal(reader.GetValue(6)),
                    reader.GetFieldValue<ulong>(7),
                    ToBigDecimal(reader.GetValue(8)),
                    ToBigDecimal(reader.GetValue(9)),
                    ToBigDecimal(reader.GetValue(10)),
                    ToBigDecimal(reader.GetValue(11)),
                    ToBigDecimal(reader.GetValue(12)),
                    reader.GetFieldValue<ulong>(13),
                    ToBigDecimal(reader.GetValue(14)),
                    ToBigDecimal(reader.GetValue(15)),
                    ToBigDecimal(reader.GetValue(16)),
                    ToBigDecimal(reader.GetValue(17)),
                    ToBigDecimal(reader.GetValue(18)),
                    ToBigDecimal(reader.GetValue(19))
                ));
            }
        }

        if (snapshots.Count == 0)
            return 0;

        // Get emissions for all unique emission epochs
        var emissionEpochs = snapshots.Select(s => s.EmissionEpoch).Distinct().ToList();
        var emissions = new Dictionary<uint, decimal>();
        foreach (var ep in emissionEpochs)
        {
            emissions[ep] = await GetTotalEmissionForEpochAsync(ep, ct);
        }

        // Reinsert snapshots with corrected emission values (with updated snapshot_at for ReplacingMergeTree)
        foreach (var s in snapshots)
        {
            var correctEmission = emissions.GetValueOrDefault(s.EmissionEpoch, 0);
            var newSnapshotAt = s.SnapshotAt.AddMilliseconds(1); // Add 1ms for versioning

            await using var cmdInsert = _connection.CreateCommand();
            cmdInsert.CommandText = $@"
                INSERT INTO miner_flow_stats (
                    epoch, emission_epoch, snapshot_at, tick_start, tick_end,
                    total_emission, computor_count, total_outflow, outflow_tx_count,
                    flow_to_exchange_direct, flow_to_exchange_1hop, flow_to_exchange_2hop, flow_to_exchange_3plus,
                    flow_to_exchange_total, flow_to_exchange_count, flow_to_other, miner_net_position,
                    hop1_volume, hop2_volume, hop3_volume, hop4_plus_volume
                ) VALUES (
                    {s.Epoch}, {s.EmissionEpoch}, '{newSnapshotAt:yyyy-MM-dd HH:mm:ss.fff}',
                    {s.TickStart}, {s.TickEnd},
                    {correctEmission}, {s.ComputorCount}, {s.TotalOutflow}, {s.OutflowTxCount},
                    {s.FlowToExchangeDirect}, {s.FlowToExchange1Hop}, {s.FlowToExchange2Hop}, {s.FlowToExchange3Plus},
                    {s.FlowToExchangeTotal}, {s.FlowToExchangeCount}, {s.FlowToOther}, {s.MinerNetPosition},
                    {s.Hop1Volume}, {s.Hop2Volume}, {s.Hop3Volume}, {s.Hop4PlusVolume}
                )";

            await cmdInsert.ExecuteNonQueryAsync(ct);
        }

        // Force merge to apply ReplacingMergeTree deduplication
        await using var cmdOptimize = _connection.CreateCommand();
        cmdOptimize.CommandText = "OPTIMIZE TABLE miner_flow_stats FINAL";
        await cmdOptimize.ExecuteNonQueryAsync(ct);

        return snapshots.Count;
    }

    /// <summary>
    /// Recalculates avg_tx_size, median_tx_size, and exchange flow columns
    /// for all network_stats_history snapshots, excluding zero-amount transfers.
    /// Returns the number of snapshots updated.
    /// </summary>
    public async Task<int> RecalculateNetworkStatsAsync(CancellationToken ct = default)
    {
        // Get all existing snapshots
        await using var cmdSelect = _connection.CreateCommand();
        cmdSelect.CommandText = @"
            SELECT epoch, snapshot_at, tick_start, tick_end
            FROM network_stats_history FINAL
            ORDER BY epoch, snapshot_at";

        var snapshots = new List<(uint Epoch, DateTime SnapshotAt, ulong TickStart, ulong TickEnd)>();
        await using (var reader = await cmdSelect.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                snapshots.Add((
                    reader.GetFieldValue<uint>(0),
                    reader.GetDateTime(1),
                    reader.GetFieldValue<ulong>(2),
                    reader.GetFieldValue<ulong>(3)
                ));
            }
        }

        if (snapshots.Count == 0)
            return 0;

        _logger.LogInformation("Recalculating {Count} network stats snapshots", snapshots.Count);

        foreach (var s in snapshots)
        {
            var isWindowSnapshot = s.TickStart > 0 && s.TickEnd > 0;
            string logFilter = isWindowSnapshot
                ? $"tick_number >= {s.TickStart} AND tick_number <= {s.TickEnd}"
                : $"epoch = {s.Epoch}";

            // Recalculate avg/median tx size (excluding zero-amount)
            await using var sizeCmd = _connection.CreateCommand();
            sizeCmd.CommandText = $@"
                SELECT
                    avg(amount) as avg_size,
                    median(amount) as median_size
                FROM logs FINAL
                WHERE {logFilter} AND log_type = 0 AND amount > 0";

            double avgTxSize = 0, medianTxSize = 0;
            await using (var reader = await sizeCmd.ExecuteReaderAsync(ct))
            {
                if (await reader.ReadAsync(ct))
                {
                    avgTxSize = ToSafeDouble(reader.GetValue(0));
                    medianTxSize = ToSafeDouble(reader.GetValue(1));
                }
            }

            // Recalculate exchange flows (excluding zero-amount)
            var exchangeFlows = isWindowSnapshot
                ? await GetExchangeFlowsForTickRangeAsync(s.TickStart, s.TickEnd, ct)
                : await GetExchangeFlowsForEpochAsync(s.Epoch, ct);

            // Insert corrected row with bumped snapshot_at for ReplacingMergeTree versioning
            var newSnapshotAt = s.SnapshotAt.AddMilliseconds(1);

            await using var cmdUpdate = _connection.CreateCommand();
            cmdUpdate.CommandText = $@"
                INSERT INTO network_stats_history
                SELECT
                    epoch, '{newSnapshotAt:yyyy-MM-dd HH:mm:ss.fff}' as snapshot_at,
                    tick_start, tick_end,
                    total_transactions, total_transfers, total_volume,
                    unique_senders, unique_receivers, total_active_addresses,
                    new_addresses, returning_addresses,
                    {(ulong)exchangeFlows.InflowVolume} as exchange_inflow_volume,
                    {exchangeFlows.InflowCount} as exchange_inflow_count,
                    {(ulong)exchangeFlows.OutflowVolume} as exchange_outflow_volume,
                    {exchangeFlows.OutflowCount} as exchange_outflow_count,
                    {(long)exchangeFlows.InflowVolume - (long)exchangeFlows.OutflowVolume} as exchange_net_flow,
                    sc_call_count, sc_unique_callers,
                    {avgTxSize} as avg_tx_size,
                    {medianTxSize} as median_tx_size,
                    new_users_100m_plus, new_users_1b_plus, new_users_10b_plus
                FROM network_stats_history FINAL
                WHERE epoch = {s.Epoch}
                  AND snapshot_at = '{s.SnapshotAt:yyyy-MM-dd HH:mm:ss.fff}'";

            await cmdUpdate.ExecuteNonQueryAsync(ct);
        }

        // Force merge to apply ReplacingMergeTree deduplication
        await using var cmdOptimize = _connection.CreateCommand();
        cmdOptimize.CommandText = "OPTIMIZE TABLE network_stats_history FINAL";
        await cmdOptimize.ExecuteNonQueryAsync(ct);

        _logger.LogInformation("Recalculated {Count} network stats snapshots", snapshots.Count);
        return snapshots.Count;
    }

    /// <summary>
    /// Gets total emission for computors in an epoch (from computor_emissions table)
    /// </summary>
    public async Task<decimal> GetTotalEmissionForEpochAsync(uint epoch, CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT COALESCE(sum(emission_amount), 0)
            FROM computor_emissions
            WHERE epoch = {epoch}";

        var result = await cmd.ExecuteScalarAsync(ct);
        return ToBigDecimal(result ?? 0);
    }

    /// <summary>
    /// Gets individual computor emissions as a dictionary of address -> emission amount.
    /// Used for initializing flow tracking state.
    /// </summary>
    public async Task<Dictionary<string, decimal>> GetComputorEmissionsAsync(uint epoch, CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT address, emission_amount
            FROM computor_emissions
            WHERE epoch = {epoch}";

        var result = new Dictionary<string, decimal>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var address = reader.GetString(0);
            var amount = ToBigDecimal(reader.GetValue(1));
            // Note: An address can be a computor multiple times, so we sum emissions
            if (result.ContainsKey(address))
                result[address] += amount;
            else
                result[address] = amount;
        }

        return result;
    }

    // =====================================================
    // FLOW TRACKING STATE
    // =====================================================

    /// <summary>
    /// Gets all pending (non-complete) tracking addresses for an emission epoch.
    /// These are addresses that still have funds to trace.
    /// Note: An address can appear multiple times with different origin addresses.
    /// </summary>
    public async Task<List<FlowTrackingStateDto>> GetPendingTrackingAddressesAsync(
        uint emissionEpoch,
        CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT
                emission_epoch, address, origin_address, address_type,
                received_amount, sent_amount, pending_amount,
                hop_level, last_tick, is_terminal, is_complete
            FROM flow_tracking_state FINAL
            WHERE emission_epoch = {emissionEpoch}
              AND is_complete = 0
            ORDER BY hop_level ASC, pending_amount DESC";

        var result = new List<FlowTrackingStateDto>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            result.Add(new FlowTrackingStateDto(
                EmissionEpoch: reader.GetFieldValue<uint>(0),
                Address: reader.GetString(1),
                AddressType: reader.GetString(3),
                OriginAddress: reader.GetString(2),
                ReceivedAmount: ToBigDecimal(reader.GetValue(4)),
                SentAmount: ToBigDecimal(reader.GetValue(5)),
                PendingAmount: ToBigDecimal(reader.GetValue(6)),
                HopLevel: reader.GetFieldValue<byte>(7),
                LastTick: reader.GetFieldValue<ulong>(8),
                IsTerminal: reader.GetFieldValue<byte>(9) == 1,
                IsComplete: reader.GetFieldValue<byte>(10) == 1
            ));
        }

        return result;
    }

    /// <summary>
    /// Initializes tracking state for computors when starting flow tracking for an emission epoch.
    /// For computors, the origin_address is the same as the address (they are their own origin).
    /// </summary>
    public async Task InitializeTrackingStateForComputorsAsync(
        uint emissionEpoch,
        Dictionary<string, decimal> computorEmissions,
        CancellationToken ct = default)
    {
        if (computorEmissions.Count == 0) return;

        var sb = new StringBuilder();
        sb.AppendLine(@"INSERT INTO flow_tracking_state (
            emission_epoch, address, origin_address, address_type,
            received_amount, sent_amount, pending_amount,
            hop_level, last_tick, is_terminal, is_complete
        ) VALUES");

        var values = computorEmissions.Select(kvp => $@"(
            {emissionEpoch},
            '{EscapeSql(kvp.Key)}',
            '{EscapeSql(kvp.Key)}',
            'computor',
            {kvp.Value},
            0,
            {kvp.Value},
            1,
            0,
            0,
            0
        )");

        sb.AppendLine(string.Join(",\n", values));

        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = sb.ToString();
        await cmd.ExecuteNonQueryAsync(ct);
    }

    /// <summary>
    /// Updates tracking state after processing transfers in a tick window.
    /// - Updates sent_amount and pending_amount for source addresses
    /// - Adds new intermediary addresses that received funds (per origin)
    /// - Marks addresses as complete when pending_amount &lt;= 0
    /// Note: The key is (emission_epoch, address, origin_address) so updates
    /// are specific to each origin-address combination.
    /// </summary>
    public async Task UpdateTrackingStateAsync(
        uint emissionEpoch,
        ulong lastTick,
        List<FlowTrackingUpdateDto> updates,
        CancellationToken ct = default)
    {
        if (updates.Count == 0) return;

        var sb = new StringBuilder();
        sb.AppendLine(@"INSERT INTO flow_tracking_state (
            emission_epoch, address, origin_address, address_type,
            received_amount, sent_amount, pending_amount,
            hop_level, last_tick, is_terminal, is_complete, updated_at
        ) VALUES");

        var values = updates.Select(u => $@"(
            {emissionEpoch},
            '{EscapeSql(u.Address)}',
            '{EscapeSql(u.OriginAddress)}',
            '{EscapeSql(u.AddressType)}',
            {u.ReceivedAmount},
            {u.SentAmount},
            {u.PendingAmount},
            {u.HopLevel},
            {lastTick},
            {(u.IsTerminal ? 1 : 0)},
            {(u.IsComplete ? 1 : 0)},
            now64(3)
        )");

        sb.AppendLine(string.Join(",\n", values));

        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = sb.ToString();
        await cmd.ExecuteNonQueryAsync(ct);
    }

    /// <summary>
    /// Gets tracking state for a specific address in an emission epoch.
    /// Returns all tracking states for the address (one per origin computor).
    /// </summary>
    public async Task<List<FlowTrackingStateDto>> GetTrackingStatesForAddressAsync(
        uint emissionEpoch,
        string address,
        CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT
                emission_epoch, address, origin_address, address_type,
                received_amount, sent_amount, pending_amount,
                hop_level, last_tick, is_terminal, is_complete
            FROM flow_tracking_state FINAL
            WHERE emission_epoch = {emissionEpoch}
              AND address = '{EscapeSql(address)}'";

        var result = new List<FlowTrackingStateDto>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            result.Add(new FlowTrackingStateDto(
                EmissionEpoch: reader.GetFieldValue<uint>(0),
                Address: reader.GetString(1),
                AddressType: reader.GetString(3),
                OriginAddress: reader.GetString(2),
                ReceivedAmount: ToBigDecimal(reader.GetValue(4)),
                SentAmount: ToBigDecimal(reader.GetValue(5)),
                PendingAmount: ToBigDecimal(reader.GetValue(6)),
                HopLevel: reader.GetFieldValue<byte>(7),
                LastTick: reader.GetFieldValue<ulong>(8),
                IsTerminal: reader.GetFieldValue<byte>(9) == 1,
                IsComplete: reader.GetFieldValue<byte>(10) == 1
            ));
        }

        return result;
    }

    /// <summary>
    /// Gets tracking state for a specific address and origin in an emission epoch.
    /// </summary>
    public async Task<FlowTrackingStateDto?> GetTrackingStateAsync(
        uint emissionEpoch,
        string address,
        string originAddress,
        CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT
                emission_epoch, address, origin_address, address_type,
                received_amount, sent_amount, pending_amount,
                hop_level, last_tick, is_terminal, is_complete
            FROM flow_tracking_state FINAL
            WHERE emission_epoch = {emissionEpoch}
              AND address = '{EscapeSql(address)}'
              AND origin_address = '{EscapeSql(originAddress)}'";

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        if (await reader.ReadAsync(ct))
        {
            return new FlowTrackingStateDto(
                EmissionEpoch: reader.GetFieldValue<uint>(0),
                Address: reader.GetString(1),
                AddressType: reader.GetString(3),
                OriginAddress: reader.GetString(2),
                ReceivedAmount: ToBigDecimal(reader.GetValue(4)),
                SentAmount: ToBigDecimal(reader.GetValue(5)),
                PendingAmount: ToBigDecimal(reader.GetValue(6)),
                HopLevel: reader.GetFieldValue<byte>(7),
                LastTick: reader.GetFieldValue<ulong>(8),
                IsTerminal: reader.GetFieldValue<byte>(9) == 1,
                IsComplete: reader.GetFieldValue<byte>(10) == 1
            );
        }

        return null;
    }

    /// <summary>
    /// Checks if tracking state has been initialized for an emission epoch.
    /// </summary>
    public async Task<bool> IsTrackingInitializedAsync(uint emissionEpoch, CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $"SELECT count() FROM flow_tracking_state FINAL WHERE emission_epoch = {emissionEpoch}";
        var result = await cmd.ExecuteScalarAsync(ct);
        return Convert.ToInt64(result ?? 0) > 0;
    }

    /// <summary>
    /// Gets all transfers FROM a specific address (e.g., Qutil) within a tick range.
    /// Used to build Qutil output mapping from logs.
    /// </summary>
    public async Task<List<ComputorFlowService.QutilOutput>> GetTransfersFromAddressAsync(
        string sourceAddress,
        ulong tickStart,
        ulong tickEnd,
        CancellationToken ct = default)
    {
        var result = new List<ComputorFlowService.QutilOutput>();

        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT
                dest_address,
                amount,
                tick_number,
                tx_hash
            FROM logs
            WHERE log_type = 0
              AND source_address = '{EscapeSql(sourceAddress)}'
              AND dest_address != '{AddressLabelService.BurnAddress}'
              AND tick_number BETWEEN {tickStart} AND {tickEnd}
            ORDER BY tick_number, log_id";

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            result.Add(new ComputorFlowService.QutilOutput(
                DestAddress: reader.GetString(0),
                Amount: Convert.ToDecimal(reader.GetFieldValue<ulong>(1)),
                TickNumber: reader.GetFieldValue<ulong>(2),
                TxHash: reader.GetString(3)
            ));
        }

        return result;
    }

    /// <summary>
    /// Gets all transfers TO a specific address (e.g., Qutil) within a tick range.
    /// Used to understand Qutil inputs.
    /// </summary>
    public async Task<List<ComputorFlowService.TransferRecord>> GetTransfersToAddressAsync(
        string destAddress,
        ulong tickStart,
        ulong tickEnd,
        CancellationToken ct = default)
    {
        var result = new List<ComputorFlowService.TransferRecord>();

        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT
                tick_number,
                timestamp,
                tx_hash,
                source_address,
                dest_address,
                amount
            FROM logs
            WHERE log_type = 0
              AND dest_address = '{EscapeSql(destAddress)}'
              AND tick_number BETWEEN {tickStart} AND {tickEnd}
            ORDER BY tick_number, log_id";

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            result.Add(new ComputorFlowService.TransferRecord(
                TickNumber: reader.GetFieldValue<ulong>(0),
                Timestamp: reader.GetFieldValue<DateTime>(1),
                TxHash: reader.GetString(2),
                SourceAddress: reader.GetString(3),
                DestAddress: reader.GetString(4),
                Amount: Convert.ToDecimal(reader.GetFieldValue<ulong>(5))
            ));
        }

        return result;
    }

    /// <summary>
    /// Builds a mapping of Qutil outputs grouped by tick number.
    /// This allows the TransferProcessor to look up what Qutil sent out in each tick.
    /// </summary>
    public async Task<Dictionary<string, List<ComputorFlowService.QutilOutput>>> BuildQutilOutputMappingAsync(
        string qutilAddress,
        ulong tickStart,
        ulong tickEnd,
        CancellationToken ct = default)
    {
        var outputs = await GetTransfersFromAddressAsync(qutilAddress, tickStart, tickEnd, ct);

        return outputs
            .GroupBy(o => o.TickNumber.ToString())
            .ToDictionary(g => g.Key, g => g.ToList());
    }

    /// <summary>
    /// Gets outgoing transfers with log_id for deterministic ordering.
    /// This is needed for the TransferProcessor to process transfers in the correct order.
    /// </summary>
    public async Task<List<ComputorFlowService.TransferRecordWithLogId>> GetOutgoingTransfersWithLogIdAsync(
        HashSet<string> sourceAddresses,
        ulong tickStart,
        ulong tickEnd,
        CancellationToken ct = default)
    {
        var result = new List<ComputorFlowService.TransferRecordWithLogId>();
        if (sourceAddresses.Count == 0) return result;

        var addressList = string.Join(",", sourceAddresses.Select(a => $"'{EscapeSql(a)}'"));

        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT
                tick_number,
                log_id,
                timestamp,
                tx_hash,
                source_address,
                dest_address,
                amount
            FROM logs
            WHERE log_type = 0
              AND source_address IN ({addressList})
              AND dest_address != '{AddressLabelService.BurnAddress}'
              AND tick_number BETWEEN {tickStart} AND {tickEnd}
            ORDER BY tick_number, log_id";

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            result.Add(new ComputorFlowService.TransferRecordWithLogId(
                TickNumber: reader.GetFieldValue<ulong>(0),
                LogId: reader.GetFieldValue<uint>(1),
                Timestamp: reader.GetFieldValue<DateTime>(2),
                TxHash: reader.GetString(3),
                SourceAddress: reader.GetString(4),
                DestAddress: reader.GetString(5),
                Amount: Convert.ToDecimal(reader.GetFieldValue<ulong>(6))
            ));
        }

        return result;
    }

    /// <summary>
    /// Gets all tracking states for an emission epoch (for validation).
    /// </summary>
    public async Task<List<FlowTrackingStateDto>> GetAllTrackingStatesAsync(
        uint emissionEpoch,
        CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT
                emission_epoch, address, origin_address, address_type,
                received_amount, sent_amount, pending_amount,
                hop_level, last_tick, is_terminal, is_complete
            FROM flow_tracking_state FINAL
            WHERE emission_epoch = {emissionEpoch}
            ORDER BY hop_level ASC, address";

        var result = new List<FlowTrackingStateDto>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            result.Add(new FlowTrackingStateDto(
                EmissionEpoch: reader.GetFieldValue<uint>(0),
                Address: reader.GetString(1),
                AddressType: reader.GetString(3),
                OriginAddress: reader.GetString(2),
                ReceivedAmount: ToBigDecimal(reader.GetValue(4)),
                SentAmount: ToBigDecimal(reader.GetValue(5)),
                PendingAmount: ToBigDecimal(reader.GetValue(6)),
                HopLevel: reader.GetFieldValue<byte>(7),
                LastTick: reader.GetFieldValue<ulong>(8),
                IsTerminal: reader.GetFieldValue<byte>(9) == 1,
                IsComplete: reader.GetFieldValue<byte>(10) == 1
            ));
        }

        return result;
    }

    // =====================================================
    // COMPUTOR FLOW: QUERY & PERSISTENCE METHODS
    // =====================================================

    /// <summary>
    /// Checks if computor list for an epoch has been imported
    /// </summary>
    public async Task<bool> IsComputorListImportedAsync(uint epoch, CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $"SELECT count() FROM computor_imports WHERE epoch = {epoch}";
        var count = Convert.ToInt64(await cmd.ExecuteScalarAsync(ct));
        return count > 0;
    }

    /// <summary>
    /// Gets addresses by type from label service
    /// </summary>
    public Task<HashSet<string>> GetAddressesByTypeAsync(string type, CancellationToken ct = default)
    {
        if (!Enum.TryParse<AddressType>(type, ignoreCase: true, out var addressType))
        {
            return Task.FromResult(new HashSet<string>());
        }

        var addresses = _labelService.GetAddressesByType(addressType)
            .Select(a => a.Address)
            .ToHashSet();
        return Task.FromResult(addresses);
    }

    /// <summary>
    /// Gets label for an address
    /// </summary>
    public Task<string?> GetAddressLabelAsync(string address, CancellationToken ct = default)
    {
        var label = _labelService.GetLabel(address);
        return Task.FromResult(label);
    }

    /// <summary>
    /// Calculates total inflow to addresses in a tick range.
    /// Excludes transfers FROM zero address (emission is tracked separately).
    /// </summary>
    public async Task<ComputorFlowService.FlowSummary> CalculateInflowToAddressesAsync(
        HashSet<string> addresses,
        ulong tickStart,
        ulong tickEnd,
        CancellationToken ct = default)
    {
        if (addresses.Count == 0)
            return new ComputorFlowService.FlowSummary(0, 0);

        var addressList = string.Join(",", addresses.Select(a => $"'{EscapeSql(a)}'"));

        await using var cmd = _connection.CreateCommand();
        // Inflow from non-zero addresses (emission is tracked separately)
        cmd.CommandText = $@"
            SELECT
                COALESCE(sum(amount), 0) as total_amount,
                count() as tx_count
            FROM logs
            WHERE log_type = 0 AND amount > 0
              AND source_address != '{AddressLabelService.BurnAddress}'
              AND dest_address IN ({addressList})
              AND tick_number BETWEEN {tickStart} AND {tickEnd}";

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        if (await reader.ReadAsync(ct))
        {
            return new ComputorFlowService.FlowSummary(
                ToBigDecimal(reader.GetValue(0)),
                reader.GetFieldValue<ulong>(1)
            );
        }

        return new ComputorFlowService.FlowSummary(0, 0);
    }

    /// <summary>
    /// Calculates total outflow from addresses in a tick range.
    /// Excludes transfers TO zero address (burns/deposits).
    /// </summary>
    public async Task<ComputorFlowService.FlowSummary> CalculateOutflowFromAddressesAsync(
        HashSet<string> addresses,
        ulong tickStart,
        ulong tickEnd,
        CancellationToken ct = default)
    {
        if (addresses.Count == 0)
            return new ComputorFlowService.FlowSummary(0, 0);

        var addressList = string.Join(",", addresses.Select(a => $"'{EscapeSql(a)}'"));

        await using var cmd = _connection.CreateCommand();
        // Exclude transfers TO zero address (these are burns/deposits, not real outflow)
        cmd.CommandText = $@"
            SELECT
                COALESCE(sum(amount), 0) as total_amount,
                count() as tx_count
            FROM logs
            WHERE log_type = 0 AND amount > 0
              AND source_address IN ({addressList})
              AND dest_address != '{AddressLabelService.BurnAddress}'
              AND tick_number BETWEEN {tickStart} AND {tickEnd}";

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        if (await reader.ReadAsync(ct))
        {
            return new ComputorFlowService.FlowSummary(
                ToBigDecimal(reader.GetValue(0)),
                reader.GetFieldValue<ulong>(1)
            );
        }

        return new ComputorFlowService.FlowSummary(0, 0);
    }

    /// <summary>
    /// Gets outgoing transfers from a set of addresses in a tick range.
    /// Excludes transfers TO zero address (burns/deposits).
    /// </summary>
    public async Task<List<ComputorFlowService.TransferRecord>> GetOutgoingTransfersAsync(
        HashSet<string> sourceAddresses,
        ulong tickStart,
        ulong tickEnd,
        CancellationToken ct = default)
    {
        var result = new List<ComputorFlowService.TransferRecord>();
        if (sourceAddresses.Count == 0) return result;

        var addressList = string.Join(",", sourceAddresses.Select(a => $"'{EscapeSql(a)}'"));

        await using var cmd = _connection.CreateCommand();
        // Exclude transfers TO zero address (burns/deposits are not real outflow)
        cmd.CommandText = $@"
            SELECT
                tick_number,
                timestamp,
                tx_hash,
                source_address,
                dest_address,
                amount
            FROM logs
            WHERE log_type = 0
              AND source_address IN ({addressList})
              AND dest_address != '{AddressLabelService.BurnAddress}'
              AND tick_number BETWEEN {tickStart} AND {tickEnd}
            ORDER BY tick_number, log_id";

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            result.Add(new ComputorFlowService.TransferRecord(
                TickNumber: reader.GetFieldValue<ulong>(0),
                Timestamp: reader.GetDateTime(1),
                TxHash: reader.GetString(2),
                SourceAddress: reader.GetString(3),
                DestAddress: reader.GetString(4),
                Amount: ToBigDecimal(reader.GetValue(5))
            ));
        }

        return result;
    }

    /// <summary>
    /// Saves flow hop records to the flow_hops table
    /// </summary>
    public async Task SaveFlowHopsAsync(List<ComputorFlowService.FlowHopRecord> hops, CancellationToken ct = default)
    {
        if (hops.Count == 0) return;

        var sb = new StringBuilder();
        sb.AppendLine(@"INSERT INTO flow_hops (
            epoch, emission_epoch, tick_number, timestamp, tx_hash, source_address, dest_address, amount,
            origin_address, origin_type, hop_level, dest_type, dest_label
        ) VALUES");

        var values = hops.Select(h => $@"(
            {h.Epoch},
            {h.EmissionEpoch},
            {h.TickNumber},
            '{h.Timestamp:yyyy-MM-dd HH:mm:ss}',
            '{EscapeSql(h.TxHash)}',
            '{EscapeSql(h.SourceAddress)}',
            '{EscapeSql(h.DestAddress)}',
            {h.Amount},
            '{EscapeSql(h.OriginAddress)}',
            '{EscapeSql(h.OriginType)}',
            {h.HopLevel},
            '{EscapeSql(h.DestType)}',
            '{EscapeSql(h.DestLabel)}'
        )");

        sb.AppendLine(string.Join(",\n", values));

        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = sb.ToString();
        await cmd.ExecuteNonQueryAsync(ct);

        _logger.LogDebug("Saved {Count} flow hops", hops.Count);
    }

    /// <summary>
    /// Gets total emissions for the specified emission epochs from emission_imports table.
    /// Used to calculate accurate total emission tracked across snapshots.
    /// </summary>
    public async Task<decimal> GetTotalEmissionsForEpochsAsync(IEnumerable<uint> epochs, CancellationToken ct = default)
    {
        var epochList = epochs.Distinct().ToList();
        if (epochList.Count == 0) return 0;

        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT COALESCE(sum(total_emission), 0)
            FROM emission_imports
            WHERE epoch IN ({string.Join(",", epochList)})";

        var result = await cmd.ExecuteScalarAsync(ct);
        return ToBigDecimal(result ?? 0);
    }

    // =====================================================
    // UTILITY METHODS
    // =====================================================

    /// <summary>
    /// Helper to convert ClickHouse UInt128 (BigInteger) to decimal
    /// </summary>
    public static decimal ToBigDecimal(object value)
    {
        if (value == null || value == DBNull.Value)
            return 0;

        if (value is BigInteger bigInt)
            return (decimal)bigInt;

        return Convert.ToDecimal(value);
    }

    /// <summary>
    /// Helper to safely convert to double, handling NaN/Infinity from empty aggregates
    /// </summary>
    public static double ToSafeDouble(object value)
    {
        if (value == null || value == DBNull.Value)
            return 0;

        var d = Convert.ToDouble(value);
        return double.IsNaN(d) || double.IsInfinity(d) ? 0 : d;
    }

    private static string EscapeSql(string value)
    {
        return value.Replace("'", "\\'").Replace("\\", "\\\\");
    }

    // =====================================================
    // FLOW DATA DELETION
    // =====================================================

    /// <summary>
    /// Deletes all flow data (flow_hops, flow_tracking_state, miner_flow_stats) for a specific emission epoch.
    /// Uses ClickHouse lightweight DELETE (async mutations).
    /// Returns row counts deleted from each table.
    /// </summary>
    public async Task<(long flowHops, long trackingState, long minerFlowStats)> DeleteFlowDataForEmissionEpochAsync(
        uint emissionEpoch, bool deleteMinerFlowStats, CancellationToken ct = default)
    {
        // Count rows before deletion
        long flowHopsCount = 0, trackingStateCount = 0, minerFlowStatsCount = 0;

        await using (var cmd = _connection.CreateCommand())
        {
            cmd.CommandText = $"SELECT count() FROM flow_hops WHERE emission_epoch = {emissionEpoch}";
            flowHopsCount = Convert.ToInt64(await cmd.ExecuteScalarAsync(ct) ?? 0);
        }

        await using (var cmd = _connection.CreateCommand())
        {
            cmd.CommandText = $"SELECT count() FROM flow_tracking_state WHERE emission_epoch = {emissionEpoch}";
            trackingStateCount = Convert.ToInt64(await cmd.ExecuteScalarAsync(ct) ?? 0);
        }

        if (deleteMinerFlowStats)
        {
            await using var cmd = _connection.CreateCommand();
            cmd.CommandText = $"SELECT count() FROM miner_flow_stats FINAL WHERE emission_epoch = {emissionEpoch}";
            minerFlowStatsCount = Convert.ToInt64(await cmd.ExecuteScalarAsync(ct) ?? 0);
        }

        _logger.LogInformation(
            "Deleting flow data for emission epoch {Epoch}: flow_hops={FlowHops}, tracking_state={TrackingState}, miner_flow_stats={MinerFlowStats}",
            emissionEpoch, flowHopsCount, trackingStateCount, minerFlowStatsCount);

        // Delete from flow_hops
        if (flowHopsCount > 0)
        {
            await using var cmd = _connection.CreateCommand();
            cmd.CommandText = $"ALTER TABLE flow_hops DELETE WHERE emission_epoch = {emissionEpoch}";
            await cmd.ExecuteNonQueryAsync(ct);
        }

        // Delete from flow_tracking_state
        if (trackingStateCount > 0)
        {
            await using var cmd = _connection.CreateCommand();
            cmd.CommandText = $"ALTER TABLE flow_tracking_state DELETE WHERE emission_epoch = {emissionEpoch}";
            await cmd.ExecuteNonQueryAsync(ct);
        }

        // Delete from miner_flow_stats
        if (deleteMinerFlowStats && minerFlowStatsCount > 0)
        {
            await using var cmd = _connection.CreateCommand();
            cmd.CommandText = $"ALTER TABLE miner_flow_stats DELETE WHERE emission_epoch = {emissionEpoch}";
            await cmd.ExecuteNonQueryAsync(ct);
        }

        return (flowHopsCount, trackingStateCount, minerFlowStatsCount);
    }

    // =====================================================
    // CUSTOM FLOW TRACKING
    // =====================================================

    public async Task<List<CustomFlowJobDto>> GetPendingCustomFlowJobsAsync(int limit, CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT job_id, alias, start_tick, addresses, balances, max_hops, status,
                   last_processed_tick, total_hops_recorded, total_terminal_amount, total_pending_amount,
                   error_message, created_at, updated_at
            FROM custom_flow_jobs FINAL
            WHERE status IN ('pending', 'processing')
            ORDER BY created_at ASC
            LIMIT {limit}";

        var result = new List<CustomFlowJobDto>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            result.Add(ReadCustomFlowJob(reader));
        }
        return result;
    }

    public async Task<CustomFlowJobDto?> GetCustomFlowJobAsync(string jobId, CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT job_id, alias, start_tick, addresses, balances, max_hops, status,
                   last_processed_tick, total_hops_recorded, total_terminal_amount, total_pending_amount,
                   error_message, created_at, updated_at
            FROM custom_flow_jobs FINAL
            WHERE job_id = '{EscapeSql(jobId)}'";

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        if (await reader.ReadAsync(ct))
        {
            return ReadCustomFlowJob(reader);
        }
        return null;
    }

    private static CustomFlowJobDto ReadCustomFlowJob(System.Data.Common.DbDataReader reader)
    {
        return new CustomFlowJobDto(
            JobId: reader.GetString(0),
            Alias: reader.GetString(1),
            StartTick: reader.GetFieldValue<ulong>(2),
            Addresses: ((string[])reader.GetValue(3)).ToList(),
            Balances: ((ulong[])reader.GetValue(4)).ToList(),
            MaxHops: reader.GetFieldValue<byte>(5),
            Status: reader.GetString(6),
            LastProcessedTick: reader.GetFieldValue<ulong>(7),
            TotalHopsRecorded: reader.GetFieldValue<ulong>(8),
            TotalTerminalAmount: ToBigDecimal(reader.GetValue(9)),
            TotalPendingAmount: ToBigDecimal(reader.GetValue(10)),
            ErrorMessage: reader.GetString(11) is { Length: > 0 } err ? err : null,
            CreatedAt: reader.GetFieldValue<DateTime>(12),
            UpdatedAt: reader.GetFieldValue<DateTime>(13)
        );
    }

    public async Task UpdateCustomFlowJobStatusAsync(
        string jobId, string status, ulong lastProcessedTick,
        ulong totalHops, decimal terminalAmount, decimal pendingAmount,
        string? error, CancellationToken ct = default)
    {
        // Read existing job first to preserve fields
        var job = await GetCustomFlowJobAsync(jobId, ct);
        if (job == null) return;

        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            INSERT INTO custom_flow_jobs (
                job_id, alias, start_tick, addresses, balances, max_hops, status,
                last_processed_tick, total_hops_recorded, total_terminal_amount, total_pending_amount,
                error_message, created_at, updated_at
            ) VALUES (
                '{EscapeSql(jobId)}',
                '{EscapeSql(job.Alias)}',
                {job.StartTick},
                [{string.Join(",", job.Addresses.Select(a => $"'{EscapeSql(a)}'"))}],
                [{string.Join(",", job.Balances)}],
                {job.MaxHops},
                '{EscapeSql(status)}',
                {lastProcessedTick},
                {totalHops},
                {terminalAmount},
                {pendingAmount},
                '{EscapeSql(error ?? "")}',
                '{job.CreatedAt:yyyy-MM-dd HH:mm:ss.fff}',
                now64(3)
            )";
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public async Task<List<CustomFlowTrackingStateDto>> GetCustomPendingAddressesAsync(
        string jobId, CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT job_id, address, origin_address, address_type,
                   received_amount, sent_amount, pending_amount,
                   hop_level, last_tick, is_terminal, is_complete
            FROM custom_flow_state FINAL
            WHERE job_id = '{EscapeSql(jobId)}'
              AND is_complete = 0
            ORDER BY hop_level ASC, pending_amount DESC";

        var result = new List<CustomFlowTrackingStateDto>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            result.Add(new CustomFlowTrackingStateDto(
                JobId: reader.GetString(0),
                Address: reader.GetString(1),
                OriginAddress: reader.GetString(2),
                AddressType: reader.GetString(3),
                ReceivedAmount: ToBigDecimal(reader.GetValue(4)),
                SentAmount: ToBigDecimal(reader.GetValue(5)),
                PendingAmount: ToBigDecimal(reader.GetValue(6)),
                HopLevel: reader.GetFieldValue<byte>(7),
                LastTick: reader.GetFieldValue<ulong>(8),
                IsTerminal: reader.GetFieldValue<byte>(9) == 1,
                IsComplete: reader.GetFieldValue<byte>(10) == 1
            ));
        }
        return result;
    }

    public async Task<bool> IsCustomTrackingInitializedAsync(string jobId, CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $"SELECT count() FROM custom_flow_state FINAL WHERE job_id = '{EscapeSql(jobId)}'";
        var result = await cmd.ExecuteScalarAsync(ct);
        return Convert.ToInt64(result ?? 0) > 0;
    }

    public async Task InitializeCustomTrackingStateAsync(
        string jobId, List<string> addresses, List<ulong> balances, CancellationToken ct = default)
    {
        if (addresses.Count == 0) return;

        var sb = new StringBuilder();
        sb.AppendLine(@"INSERT INTO custom_flow_state (
            job_id, address, origin_address, address_type,
            received_amount, sent_amount, pending_amount,
            hop_level, last_tick, is_terminal, is_complete
        ) VALUES");

        var values = new List<string>();
        for (var i = 0; i < addresses.Count; i++)
        {
            var balance = i < balances.Count ? balances[i] : 0UL;
            values.Add($@"(
                '{EscapeSql(jobId)}',
                '{EscapeSql(addresses[i])}',
                '{EscapeSql(addresses[i])}',
                'tracked',
                {balance},
                0,
                {balance},
                1,
                0,
                0,
                0
            )");
        }

        sb.AppendLine(string.Join(",\n", values));

        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = sb.ToString();
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public async Task UpdateCustomTrackingStateAsync(
        string jobId, ulong lastTick, List<FlowTrackingUpdateDto> updates, CancellationToken ct = default)
    {
        if (updates.Count == 0) return;

        var sb = new StringBuilder();
        sb.AppendLine(@"INSERT INTO custom_flow_state (
            job_id, address, origin_address, address_type,
            received_amount, sent_amount, pending_amount,
            hop_level, last_tick, is_terminal, is_complete, updated_at
        ) VALUES");

        var values = updates.Select(u => $@"(
            '{EscapeSql(jobId)}',
            '{EscapeSql(u.Address)}',
            '{EscapeSql(u.OriginAddress)}',
            '{EscapeSql(u.AddressType)}',
            {u.ReceivedAmount},
            {u.SentAmount},
            {u.PendingAmount},
            {u.HopLevel},
            {lastTick},
            {(u.IsTerminal ? 1 : 0)},
            {(u.IsComplete ? 1 : 0)},
            now64(3)
        )");

        sb.AppendLine(string.Join(",\n", values));

        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = sb.ToString();
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public async Task SaveCustomFlowHopsAsync(string jobId, List<CustomFlowHopRecord> hops, CancellationToken ct = default)
    {
        if (hops.Count == 0) return;

        var sb = new StringBuilder();
        sb.AppendLine(@"INSERT INTO custom_flow_hops (
            job_id, tick_number, timestamp, tx_hash, source_address, dest_address, amount,
            origin_address, hop_level, dest_type, dest_label
        ) VALUES");

        var values = hops.Select(h => $@"(
            '{EscapeSql(jobId)}',
            {h.TickNumber},
            '{h.Timestamp:yyyy-MM-dd HH:mm:ss}',
            '{EscapeSql(h.TxHash)}',
            '{EscapeSql(h.SourceAddress)}',
            '{EscapeSql(h.DestAddress)}',
            {h.Amount},
            '{EscapeSql(h.OriginAddress)}',
            {h.HopLevel},
            '{EscapeSql(h.DestType)}',
            '{EscapeSql(h.DestLabel)}'
        )");

        sb.AppendLine(string.Join(",\n", values));

        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = sb.ToString();
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public async Task<List<CustomFlowHopDto>> GetCustomFlowHopsAsync(
        string jobId, int maxDepth, CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT job_id, tick_number, timestamp, tx_hash,
                   source_address, dest_address, amount,
                   origin_address, hop_level, dest_type, dest_label
            FROM custom_flow_hops FINAL
            WHERE job_id = '{EscapeSql(jobId)}'
              AND hop_level <= {maxDepth}
            ORDER BY hop_level, tick_number";

        var result = new List<CustomFlowHopDto>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var sourceAddr = reader.GetString(4);
            var destAddr = reader.GetString(5);
            result.Add(new CustomFlowHopDto(
                JobId: reader.GetString(0),
                TickNumber: reader.GetFieldValue<ulong>(1),
                Timestamp: reader.GetFieldValue<DateTime>(2),
                TxHash: reader.GetString(3),
                SourceAddress: sourceAddr,
                SourceLabel: _labelService.GetLabel(sourceAddr),
                DestAddress: destAddr,
                DestLabel: reader.GetString(10) is { Length: > 0 } lbl ? lbl : _labelService.GetLabel(destAddr),
                DestType: reader.GetString(9) is { Length: > 0 } dt ? dt : null,
                Amount: Convert.ToDecimal(reader.GetFieldValue<ulong>(6)),
                OriginAddress: reader.GetString(7),
                HopLevel: reader.GetFieldValue<byte>(8)
            ));
        }
        return result;
    }

    public async Task<List<CustomFlowTrackingStateDto>> GetCustomFlowAllStatesAsync(
        string jobId, CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT job_id, address, origin_address, address_type,
                   received_amount, sent_amount, pending_amount,
                   hop_level, last_tick, is_terminal, is_complete
            FROM custom_flow_state FINAL
            WHERE job_id = '{EscapeSql(jobId)}'
            ORDER BY hop_level ASC, address";

        var result = new List<CustomFlowTrackingStateDto>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            result.Add(new CustomFlowTrackingStateDto(
                JobId: reader.GetString(0),
                Address: reader.GetString(1),
                OriginAddress: reader.GetString(2),
                AddressType: reader.GetString(3),
                ReceivedAmount: ToBigDecimal(reader.GetValue(4)),
                SentAmount: ToBigDecimal(reader.GetValue(5)),
                PendingAmount: ToBigDecimal(reader.GetValue(6)),
                HopLevel: reader.GetFieldValue<byte>(7),
                LastTick: reader.GetFieldValue<ulong>(8),
                IsTerminal: reader.GetFieldValue<byte>(9) == 1,
                IsComplete: reader.GetFieldValue<byte>(10) == 1
            ));
        }
        return result;
    }

    public async Task DeleteCustomFlowJobAsync(string jobId, CancellationToken ct = default)
    {
        var escaped = EscapeSql(jobId);

        await using (var cmd = _connection.CreateCommand())
        {
            cmd.CommandText = $"ALTER TABLE custom_flow_hops DELETE WHERE job_id = '{escaped}'";
            await cmd.ExecuteNonQueryAsync(ct);
        }

        await using (var cmd = _connection.CreateCommand())
        {
            cmd.CommandText = $"ALTER TABLE custom_flow_state DELETE WHERE job_id = '{escaped}'";
            await cmd.ExecuteNonQueryAsync(ct);
        }

        await using (var cmd = _connection.CreateCommand())
        {
            cmd.CommandText = $"ALTER TABLE custom_flow_jobs DELETE WHERE job_id = '{escaped}'";
            await cmd.ExecuteNonQueryAsync(ct);
        }
    }

    public async Task<int> DeleteOldCustomFlowJobsAsync(int days, CancellationToken ct = default)
    {
        // Get job IDs to delete
        await using var countCmd = _connection.CreateCommand();
        countCmd.CommandText = $@"
            SELECT count() FROM custom_flow_jobs FINAL
            WHERE updated_at < now64(3) - INTERVAL {days} DAY";
        var count = Convert.ToInt32(await countCmd.ExecuteScalarAsync(ct) ?? 0);

        if (count == 0) return 0;

        var cutoff = $"now64(3) - INTERVAL {days} DAY";

        await using (var cmd = _connection.CreateCommand())
        {
            cmd.CommandText = $@"ALTER TABLE custom_flow_hops DELETE
                WHERE job_id IN (SELECT job_id FROM custom_flow_jobs FINAL WHERE updated_at < {cutoff})";
            await cmd.ExecuteNonQueryAsync(ct);
        }

        await using (var cmd = _connection.CreateCommand())
        {
            cmd.CommandText = $@"ALTER TABLE custom_flow_state DELETE
                WHERE job_id IN (SELECT job_id FROM custom_flow_jobs FINAL WHERE updated_at < {cutoff})";
            await cmd.ExecuteNonQueryAsync(ct);
        }

        await using (var cmd = _connection.CreateCommand())
        {
            cmd.CommandText = $"ALTER TABLE custom_flow_jobs DELETE WHERE updated_at < {cutoff}";
            await cmd.ExecuteNonQueryAsync(ct);
        }

        return count;
    }

    /// <summary>
    /// Hop record for custom flow tracking (not tied to emission_epoch).
    /// </summary>
    public record CustomFlowHopRecord(
        ulong TickNumber,
        DateTime Timestamp,
        string TxHash,
        string SourceAddress,
        string DestAddress,
        decimal Amount,
        string OriginAddress,
        byte HopLevel,
        string DestType,
        string DestLabel
    );

    // =====================================================
    // QEARN STATS (PER-EPOCH, IMMUTABLE)
    // =====================================================

    private const string QearnAddress = "JAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAVKHO";
    private const uint QearnInitialEpoch = 138;

    /// <summary>
    /// Get epochs that already have persisted Qearn stats.
    /// </summary>
    public async Task<HashSet<uint>> GetPersistedQearnEpochsAsync(CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = "SELECT epoch FROM qearn_epoch_stats";
        var epochs = new HashSet<uint>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
            epochs.Add(reader.GetFieldValue<uint>(0));
        return epochs;
    }

    /// <summary>
    /// Compute and save Qearn stats for a single epoch.
    /// Uses a per-epoch partition scan (no FINAL needed since we filter by epoch partition).
    /// </summary>
    public async Task<bool> SaveQearnEpochStatsAsync(uint epoch, CancellationToken ct = default)
    {
        await using var queryCmd = _connection.CreateCommand();
        queryCmd.CommandText = $@"
            SELECT
                sumIf(amount, log_type = 8 AND source_address = '{QearnAddress}') AS total_burned,
                countIf(log_type = 8 AND source_address = '{QearnAddress}') AS burn_count,
                sumIf(amount, log_type = 0 AND dest_address = '{QearnAddress}') AS total_input,
                countIf(log_type = 0 AND dest_address = '{QearnAddress}') AS input_count,
                sumIf(amount, log_type = 0 AND source_address = '{QearnAddress}') AS total_output,
                countIf(log_type = 0 AND source_address = '{QearnAddress}') AS output_count,
                uniqIf(source_address, log_type = 0 AND dest_address = '{QearnAddress}') AS unique_lockers,
                uniqIf(dest_address, log_type = 0 AND source_address = '{QearnAddress}') AS unique_unlockers
            FROM logs FINAL
            WHERE epoch = {epoch}
              AND (source_address = '{QearnAddress}' OR dest_address = '{QearnAddress}')
              AND log_type IN (0, 8)";

        await using var reader = await queryCmd.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct))
            return false;

        var totalBurned = Convert.ToUInt64(reader.GetValue(0));
        var burnCount = Convert.ToUInt64(reader.GetValue(1));
        var totalInput = Convert.ToUInt64(reader.GetValue(2));
        var inputCount = Convert.ToUInt64(reader.GetValue(3));
        var totalOutput = Convert.ToUInt64(reader.GetValue(4));
        var outputCount = Convert.ToUInt64(reader.GetValue(5));
        var uniqueLockers = Convert.ToUInt64(reader.GetValue(6));
        var uniqueUnlockers = Convert.ToUInt64(reader.GetValue(7));

        // Skip epochs with no Qearn activity
        if (totalBurned == 0 && totalInput == 0 && totalOutput == 0)
            return false;

        await using var insertCmd = _connection.CreateCommand();
        insertCmd.CommandText = $@"
            INSERT INTO qearn_epoch_stats
            (epoch, total_burned, burn_count, total_input, input_count,
             total_output, output_count, unique_lockers, unique_unlockers)
            VALUES
            ({epoch}, {totalBurned}, {burnCount}, {totalInput}, {inputCount},
             {totalOutput}, {outputCount}, {uniqueLockers}, {uniqueUnlockers})";

        await insertCmd.ExecuteNonQueryAsync(ct);
        _logger.LogInformation(
            "Saved Qearn stats for epoch {Epoch}: burned={Burned}, in={Input}, out={Output}",
            epoch, totalBurned, totalInput, totalOutput);
        return true;
    }

    // =====================================================
    // DISPOSE
    // =====================================================

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _connection.Dispose();
    }
}
