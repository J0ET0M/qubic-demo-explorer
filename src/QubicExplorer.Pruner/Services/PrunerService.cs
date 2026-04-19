using ClickHouse.Client.ADO;
using Microsoft.Extensions.Options;
using QubicExplorer.Pruner.Configuration;
using QubicExplorer.Shared.Configuration;

namespace QubicExplorer.Pruner.Services;

public class PrunerService : BackgroundService
{
    private readonly ILogger<PrunerService> _logger;
    private readonly PrunerOptions _options;
    private readonly string _connectionString;

    public PrunerService(
        ILogger<PrunerService> logger,
        IOptions<PrunerOptions> options,
        IOptions<ClickHouseOptions> chOptions)
    {
        _logger = logger;
        _options = options.Value;
        _connectionString = chOptions.Value.ConnectionString;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Pruner service started (interval: {Interval}m, dryRun: {DryRun}, rules: {Count})",
            _options.IntervalMinutes, _options.DryRun, _options.Rules.Count);

        if (_options.Rules.Count == 0)
        {
            _logger.LogWarning("No prune rules configured, pruner will idle");
            return;
        }

        // Initial delay to let other services start
        await Task.Delay(TimeSpan.FromSeconds(30), stoppingToken);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await RunPrunePassAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in prune pass");
            }

            await Task.Delay(TimeSpan.FromMinutes(_options.IntervalMinutes), stoppingToken);
        }

        _logger.LogInformation("Pruner service stopped");
    }

    private async Task RunPrunePassAsync(CancellationToken ct)
    {
        await using var connection = new ClickHouseConnection(_connectionString);
        await connection.OpenAsync(ct);

        var currentEpoch = await GetCurrentEpochAsync(connection, ct);
        if (currentEpoch == null)
        {
            _logger.LogWarning("Could not determine current epoch, skipping prune pass");
            return;
        }

        _logger.LogInformation("Starting prune pass (currentEpoch={Epoch}, dryRun={DryRun})",
            currentEpoch, _options.DryRun);

        foreach (var rule in _options.Rules)
        {
            if (string.IsNullOrEmpty(rule.Name))
            {
                _logger.LogWarning("Skipping rule with empty name");
                continue;
            }

            if (!rule.KeepDays.HasValue && !rule.KeepEpochs.HasValue)
            {
                _logger.LogWarning("Rule '{Rule}': no KeepDays or KeepEpochs set, skipping", rule.Name);
                continue;
            }

            try
            {
                await ProcessRuleAsync(connection, rule, currentEpoch.Value, ct);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing rule '{Rule}'", rule.Name);
            }
        }
    }

    private async Task ProcessRuleAsync(
        ClickHouseConnection connection, PruneRule rule, uint currentEpoch, CancellationToken ct)
    {
        // Determine the cutoff: which data to keep
        var lastPrunedEpoch = await GetStateAsync(connection, $"prune_{rule.Name}_last_epoch", ct);

        // Calculate epoch cutoff
        uint? epochCutoff = null;
        if (rule.KeepEpochs.HasValue)
            epochCutoff = currentEpoch >= (uint)rule.KeepEpochs.Value
                ? currentEpoch - (uint)rule.KeepEpochs.Value
                : 0;

        // Calculate time cutoff
        DateTime? timeCutoff = null;
        if (rule.KeepDays.HasValue)
            timeCutoff = DateTime.UtcNow.AddDays(-rule.KeepDays.Value);

        // If both are set, find the epoch that corresponds to timeCutoff
        // and use whichever retains MORE data (higher epoch = keep more)
        if (epochCutoff.HasValue && timeCutoff.HasValue)
        {
            var timeEpoch = await GetEpochAtTimestampAsync(connection, timeCutoff.Value, ct);
            if (timeEpoch.HasValue)
            {
                // Keep the higher cutoff epoch (more conservative / keeps more data)
                if (timeEpoch.Value > epochCutoff.Value)
                    epochCutoff = timeEpoch.Value;
            }
        }
        else if (timeCutoff.HasValue && !epochCutoff.HasValue)
        {
            epochCutoff = await GetEpochAtTimestampAsync(connection, timeCutoff.Value, ct);
        }

        if (!epochCutoff.HasValue || epochCutoff.Value == 0)
        {
            _logger.LogDebug("Rule '{Rule}': nothing to prune (cutoff epoch = 0)", rule.Name);
            return;
        }

        // Determine starting epoch (resume from last pruned)
        var startEpoch = lastPrunedEpoch.HasValue ? lastPrunedEpoch.Value + 1 : 1;
        if (startEpoch > epochCutoff.Value)
        {
            _logger.LogDebug("Rule '{Rule}': already pruned up to epoch {Epoch}", rule.Name, lastPrunedEpoch);
            return;
        }

        _logger.LogInformation("Rule '{Rule}': pruning epochs {Start}..{End} (keeping >= {Keep})",
            rule.Name, startEpoch, epochCutoff.Value, epochCutoff.Value + 1);

        for (var epoch = startEpoch; epoch <= epochCutoff.Value; epoch++)
        {
            if (ct.IsCancellationRequested) break;
            await PruneEpochAsync(connection, rule, (uint)epoch, ct);
            await UpdateStateAsync(connection, $"prune_{rule.Name}_last_epoch", epoch, ct);
        }
    }

    private async Task PruneEpochAsync(
        ClickHouseConnection connection, PruneRule rule, uint epoch, CancellationToken ct)
    {
        if (rule.IsLogOnly)
        {
            await PruneLogsOnlyAsync(connection, rule, epoch, ct);
            return;
        }

        // Build transaction conditions
        var conditions = new List<string> { $"epoch = {epoch}" };
        if (!string.IsNullOrEmpty(rule.DestId))
            conditions.Add($"to_address = '{rule.DestId}'");
        if (!string.IsNullOrEmpty(rule.SourceId))
            conditions.Add($"from_address = '{rule.SourceId}'");
        if (rule.InputType.HasValue)
            conditions.Add($"input_type = {rule.InputType.Value}");
        if (rule.Amount.HasValue)
            conditions.Add($"amount = {rule.Amount.Value}");
        if (rule.Executed.HasValue)
            conditions.Add($"executed = {(rule.Executed.Value ? 1 : 0)}");

        var whereClause = string.Join(" AND ", conditions);

        // Count before deleting
        var txCount = await CountAsync(connection, "transactions", whereClause, ct);
        if (txCount == 0)
        {
            _logger.LogDebug("Rule '{Rule}' epoch {Epoch}: 0 transactions match, skipping", rule.Name, epoch);
            return;
        }

        // Count associated logs
        long logCount = 0;
        if (rule.PruneLogs)
        {
            logCount = await CountLogsForTransactionsAsync(connection, whereClause, epoch, ct);
        }

        if (_options.DryRun)
        {
            _logger.LogInformation("[DRY RUN] Rule '{Rule}' epoch {Epoch}: would delete {TxCount} transactions, {LogCount} logs",
                rule.Name, epoch, txCount, logCount);
            return;
        }

        _logger.LogInformation("Rule '{Rule}' epoch {Epoch}: deleting {TxCount} transactions, {LogCount} logs",
            rule.Name, epoch, txCount, logCount);

        // Delete associated logs first (by matching tx_hash)
        if (rule.PruneLogs && logCount > 0)
        {
            await ExecuteDeleteAsync(connection,
                $"ALTER TABLE logs DELETE WHERE epoch = {epoch} AND tx_hash IN " +
                $"(SELECT hash FROM transactions WHERE {whereClause})", ct);
        }

        // Delete transactions
        await ExecuteDeleteAsync(connection,
            $"ALTER TABLE transactions DELETE WHERE {whereClause}", ct);

        _logger.LogInformation("Rule '{Rule}' epoch {Epoch}: deletion mutations submitted", rule.Name, epoch);
    }

    private async Task PruneLogsOnlyAsync(
        ClickHouseConnection connection, PruneRule rule, uint epoch, CancellationToken ct)
    {
        var conditions = new List<string> { $"epoch = {epoch}" };
        if (rule.LogType.HasValue)
            conditions.Add($"log_type = {rule.LogType.Value}");

        var whereClause = string.Join(" AND ", conditions);
        var logCount = await CountAsync(connection, "logs", whereClause, ct);

        if (logCount == 0)
        {
            _logger.LogDebug("Rule '{Rule}' epoch {Epoch}: 0 logs match, skipping", rule.Name, epoch);
            return;
        }

        if (_options.DryRun)
        {
            _logger.LogInformation("[DRY RUN] Rule '{Rule}' epoch {Epoch}: would delete {Count} logs",
                rule.Name, epoch, logCount);
            return;
        }

        _logger.LogInformation("Rule '{Rule}' epoch {Epoch}: deleting {Count} logs", rule.Name, epoch, logCount);
        await ExecuteDeleteAsync(connection, $"ALTER TABLE logs DELETE WHERE {whereClause}", ct);
    }

    // ── Helpers ──────────────────────────────────────────────────────────

    private static async Task<long> CountAsync(
        ClickHouseConnection connection, string table, string whereClause, CancellationToken ct)
    {
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = $"SELECT count() FROM {table} WHERE {whereClause}";
        var result = await cmd.ExecuteScalarAsync(ct);
        return Convert.ToInt64(result);
    }

    private static async Task<long> CountLogsForTransactionsAsync(
        ClickHouseConnection connection, string txWhereClause, uint epoch, CancellationToken ct)
    {
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT count() FROM logs
            WHERE epoch = {epoch}
              AND tx_hash IN (SELECT hash FROM transactions WHERE {txWhereClause})";
        var result = await cmd.ExecuteScalarAsync(ct);
        return Convert.ToInt64(result);
    }

    private async Task ExecuteDeleteAsync(
        ClickHouseConnection connection, string deleteStatement, CancellationToken ct)
    {
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = deleteStatement;
        await cmd.ExecuteNonQueryAsync(ct);

        // Wait for the mutation to complete before proceeding
        await WaitForMutationsAsync(connection, ct);
    }

    private async Task WaitForMutationsAsync(
        ClickHouseConnection connection, CancellationToken ct, int timeoutSeconds = 600)
    {
        var deadline = DateTime.UtcNow.AddSeconds(timeoutSeconds);

        while (DateTime.UtcNow < deadline && !ct.IsCancellationRequested)
        {
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
                SELECT count() FROM system.mutations
                WHERE database = 'qubic' AND is_done = 0";
            var pending = Convert.ToInt64(await cmd.ExecuteScalarAsync(ct));

            if (pending == 0) return;

            _logger.LogDebug("Waiting for {Count} pending mutation(s)...", pending);
            await Task.Delay(5000, ct);
        }

        _logger.LogWarning("Mutation wait timed out after {Timeout}s", timeoutSeconds);
    }

    private static async Task<uint?> GetCurrentEpochAsync(
        ClickHouseConnection connection, CancellationToken ct)
    {
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT max(epoch) FROM ticks";
        var result = await cmd.ExecuteScalarAsync(ct);
        if (result == null || result == DBNull.Value) return null;
        var val = Convert.ToUInt32(result);
        return val > 0 ? val : null;
    }

    private static async Task<uint?> GetEpochAtTimestampAsync(
        ClickHouseConnection connection, DateTime timestamp, CancellationToken ct)
    {
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT max(epoch) FROM ticks
            WHERE timestamp <= '{timestamp:yyyy-MM-dd HH:mm:ss}'
              AND timestamp > '2020-01-01'";
        var result = await cmd.ExecuteScalarAsync(ct);
        if (result == null || result == DBNull.Value) return null;
        var val = Convert.ToUInt32(result);
        return val > 0 ? val : null;
    }

    private static async Task<long?> GetStateAsync(
        ClickHouseConnection connection, string key, CancellationToken ct)
    {
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = $"SELECT value FROM indexer_state FINAL WHERE key = '{key}'";
        var result = await cmd.ExecuteScalarAsync(ct);
        if (result == null || result == DBNull.Value) return null;
        return long.TryParse(result.ToString(), out var val) ? val : null;
    }

    private static async Task UpdateStateAsync(
        ClickHouseConnection connection, string key, long value, CancellationToken ct)
    {
        var now = DateTime.UtcNow;
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = $@"
            INSERT INTO indexer_state (key, value, updated_at)
            VALUES ('{key}', '{value}', '{now:yyyy-MM-dd HH:mm:ss.fff}')";
        await cmd.ExecuteNonQueryAsync(ct);
    }
}
