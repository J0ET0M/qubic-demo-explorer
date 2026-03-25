using ClickHouse.Client.ADO;
using Microsoft.Extensions.Options;
using Qubic.Bob;
using Qubic.Bob.Models;
using QubicExplorer.Shared.Configuration;
using QubicExplorer.Indexer.Configuration;
using QubicExplorer.Indexer.Models;

namespace QubicExplorer.Indexer.Services;

/// <summary>
/// Background service that sequentially cross-checks every indexed tick.
/// Runs behind the indexer (always at indexer_tick - 1 or below) and stores its own progress.
///
/// For each tick, queries ClickHouse for the current state and applies these rules:
///   1. empty + has logs + no tx  → refetch
///   2. empty + no logs + no tx   → OK
///   3. empty + no logs + tx with executed state → refetch
///   4. non-empty + no logs       → refetch
///   5. non-empty + has logs + no tx → refetch
///   6. non-empty + has logs + has tx → OK
///
/// Refetch uses Bob's direct RPC methods:
///   - qubic_getTickByNumber → tick metadata
///   - qubic_getTickLogRanges → log ID ranges
///   - qubic_getLogs (GetLogsByIdRangeAsync) → log entries
///   - qubic_getTransactionByHash → transaction data
///   - qubic_getTransactionReceipt → executed status
/// </summary>
public class TickCrossCheckService : BackgroundService
{
    private readonly ILogger<TickCrossCheckService> _logger;
    private readonly BobOptions _bobOptions;
    private readonly ClickHouseWriterService _clickHouseWriter;
    private readonly ClickHouseOptions _clickHouseOptions;
    private readonly IndexerOptions _indexerOptions;

    private const string StateKey = "crosscheck_last_tick";

    public TickCrossCheckService(
        ILogger<TickCrossCheckService> logger,
        IOptions<BobOptions> bobOptions,
        IOptions<IndexerOptions> indexerOptions,
        IOptions<ClickHouseOptions> clickHouseOptions,
        ClickHouseWriterService clickHouseWriter)
    {
        _logger = logger;
        _bobOptions = bobOptions.Value;
        _indexerOptions = indexerOptions.Value;
        _clickHouseWriter = clickHouseWriter;
        _clickHouseOptions = clickHouseOptions.Value;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Wait for the main indexer to initialize
        await Task.Delay(TimeSpan.FromSeconds(30), stoppingToken);

        _logger.LogInformation("Tick cross-check service started");

        await using var connection = new ClickHouseConnection(_clickHouseOptions.ConnectionString);
        await connection.OpenAsync(stoppingToken);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await RunCrossCheckLoopAsync(connection, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Cross-check error, will retry in 30s");
                await Task.Delay(TimeSpan.FromSeconds(30), stoppingToken);
            }
        }
    }

    private async Task RunCrossCheckLoopAsync(ClickHouseConnection connection, CancellationToken ct)
    {
        var crossCheckTick = await GetStateAsync(connection, ct);
        var indexerTick = await GetIndexerTickAsync(connection, ct);

        if (indexerTick == null)
        {
            _logger.LogDebug("Indexer has not started yet, waiting...");
            await Task.Delay(TimeSpan.FromSeconds(10), ct);
            return;
        }

        var maxTick = indexerTick.Value - 1;

        if (crossCheckTick >= maxTick)
        {
            await Task.Delay(TimeSpan.FromSeconds(5), ct);
            return;
        }

        if (crossCheckTick == 0)
        {
            var firstTick = await GetFirstTickAsync(connection, ct);
            if (firstTick == null)
            {
                await Task.Delay(TimeSpan.FromSeconds(10), ct);
                return;
            }
            crossCheckTick = firstTick.Value - 1;
        }

        var startTick = crossCheckTick + 1;
        var checkedCount = 0;
        var refetchedCount = 0;

        BobWebSocketClient? bobClient = null;

        try
        {
            for (var tick = startTick; tick <= maxTick; tick++)
            {
                var state = await GetTickStateAsync(connection, tick, ct);
                var needsRefetch = EvaluateRules(tick, state);

                if (needsRefetch)
                {
                    bobClient ??= await ConnectBobAsync(ct);

                    var success = await RefetchTickAsync(bobClient, tick, ct);
                    if (success)
                        refetchedCount++;
                    else
                        _logger.LogWarning("Failed to refetch tick {Tick}, skipping", tick);
                }

                checkedCount++;

                if (checkedCount % 1000 == 0)
                {
                    if (refetchedCount > 0)
                        await _clickHouseWriter.FlushBatchesAsync(ct);

                    await SaveStateAsync(connection, tick, ct);
                    _logger.LogInformation(
                        "Cross-check progress: tick {Tick}, checked {Checked}, refetched {Refetched}",
                        tick, checkedCount, refetchedCount);
                }

                if (checkedCount % 5000 == 0)
                {
                    indexerTick = await GetIndexerTickAsync(connection, ct);
                    maxTick = (indexerTick ?? maxTick) - 1;
                }
            }
        }
        finally
        {
            bobClient?.Dispose();
        }

        if (checkedCount > 0)
        {
            if (refetchedCount > 0)
                await _clickHouseWriter.FlushBatchesAsync(ct);

            await SaveStateAsync(connection, maxTick, ct);

            if (refetchedCount > 0)
                _logger.LogInformation(
                    "Cross-check batch complete: checked {Checked} ticks, refetched {Refetched}",
                    checkedCount, refetchedCount);
        }
    }

    // ── Tick state query ─────────────────────────────────────────────────

    private async Task<TickState> GetTickStateAsync(
        ClickHouseConnection connection, ulong tickNumber, CancellationToken ct)
    {
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT
                t.is_empty,
                (SELECT count() FROM transactions WHERE tick_number = {tickNumber}) as tx_count,
                (SELECT count() FROM logs WHERE tick_number = {tickNumber}) as log_count,
                (SELECT countIf(executed = 1) FROM transactions WHERE tick_number = {tickNumber}) as executed_count
            FROM ticks t FINAL
            WHERE t.tick_number = {tickNumber}";

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct))
            return new TickState(false, true, 0, 0, 0);

        return new TickState(
            Exists: true,
            IsEmpty: reader.GetFieldValue<byte>(0) == 1,
            TxCount: Convert.ToInt64(reader.GetValue(1)),
            LogCount: Convert.ToInt64(reader.GetValue(2)),
            ExecutedTxCount: Convert.ToInt64(reader.GetValue(3))
        );
    }

    // ── Rules ────────────────────────────────────────────────────────────

    internal bool EvaluateRules(ulong tick, TickState state)
    {
        if (!state.Exists)
        {
            _logger.LogDebug("Tick {Tick}: missing from database, needs refetch", tick);
            return true;
        }

        var hasLogs = state.LogCount > 0;
        var hasTx = state.TxCount > 0;

        if (state.IsEmpty)
        {
            // Rule 2: empty + no logs + no tx → OK
            if (!hasLogs && !hasTx) return false;

            // Rule 1: empty + has logs + no tx → refetch
            if (hasLogs && !hasTx)
            {
                _logger.LogInformation("Tick {Tick}: marked empty but has {Logs} logs", tick, state.LogCount);
                return true;
            }

            // Rule 3: empty + no logs + tx with executed → refetch
            if (!hasLogs && hasTx)
            {
                _logger.LogInformation("Tick {Tick}: marked empty but has {Txs} txs ({Exec} executed)",
                    tick, state.TxCount, state.ExecutedTxCount);
                return true;
            }

            // empty + has logs + has tx → also wrong
            _logger.LogInformation("Tick {Tick}: marked empty but has {Txs} txs and {Logs} logs",
                tick, state.TxCount, state.LogCount);
            return true;
        }

        // Rule 6: non-empty + has logs + has tx → OK
        if (hasLogs && hasTx) return false;

        // Rule 4: non-empty + no logs → refetch
        if (!hasLogs)
        {
            _logger.LogInformation("Tick {Tick}: not empty but has no logs", tick);
            return true;
        }

        // Rule 5: non-empty + has logs + no tx → refetch
        _logger.LogInformation("Tick {Tick}: not empty, has {Logs} logs but no txs", tick, state.LogCount);
        return true;
    }

    // ── Refetch via direct RPC ───────────────────────────────────────────

    /// <summary>
    /// Refetch a single tick using Bob's direct RPC methods:
    ///   1. GetTickByNumber → epoch, timestamp
    ///   2. GetTickLogRanges → log ID range
    ///   3. GetLogsByIdRange → log entries (with txHash, body)
    ///   4. Extract unique txHashes from logs
    ///   5. GetTransactionByHash → from, to, amount, inputType, inputData
    ///   6. GetTransactionReceipt → executed status
    ///   7. Re-insert tick + transactions + logs via ClickHouseWriterService
    /// </summary>
    private async Task<bool> RefetchTickAsync(BobWebSocketClient bob, ulong tickNumber, CancellationToken ct)
    {
        try
        {
            // 1. Get tick metadata
            var tickResp = await bob.GetTickByNumberAsync((uint)tickNumber, ct);
            if (tickResp == null)
            {
                _logger.LogWarning("Tick {Tick}: Bob returned no data for GetTickByNumber", tickNumber);
                return false;
            }
            var epoch = (uint)tickResp.Epoch;
            var timestamp = tickResp.Timestamp > 0
                ? DateTimeOffset.FromUnixTimeSeconds(tickResp.Timestamp).UtcDateTime
                : DateTime.UtcNow;

            // 2. Get log ranges for this tick
            var logRanges = await bob.GetTickLogRangesAsync(new[] { (uint)tickNumber }, ct);
            var logRange = logRanges.FirstOrDefault(r => r.Tick == (uint)tickNumber);

            // 3. Fetch logs
            var bobLogs = new List<BobLog>();
            if (logRange?.FromLogId != null && logRange.Length is > 0)
            {
                var endLogId = logRange.FromLogId.Value + logRange.Length.Value - 1;
                var logEntries = await bob.GetLogsByIdRangeAsync(epoch, logRange.FromLogId.Value, endLogId, ct);

                foreach (var entry in logEntries)
                {
                    if (!entry.Ok) continue;

                    bobLogs.Add(new BobLog
                    {
                        Ok = entry.Ok,
                        Tick = entry.Tick,
                        Epoch = entry.Epoch,
                        LogId = (uint)entry.LogId,
                        LogType = entry.LogType,
                        LogTypeName = entry.LogTypeName,
                        LogDigest = entry.LogDigest,
                        BodySize = entry.BodySize,
                        Timestamp = entry.GetTimestamp(),
                        TxHash = entry.TxHash,
                        Body = entry.Body
                    });
                }
            }

            // 4. Extract unique txHashes from logs
            var txHashes = bobLogs
                .Where(l => !string.IsNullOrEmpty(l.TxHash))
                .Select(l => l.TxHash!)
                .Distinct()
                .ToList();

            // 5+6. Fetch transaction data + receipts
            var bobTransactions = new List<BobTransaction>();
            foreach (var txHash in txHashes)
            {
                var txResp = await bob.GetTransactionByHashAsync(txHash, ct);
                if (txResp == null) continue;

                var receipt = await bob.GetTransactionReceiptAsync(txHash, ct);

                // Compute logIdFrom and logIdLength from the logs
                var txLogs = bobLogs.Where(l => l.TxHash == txHash).OrderBy(l => l.LogId).ToList();
                var logIdFrom = txLogs.Count > 0 ? (int)txLogs.First().LogId : -1;
                var logIdLength = (ushort)txLogs.Count;

                bobTransactions.Add(new BobTransaction
                {
                    Hash = txResp.TransactionHash,
                    From = txResp.SourceAddress,
                    To = txResp.DestAddress,
                    Amount = (ulong)txResp.AmountValue,
                    InputType = (ushort)txResp.InputType,
                    InputData = txResp.InputData,
                    Executed = receipt?.Status ?? false,
                    LogIdFrom = logIdFrom,
                    LogIdLength = logIdLength
                });
            }

            // 7. Build TickStreamData and write
            var tickData = new TickStreamData
            {
                Epoch = epoch,
                Tick = tickNumber,
                HasNoTickData = false,
                IsSkipped = false,
                IsCatchUp = false,
                Timestamp = timestamp.ToString("O"),
                TxCountTotal = (uint)bobTransactions.Count,
                TxCountFiltered = (uint)bobTransactions.Count,
                LogCountTotal = (uint)bobLogs.Count,
                LogCountFiltered = (uint)bobLogs.Count,
                Transactions = bobTransactions,
                Logs = bobLogs
            };

            await _clickHouseWriter.WriteTickDataAsync(tickData, ct);

            _logger.LogInformation(
                "Refetched tick {Tick}: {TxCount} txs, {LogCount} logs",
                tickNumber, bobTransactions.Count, bobLogs.Count);

            return true;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Error refetching tick {Tick} from Bob", tickNumber);
            return false;
        }
    }

    // ── Bob connection ───────────────────────────────────────────────────

    private async Task<BobWebSocketClient> ConnectBobAsync(CancellationToken ct)
    {
        var options = new BobWebSocketOptions
        {
            Nodes = _bobOptions.GetEffectiveNodes().ToArray(),
            ReconnectDelay = TimeSpan.FromSeconds(2),
            MaxReconnectDelay = TimeSpan.FromSeconds(30)
        };

        var client = new BobWebSocketClient(options);
        await client.ConnectAsync(ct);
        _logger.LogInformation("Cross-check connected to Bob: {Node}", client.ActiveNodeUrl);
        return client;
    }

    // ── State persistence ────────────────────────────────────────────────

    private async Task<ulong> GetStateAsync(ClickHouseConnection connection, CancellationToken ct)
    {
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = $"SELECT value FROM indexer_state FINAL WHERE key = '{StateKey}'";
        var result = await cmd.ExecuteScalarAsync(ct);
        if (result != null && result != DBNull.Value && ulong.TryParse(result.ToString(), out var tick))
            return tick;
        return 0;
    }

    private async Task SaveStateAsync(ClickHouseConnection connection, ulong tick, CancellationToken ct)
    {
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = $@"
            INSERT INTO indexer_state (key, value, updated_at)
            VALUES ('{StateKey}', '{tick}', now64(3))";
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static async Task<ulong?> GetIndexerTickAsync(ClickHouseConnection connection, CancellationToken ct)
    {
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT value FROM indexer_state FINAL WHERE key = 'last_tick'";
        var result = await cmd.ExecuteScalarAsync(ct);
        if (result != null && result != DBNull.Value && ulong.TryParse(result.ToString(), out var tick))
            return tick;
        return null;
    }

    private static async Task<ulong?> GetFirstTickAsync(ClickHouseConnection connection, CancellationToken ct)
    {
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT min(tick_number) FROM ticks WHERE tick_number > 0";
        var result = await cmd.ExecuteScalarAsync(ct);
        if (result != null && result != DBNull.Value)
        {
            var val = Convert.ToUInt64(result);
            return val > 0 ? val : null;
        }
        return null;
    }

    internal record TickState(bool Exists, bool IsEmpty, long TxCount, long LogCount, long ExecutedTxCount);
}
