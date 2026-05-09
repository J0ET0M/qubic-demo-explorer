using ClickHouse.Client.ADO;
using ClickHouse.Client.Copy;
using Microsoft.Extensions.Options;
using QubicExplorer.Shared.Configuration;

namespace QubicExplorer.Analytics.Services;

/// <summary>
/// Indexes oracle reply commit (input_type=6) and reveal (input_type=7) transactions
/// into oracle_query_events so we can analyze the per-query commit "race" and
/// each computor's oracle-revenue performance.
///
/// Wire layout (from oracle_core/oracle_transactions.h):
///   commit (type=6) input_data = N × OracleReplyCommitTransactionItem
///                                where item = queryId(8) + replyDigest(32) + knowledgeProof(32)
///                                = 72 bytes per item; total = inputSize / 72 items
///   reveal (type=7) input_data = queryId(8) + reply data (interface-specific, ignored here)
///
/// Backfill: on first run with no state, starts from initial_tick of (currentEpoch - 1).
/// </summary>
public class OracleEventService : IDisposable
{
    private readonly ClickHouseConnection _connection;
    private readonly ILogger<OracleEventService> _logger;
    private bool _disposed;

    private const string StateKey = "oracle_events_last_tick";
    private const int CommitInputType = 6;
    private const int RevealInputType = 7;
    private const int CommitItemSize = 72;
    private const int CommitDigestOffset = 8;        // queryId(8)
    private const int CommitKnowledgeProofOffset = 40; // queryId(8) + replyDigest(32)

    public OracleEventService(
        IOptions<ClickHouseOptions> options,
        ILogger<OracleEventService> logger)
    {
        _logger = logger;
        _connection = new ClickHouseConnection(options.Value.ConnectionString);
        _connection.Open();
    }

    /// <summary>
    /// Returns (eventsProcessed, hasMore). hasMore=true means we hit the per-pass
    /// cap and there's likely more work — caller should loop quickly during catch-up.
    /// </summary>
    public async Task<(int Events, bool HasMore)> ProcessAsync(uint currentEpoch, CancellationToken ct)
    {
        var lastProcessed = await GetStateAsync(ct);
        if (lastProcessed == null)
        {
            // First run: backfill from start of (currentEpoch - 1)
            lastProcessed = await GetEpochInitialTickAsync(currentEpoch > 0 ? currentEpoch - 1 : currentEpoch, ct);
            if (lastProcessed == null)
            {
                _logger.LogWarning("Oracle events: cannot determine backfill start tick, skipping");
                return (0, false);
            }
            _logger.LogInformation("Oracle events: backfill start tick = {Tick}", lastProcessed.Value);
            // Subtract 1 so the > comparison includes the initial tick on first scan
            lastProcessed = lastProcessed.Value > 0 ? lastProcessed.Value - 1 : 0;
        }

        const int BulkBatchSize = 50_000;
        const int MaxRowsPerPass = 500_000;
        int rowsRead = 0;

        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT tick_number, epoch, hash, from_address, input_type, input_data, timestamp
            FROM transactions
            WHERE input_type IN ({CommitInputType}, {RevealInputType})
              AND tick_number > {{lastTick:UInt64}}
              AND epoch <= {{currentEpoch:UInt32}}
            ORDER BY tick_number ASC, hash ASC
            LIMIT {MaxRowsPerPass}";
        AddParam(cmd, "lastTick", lastProcessed.Value);
        AddParam(cmd, "currentEpoch", currentEpoch);

        var pending = new List<object[]>(BulkBatchSize);
        var now = DateTime.UtcNow;
        Dictionary<string, int>? computorLookup = null;
        uint computorLookupEpoch = 0;

        ulong currentTickInScan = 0;
        ulong lastFlushedTick = lastProcessed.Value;
        int eventsAdded = 0;
        int skippedNoComputor = 0;
        int skippedParse = 0;

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            if (ct.IsCancellationRequested) break;
            rowsRead++;

            var tick = reader.GetFieldValue<ulong>(0);
            var epoch = reader.GetFieldValue<uint>(1);
            var hash = reader.GetString(2);
            var fromAddress = reader.GetString(3);
            var inputType = reader.GetFieldValue<ushort>(4);
            var inputDataHex = reader.GetString(5);
            var timestamp = reader.GetDateTime(6);

            // Tick boundary → opportunistic flush + state advance
            if (tick != currentTickInScan)
            {
                if (pending.Count >= BulkBatchSize && currentTickInScan > 0)
                {
                    await FlushAsync(pending, ct);
                    eventsAdded += pending.Count;
                    pending.Clear();
                    await UpdateStateAsync(currentTickInScan, ct);
                    lastFlushedTick = currentTickInScan;
                }
                currentTickInScan = tick;
            }

            if (computorLookup == null || epoch != computorLookupEpoch)
            {
                computorLookup = await GetComputorIndexLookupAsync(epoch, ct);
                computorLookupEpoch = epoch;
            }

            if (!computorLookup.TryGetValue(fromAddress, out var computorIndex))
            {
                skippedNoComputor++;
                continue;
            }

            var hex = inputDataHex.StartsWith("0x", StringComparison.OrdinalIgnoreCase)
                ? inputDataHex[2..] : inputDataHex;

            byte[] data;
            try { data = Convert.FromHexString(hex); }
            catch (FormatException) { skippedParse++; continue; }

            if (inputType == CommitInputType)
            {
                // N items × 72 bytes
                if (data.Length < CommitItemSize) { skippedParse++; continue; }
                var itemCount = data.Length / CommitItemSize;
                for (int i = 0; i < itemCount; i++)
                {
                    var off = i * CommitItemSize;
                    var queryId = BitConverter.ToUInt64(data, off);
                    var replyDigest = Convert.ToHexString(data, off + CommitDigestOffset, 32);
                    var knowledgeProof = Convert.ToHexString(data, off + CommitKnowledgeProofOffset, 32);
                    pending.Add(new object[]
                    {
                        epoch, queryId, (ushort)computorIndex, (byte)0,  // event_type 0 = commit
                        tick, timestamp, hash, replyDigest, knowledgeProof, now
                    });
                }
            }
            else // RevealInputType
            {
                if (data.Length < 8) { skippedParse++; continue; }
                var queryId = BitConverter.ToUInt64(data, 0);
                pending.Add(new object[]
                {
                    epoch, queryId, (ushort)computorIndex, (byte)1,  // event_type 1 = reveal
                    tick, timestamp, hash, "", "", now
                });
            }
        }

        if (pending.Count > 0)
        {
            await FlushAsync(pending, ct);
            eventsAdded += pending.Count;
            pending.Clear();
        }

        if (currentTickInScan > lastFlushedTick)
            await UpdateStateAsync(currentTickInScan, ct);

        var hasMore = rowsRead >= MaxRowsPerPass;

        if (eventsAdded > 0)
        {
            _logger.LogInformation(
                "Oracle events: persisted {Events} events from {Rows} txs (skippedNoComp={NC}, skippedParse={SP}, lastTick={Last}, hasMore={HasMore})",
                eventsAdded, rowsRead, skippedNoComputor, skippedParse, currentTickInScan, hasMore);
        }

        return (eventsAdded, hasMore);
    }

    private async Task FlushAsync(List<object[]> rows, CancellationToken ct)
    {
        using var bulk = new ClickHouseBulkCopy(_connection)
        {
            DestinationTableName = "oracle_query_events",
            ColumnNames = ["epoch", "query_id", "computor_index", "event_type",
                           "tick_number", "timestamp", "tx_hash",
                           "reply_digest", "knowledge_proof", "created_at"],
            BatchSize = 50_000
        };
        await bulk.InitAsync();
        await bulk.WriteToServerAsync(rows, ct);
    }

    private async Task<Dictionary<string, int>> GetComputorIndexLookupAsync(uint epoch, CancellationToken ct)
    {
        var result = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT address, computor_index FROM computors FINAL
            WHERE epoch = {{epoch:UInt32}}";
        AddParam(cmd, "epoch", epoch);

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            result[reader.GetString(0)] = reader.GetFieldValue<ushort>(1);
        }
        return result;
    }

    private async Task<ulong?> GetEpochInitialTickAsync(uint epoch, CancellationToken ct)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $"SELECT initial_tick FROM epoch_meta FINAL WHERE epoch = {{epoch:UInt32}}";
        AddParam(cmd, "epoch", epoch);
        var result = await cmd.ExecuteScalarAsync(ct);
        if (result == null || result == DBNull.Value) return null;
        return Convert.ToUInt64(result);
    }

    private async Task<ulong?> GetStateAsync(CancellationToken ct)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $"SELECT value FROM indexer_state FINAL WHERE key = '{StateKey}'";
        var result = await cmd.ExecuteScalarAsync(ct);
        if (result == null || result == DBNull.Value) return null;
        return ulong.TryParse(result.ToString(), out var val) ? val : null;
    }

    private async Task UpdateStateAsync(ulong tick, CancellationToken ct)
    {
        var now = DateTime.UtcNow;
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            INSERT INTO indexer_state (key, value, updated_at)
            VALUES ('{StateKey}', '{tick}', '{now:yyyy-MM-dd HH:mm:ss.fff}')";
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
