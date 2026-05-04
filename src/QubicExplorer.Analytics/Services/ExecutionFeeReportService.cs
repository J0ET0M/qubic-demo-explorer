using ClickHouse.Client.ADO;
using ClickHouse.Client.Copy;
using Microsoft.Extensions.Options;
using QubicExplorer.Shared.Configuration;
using QubicExplorer.Shared.Services;

namespace QubicExplorer.Analytics.Services;

/// <summary>
/// Parses execution fee report transactions (input_type = 9) and persists one
/// row per (computor, contract) into execution_fee_reports.
///
/// The on-the-wire layout of the payload (after the standard transaction header):
///   uint32 phaseNumber
///   uint32 numEntries
///   uint32 contractIndices[numEntries]
///   [optional 4-byte alignment padding when numEntries is odd]
///   uint64 executionFees[numEntries]
///   m256i  dataLock (32 bytes)
///
/// We don't need the dataLock — we just read phaseNumber + the two arrays.
/// </summary>
public class ExecutionFeeReportService : IDisposable
{
    private readonly ClickHouseConnection _connection;
    private readonly ILogger<ExecutionFeeReportService> _logger;
    private bool _disposed;

    private const string StateKey = "fee_reports_last_phase_tick";
    private const int FeeReportInputType = 9;

    public ExecutionFeeReportService(
        IOptions<ClickHouseOptions> options,
        ILogger<ExecutionFeeReportService> logger)
    {
        _logger = logger;
        _connection = new ClickHouseConnection(options.Value.ConnectionString);
        _connection.Open();
    }

    /// <summary>
    /// Process all unprocessed input_type=9 transactions in a single batched
    /// scan. Streams rows from ClickHouse in tick-order, caches the computor
    /// lookup per epoch, and bulk-inserts in 50k-row chunks. Updates progress
    /// state as we cross tick boundaries so a crash-mid-pass loses at most
    /// one tick of work.
    /// </summary>
    /// <summary>
    /// Returns (ticksProcessed, hasMore). hasMore=true means we hit the per-pass
    /// cap and there's likely more work — caller should loop quickly during catch-up.
    /// </summary>
    public async Task<(int Ticks, bool HasMore)> ProcessAsync(uint currentEpoch, CancellationToken ct)
    {
        var lastProcessed = await GetStateAsync(ct) ?? 0UL;
        const int BulkBatchSize = 50_000;
        const int MaxRowsPerPass = 2_000_000; // hard ceiling so one pass can't run forever
        int rowsRead = 0;

        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT tick_number, epoch, hash, from_address, input_data
            FROM transactions
            WHERE input_type = {FeeReportInputType}
              AND tick_number > {{lastTick:UInt64}}
              AND epoch <= {{currentEpoch:UInt32}}
            ORDER BY tick_number ASC, hash ASC
            LIMIT {MaxRowsPerPass}";
        AddParam(cmd, "lastTick", lastProcessed);
        AddParam(cmd, "currentEpoch", currentEpoch);

        var pending = new List<object[]>(BulkBatchSize);
        var now = DateTime.UtcNow;
        Dictionary<string, int>? computorLookup = null;
        uint computorLookupEpoch = 0;

        ulong currentTickInScan = 0;
        ulong ticksSeen = 0;
        ulong rowsPersisted = 0;
        int skippedNoComputor = 0;
        int skippedParse = 0;
        ulong lastFlushedTick = lastProcessed;

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            if (ct.IsCancellationRequested) break;
            rowsRead++;

            var tick = reader.GetFieldValue<ulong>(0);
            var epoch = reader.GetFieldValue<uint>(1);
            var hash = reader.GetString(2);
            var fromAddress = reader.GetString(3);
            var inputDataHex = reader.GetString(4);

            // When we cross a tick boundary, the previous tick is fully consumed.
            // If the buffer is already large, flush + advance state for resumability.
            if (tick != currentTickInScan)
            {
                if (pending.Count >= BulkBatchSize && currentTickInScan > 0)
                {
                    await FlushAsync(pending, ct);
                    rowsPersisted += (ulong)pending.Count;
                    pending.Clear();
                    await UpdateStateAsync(currentTickInScan, ct);
                    lastFlushedTick = currentTickInScan;
                }
                currentTickInScan = tick;
                ticksSeen++;
            }

            // Refresh computor lookup on epoch change
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

            if (!TryParseFeeReport(inputDataHex, out var phaseNumber, out var entries))
            {
                skippedParse++;
                continue;
            }

            foreach (var (contractIndex, fee) in entries)
            {
                pending.Add(new object[]
                {
                    epoch, phaseNumber, tick,
                    (ushort)computorIndex, contractIndex, fee, hash, now
                });
            }
        }

        // Final flush
        if (pending.Count > 0)
        {
            await FlushAsync(pending, ct);
            rowsPersisted += (ulong)pending.Count;
            pending.Clear();
        }

        if (currentTickInScan > lastFlushedTick)
        {
            await UpdateStateAsync(currentTickInScan, ct);
        }

        // Did we hit the row cap? If so there's likely more pending work.
        var hasMore = rowsRead >= MaxRowsPerPass;

        if (ticksSeen > 0 || rowsPersisted > 0)
        {
            _logger.LogInformation(
                "Fee reports: scanned {Ticks} ticks, persisted {Rows} rows (skippedNoComp={NC}, skippedParse={SP}, lastTick={Last}, hasMore={HasMore})",
                ticksSeen, rowsPersisted, skippedNoComputor, skippedParse, currentTickInScan, hasMore);
        }

        return ((int)ticksSeen, hasMore);
    }

    private async Task FlushAsync(List<object[]> rows, CancellationToken ct)
    {
        using var bulk = new ClickHouseBulkCopy(_connection)
        {
            DestinationTableName = "execution_fee_reports",
            ColumnNames = ["epoch", "phase_number", "phase_tick", "computor_index",
                           "contract_index", "reported_fee", "tx_hash", "created_at"],
            BatchSize = 50_000
        };
        await bulk.InitAsync();
        await bulk.WriteToServerAsync(rows, ct);
    }

    private static bool TryParseFeeReport(string inputDataHex, out uint phaseNumber, out List<(ushort ContractIndex, ulong Fee)> entries)
    {
        phaseNumber = 0;
        entries = new();

        if (string.IsNullOrEmpty(inputDataHex)) return false;

        var hex = inputDataHex.StartsWith("0x", StringComparison.OrdinalIgnoreCase)
            ? inputDataHex[2..] : inputDataHex;

        byte[] data;
        try { data = Convert.FromHexString(hex); }
        catch (FormatException) { return false; }

        // Need at least phaseNumber (4) + numEntries (4) + dataLock (32) = 40 bytes
        if (data.Length < 40) return false;

        phaseNumber = BitConverter.ToUInt32(data, 0);
        var numEntries = BitConverter.ToUInt32(data, 4);

        if (numEntries == 0 || numEntries > 65536) return false;

        // Layout after numEntries:
        //   contractIndices[numEntries]  (4 * N bytes)
        //   [4-byte pad if numEntries is odd]
        //   executionFees[numEntries]    (8 * N bytes)
        //   dataLock (32 bytes)
        var indicesStart = 8;
        var indicesEnd = indicesStart + 4 * (int)numEntries;
        var alignment = (numEntries % 2 == 1) ? 4 : 0;
        var feesStart = indicesEnd + alignment;
        var feesEnd = feesStart + 8 * (int)numEntries;
        var expectedTotal = feesEnd + 32; // + dataLock

        if (data.Length < expectedTotal) return false;

        entries.Capacity = (int)numEntries;
        for (int i = 0; i < numEntries; i++)
        {
            var ci = BitConverter.ToUInt32(data, indicesStart + 4 * i);
            var fee = BitConverter.ToUInt64(data, feesStart + 8 * i);
            // Defensive cast: contract indices fit in 16 bits in practice
            if (ci > ushort.MaxValue) return false;
            entries.Add(((ushort)ci, fee));
        }

        return true;
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
            var address = reader.GetString(0);
            var index = (int)reader.GetFieldValue<ushort>(1);
            result[address] = index;
        }
        return result;
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
