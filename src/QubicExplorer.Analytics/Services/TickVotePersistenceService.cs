using ClickHouse.Client.ADO;
using ClickHouse.Client.Copy;
using Microsoft.Extensions.Options;
using Qubic.Core;
using QubicExplorer.Shared.Configuration;
using QubicExplorer.Shared.Services;

namespace QubicExplorer.Analytics.Services;

/// <summary>
/// Persists accumulated tick vote snapshots every 676 ticks (one full computor cycle).
/// Creates a time-series of vote progression throughout an epoch, useful for tracking
/// how vote scores build up over time.
///
/// Each snapshot covers a 676-tick window and stores the accumulated vote count
/// per computor for that window.
/// </summary>
public class TickVotePersistenceService : IDisposable
{
    private readonly ClickHouseConnection _connection;
    private readonly BobProxyService _bobProxy;
    private readonly ILogger<TickVotePersistenceService> _logger;
    private bool _disposed;

    private const int WindowSize = QubicConstants.NumberOfComputors; // 676

    public TickVotePersistenceService(
        IOptions<ClickHouseOptions> options,
        BobProxyService bobProxy,
        ILogger<TickVotePersistenceService> logger)
    {
        _bobProxy = bobProxy;
        _logger = logger;
        _connection = new ClickHouseConnection(options.Value.ConnectionString);
        _connection.Open();
    }

    /// <summary>
    /// Process the next 676-tick window of vote data and persist it.
    /// Returns true if a window was processed (caller should loop), false if no more work.
    /// </summary>
    public async Task<bool> ProcessNextWindowAsync(uint currentEpoch, CancellationToken ct)
    {
        // 1. Determine where we left off
        var (lastEpoch, lastTick) = await GetLastPersistedStateAsync(ct);

        // 2. Get epoch metadata for initial tick
        var epochMeta = await GetEpochMetaAsync(currentEpoch, ct);
        if (epochMeta == null)
        {
            _logger.LogDebug("No epoch_meta for epoch {Epoch}, skipping tick vote persistence", currentEpoch);
            return false;
        }

        uint epoch;
        ulong startTick;

        if (lastEpoch == 0 || lastEpoch < currentEpoch - 1)
        {
            // First run or far behind — start from current epoch's initial tick
            epoch = currentEpoch;
            startTick = epochMeta.InitialTick;
        }
        else if (lastEpoch < currentEpoch)
        {
            // Previous epoch — check if we've finished it
            var prevMeta = await GetEpochMetaAsync(lastEpoch, ct);
            if (prevMeta != null && lastTick + 1 < epochMeta.InitialTick)
            {
                // Still have ticks to process in the previous epoch
                epoch = lastEpoch;
                startTick = lastTick + 1;
                // Cap at end of previous epoch
                var endOfPrevEpoch = epochMeta.InitialTick - 1;
                if (startTick + (ulong)WindowSize - 1 > endOfPrevEpoch)
                {
                    // Remaining window is smaller than 676 — process what's left
                    return await ProcessWindowAsync(epoch, startTick, endOfPrevEpoch, ct);
                }
            }
            else
            {
                // Move to current epoch
                epoch = currentEpoch;
                startTick = epochMeta.InitialTick;
            }
        }
        else
        {
            // Same epoch — continue from where we left off
            epoch = currentEpoch;
            startTick = lastTick + 1;
        }

        var endTick = startTick + (ulong)WindowSize - 1;

        // 3. Check that the data exists (don't process beyond indexed ticks)
        var maxIndexedTick = await GetMaxIndexedTickAsync(epoch, ct);
        if (maxIndexedTick == null || endTick > maxIndexedTick.Value)
        {
            _logger.LogDebug("Tick vote: waiting for more data (need tick {EndTick}, have {MaxTick})",
                endTick, maxIndexedTick?.ToString() ?? "none");
            return false;
        }

        return await ProcessWindowAsync(epoch, startTick, endTick, ct);
    }

    private async Task<bool> ProcessWindowAsync(uint epoch, ulong startTick, ulong endTick, CancellationToken ct)
    {
        // 4. Build computor address → index lookup
        var computorsResult = await _bobProxy.GetComputorsAsync(epoch, ct);
        var computorIndexByAddress = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        if (computorsResult?.Computors != null)
        {
            for (int i = 0; i < computorsResult.Computors.Count; i++)
                computorIndexByAddress[computorsResult.Computors[i]] = i;
        }

        // 5. Accumulate votes in this window
        var scores = new ulong[QubicConstants.NumberOfComputors];
        var burnAddress = AddressLabelService.BurnAddress;
        var exactHexLen = CoreTransactionInputTypes.PackedComputorInputSize * 2;
        var dataHexLen = CoreTransactionInputTypes.PackedComputorDataSize * 2;

        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT tick_number, from_address, input_data FROM transactions
            WHERE epoch = {epoch}
              AND input_type = {CoreTransactionInputTypes.VoteCounter}
              AND to_address = '{burnAddress}'
              AND executed = 1
              AND tick_number >= {startTick}
              AND tick_number <= {endTick}
            ORDER BY tick_number, hash";

        var seenTicks = new HashSet<ulong>();
        int processed = 0, skipped = 0;

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var tickNumber = reader.GetFieldValue<ulong>(0);
            if (!seenTicks.Add(tickNumber)) { skipped++; continue; }

            var fromAddress = reader.GetString(1);
            var inputDataHex = reader.GetString(2);
            if (string.IsNullOrEmpty(inputDataHex)) { skipped++; continue; }

            var hex = inputDataHex.StartsWith("0x", StringComparison.OrdinalIgnoreCase)
                ? inputDataHex[2..] : inputDataHex;
            if (hex.Length != exactHexLen) { skipped++; continue; }

            byte[] data;
            try { data = Convert.FromHexString(hex[..dataHexLen]); }
            catch (FormatException) { skipped++; continue; }

            var computorIdx = computorIndexByAddress.GetValueOrDefault(fromAddress, -1);
            if (!ValidateVotePacket(data, computorIdx)) { skipped++; continue; }

            for (int i = 0; i < QubicConstants.NumberOfComputors; i++)
                scores[i] += Extract10Bit(data, i);

            processed++;
        }

        if (processed == 0)
        {
            _logger.LogDebug("Tick vote: no valid vote packets in ticks {Start}-{End} (epoch {Epoch})",
                startTick, endTick, epoch);
            // Still advance the state so we don't get stuck
            await UpdateStateAsync(epoch, endTick, ct);
            return true;
        }

        // 6. Persist to ClickHouse
        await PersistVotesAsync(epoch, endTick, scores, ct);
        await UpdateStateAsync(epoch, endTick, ct);

        _logger.LogInformation(
            "Tick vote: persisted {Count} vote packets for ticks {Start}-{End} (epoch {Epoch})",
            processed, startTick, endTick, epoch);

        return true;
    }

    private async Task PersistVotesAsync(uint epoch, ulong endTick, ulong[] scores, CancellationToken ct)
    {
        var now = DateTime.UtcNow;

        using var bulk = new ClickHouseBulkCopy(_connection)
        {
            DestinationTableName = "tick_votes",
            ColumnNames = ["epoch", "tick", "computor_index", "accumulated_votes", "created_at"],
            BatchSize = QubicConstants.NumberOfComputors
        };
        await bulk.InitAsync();

        var rows = new List<object[]>(QubicConstants.NumberOfComputors);
        for (int i = 0; i < QubicConstants.NumberOfComputors; i++)
        {
            rows.Add([(uint)epoch, endTick, (ushort)i, scores[i], now]);
        }

        await bulk.WriteToServerAsync(rows, ct);
    }

    private async Task<(uint Epoch, ulong Tick)> GetLastPersistedStateAsync(CancellationToken ct)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = @"
            SELECT value FROM indexer_state FINAL
            WHERE key = 'tick_votes_last_tick'";
        var tickResult = await cmd.ExecuteScalarAsync(ct);

        await using var epochCmd = _connection.CreateCommand();
        epochCmd.CommandText = @"
            SELECT value FROM indexer_state FINAL
            WHERE key = 'tick_votes_last_epoch'";
        var epochResult = await epochCmd.ExecuteScalarAsync(ct);

        var tick = tickResult != null && tickResult != DBNull.Value
            ? ulong.TryParse(tickResult.ToString(), out var t) ? t : 0UL
            : 0UL;
        var epoch = epochResult != null && epochResult != DBNull.Value
            ? uint.TryParse(epochResult.ToString(), out var e) ? e : 0U
            : 0U;

        return (epoch, tick);
    }

    private async Task UpdateStateAsync(uint epoch, ulong tick, CancellationToken ct)
    {
        var now = DateTime.UtcNow;

        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            INSERT INTO indexer_state (key, value, updated_at) VALUES
            ('tick_votes_last_tick', '{tick}', '{now:yyyy-MM-dd HH:mm:ss.fff}'),
            ('tick_votes_last_epoch', '{epoch}', '{now:yyyy-MM-dd HH:mm:ss.fff}')";
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private async Task<ulong?> GetMaxIndexedTickAsync(uint epoch, CancellationToken ct)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $"SELECT max(tick_number) FROM ticks WHERE epoch = {epoch}";
        var result = await cmd.ExecuteScalarAsync(ct);
        if (result == null || result == DBNull.Value) return null;
        var val = Convert.ToUInt64(result);
        return val == 0 ? null : val;
    }

    private async Task<EpochMetaInfo?> GetEpochMetaAsync(uint epoch, CancellationToken ct)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $"SELECT initial_tick, end_tick FROM epoch_meta WHERE epoch = {epoch}";
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct)) return null;
        return new EpochMetaInfo(reader.GetFieldValue<ulong>(0), reader.GetFieldValue<ulong>(1));
    }

    private record EpochMetaInfo(ulong InitialTick, ulong EndTick);

    private static bool ValidateVotePacket(byte[] data, int computorIdx)
    {
        ulong sum = 0;
        var values = new uint[QubicConstants.NumberOfComputors];
        for (int i = 0; i < QubicConstants.NumberOfComputors; i++)
        {
            values[i] = Extract10Bit(data, i);
            sum += values[i];
        }

        if (sum < (ulong)(QubicConstants.NumberOfComputors - 1) * (ulong)QubicConstants.Quorum)
            return false;

        if (computorIdx >= 0 && computorIdx < QubicConstants.NumberOfComputors && values[computorIdx] != 0)
            return false;

        return true;
    }

    private static uint Extract10Bit(byte[] data, int idx)
    {
        int byteOffset = idx + (idx >> 2);
        int lastBit0 = 8 - (idx & 3) * 2;
        int firstBit1 = 10 - lastBit0;
        uint res = (uint)(data[byteOffset] & ((1 << lastBit0) - 1)) << firstBit1;
        res |= (uint)(data[byteOffset + 1] >> (8 - firstBit1));
        return res;
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _connection.Dispose();
    }
}
