using ClickHouse.Client.ADO;
using Microsoft.Extensions.Options;
using Qubic.Core;
using QubicExplorer.Shared.Configuration;
using QubicExplorer.Shared.DTOs;
using QubicExplorer.Shared.Services;

namespace QubicExplorer.Analytics.Services;

/// <summary>
/// Computes per-computor revenue for an epoch.
///
/// For epochs ≥ <see cref="RevenueV2Calculator.DefaultV2FromEpoch"/> (qubic core PR #829)
/// uses the V2 additive-bonus formula: M = (17·tx + 3·oracle)/20 combined with a DOGE
/// mining factor via revenue = IPC × M × (S² + B·E) / (S·(S+B)·S). TX uses a 1351-tick
/// sliding window (per-tick log score normalised by window sum), oracle replaces vote
/// (vote is kept only as a metric), and DOGE-only mining (XMR removed in PR #844).
///
/// For older epochs, computes the legacy V1 multiplicative formula
/// (txFactor × voteFactor × miningFactor × IPC / S³).
///
/// Runs periodically in the Analytics service and persists to ClickHouse.
/// </summary>
public class ComputorRevenueService : IDisposable
{
    private readonly ClickHouseConnection _connection;
    private readonly BobProxyService _bobProxy;
    private readonly AddressLabelService _labelService;
    private readonly ILogger<ComputorRevenueService> _logger;
    private bool _disposed;

    private const ulong ScalingThreshold = (ulong)QubicConstants.RevenueScalingThreshold;
    private const uint V2FromEpoch = RevenueV2Calculator.DefaultV2FromEpoch;

    public ComputorRevenueService(
        IOptions<ClickHouseOptions> options,
        BobProxyService bobProxy,
        AddressLabelService labelService,
        ILogger<ComputorRevenueService> logger)
    {
        _bobProxy = bobProxy;
        _labelService = labelService;
        _logger = logger;
        _connection = new ClickHouseConnection(options.Value.ConnectionString);
        _connection.Open();
    }

    /// <summary>
    /// Calculate revenue for the current epoch and persist to ClickHouse.
    /// Called periodically by AnalyticsSnapshotService.
    /// </summary>
    public async Task ComputeAndPersistAsync(uint epoch, CancellationToken ct)
    {
        var result = await CalculateRevenueAsync(epoch, ct);
        if (result == null) return;

        await PersistRevenueAsync(result, ct);
    }

    public async Task<ComputorRevenueDto?> CalculateRevenueAsync(uint epoch, CancellationToken ct)
    {
        _logger.LogInformation("Calculating computor revenue for epoch {Epoch}", epoch);

        // Get computor addresses for this epoch
        var computorsResult = await _bobProxy.GetComputorsAsync(epoch, ct);
        if (computorsResult?.Computors == null || computorsResult.Computors.Count != QubicConstants.NumberOfComputors)
        {
            _logger.LogWarning("Could not get computor list for epoch {Epoch}", epoch);
            return null;
        }

        var addresses = computorsResult.Computors;
        var useV2 = epoch >= V2FromEpoch;

        // V1 / shared score sources
        var txScoresV1 = await CalculateTxScoresAsync(epoch, ct);
        var voteScores = await CalculateVoteScoresAsync(epoch, ct);
        var miningScores = await CalculateMiningScoresAsync(epoch, ct);

        // V1 quorum + factors (kept for divergence monitoring; older epochs use this as "active").
        var txQuorum = GetQuorumScore(txScoresV1);
        var voteQuorum = GetQuorumScore(voteScores);
        var miningQuorumV1 = GetQuorumScore(miningScores);

        var txFactorsV1 = ComputeFactors(txScoresV1, txQuorum);
        var voteFactorsV1 = ComputeFactors(voteScores, voteQuorum);
        var miningFactorsV1 = ComputeFactors(miningScores, miningQuorumV1);

        // V1 revenue = (tx × vote × mining) × IPC / S³.
        var revenuesV1 = new long[QubicConstants.NumberOfComputors];
        for (int i = 0; i < QubicConstants.NumberOfComputors; i++)
        {
            ulong combined = txFactorsV1[i] * voteFactorsV1[i] * miningFactorsV1[i];
            revenuesV1[i] = (long)(combined * (ulong)QubicConstants.IssuancePerComputor
                / ScalingThreshold / ScalingThreshold / ScalingThreshold);
        }

        // V2 — sliding-window TX, oracle factor, DOGE-only mining, additive bonus formula.
        // For epochs before V2FromEpoch we still compute V2 with what we have but don't make it active.
        var (perTickTxCount, initialTick) = await GetPerTickTxCountsAsync(epoch, ct);
        var oracleScores = await CalculateOracleScoresAsync(epoch, ct);

        var txRevenuePoints = QubicConstants.TxRevenuePoints.ToArray();
        var v2 = RevenueV2Calculator.Compute(
            (long)initialTick, perTickTxCount, oracleScores, miningScores, txRevenuePoints);

        // Quorum scores for the V2 inputs (sliding-window TX and oracle), reported at top level.
        ulong slidingTxQuorum = ComputeQuorumScore(v2.SlidingWindowTxScoreFull);
        ulong oracleQuorum = ComputeQuorumScore(oracleScores);
        // For V2, mining is the same DOGE counter as V1 → reuse miningQuorumV1.

        // Build result entries
        var entries = new ComputorRevenueEntryDto[QubicConstants.NumberOfComputors];
        long totalRevenue = 0;
        for (int i = 0; i < QubicConstants.NumberOfComputors; i++)
        {
            var address = addresses[i];
            long active = useV2 ? v2.Revenue[i] : revenuesV1[i];
            totalRevenue += active;

            entries[i] = new ComputorRevenueEntryDto(
                ComputorIndex: (ushort)i,
                Address: address,
                Label: _labelService.GetLabel(address),
                TxScore: txScoresV1[i],
                VoteScore: voteScores[i],
                MiningScore: miningScores[i],
                SlidingWindowTxScore: v2.SlidingWindowTxScoreFull[i],
                OracleScore: oracleScores[i],
                TxFactor: useV2 ? v2.TxFactor[i] : txFactorsV1[i],
                VoteFactor: voteFactorsV1[i],
                OracleFactor: v2.OracleFactor[i],
                MiningFactor: useV2 ? v2.MiningFactor[i] : miningFactorsV1[i],
                CombinedMandatoryFactor: v2.CombinedMandatoryFactor[i],
                RevenueV1: revenuesV1[i],
                RevenueV2: v2.Revenue[i],
                RevenueFormula: useV2 ? 2 : 1,
                Revenue: active
            );
        }

        _logger.LogInformation(
            "Revenue computed for epoch {Epoch}: useV2={UseV2}, totalRev={Total} (V1 sum={V1Sum}, V2 sum={V2Sum})",
            epoch, useV2, totalRevenue, revenuesV1.Sum(), v2.Revenue.Sum());

        return new ComputorRevenueDto(
            Epoch: epoch,
            ComputorCount: QubicConstants.NumberOfComputors,
            IssuanceRate: QubicConstants.IssuanceRate,
            TxQuorumScore: useV2 ? slidingTxQuorum : txQuorum,
            VoteQuorumScore: voteQuorum,
            OracleQuorumScore: oracleQuorum,
            MiningQuorumScore: miningQuorumV1,
            ActiveFormula: useV2 ? 2 : 1,
            TotalComputorRevenue: totalRevenue,
            ArbRevenue: QubicConstants.IssuanceRate - totalRevenue,
            Computors: entries
        );
    }

    /// <summary>
    /// Per-tick TX counts for the epoch range. Returns (counts, initialTick) where
    /// counts is indexed by (tick - initialTick). Length = (latestTick - initialTick + 1).
    /// </summary>
    private async Task<(ushort[] PerTick, ulong InitialTick)> GetPerTickTxCountsAsync(uint epoch, CancellationToken ct)
    {
        // Get the epoch's initial tick (start of the window).
        ulong initialTick = 0;
        await using (var metaCmd = _connection.CreateCommand())
        {
            metaCmd.CommandText = $"SELECT initial_tick FROM epoch_meta FINAL WHERE epoch = {epoch}";
            var r = await metaCmd.ExecuteScalarAsync(ct);
            if (r != null && r != DBNull.Value)
                initialTick = Convert.ToUInt64(r);
        }

        ulong maxTick = 0;
        await using (var maxCmd = _connection.CreateCommand())
        {
            maxCmd.CommandText = $"SELECT max(tick_number) FROM ticks WHERE epoch = {epoch}";
            var r = await maxCmd.ExecuteScalarAsync(ct);
            if (r != null && r != DBNull.Value)
                maxTick = Convert.ToUInt64(r);
        }

        if (initialTick == 0 || maxTick < initialTick)
            return (Array.Empty<ushort>(), initialTick);

        var totalTicks = (int)(maxTick - initialTick + 1);
        var perTick = new ushort[totalTicks];

        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $"SELECT tick_number, tx_count FROM ticks WHERE epoch = {epoch} AND is_empty = 0 AND tx_count > 0";
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var t = reader.GetFieldValue<ulong>(0);
            var c = reader.GetFieldValue<uint>(1);
            int idx = (int)(t - initialTick);
            if (idx >= 0 && idx < totalTicks)
                perTick[idx] = (ushort)Math.Min(c, ushort.MaxValue);
        }

        return (perTick, initialTick);
    }

    /// <summary>
    /// Per-computor oracle revenue points for the epoch (= our estimated_points).
    /// Reads from the aggregate table if present, otherwise returns zeros (V2 will fall back to "all full").
    /// </summary>
    private async Task<ulong[]> CalculateOracleScoresAsync(uint epoch, CancellationToken ct)
    {
        var scores = new ulong[QubicConstants.NumberOfComputors];

        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT computor_index, estimated_points
            FROM oracle_computor_summary FINAL
            WHERE epoch = {epoch}";
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            int idx = reader.GetFieldValue<ushort>(0);
            long pts = Convert.ToInt64(reader.GetValue(1));
            if (idx >= 0 && idx < scores.Length && pts > 0)
                scores[idx] = (ulong)pts;
        }

        return scores;
    }

    private static ulong ComputeQuorumScore(ulong[] scores)
    {
        if (scores.Length == 0) return 0;
        var sorted = (ulong[])scores.Clone();
        Array.Sort(sorted);
        Array.Reverse(sorted);
        int rank = RevenueV2Calculator.GlobalQuorumRank(scores.Length);
        return sorted[rank - 1];
    }

    private async Task PersistRevenueAsync(ComputorRevenueDto result, CancellationToken ct)
    {
        var computorsJson = System.Text.Json.JsonSerializer.Serialize(result.Computors);
        var escapedJson = computorsJson.Replace("'", "\\'");

        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            INSERT INTO computor_revenue
            (epoch, computor_count, issuance_rate, tx_quorum_score, vote_quorum_score,
             oracle_quorum_score, mining_quorum_score, active_formula,
             total_computor_revenue, arb_revenue, computors)
            VALUES
            ({result.Epoch}, {result.ComputorCount}, {result.IssuanceRate},
             {result.TxQuorumScore}, {result.VoteQuorumScore},
             {result.OracleQuorumScore}, {result.MiningQuorumScore}, {result.ActiveFormula},
             {result.TotalComputorRevenue}, {result.ArbRevenue}, '{escapedJson}')";

        await cmd.ExecuteNonQueryAsync(ct);
        _logger.LogInformation("Persisted computor revenue for epoch {Epoch} (formula V{Formula}, {Active} active computors)",
            result.Epoch, result.ActiveFormula, result.Computors.Count(c => c.Revenue > 0));
    }

    /// <summary>
    /// Calculate TX scores from tick data. For each non-empty tick, the computor at
    /// (tick_number % 676) gets gTxRevenuePoints[min(tx_count, 1024)] added to their score.
    /// </summary>
    private async Task<ulong[]> CalculateTxScoresAsync(uint epoch, CancellationToken ct)
    {
        var scores = new ulong[QubicConstants.NumberOfComputors];

        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $"SELECT tick_number, tx_count FROM ticks WHERE epoch = {epoch} AND is_empty = 0 AND tx_count > 0";

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var tickNumber = reader.GetFieldValue<ulong>(0);
            var txCount = reader.GetFieldValue<uint>(1);
            var computorIdx = (int)(tickNumber % QubicConstants.NumberOfComputors);
            var pointIdx = Math.Min(txCount, 1024);
            scores[computorIdx] += QubicConstants.TxRevenuePoints[(int)pointIdx];
        }

        return scores;
    }

    private async Task<ulong[]> CalculateVoteScoresAsync(uint epoch, CancellationToken ct)
    {
        return await CalculatePackedScoresAsync(epoch, CoreTransactionInputTypes.VoteCounter, ct);
    }

    private async Task<ulong[]> CalculateMiningScoresAsync(uint epoch, CancellationToken ct)
    {
        // qubic core PR #844 removed XMR (input_type 8) and replaced it with DOGE (input_type 11).
        // V2 epochs read DOGE; older epochs keep reading the legacy XMR counter so historical V1
        // numbers stay reproducible.
        var inputType = epoch >= V2FromEpoch
            ? RevenueV2Calculator.DogeSharesInputType
            : CoreTransactionInputTypes.CustomMiningShareCounter;
        return await CalculatePackedScoresAsync(epoch, inputType, ct);
    }

    /// <summary>
    /// Generic method to calculate scores from packed 10-bit transaction data.
    /// Used for both vote counter (type 1) and custom mining shares (type 8).
    /// </summary>
    private async Task<ulong[]> CalculatePackedScoresAsync(uint epoch, ushort inputType, CancellationToken ct)
    {
        // Build computor address → index lookup for validation
        var computorsResult = await _bobProxy.GetComputorsAsync(epoch, ct);
        var computorIndexByAddress = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        if (computorsResult?.Computors != null)
        {
            for (int i = 0; i < computorsResult.Computors.Count; i++)
                computorIndexByAddress[computorsResult.Computors[i]] = i;
        }

        var isVoteCounter = inputType == CoreTransactionInputTypes.VoteCounter;
        var scores = new ulong[QubicConstants.NumberOfComputors];
        var burnAddress = AddressLabelService.BurnAddress;
        var exactHexLen = CoreTransactionInputTypes.PackedComputorInputSize * 2;
        var dataHexLen = CoreTransactionInputTypes.PackedComputorDataSize * 2;

        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT tick_number, from_address, input_data FROM transactions
            WHERE epoch = {epoch}
              AND input_type = {inputType}
              AND to_address = '{burnAddress}'
              AND executed = 1
            ORDER BY tick_number, hash";

        var seenTicks = new HashSet<ulong>();
        int processed = 0, skippedSize = 0, skippedDuplicate = 0, skippedValidation = 0;

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var tickNumber = reader.GetFieldValue<ulong>(0);

            if (!seenTicks.Add(tickNumber))
            {
                skippedDuplicate++;
                continue;
            }

            var fromAddress = reader.GetString(1);
            var inputDataHex = reader.GetString(2);
            if (string.IsNullOrEmpty(inputDataHex)) { skippedSize++; continue; }

            var hex = inputDataHex.StartsWith("0x", StringComparison.OrdinalIgnoreCase)
                ? inputDataHex[2..] : inputDataHex;

            if (hex.Length != exactHexLen) { skippedSize++; continue; }

            byte[] data;
            try { data = Convert.FromHexString(hex[..dataHexLen]); }
            catch (FormatException) { skippedSize++; continue; }

            // Validate packet
            var computorIdx = computorIndexByAddress.GetValueOrDefault(fromAddress, -1);
            if (!ValidatePackedPacket(data, computorIdx, isVoteCounter))
            {
                skippedValidation++;
                continue;
            }

            for (int i = 0; i < QubicConstants.NumberOfComputors; i++)
                scores[i] += Extract10Bit(data, i);

            processed++;
        }

        _logger.LogDebug(
            "Parsed input_type={Type} for epoch {Epoch}: {Processed} processed, {SkippedSize} skipped (size), {SkippedDup} skipped (dup), {SkippedVal} skipped (validation)",
            inputType, epoch, processed, skippedSize, skippedDuplicate, skippedValidation);
        return scores;
    }

    private static bool ValidatePackedPacket(byte[] data, int computorIdx, bool isVoteCounter)
    {
        if (isVoteCounter)
        {
            ulong sum = 0;
            var values = new uint[QubicConstants.NumberOfComputors];
            for (int i = 0; i < QubicConstants.NumberOfComputors; i++)
            {
                values[i] = Extract10Bit(data, i);
                sum += values[i];
            }

            // Sum of all votes must be >= (676 - 1) * 451 = 304,425
            if (sum < (ulong)(QubicConstants.NumberOfComputors - 1) * (ulong)QubicConstants.Quorum)
                return false;

            // Own vote count must be zero
            if (computorIdx >= 0 && computorIdx < QubicConstants.NumberOfComputors && values[computorIdx] != 0)
                return false;
        }
        else
        {
            // Mining packets: own count must be zero
            if (computorIdx >= 0 && computorIdx < QubicConstants.NumberOfComputors)
            {
                var ownValue = Extract10Bit(data, computorIdx);
                if (ownValue != 0)
                    return false;
            }
        }

        return true;
    }

    /// <summary>
    /// Extract a 10-bit value from packed data at the given computor index.
    /// Port of the C++ extract10Bit function from vote_counter.h / mining.h.
    /// </summary>
    private static uint Extract10Bit(byte[] data, int idx)
    {
        int byteOffset = idx + (idx >> 2);
        if (byteOffset + 1 >= data.Length) return 0;

        uint byte0 = data[byteOffset];
        uint byte1 = data[byteOffset + 1];
        int lastBit0 = 8 - (idx & 3) * 2;
        int firstBit1 = 10 - lastBit0;
        uint res = (byte0 & (uint)((1 << lastBit0) - 1)) << firstBit1;
        res |= byte1 >> (8 - firstBit1);
        return res;
    }

    private static ulong GetQuorumScore(ulong[] scores)
    {
        var sorted = new ulong[QubicConstants.Quorum + 1];
        for (int i = 0; i < QubicConstants.NumberOfComputors; i++)
        {
            sorted[QubicConstants.Quorum] = scores[i];
            int j = QubicConstants.Quorum;
            while (j > 0 && sorted[j - 1] < sorted[j])
            {
                (sorted[j - 1], sorted[j]) = (sorted[j], sorted[j - 1]);
                j--;
            }
        }
        return sorted[QubicConstants.Quorum - 1] == 0 ? 1 : sorted[QubicConstants.Quorum - 1];
    }

    private static ulong[] ComputeFactors(ulong[] scores, ulong quorumScore)
    {
        var factors = new ulong[QubicConstants.NumberOfComputors];
        for (int i = 0; i < QubicConstants.NumberOfComputors; i++)
        {
            if (scores[i] == 0)
                factors[i] = 0;
            else if (scores[i] >= quorumScore)
                factors[i] = ScalingThreshold;
            else
                factors[i] = ScalingThreshold * scores[i] / quorumScore;
        }
        return factors;
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            _connection.Dispose();
            _disposed = true;
        }
    }
}
