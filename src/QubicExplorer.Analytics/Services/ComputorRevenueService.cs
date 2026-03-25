using ClickHouse.Client.ADO;
using Microsoft.Extensions.Options;
using Qubic.Core;
using QubicExplorer.Shared.Configuration;
using QubicExplorer.Shared.DTOs;
using QubicExplorer.Shared.Services;

namespace QubicExplorer.Analytics.Services;

/// <summary>
/// Computes per-computor revenue for an epoch by replicating the C++ revenue calculation
/// from the Qubic core (revenue.h). Uses three multiplicative score factors:
/// TX points × vote points × custom mining points.
///
/// Runs periodically in the Analytics service and persists results to ClickHouse.
/// </summary>
public class ComputorRevenueService : IDisposable
{
    private readonly ClickHouseConnection _connection;
    private readonly BobProxyService _bobProxy;
    private readonly AddressLabelService _labelService;
    private readonly ILogger<ComputorRevenueService> _logger;
    private bool _disposed;

    private const ulong ScalingThreshold = (ulong)QubicConstants.RevenueScalingThreshold;

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

        // Calculate all three score types
        var txScores = await CalculateTxScoresAsync(epoch, ct);
        var voteScores = await CalculateVoteScoresAsync(epoch, ct);
        var miningScores = await CalculateMiningScoresAsync(epoch, ct);

        // Compute factors (replicates computeRevFactor from revenue.h)
        var txQuorum = GetQuorumScore(txScores);
        var voteQuorum = GetQuorumScore(voteScores);
        var miningQuorum = GetQuorumScore(miningScores);

        var txFactors = ComputeFactors(txScores, txQuorum);
        var voteFactors = ComputeFactors(voteScores, voteQuorum);
        var miningFactors = ComputeFactors(miningScores, miningQuorum);

        // Compute per-computor revenue (replicates computeRevenue from revenue.h)
        var revenues = new long[QubicConstants.NumberOfComputors];
        long totalRevenue = 0;
        for (int i = 0; i < QubicConstants.NumberOfComputors; i++)
        {
            ulong combined = txFactors[i] * voteFactors[i] * miningFactors[i];
            revenues[i] = (long)(combined * (ulong)QubicConstants.IssuancePerComputor
                / ScalingThreshold / ScalingThreshold / ScalingThreshold);
            totalRevenue += revenues[i];
        }

        // Build result entries
        var entries = new ComputorRevenueEntryDto[QubicConstants.NumberOfComputors];
        for (int i = 0; i < QubicConstants.NumberOfComputors; i++)
        {
            var address = addresses[i];
            entries[i] = new ComputorRevenueEntryDto(
                ComputorIndex: (ushort)i,
                Address: address,
                Label: _labelService.GetLabel(address),
                TxScore: txScores[i],
                VoteScore: voteScores[i],
                MiningScore: miningScores[i],
                TxFactor: txFactors[i],
                VoteFactor: voteFactors[i],
                MiningFactor: miningFactors[i],
                Revenue: revenues[i]
            );
        }

        return new ComputorRevenueDto(
            Epoch: epoch,
            ComputorCount: QubicConstants.NumberOfComputors,
            IssuanceRate: QubicConstants.IssuanceRate,
            TxQuorumScore: txQuorum,
            VoteQuorumScore: voteQuorum,
            MiningQuorumScore: miningQuorum,
            TotalComputorRevenue: totalRevenue,
            ArbRevenue: QubicConstants.IssuanceRate - totalRevenue,
            Computors: entries
        );
    }

    private async Task PersistRevenueAsync(ComputorRevenueDto result, CancellationToken ct)
    {
        var computorsJson = System.Text.Json.JsonSerializer.Serialize(result.Computors);
        var escapedJson = computorsJson.Replace("'", "\\'");

        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            INSERT INTO computor_revenue
            (epoch, computor_count, issuance_rate, tx_quorum_score, vote_quorum_score,
             mining_quorum_score, total_computor_revenue, arb_revenue, computors)
            VALUES
            ({result.Epoch}, {result.ComputorCount}, {result.IssuanceRate},
             {result.TxQuorumScore}, {result.VoteQuorumScore}, {result.MiningQuorumScore},
             {result.TotalComputorRevenue}, {result.ArbRevenue}, '{escapedJson}')";

        await cmd.ExecuteNonQueryAsync(ct);
        _logger.LogInformation("Persisted computor revenue for epoch {Epoch} ({Active} active computors)",
            result.Epoch, result.Computors.Count(c => c.Revenue > 0));
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
        return await CalculatePackedScoresAsync(epoch, CoreTransactionInputTypes.CustomMiningShareCounter, ct);
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
              AND amount = 0
              AND to_address = '{burnAddress}'
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
