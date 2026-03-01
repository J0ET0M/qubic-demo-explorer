using System.Buffers.Binary;
using System.Text.Json.Serialization;
using Microsoft.Extensions.Caching.Memory;
using Qubic.Bob;
using Qubic.Core;
using Qubic.Core.Contracts.Ccf;
using Qubic.Core.Contracts.Gqmprop;
using Qubic.Crypto;
using QubicExplorer.Shared.Models;

namespace QubicExplorer.Api.Services;

/// <summary>
/// Provides cached RPC queries over the shared BobWebSocketClient.
/// Replaces the previous hand-rolled WebSocket management with Qubic.Bob.
/// </summary>
public class BobProxyService
{
    private readonly BobWebSocketClient _bobClient;
    private readonly IMemoryCache _cache;
    private readonly ILogger<BobProxyService> _logger;

    public BobProxyService(
        BobWebSocketClient bobClient,
        IMemoryCache cache,
        ILogger<BobProxyService> logger)
    {
        _bobClient = bobClient;
        _cache = cache;
        _logger = logger;
    }

    public async Task<BalanceResult?> GetBalanceAsync(string address, CancellationToken ct = default)
    {
        var cacheKey = $"balance:{address}";

        if (_cache.TryGetValue(cacheKey, out BalanceResult? cachedResult))
        {
            return cachedResult;
        }

        try
        {
            var response = await _bobClient.GetBalanceAsync(address, ct);

            var result = new BalanceResult
            {
                Balance = response.GetBalance(),
                CurrentTick = response.CurrentTick,
                Identity = response.Identity,
                IncomingAmount = response.GetIncomingAmount(),
                OutgoingAmount = response.GetOutgoingAmount(),
                NumberOfIncomingTransfers = response.NumberOfIncomingTransfers,
                NumberOfOutgoingTransfers = response.NumberOfOutgoingTransfers,
                LatestIncomingTransferTick = response.LatestIncomingTransferTick,
                LatestOutgoingTransferTick = response.LatestOutgoingTransferTick
            };

            _cache.Set(cacheKey, result, TimeSpan.FromSeconds(10));
            return result;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to get balance for address {Address}", address);
            return null;
        }
    }

    public async Task<T?> CallAsync<T>(string method, object? parameters = null, CancellationToken ct = default)
    {
        try
        {
            return await _bobClient.CallAsync<T>(method, parameters, ct);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to call Bob method {Method}", method);
            return default;
        }
    }

    public async Task<EpochInfoResult?> GetEpochInfoAsync(uint epoch, CancellationToken ct = default)
    {
        try
        {
            var response = await _bobClient.GetEpochInfoAsync((int)epoch, ct);

            return new EpochInfoResult
            {
                Epoch = response.Epoch,
                InitialTick = response.GetInitialTick(),
                EndTick = response.GetEndTick(),
                FinalTick = response.GetFinalTick(),
                EndTickStartLogId = response.GetEndTickStartLogId(),
                EndTickEndLogId = response.GetEndTickEndLogId(),
                NumberOfTransactions = response.GetNumberOfTransactions()
            };
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to get epoch info for epoch {Epoch}", epoch);
            return null;
        }
    }

    public async Task<List<BobLog>?> GetEndEpochLogsAsync(uint epoch, CancellationToken ct = default)
    {
        try
        {
            // Use the raw CallAsync to get BobLog[] directly, since BobLog from Shared has
            // the rich helper methods (GetSourceAddress, GetDestAddress, etc.)
            return await _bobClient.CallAsync<List<BobLog>>("qubic_getEndEpochLogs", new object[] { epoch }, ct);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to get end epoch logs for epoch {Epoch}", epoch);
            return null;
        }
    }

    /// <summary>
    /// Gets log entries by ID range for a specific epoch. Used to backfill missing logs.
    /// </summary>
    public async Task<List<BobLog>?> GetLogsByIdRangeAsync(uint epoch, long startLogId, long endLogId, CancellationToken ct = default)
    {
        try
        {
            // Use the raw CallAsync to get BobLog[] directly (same pattern as GetEndEpochLogsAsync).
            // The JSON fields from qubic_getLogsByIdRange match BobLog's property names.
            return await _bobClient.CallAsync<List<BobLog>>(
                "qubic_getLogsByIdRange",
                new object[] { (int)epoch, startLogId, endLogId },
                ct);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to get logs by ID range for epoch {Epoch} ({Start}-{End})",
                epoch, startLogId, endLogId);
            return null;
        }
    }

    /// <summary>
    /// Gets the list of 676 computors for a specific epoch.
    /// </summary>
    public async Task<ComputorsResult?> GetComputorsAsync(uint epoch, CancellationToken ct = default)
    {
        var cacheKey = $"computors:{epoch}";

        if (_cache.TryGetValue(cacheKey, out ComputorsResult? cachedResult))
        {
            return cachedResult;
        }

        try
        {
            var response = await _bobClient.GetComputorsAsync(epoch, ct);

            var result = new ComputorsResult
            {
                Computors = response.Computors
            };

            // Cache computor lists longer since they don't change once epoch is complete
            _cache.Set(cacheKey, result, TimeSpan.FromHours(1));
            return result;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to get computors for epoch {Epoch}", epoch);
            return null;
        }
    }

    /// <summary>
    /// Queries the GQMPROP contract to get the current revenue donation table.
    /// Returns the list of active donation recipient addresses and their percentage amounts.
    /// Cached for 10 minutes since the table changes rarely (only via quorum voting).
    /// </summary>
    public async Task<List<RevenueDonationEntry>> GetRevenueDonationTableAsync(CancellationToken ct = default)
    {
        const string cacheKey = "gqmprop:revenueDonation";

        if (_cache.TryGetValue(cacheKey, out List<RevenueDonationEntry>? cached))
            return cached!;

        try
        {
            // Query GQMPROP (contract index 6), function 5 (GetRevenueDonation), empty input
            var hexResult = await _bobClient.QuerySmartContractAsync(
                QubicContracts.Gqmprop, 5, "", ct);

            var bytes = Convert.FromHexString(hexResult);
            var output = GetRevenueDonationOutput.FromBytes(bytes);
            var crypt = new QubicCrypt();

            var entries = new List<RevenueDonationEntry>();
            foreach (var entry in output.Entries)
            {
                // Skip empty entries (zero public key)
                if (entry.DestinationPublicKey.All(b => b == 0))
                    continue;
                if (entry.MillionthAmount <= 0)
                    continue;

                var address = crypt.GetIdentityFromPublicKey(entry.DestinationPublicKey);
                entries.Add(new RevenueDonationEntry
                {
                    Address = address,
                    MillionthAmount = entry.MillionthAmount,
                    Percentage = entry.MillionthAmount / 10_000.0, // Convert to percentage
                    FirstEpoch = entry.FirstEpoch
                });
            }

            _cache.Set(cacheKey, entries, TimeSpan.FromMinutes(10));
            return entries;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to query GQMPROP revenue donation table");
            return new List<RevenueDonationEntry>();
        }
    }

    public class RevenueDonationEntry
    {
        [JsonPropertyName("address")]
        public string Address { get; set; } = "";

        [JsonPropertyName("millionthAmount")]
        public long MillionthAmount { get; set; }

        [JsonPropertyName("percentage")]
        public double Percentage { get; set; }

        [JsonPropertyName("firstEpoch")]
        public ushort FirstEpoch { get; set; }
    }

    // =====================================================
    // CCF (Computor Controlled Fund) contract queries
    // =====================================================

    /// <summary>
    /// Get the latest 128 one-time transfers from the CCF contract.
    /// </summary>
    public async Task<List<ParsedTransferEntry>> GetCcfLatestTransfersAsync(CancellationToken ct = default)
    {
        try
        {
            var hexResult = await _bobClient.QuerySmartContractAsync(
                QubicContracts.Ccf, (int)CcfContract.Functions.GetLatestTransfers, "", ct);
            var bytes = Convert.FromHexString(hexResult);
            return CcfContractParser.ParseLatestTransfers(bytes);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to query CCF GetLatestTransfers");
            return [];
        }
    }

    /// <summary>
    /// Get the latest 128 regular/subscription payments from the CCF contract.
    /// </summary>
    public async Task<List<ParsedRegularPaymentEntry>> GetCcfRegularPaymentsAsync(CancellationToken ct = default)
    {
        try
        {
            var hexResult = await _bobClient.QuerySmartContractAsync(
                QubicContracts.Ccf, (int)CcfContract.Functions.GetRegularPayments, "", ct);
            var bytes = Convert.FromHexString(hexResult);
            return CcfContractParser.ParseRegularPayments(bytes);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to query CCF GetRegularPayments");
            return [];
        }
    }

    /// <summary>
    /// Get proposal indices (active or all). Returns up to 64 indices per call.
    /// Use prevProposalIndex for pagination (-1 to start from beginning).
    /// </summary>
    public async Task<(ushort count, ushort[] indices)> GetCcfProposalIndicesAsync(
        bool active, int prevProposalIndex, CancellationToken ct = default)
    {
        try
        {
            var input = new Qubic.Core.Contracts.Ccf.GetProposalIndicesInput
            {
                ActiveProposals = active,
                PrevProposalIndex = prevProposalIndex
            };
            var hexResult = await _bobClient.QuerySmartContractAsync(
                QubicContracts.Ccf, (int)CcfContract.Functions.GetProposalIndices,
                Convert.ToHexString(input.ToBytes()), ct);
            var bytes = Convert.FromHexString(hexResult);
            var output = Qubic.Core.Contracts.Ccf.GetProposalIndicesOutput.FromBytes(bytes);
            return (output.NumOfIndices, output.Indices);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to query CCF GetProposalIndices");
            return (0, []);
        }
    }

    /// <summary>
    /// Get full proposal details including subscription info.
    /// </summary>
    public async Task<ParsedGetProposalOutput?> GetCcfProposalAsync(
        ushort proposalIndex, CancellationToken ct = default)
    {
        try
        {
            var input = new Qubic.Core.Contracts.Ccf.GetProposalInput
            {
                SubscriptionDestination = new byte[32],
                ProposalIndex = proposalIndex
            };
            var hexResult = await _bobClient.QuerySmartContractAsync(
                QubicContracts.Ccf, (int)CcfContract.Functions.GetProposal,
                Convert.ToHexString(input.ToBytes()), ct);
            var bytes = Convert.FromHexString(hexResult);
            return CcfContractParser.ParseGetProposalOutput(bytes);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to query CCF GetProposal for index {Index}", proposalIndex);
            return null;
        }
    }

    /// <summary>
    /// Get summarized voting results for a proposal.
    /// </summary>
    public async Task<ParsedVotingResults?> GetCcfVotingResultsAsync(
        ushort proposalIndex, CancellationToken ct = default)
    {
        try
        {
            var input = new Qubic.Core.Contracts.Ccf.GetVotingResultsInput { ProposalIndex = proposalIndex };
            var hexResult = await _bobClient.QuerySmartContractAsync(
                QubicContracts.Ccf, (int)CcfContract.Functions.GetVotingResults,
                Convert.ToHexString(input.ToBytes()), ct);
            var bytes = Convert.FromHexString(hexResult);
            return CcfContractParser.ParseGetVotingResultsOutput(bytes);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to query CCF GetVotingResults for index {Index}", proposalIndex);
            return null;
        }
    }

    /// <summary>
    /// Get the current proposal fee.
    /// </summary>
    public async Task<uint> GetCcfProposalFeeAsync(CancellationToken ct = default)
    {
        try
        {
            var hexResult = await _bobClient.QuerySmartContractAsync(
                QubicContracts.Ccf, (int)CcfContract.Functions.GetProposalFee, "", ct);
            var bytes = Convert.FromHexString(hexResult);
            var output = GetProposalFeeOutput.FromBytes(bytes);
            return output.ProposalFee;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to query CCF GetProposalFee");
            return 0;
        }
    }

    // Public result types — kept for compatibility with existing consumers

    public class EpochInfoResult
    {
        [JsonPropertyName("epoch")]
        public uint Epoch { get; set; }

        [JsonPropertyName("initialTick")]
        public ulong InitialTick { get; set; }

        [JsonPropertyName("endTick")]
        public ulong EndTick { get; set; }

        [JsonPropertyName("finalTick")]
        public ulong FinalTick { get; set; }

        [JsonPropertyName("endTickStartLogId")]
        public ulong EndTickStartLogId { get; set; }

        [JsonPropertyName("endTickEndLogId")]
        public ulong EndTickEndLogId { get; set; }

        [JsonPropertyName("numberOfTransactions")]
        public ulong NumberOfTransactions { get; set; }
    }

    public class ComputorsResult
    {
        [JsonPropertyName("computors")]
        public List<string> Computors { get; set; } = new();
    }

    public class BalanceResult
    {
        [JsonPropertyName("balance")]
        public ulong Balance { get; set; }

        [JsonPropertyName("currentTick")]
        public ulong CurrentTick { get; set; }

        [JsonPropertyName("identity")]
        public string? Identity { get; set; }

        [JsonPropertyName("incomingAmount")]
        public ulong IncomingAmount { get; set; }

        [JsonPropertyName("outgoingAmount")]
        public ulong OutgoingAmount { get; set; }

        [JsonPropertyName("numberOfIncomingTransfers")]
        public uint NumberOfIncomingTransfers { get; set; }

        [JsonPropertyName("numberOfOutgoingTransfers")]
        public uint NumberOfOutgoingTransfers { get; set; }

        [JsonPropertyName("latestIncomingTransferTick")]
        public uint LatestIncomingTransferTick { get; set; }

        [JsonPropertyName("latestOutgoingTransferTick")]
        public uint LatestOutgoingTransferTick { get; set; }
    }
}
