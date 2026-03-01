using System.Text;
using System.Text.Json.Serialization;
using Microsoft.Extensions.Caching.Memory;
using Qubic.Bob;
using Qubic.Core;
using Qubic.Core.Contracts.Ccf;
using Qubic.Crypto;
using QubicExplorer.Shared.Models;

namespace QubicExplorer.Analytics.Services;

/// <summary>
/// Provides cached RPC queries over the shared BobWebSocketClient.
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
            return await _bobClient.CallAsync<List<BobLog>>("qubic_getEndEpochLogs", new object[] { epoch }, ct);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to get end epoch logs for epoch {Epoch}", epoch);
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

            _cache.Set(cacheKey, result, TimeSpan.FromHours(1));
            return result;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to get computors for epoch {Epoch}", epoch);
            return null;
        }
    }

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

    // =====================================================
    // CCF contract queries for persistence
    // =====================================================

    /// <summary>
    /// Get the latest 128 one-time transfers from the CCF contract.
    /// Returns parsed entries with empty slots filtered out.
    /// </summary>
    public async Task<List<CcfParsedTransfer>> GetCcfLatestTransfersAsync(CancellationToken ct = default)
    {
        try
        {
            var hexResult = await _bobClient.QuerySmartContractAsync(
                QubicContracts.Ccf, (int)CcfContract.Functions.GetLatestTransfers, "", ct);
            var bytes = Convert.FromHexString(hexResult);
            var output = GetLatestTransfersOutput.FromBytes(bytes);
            var crypt = new QubicCrypt();
            var result = new List<CcfParsedTransfer>();

            foreach (var entry in output.Entries)
            {
                if (entry.Tick == 0 && entry.Amount == 0) continue;
                if (entry.Destination.All(b => b == 0)) continue;

                result.Add(new CcfParsedTransfer
                {
                    Destination = crypt.GetIdentityFromPublicKey(entry.Destination),
                    Url = ReadNullTerminatedString(entry.Url),
                    Amount = entry.Amount,
                    Tick = entry.Tick,
                    Success = entry.Success
                });
            }

            return result;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to query CCF GetLatestTransfers");
            return [];
        }
    }

    /// <summary>
    /// Get the latest 128 regular/subscription payments from the CCF contract.
    /// Returns parsed entries with empty slots filtered out.
    /// </summary>
    public async Task<List<CcfParsedRegularPayment>> GetCcfRegularPaymentsAsync(CancellationToken ct = default)
    {
        try
        {
            var hexResult = await _bobClient.QuerySmartContractAsync(
                QubicContracts.Ccf, (int)CcfContract.Functions.GetRegularPayments, "", ct);
            var bytes = Convert.FromHexString(hexResult);
            var output = GetRegularPaymentsOutput.FromBytes(bytes);
            var crypt = new QubicCrypt();
            var result = new List<CcfParsedRegularPayment>();

            foreach (var entry in output.Entries)
            {
                if (entry.Tick == 0 && entry.Amount == 0) continue;
                if (entry.Destination.All(b => b == 0)) continue;

                result.Add(new CcfParsedRegularPayment
                {
                    Destination = crypt.GetIdentityFromPublicKey(entry.Destination),
                    Url = ReadNullTerminatedString(entry.Url),
                    Amount = entry.Amount,
                    Tick = entry.Tick,
                    PeriodIndex = entry.PeriodIndex,
                    Success = entry.Success
                });
            }

            return result;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to query CCF GetRegularPayments");
            return [];
        }
    }

    private static string ReadNullTerminatedString(byte[] data)
    {
        var nullIdx = Array.IndexOf(data, (byte)0);
        var len = nullIdx >= 0 ? nullIdx : data.Length;
        return Encoding.ASCII.GetString(data, 0, len).Trim();
    }

    public class CcfParsedTransfer
    {
        public string Destination { get; init; } = "";
        public string Url { get; init; } = "";
        public long Amount { get; init; }
        public uint Tick { get; init; }
        public bool Success { get; init; }
    }

    public class CcfParsedRegularPayment
    {
        public string Destination { get; init; } = "";
        public string Url { get; init; } = "";
        public long Amount { get; init; }
        public uint Tick { get; init; }
        public int PeriodIndex { get; init; }
        public bool Success { get; init; }
    }
}
