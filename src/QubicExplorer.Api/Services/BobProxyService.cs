using System.Text.Json.Serialization;
using Microsoft.Extensions.Caching.Memory;
using Qubic.Bob;
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
