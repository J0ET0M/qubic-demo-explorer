using Microsoft.Extensions.Caching.Memory;
using QubicExplorer.Shared.DTOs;

namespace QubicExplorer.Api.Services;

/// <summary>
/// In-memory caching service for analytics data.
/// Uses short TTLs for real-time metrics and longer TTLs for historical data.
/// </summary>
public class AnalyticsCacheService
{
    private readonly IMemoryCache _cache;
    private readonly ILogger<AnalyticsCacheService> _logger;

    // Cache durations
    private static readonly TimeSpan NetworkStatsTtl = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan HolderDistributionTtl = TimeSpan.FromMinutes(5);
    private static readonly TimeSpan ActiveAddressTtl = TimeSpan.FromMinutes(10);
    private static readonly TimeSpan NewVsReturningTtl = TimeSpan.FromMinutes(15);
    private static readonly TimeSpan ExchangeFlowsTtl = TimeSpan.FromMinutes(10);
    private static readonly TimeSpan AvgTxSizeTtl = TimeSpan.FromMinutes(10);
    private static readonly TimeSpan TopAddressesTtl = TimeSpan.FromMinutes(5);
    private static readonly TimeSpan SmartContractUsageTtl = TimeSpan.FromMinutes(10);

    // Cache keys
    private const string NetworkStatsKey = "analytics:network-stats";
    private const string HolderDistributionKey = "analytics:holder-distribution";
    private const string ActiveAddressKeyPrefix = "analytics:active-addresses";
    private const string NewVsReturningKeyPrefix = "analytics:new-vs-returning";
    private const string ExchangeFlowsKeyPrefix = "analytics:exchange-flows";
    private const string AvgTxSizeKeyPrefix = "analytics:avg-tx-size";
    private const string TopAddressesKeyPrefix = "analytics:top-addresses";
    private const string SmartContractUsageKeyPrefix = "analytics:sc-usage";

    public AnalyticsCacheService(IMemoryCache cache, ILogger<AnalyticsCacheService> logger)
    {
        _cache = cache;
        _logger = logger;
    }

    // Network Stats
    public NetworkStatsDto? GetNetworkStats()
    {
        return _cache.Get<NetworkStatsDto>(NetworkStatsKey);
    }

    public void SetNetworkStats(NetworkStatsDto data)
    {
        _cache.Set(NetworkStatsKey, data, NetworkStatsTtl);
        _logger.LogDebug("Cached network stats for {Ttl}", NetworkStatsTtl);
    }

    // Holder Distribution
    public HolderDistributionDto? GetHolderDistribution()
    {
        return _cache.Get<HolderDistributionDto>(HolderDistributionKey);
    }

    public void SetHolderDistribution(HolderDistributionDto data)
    {
        _cache.Set(HolderDistributionKey, data, HolderDistributionTtl);
        _logger.LogDebug("Cached holder distribution for {Ttl}", HolderDistributionTtl);
    }

    // Active Address Trends
    public List<ActiveAddressTrendDto>? GetActiveAddressTrends(string period, int limit)
    {
        var key = $"{ActiveAddressKeyPrefix}:{period}:{limit}";
        return _cache.Get<List<ActiveAddressTrendDto>>(key);
    }

    public void SetActiveAddressTrends(string period, int limit, List<ActiveAddressTrendDto> data)
    {
        var key = $"{ActiveAddressKeyPrefix}:{period}:{limit}";
        _cache.Set(key, data, ActiveAddressTtl);
        _logger.LogDebug("Cached active address trends ({Period}, {Limit}) for {Ttl}", period, limit, ActiveAddressTtl);
    }

    // New vs Returning Addresses
    public List<NewVsReturningDto>? GetNewVsReturning(int limit)
    {
        var key = $"{NewVsReturningKeyPrefix}:{limit}";
        return _cache.Get<List<NewVsReturningDto>>(key);
    }

    public void SetNewVsReturning(int limit, List<NewVsReturningDto> data)
    {
        var key = $"{NewVsReturningKeyPrefix}:{limit}";
        _cache.Set(key, data, NewVsReturningTtl);
        _logger.LogDebug("Cached new vs returning addresses ({Limit}) for {Ttl}", limit, NewVsReturningTtl);
    }

    // Exchange Flows
    public ExchangeFlowDto? GetExchangeFlows(int limit)
    {
        var key = $"{ExchangeFlowsKeyPrefix}:{limit}";
        return _cache.Get<ExchangeFlowDto>(key);
    }

    public void SetExchangeFlows(int limit, ExchangeFlowDto data)
    {
        var key = $"{ExchangeFlowsKeyPrefix}:{limit}";
        _cache.Set(key, data, ExchangeFlowsTtl);
        _logger.LogDebug("Cached exchange flows ({Limit}) for {Ttl}", limit, ExchangeFlowsTtl);
    }

    // Average Transaction Size Trends
    public List<AvgTxSizeTrendDto>? GetAvgTxSizeTrends(string period, int limit)
    {
        var key = $"{AvgTxSizeKeyPrefix}:{period}:{limit}";
        return _cache.Get<List<AvgTxSizeTrendDto>>(key);
    }

    public void SetAvgTxSizeTrends(string period, int limit, List<AvgTxSizeTrendDto> data)
    {
        var key = $"{AvgTxSizeKeyPrefix}:{period}:{limit}";
        _cache.Set(key, data, AvgTxSizeTtl);
        _logger.LogDebug("Cached avg tx size trends ({Period}, {Limit}) for {Ttl}", period, limit, AvgTxSizeTtl);
    }

    // Top Addresses
    public List<TopAddressDto>? GetTopAddresses(int limit, uint? epoch)
    {
        var key = $"{TopAddressesKeyPrefix}:{limit}:{epoch ?? 0}";
        return _cache.Get<List<TopAddressDto>>(key);
    }

    public void SetTopAddresses(int limit, uint? epoch, List<TopAddressDto> data)
    {
        var key = $"{TopAddressesKeyPrefix}:{limit}:{epoch ?? 0}";
        _cache.Set(key, data, TopAddressesTtl);
        _logger.LogDebug("Cached top addresses ({Limit}, epoch={Epoch}) for {Ttl}", limit, epoch, TopAddressesTtl);
    }

    // Smart Contract Usage
    public List<SmartContractUsageDto>? GetSmartContractUsage(uint? epoch)
    {
        var key = $"{SmartContractUsageKeyPrefix}:{epoch ?? 0}";
        return _cache.Get<List<SmartContractUsageDto>>(key);
    }

    public void SetSmartContractUsage(uint? epoch, List<SmartContractUsageDto> data)
    {
        var key = $"{SmartContractUsageKeyPrefix}:{epoch ?? 0}";
        _cache.Set(key, data, SmartContractUsageTtl);
        _logger.LogDebug("Cached smart contract usage (epoch={Epoch}) for {Ttl}", epoch, SmartContractUsageTtl);
    }

    /// <summary>
    /// Clear all analytics cache entries
    /// </summary>
    public void ClearAll()
    {
        // Note: IMemoryCache doesn't have a built-in clear method
        // We would need to track keys or use a different cache implementation
        // For now, entries will expire naturally based on TTL
        _logger.LogInformation("Analytics cache clear requested (entries will expire based on TTL)");
    }
}
