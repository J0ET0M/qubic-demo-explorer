using Microsoft.Extensions.Caching.Memory;

namespace QubicExplorer.Api.Services;

/// <summary>
/// In-memory caching service for analytics data.
/// Uses short TTLs for real-time metrics and longer TTLs for historical/snapshot data.
/// </summary>
public class AnalyticsCacheService
{
    private readonly IMemoryCache _cache;
    private readonly ILogger<AnalyticsCacheService> _logger;

    // Cache durations - real-time
    public static readonly TimeSpan NetworkStatsTtl = TimeSpan.FromSeconds(30);
    public static readonly TimeSpan TxVolumeChartTtl = TimeSpan.FromMinutes(5);
    public static readonly TimeSpan TopAddressesTtl = TimeSpan.FromMinutes(5);
    public static readonly TimeSpan HolderDistributionTtl = TimeSpan.FromMinutes(5);
    public static readonly TimeSpan ActiveAddressTtl = TimeSpan.FromMinutes(10);
    public static readonly TimeSpan ExchangeFlowsTtl = TimeSpan.FromMinutes(10);
    public static readonly TimeSpan AvgTxSizeTtl = TimeSpan.FromMinutes(10);
    public static readonly TimeSpan SmartContractUsageTtl = TimeSpan.FromMinutes(10);
    public static readonly TimeSpan NewVsReturningTtl = TimeSpan.FromMinutes(15);

    // Cache durations - snapshot-based (data changes every 4 hours)
    public static readonly TimeSpan SnapshotHistoryTtl = TimeSpan.FromMinutes(30);
    public static readonly TimeSpan SnapshotExtendedTtl = TimeSpan.FromMinutes(30);

    // Cache durations - exchange senders
    public static readonly TimeSpan ExchangeSendersTtl = TimeSpan.FromMinutes(10);

    // Cache durations - rich list and supply
    public static readonly TimeSpan RichListTtl = TimeSpan.FromMinutes(5);
    public static readonly TimeSpan SupplyDashboardTtl = TimeSpan.FromMinutes(10);

    // Cache durations - assets
    public static readonly TimeSpan AssetListTtl = TimeSpan.FromMinutes(10);
    public static readonly TimeSpan AssetDetailTtl = TimeSpan.FromMinutes(5);

    // Cache durations - whale alerts
    public static readonly TimeSpan WhaleAlertsTtl = TimeSpan.FromSeconds(30);

    // Cache durations - address activity range
    public static readonly TimeSpan AddressActivityRangeTtl = TimeSpan.FromMinutes(5);

    // Cache durations - epoch countdown
    public static readonly TimeSpan EpochCountdownTtl = TimeSpan.FromMinutes(1);

    // Cache durations - miner flow (static per epoch)
    public static readonly TimeSpan MinerFlowStatsTtl = TimeSpan.FromMinutes(30);
    public static readonly TimeSpan MinerFlowVisualizationTtl = TimeSpan.FromHours(1);
    public static readonly TimeSpan ComputorsTtl = TimeSpan.FromHours(1);
    public static readonly TimeSpan EmissionsTtl = TimeSpan.FromHours(1);

    public AnalyticsCacheService(IMemoryCache cache, ILogger<AnalyticsCacheService> logger)
    {
        _cache = cache;
        _logger = logger;
    }

    /// <summary>
    /// Generic cache-aside: returns cached value or calls factory, caches the result, and returns it.
    /// </summary>
    public async Task<T> GetOrSetAsync<T>(string key, TimeSpan ttl, Func<Task<T>> factory)
    {
        if (_cache.TryGetValue(key, out T? cached) && cached != null)
        {
            _logger.LogDebug("Cache hit: {Key}", key);
            return cached;
        }

        _logger.LogDebug("Cache miss: {Key}, fetching from source", key);
        var result = await factory();
        _cache.Set(key, result, ttl);
        return result;
    }
}
