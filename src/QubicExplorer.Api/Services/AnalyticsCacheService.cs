using System.Collections.Concurrent;
using Microsoft.Extensions.Caching.Memory;

namespace QubicExplorer.Api.Services;

/// <summary>
/// In-memory caching service for analytics data.
/// Uses short TTLs for real-time metrics and longer TTLs for historical/snapshot data.
/// Includes stampede protection: only one concurrent request per cache key hits the DB.
/// </summary>
public class AnalyticsCacheService
{
    private readonly IMemoryCache _cache;
    private readonly ILogger<AnalyticsCacheService> _logger;
    private readonly ConcurrentDictionary<string, SemaphoreSlim> _locks = new();

    // Cache durations - real-time (ticks are ~2-3s, so 2-5min is plenty)
    public static readonly TimeSpan NetworkStatsTtl = TimeSpan.FromMinutes(2);
    public static readonly TimeSpan TxVolumeChartTtl = TimeSpan.FromMinutes(30);
    public static readonly TimeSpan TopAddressesTtl = TimeSpan.FromMinutes(30);
    public static readonly TimeSpan HolderDistributionTtl = TimeSpan.FromMinutes(30);
    public static readonly TimeSpan ActiveAddressTtl = TimeSpan.FromMinutes(30);
    public static readonly TimeSpan ExchangeFlowsTtl = TimeSpan.FromMinutes(30);
    public static readonly TimeSpan AvgTxSizeTtl = TimeSpan.FromMinutes(30);
    public static readonly TimeSpan SmartContractUsageTtl = TimeSpan.FromMinutes(30);
    public static readonly TimeSpan NewVsReturningTtl = TimeSpan.FromHours(1);

    // Cache durations - snapshot-based (data changes every 4 hours)
    public static readonly TimeSpan SnapshotHistoryTtl = TimeSpan.FromHours(2);
    public static readonly TimeSpan SnapshotExtendedTtl = TimeSpan.FromHours(2);

    // Cache durations - exchange senders
    public static readonly TimeSpan ExchangeSendersTtl = TimeSpan.FromMinutes(15);

    // Cache durations - rich list and supply
    public static readonly TimeSpan RichListTtl = TimeSpan.FromMinutes(30);
    public static readonly TimeSpan SupplyDashboardTtl = TimeSpan.FromMinutes(30);

    // Cache durations - address summary
    public static readonly TimeSpan AddressSummaryTtl = TimeSpan.FromMinutes(5);

    // Cache durations - assets
    public static readonly TimeSpan AssetListTtl = TimeSpan.FromMinutes(10);
    public static readonly TimeSpan AssetDetailTtl = TimeSpan.FromMinutes(5);

    // Cache durations - whale alerts
    public static readonly TimeSpan WhaleAlertsTtl = TimeSpan.FromMinutes(2);

    // Cache durations - address activity range
    public static readonly TimeSpan AddressActivityRangeTtl = TimeSpan.FromMinutes(5);

    // Cache durations - epoch countdown
    public static readonly TimeSpan EpochCountdownTtl = TimeSpan.FromMinutes(5);

    // Cache durations - qearn
    public static readonly TimeSpan QearnStatsTtl = TimeSpan.FromMinutes(30);

    // Cache durations - CCF
    public static readonly TimeSpan CcfStatsTtl = TimeSpan.FromMinutes(10);

    // Cache durations - computor revenue
    public static readonly TimeSpan ComputorRevenueTtl = TimeSpan.FromMinutes(5);

    // Cache durations - miner flow (static per epoch)
    public static readonly TimeSpan MinerFlowStatsTtl = TimeSpan.FromMinutes(30);
    public static readonly TimeSpan MinerFlowVisualizationTtl = TimeSpan.FromHours(1);
    public static readonly TimeSpan ComputorsTtl = TimeSpan.FromHours(1);
    public static readonly TimeSpan EmissionsTtl = TimeSpan.FromHours(1);
    public static readonly TimeSpan EpochStatsTtl = TimeSpan.FromHours(1);

    public AnalyticsCacheService(IMemoryCache cache, ILogger<AnalyticsCacheService> logger)
    {
        _cache = cache;
        _logger = logger;
    }

    /// <summary>
    /// Generic cache-aside with stampede protection: only one concurrent caller per key
    /// executes the factory; all others wait and get the cached result.
    /// </summary>
    public async Task<T> GetOrSetAsync<T>(string key, TimeSpan ttl, Func<Task<T>> factory)
    {
        if (_cache.TryGetValue(key, out T? cached) && cached != null)
        {
            _logger.LogDebug("Cache hit: {Key}", key);
            return cached;
        }

        var semaphore = _locks.GetOrAdd(key, _ => new SemaphoreSlim(1, 1));
        await semaphore.WaitAsync();
        try
        {
            // Double-check after acquiring lock — another thread may have populated it
            if (_cache.TryGetValue(key, out cached) && cached != null)
            {
                _logger.LogDebug("Cache hit (after lock): {Key}", key);
                return cached;
            }

            _logger.LogDebug("Cache miss: {Key}, fetching from source", key);
            var result = await factory();
            _cache.Set(key, result, ttl);
            return result;
        }
        finally
        {
            semaphore.Release();
        }
    }
}
