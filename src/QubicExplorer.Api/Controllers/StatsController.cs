using Microsoft.AspNetCore.Mvc;
using QubicExplorer.Api.Services;

namespace QubicExplorer.Api.Controllers;

[ApiController]
[Route("api/[controller]")]
public class StatsController : ControllerBase
{
    private readonly ClickHouseQueryService _queryService;
    private readonly AnalyticsCacheService _cache;

    public StatsController(ClickHouseQueryService queryService, AnalyticsCacheService cache)
    {
        _queryService = queryService;
        _cache = cache;
    }

    [HttpGet]
    public async Task<IActionResult> GetStats(CancellationToken ct = default)
    {
        var result = await _cache.GetOrSetAsync(
            "stats:network",
            AnalyticsCacheService.NetworkStatsTtl,
            () => _queryService.GetNetworkStatsAsync(ct));
        return Ok(result);
    }

    [HttpGet("chart/tx-volume")]
    public async Task<IActionResult> GetTxVolumeChart(
        [FromQuery] string period = "day",
        CancellationToken ct = default)
    {
        if (period != "day" && period != "week" && period != "month")
            period = "day";

        var result = await _cache.GetOrSetAsync(
            $"stats:tx-volume:{period}",
            AnalyticsCacheService.TxVolumeChartTtl,
            () => _queryService.GetTxVolumeChartAsync(period, ct));
        return Ok(result);
    }

    [HttpGet("top-addresses")]
    public async Task<IActionResult> GetTopAddresses(
        [FromQuery] int limit = 20,
        [FromQuery] uint? epoch = null,
        CancellationToken ct = default)
    {
        if (limit < 1) limit = 1;
        if (limit > 100) limit = 100;

        var result = await _cache.GetOrSetAsync(
            $"stats:top-addresses:{limit}:{epoch ?? 0}",
            AnalyticsCacheService.TopAddressesTtl,
            () => _queryService.GetTopAddressesByVolumeAsync(limit, epoch, ct));
        return Ok(result);
    }

    [HttpGet("smart-contract-usage")]
    public async Task<IActionResult> GetSmartContractUsage(
        [FromQuery] uint? epoch = null,
        CancellationToken ct = default)
    {
        var result = await _cache.GetOrSetAsync(
            $"stats:sc-usage:{epoch ?? 0}",
            AnalyticsCacheService.SmartContractUsageTtl,
            () => _queryService.GetSmartContractUsageAsync(epoch, ct));
        return Ok(result);
    }

    // =====================================================
    // GLASSNODE-STYLE ANALYTICS
    // =====================================================

    [HttpGet("active-addresses")]
    public async Task<IActionResult> GetActiveAddressTrends(
        [FromQuery] string period = "epoch",
        [FromQuery] int limit = 50,
        CancellationToken ct = default)
    {
        if (period != "epoch" && period != "daily")
            period = "epoch";
        if (limit < 1) limit = 1;
        if (limit > 100) limit = 100;

        var result = await _cache.GetOrSetAsync(
            $"stats:active-addresses:{period}:{limit}",
            AnalyticsCacheService.ActiveAddressTtl,
            () => _queryService.GetActiveAddressTrendsAsync(period, limit, ct));
        return Ok(result);
    }

    [HttpGet("new-vs-returning")]
    public async Task<IActionResult> GetNewVsReturningAddresses(
        [FromQuery] int limit = 50,
        CancellationToken ct = default)
    {
        if (limit < 1) limit = 1;
        if (limit > 100) limit = 100;

        var result = await _cache.GetOrSetAsync(
            $"stats:new-vs-returning:{limit}",
            AnalyticsCacheService.NewVsReturningTtl,
            () => _queryService.GetNewVsReturningAddressesAsync(limit, ct));
        return Ok(result);
    }

    [HttpGet("exchange-flows")]
    public async Task<IActionResult> GetExchangeFlows(
        [FromQuery] int limit = 50,
        CancellationToken ct = default)
    {
        if (limit < 1) limit = 1;
        if (limit > 100) limit = 100;

        var result = await _cache.GetOrSetAsync(
            $"stats:exchange-flows:{limit}",
            AnalyticsCacheService.ExchangeFlowsTtl,
            () => _queryService.GetExchangeFlowsAsync(limit, ct));
        return Ok(result);
    }

    [HttpGet("exchange-senders")]
    public async Task<IActionResult> GetExchangeSenders(
        [FromQuery] uint epochs = 5,
        [FromQuery] ulong minAmount = 1_000_000_000,
        [FromQuery] int limit = 100,
        CancellationToken ct = default)
    {
        if (epochs < 1) epochs = 1;
        if (epochs > 50) epochs = 50;
        if (limit < 1) limit = 1;
        if (limit > 500) limit = 500;

        var result = await _cache.GetOrSetAsync(
            $"stats:exchange-senders:{epochs}:{minAmount}:{limit}",
            AnalyticsCacheService.ExchangeSendersTtl,
            () => _queryService.GetExchangeSendersAsync(epochs, minAmount, limit, ct));
        return Ok(result);
    }

    [HttpGet("holder-distribution")]
    public async Task<IActionResult> GetHolderDistribution(CancellationToken ct = default)
    {
        var result = await _cache.GetOrSetAsync(
            "stats:holder-distribution",
            AnalyticsCacheService.HolderDistributionTtl,
            () => _queryService.GetHolderDistributionWithConcentrationAsync(ct));
        return Ok(result);
    }

    [HttpGet("holder-distribution/extended")]
    public async Task<IActionResult> GetHolderDistributionExtended(
        [FromQuery] int historyLimit = 30,
        [FromQuery] DateTime? from = null,
        [FromQuery] DateTime? to = null,
        CancellationToken ct = default)
    {
        if (historyLimit < 1) historyLimit = 1;
        if (historyLimit > 500) historyLimit = 500;

        var result = await _cache.GetOrSetAsync(
            $"stats:holder-dist-ext:{historyLimit}:{from?.Ticks ?? 0}:{to?.Ticks ?? 0}",
            AnalyticsCacheService.SnapshotExtendedTtl,
            () => _queryService.GetHolderDistributionExtendedAsync(historyLimit, from, to, ct));
        return Ok(result);
    }

    [HttpGet("holder-distribution/history")]
    public async Task<IActionResult> GetHolderDistributionHistory(
        [FromQuery] int limit = 30,
        [FromQuery] DateTime? from = null,
        [FromQuery] DateTime? to = null,
        CancellationToken ct = default)
    {
        if (limit < 1) limit = 1;
        if (limit > 500) limit = 500;

        var result = await _cache.GetOrSetAsync(
            $"stats:holder-dist-hist:{limit}:{from?.Ticks ?? 0}:{to?.Ticks ?? 0}",
            AnalyticsCacheService.SnapshotHistoryTtl,
            () => _queryService.GetHolderDistributionHistoryAsync(limit, from, to, ct));
        return Ok(result);
    }

    // Manual snapshot endpoint removed - snapshots are now automatically created
    // by AnalyticsSnapshotService every 4 hours using tick-based windows

    [HttpGet("avg-tx-size")]
    public async Task<IActionResult> GetAvgTxSizeTrends(
        [FromQuery] string period = "epoch",
        [FromQuery] int limit = 50,
        CancellationToken ct = default)
    {
        if (period != "epoch" && period != "daily")
            period = "epoch";
        if (limit < 1) limit = 1;
        if (limit > 100) limit = 100;

        var result = await _cache.GetOrSetAsync(
            $"stats:avg-tx-size:{period}:{limit}",
            AnalyticsCacheService.AvgTxSizeTtl,
            () => _queryService.GetAvgTxSizeTrendsAsync(period, limit, ct));
        return Ok(result);
    }

    // =====================================================
    // NETWORK STATS HISTORY
    // =====================================================

    [HttpGet("network-stats/history")]
    public async Task<IActionResult> GetNetworkStatsHistory(
        [FromQuery] int limit = 30,
        [FromQuery] DateTime? from = null,
        [FromQuery] DateTime? to = null,
        CancellationToken ct = default)
    {
        if (limit < 1) limit = 1;
        if (limit > 500) limit = 500;

        var result = await _cache.GetOrSetAsync(
            $"stats:net-hist:{limit}:{from?.Ticks ?? 0}:{to?.Ticks ?? 0}",
            AnalyticsCacheService.SnapshotHistoryTtl,
            () => _queryService.GetNetworkStatsHistoryAsync(limit, from, to, ct));
        return Ok(result);
    }

    [HttpGet("network-stats/extended")]
    public async Task<IActionResult> GetNetworkStatsExtended(
        [FromQuery] int historyLimit = 30,
        [FromQuery] DateTime? from = null,
        [FromQuery] DateTime? to = null,
        CancellationToken ct = default)
    {
        if (historyLimit < 1) historyLimit = 1;
        if (historyLimit > 500) historyLimit = 500;

        var result = await _cache.GetOrSetAsync(
            $"stats:net-ext:{historyLimit}:{from?.Ticks ?? 0}:{to?.Ticks ?? 0}",
            AnalyticsCacheService.SnapshotExtendedTtl,
            () => _queryService.GetNetworkStatsExtendedAsync(historyLimit, from, to, ct));
        return Ok(result);
    }

    // =====================================================
    // BURN STATS HISTORY
    // =====================================================

    [HttpGet("burn-stats/history")]
    public async Task<IActionResult> GetBurnStatsHistory(
        [FromQuery] int limit = 30,
        [FromQuery] DateTime? from = null,
        [FromQuery] DateTime? to = null,
        CancellationToken ct = default)
    {
        if (limit < 1) limit = 1;
        if (limit > 500) limit = 500;

        var result = await _cache.GetOrSetAsync(
            $"stats:burn-hist:{limit}:{from?.Ticks ?? 0}:{to?.Ticks ?? 0}",
            AnalyticsCacheService.SnapshotHistoryTtl,
            () => _queryService.GetBurnStatsHistoryAsync(limit, from, to, ct));
        return Ok(result);
    }

    [HttpGet("burn-stats/extended")]
    public async Task<IActionResult> GetBurnStatsExtended(
        [FromQuery] int historyLimit = 30,
        [FromQuery] DateTime? from = null,
        [FromQuery] DateTime? to = null,
        CancellationToken ct = default)
    {
        if (historyLimit < 1) historyLimit = 1;
        if (historyLimit > 500) historyLimit = 500;

        var result = await _cache.GetOrSetAsync(
            $"stats:burn-ext:{historyLimit}:{from?.Ticks ?? 0}:{to?.Ticks ?? 0}",
            AnalyticsCacheService.SnapshotExtendedTtl,
            () => _queryService.GetBurnStatsExtendedAsync(historyLimit, from, to, ct));
        return Ok(result);
    }
}
