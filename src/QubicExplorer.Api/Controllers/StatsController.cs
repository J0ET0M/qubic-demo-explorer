using Microsoft.AspNetCore.Mvc;
using QubicExplorer.Api.Attributes;
using QubicExplorer.Api.Services;

namespace QubicExplorer.Api.Controllers;

[ApiController]
[Route("api/[controller]")]
public class StatsController : ControllerBase
{
    private readonly ClickHouseQueryService _queryService;

    public StatsController(ClickHouseQueryService queryService)
    {
        _queryService = queryService;
    }

    [HttpGet]
    public async Task<IActionResult> GetStats(CancellationToken ct = default)
    {
        var result = await _queryService.GetNetworkStatsAsync(ct);
        return Ok(result);
    }

    [HttpGet("chart/tx-volume")]
    public async Task<IActionResult> GetTxVolumeChart(
        [FromQuery] string period = "day",
        CancellationToken ct = default)
    {
        if (period != "day" && period != "week" && period != "month")
            period = "day";

        var result = await _queryService.GetTxVolumeChartAsync(period, ct);
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

        var result = await _queryService.GetTopAddressesByVolumeAsync(limit, epoch, ct);
        return Ok(result);
    }

    [HttpGet("smart-contract-usage")]
    public async Task<IActionResult> GetSmartContractUsage(
        [FromQuery] uint? epoch = null,
        CancellationToken ct = default)
    {
        var result = await _queryService.GetSmartContractUsageAsync(epoch, ct);
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

        var result = await _queryService.GetActiveAddressTrendsAsync(period, limit, ct);
        return Ok(result);
    }

    [HttpGet("new-vs-returning")]
    public async Task<IActionResult> GetNewVsReturningAddresses(
        [FromQuery] int limit = 50,
        CancellationToken ct = default)
    {
        if (limit < 1) limit = 1;
        if (limit > 100) limit = 100;

        var result = await _queryService.GetNewVsReturningAddressesAsync(limit, ct);
        return Ok(result);
    }

    [HttpGet("exchange-flows")]
    public async Task<IActionResult> GetExchangeFlows(
        [FromQuery] int limit = 50,
        CancellationToken ct = default)
    {
        if (limit < 1) limit = 1;
        if (limit > 100) limit = 100;

        var result = await _queryService.GetExchangeFlowsAsync(limit, ct);
        return Ok(result);
    }

    [HttpGet("holder-distribution")]
    public async Task<IActionResult> GetHolderDistribution(CancellationToken ct = default)
    {
        var result = await _queryService.GetHolderDistributionWithConcentrationAsync(ct);
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

        var result = await _queryService.GetHolderDistributionExtendedAsync(historyLimit, from, to, ct);
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

        var result = await _queryService.GetHolderDistributionHistoryAsync(limit, from, to, ct);
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

        var result = await _queryService.GetAvgTxSizeTrendsAsync(period, limit, ct);
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

        var result = await _queryService.GetNetworkStatsHistoryAsync(limit, from, to, ct);
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

        var result = await _queryService.GetNetworkStatsExtendedAsync(historyLimit, from, to, ct);
        return Ok(result);
    }

    [HttpPost("network-stats/snapshot/{epoch}")]
    [AdminApiKey]
    public async Task<IActionResult> SaveNetworkStatsSnapshot(
        uint epoch,
        [FromQuery] ulong tickStart = 0,
        [FromQuery] ulong tickEnd = 0,
        CancellationToken ct = default)
    {
        await _queryService.SaveNetworkStatsSnapshotAsync(epoch, tickStart, tickEnd, ct);
        return Ok(new { success = true, epoch, tickStart, tickEnd });
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

        var result = await _queryService.GetBurnStatsHistoryAsync(limit, from, to, ct);
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

        var result = await _queryService.GetBurnStatsExtendedAsync(historyLimit, from, to, ct);
        return Ok(result);
    }
}
