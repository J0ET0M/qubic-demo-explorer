using Microsoft.AspNetCore.Mvc;
using QubicExplorer.Api.Attributes;
using QubicExplorer.Api.Services;
using QubicExplorer.Shared.DTOs;

namespace QubicExplorer.Api.Controllers;

[ApiController]
[Route("api/[controller]")]
public class StatsController : ControllerBase
{
    private readonly ClickHouseQueryService _queryService;
    private readonly AnalyticsCacheService _cache;
    private readonly BobProxyService _bobProxy;

    public StatsController(ClickHouseQueryService queryService, AnalyticsCacheService cache, BobProxyService bobProxy)
    {
        _queryService = queryService;
        _cache = cache;
        _bobProxy = bobProxy;
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

    [HttpGet("rich-list")]
    public async Task<IActionResult> GetRichList(
        [FromQuery] int page = 1,
        [FromQuery] int limit = 50,
        CancellationToken ct = default)
    {
        if (page < 1) page = 1;
        if (limit < 1) limit = 1;
        if (limit > 100) limit = 100;

        var result = await _cache.GetOrSetAsync(
            $"stats:rich-list:{page}:{limit}",
            AnalyticsCacheService.RichListTtl,
            () => _queryService.GetRichListAsync(page, limit, ct));
        return Ok(result);
    }

    [HttpGet("supply")]
    public async Task<IActionResult> GetSupplyDashboard(CancellationToken ct = default)
    {
        var result = await _cache.GetOrSetAsync(
            "stats:supply",
            AnalyticsCacheService.SupplyDashboardTtl,
            () => _queryService.GetSupplyDashboardAsync(ct));
        return Ok(result);
    }

    [HttpGet("whale-alerts")]
    public async Task<IActionResult> GetWhaleAlerts(
        [FromQuery] ulong threshold = 10_000_000_000,
        [FromQuery] int limit = 50,
        CancellationToken ct = default)
    {
        if (limit < 1) limit = 1;
        if (limit > 200) limit = 200;

        var result = await _cache.GetOrSetAsync(
            $"stats:whale-alerts:{threshold}:{limit}",
            AnalyticsCacheService.WhaleAlertsTtl,
            () => _queryService.GetWhaleAlertsAsync(threshold, limit, ct));
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
        [FromQuery] int depth = 1,
        CancellationToken ct = default)
    {
        if (epochs < 1) epochs = 1;
        if (epochs > 50) epochs = 50;
        if (limit < 1) limit = 1;
        if (limit > 500) limit = 500;
        if (depth < 1) depth = 1;
        if (depth > 2) depth = 2;

        var result = await _cache.GetOrSetAsync(
            $"stats:exchange-senders:{epochs}:{minAmount}:{limit}:{depth}",
            AnalyticsCacheService.ExchangeSendersTtl,
            () => _queryService.GetExchangeSendersAsync(epochs, minAmount, limit, depth, ct));
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

    // =====================================================
    // QEARN STATS
    // =====================================================

    [HttpGet("qearn")]
    public async Task<IActionResult> GetQearnStats(CancellationToken ct = default)
    {
        var result = await _cache.GetOrSetAsync(
            "stats:qearn",
            AnalyticsCacheService.QearnStatsTtl,
            () => _queryService.GetQearnStatsAsync(ct));
        return Ok(result);
    }

    [HttpPost("qearn/backfill")]
    [AdminApiKey]
    public async Task<IActionResult> BackfillQearnStats(CancellationToken ct = default)
    {
        var (backfilled, epochs) = await _queryService.BackfillQearnEpochStatsAsync(ct);
        return Ok(new { success = true, backfilled, epochs });
    }

    [HttpPost("emission/backfill")]
    [AdminApiKey]
    public async Task<IActionResult> BackfillEmissionStats(CancellationToken ct = default)
    {
        var donationTable = await _bobProxy.GetRevenueDonationTableAsync(ct);
        var (backfilled, epochs) = await _queryService.BackfillEmissionStatsAsync(donationTable, ct);
        return Ok(new { success = true, backfilled, epochs });
    }

    // =====================================================
    // CCF (COMPUTOR CONTROLLED FUND) STATS
    // =====================================================

    [HttpGet("ccf")]
    public async Task<IActionResult> GetCcfStats(CancellationToken ct = default)
    {
        var result = await _cache.GetOrSetAsync(
            "stats:ccf",
            AnalyticsCacheService.CcfStatsTtl,
            () => BuildCcfStatsAsync(ct));
        return Ok(result);
    }

    private async Task<CcfStatsDto> BuildCcfStatsAsync(CancellationToken ct)
    {
        // Parallel: persisted data from ClickHouse + live contract data from Bob
        var transfersTask = _queryService.GetCcfTransfersAsync(ct);
        var regularPaymentsTask = _queryService.GetCcfRegularPaymentsAsync(ct);
        var spendingByEpochTask = _queryService.GetCcfSpendingByEpochAsync(ct);
        var totalSpendingTask = _queryService.GetCcfTotalSpendingAsync(ct);
        var proposalFeeTask = _bobProxy.GetCcfProposalFeeAsync(ct);

        await Task.WhenAll(transfersTask, regularPaymentsTask, spendingByEpochTask,
            totalSpendingTask, proposalFeeTask);

        var transfers = await transfersTask;
        var regularPayments = await regularPaymentsTask;
        var spendingByEpoch = await spendingByEpochTask;
        var (totalSpent, totalCount) = await totalSpendingTask;
        var proposalFee = await proposalFeeTask;

        // Fetch proposals from contract (active + past)
        var activeProposals = await GetCcfProposalsAsync(active: true, ct);
        var pastProposals = await GetCcfProposalsAsync(active: false, ct);

        // Fetch active subscriptions from proposals that have them
        var subscriptions = new List<CcfSubscriptionDto>();
        foreach (var p in activeProposals.Concat(pastProposals))
        {
            var proposal = await _bobProxy.GetCcfProposalAsync(p.ProposalIndex, ct);
            if (proposal?.HasActiveSubscription == true && proposal.Subscription is { } sub
                && sub.Destination != null)
            {
                subscriptions.Add(new CcfSubscriptionDto(
                    Destination: sub.Destination,
                    Url: sub.Url,
                    AmountPerPeriod: sub.AmountPerPeriod,
                    NumberOfPeriods: sub.NumberOfPeriods,
                    CurrentPeriod: sub.CurrentPeriod,
                    WeeksPerPeriod: sub.WeeksPerPeriod,
                    StartEpoch: sub.StartEpoch
                ));
            }
        }

        return new CcfStatsDto(
            ActiveProposals: activeProposals,
            PastProposals: pastProposals,
            Transfers: transfers,
            RegularPayments: regularPayments,
            ActiveSubscriptions: subscriptions,
            TotalSpent: totalSpent,
            TotalTransferCount: totalCount,
            ProposalFee: proposalFee,
            SpendingByEpoch: spendingByEpoch
        );
    }

    private async Task<List<CcfProposalDto>> GetCcfProposalsAsync(bool active, CancellationToken ct)
    {
        var result = new List<CcfProposalDto>();
        var prevIndex = -1;

        // Paginate through all proposal indices (64 per call)
        while (true)
        {
            var (count, indices) = await _bobProxy.GetCcfProposalIndicesAsync(active, prevIndex, ct);
            if (count == 0) break;

            for (var i = 0; i < count; i++)
            {
                var idx = indices[i];
                var proposal = await _bobProxy.GetCcfProposalAsync(idx, ct);
                if (proposal is not { Okay: true, Proposal: not null }) continue;

                var voting = await _bobProxy.GetCcfVotingResultsAsync(idx, ct);

                var noVotes = voting != null && voting.OptionCount >= 2
                    ? (int)voting.OptionVoteCounts[0] : 0;
                var yesVotes = voting != null && voting.OptionCount >= 2
                    ? (int)voting.OptionVoteCounts[1] : 0;
                var totalCast = voting != null ? (int)voting.TotalVotesCast : 0;
                var passed = yesVotes > CcfContractParser.Quorum / 2 && totalCast >= CcfContractParser.Quorum;

                result.Add(new CcfProposalDto(
                    ProposalIndex: idx,
                    ProposerAddress: proposal.ProposerAddress ?? "",
                    Url: proposal.Proposal.Url,
                    ProposalType: proposal.Proposal.ProposalType,
                    ProposalTick: proposal.Proposal.Tick,
                    Epoch: proposal.Proposal.Epoch,
                    TransferDestination: proposal.Proposal.TransferDestination,
                    TransferAmount: proposal.Proposal.TransferAmount,
                    TotalVotesAuthorized: voting != null ? (int)voting.TotalVotesAuthorized : CcfContractParser.NumberOfComputors,
                    TotalVotesCast: totalCast,
                    NoVotes: noVotes,
                    YesVotes: yesVotes,
                    Passed: passed,
                    IsActive: active
                ));
            }

            if (count < 64) break;
            prevIndex = indices[count - 1];
        }

        return result;
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

    [HttpGet("burn-stats/by-epoch")]
    public async Task<IActionResult> GetBurnStatsByEpoch(
        [FromQuery] int limit = 50,
        CancellationToken ct = default)
    {
        if (limit < 1) limit = 1;
        if (limit > 200) limit = 200;

        var result = await _cache.GetOrSetAsync(
            $"stats:burn-epoch:{limit}",
            AnalyticsCacheService.SnapshotHistoryTtl,
            () => _queryService.GetBurnStatsByEpochAsync(limit, ct));
        return Ok(result);
    }

    // =====================================================
    // COMPUTOR REVENUE
    // =====================================================

    [HttpGet("computor-revenue")]
    public async Task<IActionResult> GetComputorRevenue(CancellationToken ct = default)
    {
        var epoch = await _queryService.GetCurrentEpochAsync(ct);
        if (epoch == null) return NotFound("No epoch data available");

        var result = await _cache.GetOrSetAsync(
            $"stats:computor-revenue:{epoch}",
            AnalyticsCacheService.ComputorRevenueTtl,
            () => _queryService.GetComputorRevenueAsync(epoch.Value, ct));
        if (result == null) return NotFound("No computor revenue data available for current epoch");
        return Ok(result);
    }

    [HttpGet("computor-revenue/{epoch:int}")]
    public async Task<IActionResult> GetComputorRevenueByEpoch(uint epoch, CancellationToken ct = default)
    {
        var result = await _cache.GetOrSetAsync(
            $"stats:computor-revenue:{epoch}",
            AnalyticsCacheService.ComputorRevenueTtl,
            () => _queryService.GetComputorRevenueAsync(epoch, ct));
        if (result == null) return NotFound("No computor revenue data available for this epoch");
        return Ok(result);
    }

    /// <summary>
    /// Filtered computor revenue — return only the requested entries by computor
    /// index and/or address. Both filters can be supplied together; an entry is
    /// returned if it matches EITHER list. Comma-separated, max 676 entries each.
    /// Epoch path variant.
    /// </summary>
    /// <param name="epoch">Target epoch.</param>
    /// <param name="indices">Comma-separated computor indices (0–675), e.g. <c>0,5,42</c>.</param>
    /// <param name="addresses">Comma-separated 60-char Qubic addresses.</param>
    /// <remarks>
    /// Example:
    /// <code>GET /api/stats/computor-revenue/217/select?indices=0,5,42</code>
    /// <code>GET /api/stats/computor-revenue/217/select?addresses=ABCD…,EFGH…</code>
    /// <code>GET /api/stats/computor-revenue/217/select?indices=0,5&amp;addresses=ABCD…</code>
    /// <para>
    /// Reuses the full-epoch cache, so this is essentially free on a cache hit.
    /// Headline totals (TotalComputorRevenue, ArbRevenue, etc.) are unchanged from
    /// the full DTO — they describe the epoch, not the filtered subset. Sum the
    /// returned <c>computors[].revenue</c> values for a subset total.
    /// </para>
    /// </remarks>
    [HttpGet("computor-revenue/{epoch:int}/select")]
    public async Task<IActionResult> GetComputorRevenueSelectedByEpoch(
        uint epoch,
        [FromQuery] string? indices = null,
        [FromQuery] string? addresses = null,
        CancellationToken ct = default)
    {
        return await SelectComputorRevenueAsync(epoch, indices, addresses, ct);
    }

    /// <summary>
    /// Filtered computor revenue for the current epoch — same shape as the
    /// epoch-pinned variant.
    /// </summary>
    [HttpGet("computor-revenue/select")]
    public async Task<IActionResult> GetComputorRevenueSelected(
        [FromQuery] string? indices = null,
        [FromQuery] string? addresses = null,
        CancellationToken ct = default)
    {
        var epoch = await _queryService.GetCurrentEpochAsync(ct);
        if (epoch == null) return NotFound("No epoch data available");
        return await SelectComputorRevenueAsync(epoch.Value, indices, addresses, ct);
    }

    private async Task<IActionResult> SelectComputorRevenueAsync(
        uint epoch, string? indices, string? addresses, CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(indices) && string.IsNullOrWhiteSpace(addresses))
            return BadRequest("At least one of 'indices' or 'addresses' is required.");

        var wantedIndices = ParseIndices(indices);
        var wantedAddresses = ParseAddresses(addresses);

        var full = await _cache.GetOrSetAsync(
            $"stats:computor-revenue:{epoch}",
            AnalyticsCacheService.ComputorRevenueTtl,
            () => _queryService.GetComputorRevenueAsync(epoch, ct));
        if (full == null) return NotFound("No computor revenue data available for this epoch");

        var filtered = full.Computors
            .Where(c =>
                (wantedIndices.Count > 0 && wantedIndices.Contains(c.ComputorIndex)) ||
                (wantedAddresses.Count > 0 && wantedAddresses.Contains(c.Address)))
            .ToArray();

        return Ok(full with { Computors = filtered });
    }

    private static HashSet<ushort> ParseIndices(string? raw)
    {
        var set = new HashSet<ushort>();
        if (string.IsNullOrWhiteSpace(raw)) return set;
        foreach (var part in raw.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            if (ushort.TryParse(part, out var v) && v < 676) set.Add(v);
        }
        return set;
    }

    private static HashSet<string> ParseAddresses(string? raw)
    {
        var set = new HashSet<string>(StringComparer.Ordinal);
        if (string.IsNullOrWhiteSpace(raw)) return set;
        foreach (var part in raw.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            // Qubic addresses are exactly 60 uppercase A–Z; anything else is rejected.
            if (part.Length == 60) set.Add(part);
        }
        return set;
    }

    /// <summary>
    /// List of epochs that have a persisted owner summary — used by the
    /// frontend selector so users can only pick epochs we have data for.
    /// </summary>
    [HttpGet("owners/epochs")]
    public async Task<IActionResult> GetOwnerSummaryEpochs(CancellationToken ct = default)
    {
        var epochs = await _cache.GetOrSetAsync(
            "stats:owners:epochs",
            TimeSpan.FromMinutes(5),
            () => _queryService.GetOwnerSummaryEpochsAsync(ct));
        return Ok(epochs);
    }

    /// <summary>
    /// Per-owner roll-up of computor revenue + DOGE participation for an epoch.
    /// Groups all 676 computors by their owner label (from fattydoge snapshot)
    /// and returns per-owner totals, averages, and DOGE-mining indicators.
    /// </summary>
    [HttpGet("computor-revenue/{epoch:int}/by-owner")]
    public async Task<IActionResult> GetComputorRevenueByOwner(uint epoch, CancellationToken ct = default)
    {
        var result = await _cache.GetOrSetAsync(
            $"stats:computor-revenue:{epoch}:by-owner",
            AnalyticsCacheService.ComputorRevenueTtl,
            () => _queryService.GetComputorRevenueByOwnerAsync(epoch, ct));
        if (result == null) return NotFound("No computor revenue data available for this epoch");
        return Ok(result);
    }

    /// <summary>By-owner roll-up for the current epoch.</summary>
    [HttpGet("computor-revenue/by-owner")]
    public async Task<IActionResult> GetComputorRevenueByOwnerCurrent(CancellationToken ct = default)
    {
        var epoch = await _queryService.GetCurrentEpochAsync(ct);
        if (epoch == null) return NotFound("No epoch data available");
        return await GetComputorRevenueByOwner(epoch.Value, ct);
    }

    /// <summary>
    /// Owners overview for an epoch — alias for the by-owner endpoint. Returns
    /// the same shape; provided so the URL path matches the "owned computors
    /// per epoch and per owner" semantics.
    /// </summary>
    [HttpGet("owners/{epoch:int}")]
    public Task<IActionResult> GetOwnersOverview(uint epoch, CancellationToken ct = default)
        => GetComputorRevenueByOwner(epoch, ct);

    /// <summary>Drill into one owner's computors for the epoch.</summary>
    [HttpGet("owners/{epoch:int}/{owner}")]
    public async Task<IActionResult> GetOwnerDetail(uint epoch, string owner, CancellationToken ct = default)
    {
        var result = await _cache.GetOrSetAsync(
            $"stats:owners:{epoch}:{owner}",
            AnalyticsCacheService.ComputorRevenueTtl,
            () => _queryService.GetOwnerDetailAsync(epoch, owner, ct));
        if (result == null) return NotFound($"Owner '{owner}' not found for epoch {epoch}");
        return Ok(result);
    }

    /// <summary>
    /// Simulate computor revenue with custom tick cutoffs per score category.
    /// Recalculates revenue on-the-fly using data up to the specified tick for each category.
    /// </summary>
    [HttpGet("computor-revenue/{epoch:int}/simulate")]
    public async Task<IActionResult> SimulateComputorRevenue(
        uint epoch,
        [FromQuery] ulong? txTick = null,
        [FromQuery] ulong? voteTick = null,
        [FromQuery] ulong? miningTick = null,
        CancellationToken ct = default)
    {
        var result = await _queryService.SimulateComputorRevenueAsync(epoch, txTick, voteTick, miningTick, ct);
        if (result == null) return NotFound("Could not calculate revenue — computor list unavailable for this epoch");
        return Ok(result);
    }

    // =====================================================
    // TICK VOTES
    // =====================================================

    [HttpGet("tick-votes/{epoch:int}")]
    public async Task<IActionResult> GetTickVotes(
        uint epoch,
        [FromQuery] int? computorIndex = null,
        CancellationToken ct = default)
    {
        var result = await _cache.GetOrSetAsync(
            $"stats:tick-votes:{epoch}:{computorIndex}",
            AnalyticsCacheService.SnapshotHistoryTtl,
            () => _queryService.GetTickVotesAsync(epoch, computorIndex, ct));
        return Ok(result);
    }

    // =====================================================
    // EXECUTION FEE REPORTS
    // =====================================================

    // =====================================================
    // ORACLE REVENUE ANALYTICS
    // =====================================================

    [HttpGet("oracle/{epoch:int}")]
    public async Task<IActionResult> GetOracleEpochSummary(uint epoch, CancellationToken ct = default)
    {
        var ttl = await GetOracleTtlAsync(epoch, ct);
        var result = await _cache.GetOrSetAsync(
            $"stats:oracle:{epoch}",
            ttl,
            () => _queryService.GetOracleEpochSummaryAsync(epoch, ct));
        return Ok(result);
    }

    /// <summary>
    /// Forgery leaderboard. A forgery is a commit that matched the eventual
    /// reveal's digest but whose knowledge_proof can't be reproduced from the
    /// revealed reply — i.e. the committor copied the digest from a peer
    /// without knowing the actual reply data. The protocol drops these
    /// (no revenue point) but they remain on-chain as evidence.
    /// </summary>
    [HttpGet("oracle/forgeries")]
    public async Task<IActionResult> GetOracleForgeryLeaderboard(
        [FromQuery] uint epochFrom,
        [FromQuery] uint epochTo,
        CancellationToken ct = default)
    {
        if (epochFrom == 0 || epochTo == 0)
            return BadRequest("epochFrom and epochTo are required");

        var result = await _cache.GetOrSetAsync(
            $"stats:oracle:forgeries:{epochFrom}:{epochTo}",
            TimeSpan.FromMinutes(2),
            () => _queryService.GetOracleForgeryLeaderboardAsync(epochFrom, epochTo, ct));
        return Ok(result);
    }

    [HttpGet("oracle/forgeries/details")]
    public async Task<IActionResult> GetOracleForgeryDetails(
        [FromQuery] uint epochFrom,
        [FromQuery] uint epochTo,
        [FromQuery] ushort? computorIndex = null,
        [FromQuery] int limit = 200,
        CancellationToken ct = default)
    {
        if (epochFrom == 0 || epochTo == 0)
            return BadRequest("epochFrom and epochTo are required");

        var result = await _queryService.GetOracleForgeryDetailsAsync(
            epochFrom, epochTo, computorIndex, limit, ct);
        return Ok(result);
    }

    [HttpGet("oracle/{epoch:int}/queries")]
    public async Task<IActionResult> GetOracleQueryList(
        uint epoch,
        [FromQuery] int limit = 50,
        [FromQuery] int offset = 0,
        CancellationToken ct = default)
    {
        if (limit < 1 || limit > 500) limit = 50;
        if (offset < 0) offset = 0;

        var ttl = await GetOracleTtlAsync(epoch, ct);
        var result = await _cache.GetOrSetAsync(
            $"stats:oracle:{epoch}:queries:{limit}:{offset}",
            ttl,
            () => _queryService.GetOracleQueryListAsync(epoch, limit, offset, ct));
        return Ok(result);
    }

    [HttpGet("oracle/{epoch:int}/query/{queryId:long}")]
    public async Task<IActionResult> GetOracleQueryDetail(
        uint epoch, long queryId, CancellationToken ct = default)
    {
        if (queryId < 0) return BadRequest("queryId must be non-negative");

        var ttl = await GetOracleTtlAsync(epoch, ct);
        var result = await _cache.GetOrSetAsync(
            $"stats:oracle:{epoch}:query:{queryId}",
            ttl,
            () => _queryService.GetOracleQueryDetailAsync(epoch, (ulong)queryId, ct));
        if (result == null) return NotFound();
        return Ok(result);
    }

    [HttpGet("oracle/{epoch:int}/computor/{computorIndex:int}")]
    public async Task<IActionResult> GetOracleComputorProfile(
        uint epoch, int computorIndex,
        [FromQuery] int limit = 100, [FromQuery] int offset = 0,
        CancellationToken ct = default)
    {
        if (computorIndex < 0 || computorIndex >= 676)
            return BadRequest("computorIndex out of range");
        if (limit < 1 || limit > 1000) return BadRequest("limit must be 1..1000");
        if (offset < 0) return BadRequest("offset must be >= 0");

        var ttl = await GetOracleTtlAsync(epoch, ct);
        var result = await _cache.GetOrSetAsync(
            $"stats:oracle:{epoch}:computor:{computorIndex}:{limit}:{offset}",
            ttl,
            () => _queryService.GetOracleComputorProfileAsync(epoch, computorIndex, limit, offset, ct));
        if (result == null) return NotFound();
        return Ok(result);
    }

    /// <summary>
    /// 10 minutes for the current epoch (events still arriving),
    /// 24 hours for completed epochs (immutable).
    /// </summary>
    private async Task<TimeSpan> GetOracleTtlAsync(uint epoch, CancellationToken ct)
    {
        var currentEpoch = await _queryService.GetCurrentEpochAsync(ct);
        return currentEpoch.HasValue && epoch >= currentEpoch.Value
            ? TimeSpan.FromMinutes(10)
            : TimeSpan.FromHours(24);
    }

    [HttpGet("execution-fees/{epoch:int}")]
    public async Task<IActionResult> GetExecutionFeeSummary(uint epoch, CancellationToken ct = default)
    {
        var ttl = await GetExecutionFeesTtlAsync(epoch, ct);
        var result = await _cache.GetOrSetAsync(
            $"stats:exec-fees:{epoch}",
            ttl,
            () => _queryService.GetExecutionFeeSummaryAsync(epoch, ct));
        return Ok(result);
    }

    [HttpGet("execution-fees/{epoch:int}/contract/{contractIndex:int}")]
    public async Task<IActionResult> GetExecutionFeeContract(
        uint epoch, int contractIndex, CancellationToken ct = default)
    {
        if (contractIndex < 0 || contractIndex > ushort.MaxValue)
            return BadRequest("contractIndex out of range");
        var ttl = await GetExecutionFeesTtlAsync(epoch, ct);
        var result = await _cache.GetOrSetAsync(
            $"stats:exec-fees:{epoch}:contract:{contractIndex}",
            ttl,
            () => _queryService.GetExecutionFeeContractAsync(epoch, (ushort)contractIndex, ct));
        return Ok(result);
    }

    [HttpGet("execution-fees/{epoch:int}/phase/{phaseNumber:int}")]
    public async Task<IActionResult> GetExecutionFeePhase(
        uint epoch, uint phaseNumber, CancellationToken ct = default)
    {
        var ttl = await GetExecutionFeesTtlAsync(epoch, ct);
        var result = await _cache.GetOrSetAsync(
            $"stats:exec-fees:{epoch}:phase:{phaseNumber}",
            ttl,
            () => _queryService.GetExecutionFeePhaseAsync(epoch, phaseNumber, ct));
        return Ok(result);
    }

    /// <summary>
    /// 10 minutes for the current epoch (new phases land every 7-15 min),
    /// 24 hours for completed epochs (data is immutable).
    /// </summary>
    private async Task<TimeSpan> GetExecutionFeesTtlAsync(uint epoch, CancellationToken ct)
    {
        var currentEpoch = await _queryService.GetCurrentEpochAsync(ct);
        return currentEpoch.HasValue && epoch >= currentEpoch.Value
            ? AnalyticsCacheService.ExecutionFeesCurrentEpochTtl
            : AnalyticsCacheService.ExecutionFeesCompletedEpochTtl;
    }

    [HttpGet("tick-votes/{epoch:int}/compare")]
    public async Task<IActionResult> GetTickVotesCompare(
        uint epoch,
        [FromQuery] string indices,
        CancellationToken ct = default)
    {
        if (string.IsNullOrEmpty(indices))
            return BadRequest("indices parameter required (comma-separated computor indices)");

        var indexList = indices.Split(',')
            .Select(s => int.TryParse(s.Trim(), out var v) ? v : -1)
            .Where(v => v >= 0 && v < 676)
            .ToList();

        if (indexList.Count == 0)
            return BadRequest("No valid computor indices provided");
        var cacheKey = $"stats:tick-votes-compare:{epoch}:{string.Join(",", indexList.OrderBy(x => x))}";
        var result = await _cache.GetOrSetAsync(
            cacheKey,
            AnalyticsCacheService.SnapshotHistoryTtl,
            () => _queryService.GetTickVotesCompareAsync(epoch, indexList, ct));
        return Ok(result);
    }

    // =====================================================
    // PROPOSALS (GQMPROP + CCF)
    // =====================================================

    /// <summary>Epochs (desc) with proposal activity — used to populate the epoch selector.</summary>
    [HttpGet("proposals/epochs")]
    public async Task<IActionResult> GetProposalsEpochs(CancellationToken ct = default)
    {
        var result = await _cache.GetOrSetAsync(
            "stats:proposals:epochs",
            TimeSpan.FromMinutes(5),
            async () => new ProposalsEpochsDto(await _queryService.GetProposalEpochsAsync(ct)));
        return Ok(result);
    }

    /// <summary>List of proposals in one epoch. contract=0 both; 6 GQMPROP; 8 CCF.</summary>
    [HttpGet("proposals/{epoch:int}")]
    public async Task<IActionResult> GetProposalsForEpoch(
        uint epoch, [FromQuery] int contract = 0, CancellationToken ct = default)
    {
        if (contract != 0 && contract != 6 && contract != 8)
            return BadRequest("contract must be 0 (both), 6 (GQMPROP), or 8 (CCF)");
        var ttl = await GetProposalsTtlAsync(epoch, ct);
        var result = await _cache.GetOrSetAsync(
            $"stats:proposals:{epoch}:c{contract}",
            ttl,
            () => _queryService.GetProposalsForEpochAsync(epoch, contract, ct));
        return Ok(result);
    }

    /// <summary>Full detail for one proposal, keyed by (epoch, contract, proposalTick).</summary>
    [HttpGet("proposals/{epoch:int}/{contract:int}/{proposalTick:long}")]
    public async Task<IActionResult> GetProposalDetail(
        uint epoch, int contract, ulong proposalTick, CancellationToken ct = default)
    {
        if (contract != 6 && contract != 8)
            return BadRequest("contract must be 6 (GQMPROP) or 8 (CCF)");
        var ttl = await GetProposalsTtlAsync(epoch, ct);
        var result = await _cache.GetOrSetAsync(
            $"stats:proposals:{epoch}:c{contract}:t{proposalTick}",
            ttl,
            () => _queryService.GetProposalDetailAsync(epoch, contract, proposalTick, ct));
        if (result == null) return NotFound();
        return Ok(result);
    }

    /// <summary>1 min for current epoch (votes still flowing), 24 h for closed epochs.</summary>
    private async Task<TimeSpan> GetProposalsTtlAsync(uint epoch, CancellationToken ct)
    {
        var currentEpoch = await _queryService.GetCurrentEpochAsync(ct);
        return currentEpoch.HasValue && epoch >= currentEpoch.Value
            ? TimeSpan.FromMinutes(1)
            : TimeSpan.FromHours(24);
    }
}
