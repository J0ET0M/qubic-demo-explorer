using Microsoft.AspNetCore.Mvc;
using QubicExplorer.Api.Services;
using QubicExplorer.Shared.DTOs;

namespace QubicExplorer.Api.Controllers;

/// <summary>
/// Read-only controller for miner/computor flow data.
/// Admin/write operations are in the Analytics service's AdminController.
/// </summary>
[ApiController]
[Route("api/miner-flow")]
public class MinerFlowController : ControllerBase
{
    private readonly ClickHouseQueryService _queryService;
    private readonly AnalyticsCacheService _cache;

    public MinerFlowController(ClickHouseQueryService queryService, AnalyticsCacheService cache)
    {
        _queryService = queryService;
        _cache = cache;
    }

    /// <summary>
    /// Gets the list of computors for a specific epoch.
    /// </summary>
    [HttpGet("computors/{epoch}")]
    public async Task<IActionResult> GetComputors(uint epoch, CancellationToken ct = default)
    {
        var result = await _cache.GetOrSetAsync(
            $"miner:computors:{epoch}",
            AnalyticsCacheService.ComputorsTtl,
            () => _queryService.GetComputorsAsync(epoch, ct));
        if (result == null)
        {
            return NotFound(new { error = $"Computors not available for epoch {epoch}" });
        }
        return Ok(result);
    }

    /// <summary>
    /// Gets the historical miner flow statistics summary.
    /// Returns the latest snapshot and history for charting.
    /// </summary>
    [HttpGet("stats")]
    public async Task<IActionResult> GetMinerFlowStats(
        [FromQuery] int limit = 30,
        [FromQuery] DateTime? from = null,
        [FromQuery] DateTime? to = null,
        CancellationToken ct = default)
    {
        if (limit < 1) limit = 1;
        if (limit > 500) limit = 500;

        var result = await _cache.GetOrSetAsync(
            $"miner:stats:{limit}:{from?.Ticks ?? 0}:{to?.Ticks ?? 0}",
            AnalyticsCacheService.MinerFlowStatsTtl,
            async () =>
            {
                var history = await _queryService.GetMinerFlowStatsHistoryAsync(limit, from, to, ct);
                var latest = history.FirstOrDefault();

                var uniqueEmissionEpochs = history.Select(s => s.EmissionEpoch).Distinct().ToList();
                var totalEmission = await _queryService.GetTotalEmissionsForEpochsAsync(uniqueEmissionEpochs, ct);

                var totalToExchange = history.Sum(s => s.FlowToExchangeTotal);
                var avgPercent = totalEmission > 0 ? (totalToExchange / totalEmission) * 100 : 0;

                return new MinerFlowSummaryDto(
                    Latest: latest,
                    History: history,
                    TotalEmissionTracked: totalEmission,
                    TotalFlowToExchange: totalToExchange,
                    AverageExchangeFlowPercent: avgPercent
                );
            });
        return Ok(result);
    }

    /// <summary>
    /// Gets flow visualization data for Sankey diagram.
    /// The emissionEpoch parameter specifies which emission to track.
    /// Returns all flow hops for that emission across all tracked tick windows.
    /// </summary>
    [HttpGet("visualization/{emissionEpoch}")]
    public async Task<IActionResult> GetFlowVisualization(
        uint emissionEpoch,
        [FromQuery] int maxDepth = 10,
        CancellationToken ct = default)
    {
        if (maxDepth < 1) maxDepth = 1;
        if (maxDepth > 10) maxDepth = 10;

        var result = await _cache.GetOrSetAsync(
            $"miner:viz:{emissionEpoch}:{maxDepth}",
            AnalyticsCacheService.MinerFlowVisualizationTtl,
            async () =>
            {
                var hops = await _queryService.GetFlowHopsByEmissionEpochAsync(emissionEpoch, maxDepth, ct);
                if (hops.Count == 0)
                    return (FlowVisualizationDto?)null;

                return BuildFlowVisualization(emissionEpoch, hops);
            });

        if (result == null)
            return NotFound(new { error = $"No flow visualization data available for emission epoch {emissionEpoch}" });

        return Ok(result);
    }

    /// <summary>
    /// Gets raw flow hops for detailed analysis.
    /// </summary>
    [HttpGet("hops/{epoch}")]
    public async Task<IActionResult> GetFlowHops(
        uint epoch,
        [FromQuery] ulong tickStart = 0,
        [FromQuery] ulong tickEnd = 0,
        [FromQuery] int maxDepth = 5,
        [FromQuery] int limit = 1000,
        CancellationToken ct = default)
    {
        if (maxDepth < 1) maxDepth = 1;
        if (maxDepth > 10) maxDepth = 10;
        if (limit < 1) limit = 1;
        if (limit > 10000) limit = 10000;

        if (tickStart == 0 || tickEnd == 0)
        {
            var latestStats = await _queryService.GetMinerFlowStatsHistoryAsync(1, ct: ct);
            var epochStats = latestStats.FirstOrDefault(s => s.Epoch == epoch);
            if (epochStats != null)
            {
                tickStart = epochStats.TickStart;
                tickEnd = epochStats.TickEnd;
            }
            else
            {
                return NotFound(new { error = $"No flow data available for epoch {epoch}" });
            }
        }

        var hops = await _cache.GetOrSetAsync(
            $"miner:hops:{epoch}:{tickStart}:{tickEnd}:{maxDepth}",
            AnalyticsCacheService.MinerFlowStatsTtl,
            () => _queryService.GetFlowHopsAsync(epoch, tickStart, tickEnd, maxDepth, ct));

        return Ok(new
        {
            epoch,
            tickStart,
            tickEnd,
            maxDepth,
            totalHops = hops.Count,
            hops = hops.Take(limit).ToList()
        });
    }

    // =====================================================
    // EMISSIONS ENDPOINTS (read-only)
    // =====================================================

    /// <summary>
    /// Gets emission summary for an epoch.
    /// </summary>
    [HttpGet("emissions/{epoch}")]
    public async Task<IActionResult> GetEmissions(uint epoch, CancellationToken ct = default)
    {
        var summary = await _cache.GetOrSetAsync(
            $"miner:emissions:{epoch}",
            AnalyticsCacheService.EmissionsTtl,
            () => _queryService.GetEmissionSummaryAsync(epoch, ct));
        if (summary == null)
        {
            return NotFound(new { error = $"No emission data available for epoch {epoch}" });
        }
        return Ok(summary);
    }

    /// <summary>
    /// Gets detailed emissions for all computors in an epoch.
    /// </summary>
    [HttpGet("emissions/{epoch}/details")]
    public async Task<IActionResult> GetEmissionDetails(uint epoch, CancellationToken ct = default)
    {
        var summary = await _cache.GetOrSetAsync(
            $"miner:emissions:{epoch}",
            AnalyticsCacheService.EmissionsTtl,
            () => _queryService.GetEmissionSummaryAsync(epoch, ct));
        if (summary == null)
        {
            return NotFound(new { error = $"No emission data available for epoch {epoch}" });
        }

        var emissions = await _cache.GetOrSetAsync(
            $"miner:emission-details:{epoch}",
            AnalyticsCacheService.EmissionsTtl,
            () => _queryService.GetEmissionsForEpochAsync(epoch, ct));

        return Ok(new
        {
            epoch,
            computorCount = summary.ComputorCount,
            totalEmission = summary.TotalEmission,
            emissionTick = summary.EmissionTick,
            importedAt = summary.ImportedAt,
            emissions
        });
    }

    /// <summary>
    /// Gets emission for a specific computor address in an epoch.
    /// </summary>
    [HttpGet("emissions/{epoch}/address/{address}")]
    public async Task<IActionResult> GetComputorEmission(uint epoch, string address, CancellationToken ct = default)
    {
        var emission = await _queryService.GetComputorEmissionAsync(epoch, address, ct);
        if (emission == 0)
        {
            return NotFound(new { error = $"No emission found for address {address} in epoch {epoch}" });
        }

        return Ok(new
        {
            epoch,
            address,
            emission
        });
    }

    /// <summary>
    /// Builds flow visualization (Sankey diagram data) from raw flow hops.
    /// </summary>
    private static FlowVisualizationDto BuildFlowVisualization(uint emissionEpoch, List<FlowHopDto> hops)
    {
        var nodeMinDepth = new Dictionary<string, int>();
        var nodeTypes = new Dictionary<string, string>();
        var nodeLabels = new Dictionary<string, string?>();

        // Identify computors (sources at hop level 1)
        foreach (var hop in hops.Where(h => h.HopLevel == 1))
        {
            nodeMinDepth[hop.SourceAddress] = 0;
            nodeTypes[hop.SourceAddress] = "computor";
            if (!string.IsNullOrEmpty(hop.SourceLabel))
                nodeLabels[hop.SourceAddress] = hop.SourceLabel;
        }

        // Determine depths for all nodes
        foreach (var hop in hops.OrderBy(h => h.HopLevel))
        {
            if (!nodeMinDepth.ContainsKey(hop.SourceAddress))
            {
                nodeMinDepth[hop.SourceAddress] = hop.HopLevel - 1;
                nodeTypes[hop.SourceAddress] = "intermediary";
            }
            if (!string.IsNullOrEmpty(hop.SourceLabel) && !nodeLabels.ContainsKey(hop.SourceAddress))
                nodeLabels[hop.SourceAddress] = hop.SourceLabel;

            if (!nodeMinDepth.ContainsKey(hop.DestAddress))
            {
                nodeMinDepth[hop.DestAddress] = hop.HopLevel;
                nodeTypes[hop.DestAddress] = hop.DestType ?? "unknown";
            }
            else if (hop.HopLevel < nodeMinDepth[hop.DestAddress])
            {
                nodeMinDepth[hop.DestAddress] = hop.HopLevel;
            }
            if (!string.IsNullOrEmpty(hop.DestLabel) && !nodeLabels.ContainsKey(hop.DestAddress))
                nodeLabels[hop.DestAddress] = hop.DestLabel;
        }

        // Build nodes
        var nodes = new Dictionary<string, FlowVisualizationNodeDto>();
        foreach (var (address, depth) in nodeMinDepth)
        {
            nodes[address] = new FlowVisualizationNodeDto(
                Id: address,
                Address: address,
                Label: nodeLabels.GetValueOrDefault(address),
                Type: nodeTypes.GetValueOrDefault(address, "unknown"),
                TotalInflow: 0,
                TotalOutflow: 0,
                Depth: depth
            );
        }

        // Process hops to update flows and links
        var links = new Dictionary<(string, string), (decimal Amount, uint Count)>();
        foreach (var hop in hops)
        {
            var sourceNode = nodes[hop.SourceAddress];
            nodes[hop.SourceAddress] = sourceNode with { TotalOutflow = sourceNode.TotalOutflow + hop.Amount };

            var destNode = nodes[hop.DestAddress];
            nodes[hop.DestAddress] = destNode with { TotalInflow = destNode.TotalInflow + hop.Amount };

            var linkKey = (hop.SourceAddress, hop.DestAddress);
            if (links.TryGetValue(linkKey, out var existing))
                links[linkKey] = (existing.Amount + hop.Amount, existing.Count + 1);
            else
                links[linkKey] = (hop.Amount, 1);
        }

        var vizLinks = links.Select(kvp => new FlowVisualizationLinkDto(
            SourceId: kvp.Key.Item1,
            TargetId: kvp.Key.Item2,
            Amount: kvp.Value.Amount,
            TransactionCount: kvp.Value.Count
        )).ToList();

        return new FlowVisualizationDto(
            Epoch: emissionEpoch,
            TickStart: hops.Min(h => h.TickNumber),
            TickEnd: hops.Max(h => h.TickNumber),
            Nodes: nodes.Values.ToList(),
            Links: vizLinks,
            MaxDepth: hops.Max(h => h.HopLevel),
            TotalTrackedVolume: hops.Where(h => h.HopLevel == 1).Sum(h => h.Amount)
        );
    }
}
