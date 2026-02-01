using Microsoft.AspNetCore.Mvc;
using QubicExplorer.Api.Attributes;
using QubicExplorer.Api.Services;

namespace QubicExplorer.Api.Controllers;

/// <summary>
/// Controller for miner/computor flow tracking and analysis.
/// Tracks money flow from computors (who receive epoch emission) through multiple hops
/// to identify flow to exchanges and other destinations.
/// </summary>
[ApiController]
[Route("api/miner-flow")]
public class MinerFlowController : ControllerBase
{
    private readonly ComputorFlowService _flowService;
    private readonly ClickHouseQueryService _queryService;
    private readonly ILogger<MinerFlowController> _logger;

    public MinerFlowController(
        ComputorFlowService flowService,
        ClickHouseQueryService queryService,
        ILogger<MinerFlowController> logger)
    {
        _flowService = flowService;
        _queryService = queryService;
        _logger = logger;
    }

    /// <summary>
    /// Gets the list of computors for a specific epoch.
    /// Fetches from RPC and caches if not already imported.
    /// </summary>
    [HttpGet("computors/{epoch}")]
    public async Task<IActionResult> GetComputors(uint epoch, CancellationToken ct = default)
    {
        var result = await _flowService.GetComputorsAsync(epoch, ct);
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
        CancellationToken ct = default)
    {
        if (limit < 1) limit = 1;
        if (limit > 100) limit = 100;

        var result = await _flowService.GetMinerFlowHistoryAsync(limit, ct);
        return Ok(result);
    }

    /// <summary>
    /// Gets flow visualization data for Sankey diagram.
    /// The emissionEpoch parameter specifies which emission to track (e.g., EP195 = computors from epoch 195).
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

        // Get all flow hops for this emission epoch (across all tick windows)
        var result = await _flowService.GetFlowVisualizationByEmissionEpochAsync(emissionEpoch, maxDepth, ct);
        if (result == null)
        {
            return NotFound(new { error = $"No flow visualization data available for emission epoch {emissionEpoch}" });
        }
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

        // If no tick range provided, get the latest window for this epoch
        if (tickStart == 0 || tickEnd == 0)
        {
            var latestStats = await _queryService.GetMinerFlowStatsHistoryAsync(1, ct);
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

        var hops = await _queryService.GetFlowHopsAsync(epoch, tickStart, tickEnd, maxDepth, ct);
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

    /// <summary>
    /// Triggers flow analysis for a specific epoch/window.
    /// Admin only - normally triggered by the scheduled snapshot service.
    /// </summary>
    [HttpPost("analyze/{currentEpoch}")]
    [AdminApiKey]
    public async Task<IActionResult> AnalyzeFlow(
        uint currentEpoch,
        [FromQuery] uint? emissionEpoch = null,
        [FromQuery] ulong tickStart = 0,
        [FromQuery] ulong tickEnd = 0,
        CancellationToken ct = default)
    {
        // Emission epoch defaults to current-1 (computors from previous epoch receive rewards in current)
        var effectiveEmissionEpoch = emissionEpoch ?? (currentEpoch > 0 ? currentEpoch - 1 : 0);

        if (tickStart == 0 || tickEnd == 0)
        {
            return BadRequest(new { error = "tickStart and tickEnd are required" });
        }

        _logger.LogInformation(
            "Manual flow analysis triggered for epoch {Epoch} (emission from {EmissionEpoch}), ticks {TickStart}-{TickEnd}",
            currentEpoch, effectiveEmissionEpoch, tickStart, tickEnd);

        var result = await _flowService.AnalyzeFlowForWindowAsync(
            currentEpoch, effectiveEmissionEpoch, tickStart, tickEnd, ct);

        if (result == null)
        {
            return BadRequest(new { error = "Flow analysis failed - check logs for details" });
        }

        return Ok(result);
    }

    /// <summary>
    /// Triggers flow analysis for a specific emission epoch.
    /// Admin only - this analyzes flow from emissionEpoch+1 initial tick to the latest tick available.
    ///
    /// The emission happens at the END of emissionEpoch, so we track transfers starting from
    /// the beginning of emissionEpoch+1 onwards (when computors can start moving their rewards).
    /// </summary>
    [HttpPost("analyze-emission/{emissionEpoch}")]
    [AdminApiKey]
    public async Task<IActionResult> AnalyzeEmissionFlow(
        uint emissionEpoch,
        [FromQuery] int batchSize = 10000,
        CancellationToken ct = default)
    {
        // Get the starting epoch (emissionEpoch + 1) tick range
        var startEpoch = emissionEpoch + 1;
        var startTickRange = await _queryService.GetTickRangeForEpochAsync(startEpoch, ct);
        if (!startTickRange.HasValue)
        {
            return NotFound(new { error = $"No ticks found for epoch {startEpoch} (emission epoch + 1)" });
        }

        var tickStart = startTickRange.Value.MinTick;

        // Get the latest tick in the database (across all epochs)
        var networkStats = await _queryService.GetNetworkStatsAsync(ct);
        var tickEnd = networkStats?.LatestTick ?? startTickRange.Value.MaxTick;

        // Get the current epoch for the tick end
        var currentEpoch = networkStats?.CurrentEpoch ?? startEpoch;

        _logger.LogInformation(
            "Full emission flow analysis triggered for emission epoch {EmissionEpoch}, " +
            "tracking from tick {TickStart} (epoch {StartEpoch}) to tick {TickEnd} (epoch {CurrentEpoch})",
            emissionEpoch, tickStart, startEpoch, tickEnd, currentEpoch);

        // Process in batches to avoid timeouts and memory issues
        var batchCount = 0;
        var currentTick = tickStart;
        var processedEpochs = new HashSet<uint>();

        while (currentTick <= tickEnd)
        {
            var batchEnd = Math.Min(currentTick + (ulong)batchSize - 1, tickEnd);
            batchCount++;

            // Determine which epoch this batch belongs to (for logging)
            // We use the emissionEpoch for all batches since we're tracking that emission
            var batchEpoch = startEpoch; // Could be refined to track actual epoch per tick
            processedEpochs.Add(batchEpoch);

            _logger.LogInformation(
                "Processing batch {Batch}: ticks {Start}-{End}",
                batchCount, currentTick, batchEnd);

            // Always use startEpoch as the "current epoch" parameter since we're tracking
            // a specific emission epoch's flow
            await _flowService.AnalyzeFlowForWindowAsync(
                startEpoch, emissionEpoch, currentTick, batchEnd, ct);

            currentTick = batchEnd + 1;
        }

        // Get final flow visualization stats
        var flowViz = await _flowService.GetFlowVisualizationByEmissionEpochAsync(emissionEpoch, 10, ct);

        return Ok(new
        {
            success = true,
            emissionEpoch,
            tickStart,
            tickEnd,
            startEpoch,
            currentEpoch,
            batchCount,
            batchSize,
            totalTicks = tickEnd - tickStart + 1,
            nodesTracked = flowViz?.Nodes.Count ?? 0,
            linksTracked = flowViz?.Links.Count ?? 0
        });
    }

    /// <summary>
    /// Validates flow conservation for an emission epoch.
    /// Checks data integrity: computor emissions match, no negative pending amounts,
    /// flow conservation across hop levels.
    /// </summary>
    [HttpGet("validate/{emissionEpoch}")]
    public async Task<IActionResult> ValidateFlowConservation(uint emissionEpoch, CancellationToken ct = default)
    {
        var result = await _flowService.ValidateFlowConservationAsync(emissionEpoch, ct);
        return Ok(result);
    }

    /// <summary>
    /// Imports computors for a specific epoch from RPC.
    /// Admin only - normally done automatically during analysis.
    /// </summary>
    [HttpPost("import-computors/{epoch}")]
    [AdminApiKey]
    public async Task<IActionResult> ImportComputors(uint epoch, CancellationToken ct = default)
    {
        var success = await _flowService.EnsureComputorsImportedAsync(epoch, ct);
        if (!success)
        {
            return BadRequest(new { error = $"Failed to import computors for epoch {epoch}" });
        }

        var computors = await _flowService.GetComputorsAsync(epoch, ct);
        return Ok(new
        {
            success = true,
            epoch,
            count = computors?.Count ?? 0
        });
    }

    // =====================================================
    // EMISSIONS ENDPOINTS
    // =====================================================

    /// <summary>
    /// Manually captures emissions for a specific epoch.
    /// Admin only - use this to backfill emissions for historical epochs.
    /// </summary>
    [HttpPost("emissions/{epoch}/capture")]
    [AdminApiKey]
    public async Task<IActionResult> CaptureEmissions(uint epoch, CancellationToken ct = default)
    {
        // Check if already captured
        if (await _queryService.IsEmissionImportedAsync(epoch, ct))
        {
            var existing = await _queryService.GetEmissionSummaryAsync(epoch, ct);
            return Ok(new
            {
                success = true,
                message = "Emissions already captured",
                epoch,
                computorCount = existing?.ComputorCount ?? 0,
                totalEmission = existing?.TotalEmission ?? 0
            });
        }

        // Get epoch metadata to find the end tick
        var epochMeta = await _queryService.GetEpochMetaAsync(epoch, ct);
        if (epochMeta == null || epochMeta.EndTick == 0)
        {
            return BadRequest(new { error = $"Epoch {epoch} metadata not found or incomplete. EndTick is required." });
        }

        // Ensure computors are imported
        if (!await _flowService.EnsureComputorsImportedAsync(epoch, ct))
        {
            return BadRequest(new { error = $"Failed to import computors for epoch {epoch}" });
        }

        // Get computor addresses
        var computorList = await _queryService.GetComputorsAsync(epoch, ct);
        if (computorList == null || computorList.Computors.Count == 0)
        {
            return BadRequest(new { error = $"No computors found for epoch {epoch}" });
        }

        var computorAddresses = computorList.Computors.Select(c => c.Address).ToHashSet();
        var addressToIndex = computorList.Computors.ToDictionary(c => c.Address, c => (int)c.Index);

        // Capture emissions
        var (count, total) = await _queryService.CaptureEmissionsForEpochAsync(
            epoch, epochMeta.EndTick, computorAddresses, addressToIndex, ct);

        return Ok(new
        {
            success = count > 0,
            epoch,
            endTick = epochMeta.EndTick,
            computorCount = count,
            totalEmission = total
        });
    }

    /// <summary>
    /// Gets emission summary for an epoch.
    /// Returns total emission and computor count.
    /// </summary>
    [HttpGet("emissions/{epoch}")]
    public async Task<IActionResult> GetEmissions(uint epoch, CancellationToken ct = default)
    {
        var summary = await _queryService.GetEmissionSummaryAsync(epoch, ct);
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
        var summary = await _queryService.GetEmissionSummaryAsync(epoch, ct);
        if (summary == null)
        {
            return NotFound(new { error = $"No emission data available for epoch {epoch}" });
        }

        var emissions = await _queryService.GetEmissionsForEpochAsync(epoch, ct);
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
    /// Recalculates all miner_flow_stats snapshots with correct emission values from emission_imports.
    /// Admin only - use this after backfilling emissions to fix historical snapshots.
    /// </summary>
    [HttpPost("recalculate-emissions")]
    [AdminApiKey]
    public async Task<IActionResult> RecalculateEmissions(CancellationToken ct = default)
    {
        _logger.LogInformation("Manual recalculation of miner flow stats emissions triggered");

        var updatedCount = await _queryService.RecalculateMinerFlowStatsEmissionsAsync(ct);

        _logger.LogInformation("Recalculated {Count} miner flow stats snapshots", updatedCount);

        return Ok(new
        {
            success = true,
            message = $"Recalculated {updatedCount} snapshots with correct emission values",
            snapshotsUpdated = updatedCount
        });
    }
}
