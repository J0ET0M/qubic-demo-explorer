using Microsoft.AspNetCore.Mvc;
using QubicExplorer.Analytics.Attributes;
using QubicExplorer.Analytics.Services;

namespace QubicExplorer.Analytics.Controllers;

/// <summary>
/// Admin controller for analytics management operations.
/// All endpoints require an admin API key.
/// </summary>
[ApiController]
[Route("api/admin")]
[AdminApiKey]
public class AdminController : ControllerBase
{
    private readonly ComputorFlowService _flowService;
    private readonly AnalyticsQueryService _queryService;
    private readonly ILogger<AdminController> _logger;

    public AdminController(
        ComputorFlowService flowService,
        AnalyticsQueryService queryService,
        ILogger<AdminController> logger)
    {
        _flowService = flowService;
        _queryService = queryService;
        _logger = logger;
    }

    // =====================================================
    // NETWORK STATS ADMIN
    // =====================================================

    /// <summary>
    /// Manually creates a network stats snapshot for a specific epoch/tick range.
    /// </summary>
    [HttpPost("network-stats/snapshot/{epoch}")]
    public async Task<IActionResult> SaveNetworkStatsSnapshot(
        uint epoch,
        [FromQuery] ulong tickStart = 0,
        [FromQuery] ulong tickEnd = 0,
        CancellationToken ct = default)
    {
        await _queryService.SaveNetworkStatsSnapshotAsync(epoch, tickStart, tickEnd, ct);
        return Ok(new { success = true, epoch, tickStart, tickEnd });
    }

    /// <summary>
    /// Recalculates avg_tx_size, median_tx_size, and exchange flow columns
    /// for all network_stats_history snapshots (excludes zero-amount transfers).
    /// </summary>
    [HttpPost("network-stats/recalculate")]
    public async Task<IActionResult> RecalculateNetworkStats(CancellationToken ct = default)
    {
        _logger.LogInformation("Manual recalculation of network stats triggered");

        var updatedCount = await _queryService.RecalculateNetworkStatsAsync(ct);

        return Ok(new
        {
            success = true,
            message = $"Recalculated {updatedCount} snapshots with corrected avg/median tx size and exchange flows",
            snapshotsUpdated = updatedCount
        });
    }

    // =====================================================
    // MINER FLOW ADMIN
    // =====================================================

    /// <summary>
    /// Triggers flow analysis for a specific epoch/window.
    /// </summary>
    [HttpPost("miner-flow/analyze/{currentEpoch}")]
    public async Task<IActionResult> AnalyzeFlow(
        uint currentEpoch,
        [FromQuery] uint? emissionEpoch = null,
        [FromQuery] ulong tickStart = 0,
        [FromQuery] ulong tickEnd = 0,
        CancellationToken ct = default)
    {
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
    /// Analyzes flow from emissionEpoch+1 initial tick to the latest tick available.
    /// </summary>
    [HttpPost("miner-flow/analyze-emission/{emissionEpoch}")]
    public async Task<IActionResult> AnalyzeEmissionFlow(
        uint emissionEpoch,
        [FromQuery] int batchSize = 10000,
        CancellationToken ct = default)
    {
        var startEpoch = emissionEpoch + 1;
        var startTickRange = await _queryService.GetTickRangeForEpochAsync(startEpoch, ct);
        if (!startTickRange.HasValue)
        {
            return NotFound(new { error = $"No ticks found for epoch {startEpoch} (emission epoch + 1)" });
        }

        var tickStart = startTickRange.Value.MinTick;
        var networkStats = await _queryService.GetNetworkStatsAsync(ct);
        var tickEnd = networkStats?.LatestTick ?? startTickRange.Value.MaxTick;
        var currentEpoch = networkStats?.CurrentEpoch ?? startEpoch;

        _logger.LogInformation(
            "Full emission flow analysis triggered for emission epoch {EmissionEpoch}, " +
            "tracking from tick {TickStart} (epoch {StartEpoch}) to tick {TickEnd} (epoch {CurrentEpoch})",
            emissionEpoch, tickStart, startEpoch, tickEnd, currentEpoch);

        var batchCount = 0;
        var currentTick = tickStart;

        while (currentTick <= tickEnd)
        {
            var batchEnd = Math.Min(currentTick + (ulong)batchSize - 1, tickEnd);
            batchCount++;

            _logger.LogInformation(
                "Processing batch {Batch}: ticks {Start}-{End}",
                batchCount, currentTick, batchEnd);

            await _flowService.AnalyzeFlowForWindowAsync(
                startEpoch, emissionEpoch, currentTick, batchEnd, ct);

            currentTick = batchEnd + 1;
        }

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

    // =====================================================
    // EMISSIONS ADMIN
    // =====================================================

    /// <summary>
    /// Manually captures emissions for a specific epoch.
    /// </summary>
    [HttpPost("emissions/{epoch}/capture")]
    public async Task<IActionResult> CaptureEmissions(uint epoch, CancellationToken ct = default)
    {
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

        var epochMeta = await _queryService.GetEpochMetaAsync(epoch, ct);
        if (epochMeta == null || epochMeta.EndTick == 0)
        {
            return BadRequest(new { error = $"Epoch {epoch} metadata not found or incomplete. EndTick is required." });
        }

        if (!await _flowService.EnsureComputorsImportedAsync(epoch, ct))
        {
            return BadRequest(new { error = $"Failed to import computors for epoch {epoch}" });
        }

        var computorList = await _queryService.GetComputorsAsync(epoch, ct);
        if (computorList == null || computorList.Computors.Count == 0)
        {
            return BadRequest(new { error = $"No computors found for epoch {epoch}" });
        }

        var computorAddresses = computorList.Computors.Select(c => c.Address).ToHashSet();
        var addressToIndex = computorList.Computors.ToDictionary(c => c.Address, c => (int)c.Index);

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
    /// Imports computors for a specific epoch from RPC.
    /// </summary>
    [HttpPost("import-computors/{epoch}")]
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

    /// <summary>
    /// Recalculates all miner_flow_stats snapshots with correct emission values.
    /// </summary>
    [HttpPost("recalculate-emissions")]
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

    // =====================================================
    // FLOW DATA DELETION
    // =====================================================

    /// <summary>
    /// Deletes flow data (flow_hops, flow_tracking_state) for a specific emission epoch.
    /// Optionally also deletes miner_flow_stats snapshots.
    /// After deletion, you can re-trigger analysis via POST /api/admin/miner-flow/analyze-emission/{emissionEpoch}.
    /// </summary>
    [HttpDelete("miner-flow/{emissionEpoch}")]
    public async Task<IActionResult> DeleteFlowData(
        uint emissionEpoch,
        [FromQuery] bool includeStats = false,
        CancellationToken ct = default)
    {
        _logger.LogWarning("Deleting flow data for emission epoch {Epoch} (includeStats={IncludeStats})",
            emissionEpoch, includeStats);

        var (flowHops, trackingState, minerFlowStats) =
            await _queryService.DeleteFlowDataForEmissionEpochAsync(emissionEpoch, includeStats, ct);

        return Ok(new
        {
            success = true,
            emissionEpoch,
            deleted = new
            {
                flowHops,
                trackingState,
                minerFlowStats
            },
            message = "Deletion mutations submitted. Use GET /api/admin/miner-flow/validate/{emissionEpoch} to verify, " +
                      "then POST /api/admin/miner-flow/analyze-emission/{emissionEpoch} to re-analyze."
        });
    }

    // =====================================================
    // VALIDATION / DIAGNOSTICS
    // =====================================================

    /// <summary>
    /// Validates flow conservation for an emission epoch.
    /// Checks data integrity: computor emissions match, no negative pending amounts,
    /// flow conservation across hop levels.
    /// </summary>
    [HttpGet("miner-flow/validate/{emissionEpoch}")]
    public async Task<IActionResult> ValidateFlowConservation(uint emissionEpoch, CancellationToken ct = default)
    {
        var result = await _flowService.ValidateFlowConservationAsync(emissionEpoch, ct);
        return Ok(result);
    }

    // =====================================================
    // CUSTOM FLOW TRACKING ADMIN
    // =====================================================

    /// <summary>
    /// Deletes a specific custom flow tracking job and all associated data.
    /// </summary>
    [HttpDelete("custom-flow/{jobId}")]
    public async Task<IActionResult> DeleteCustomFlowJob(string jobId, CancellationToken ct = default)
    {
        var job = await _queryService.GetCustomFlowJobAsync(jobId, ct);
        if (job == null)
        {
            return NotFound(new { error = $"Custom flow job {jobId} not found" });
        }

        _logger.LogWarning("Deleting custom flow job {JobId} (alias: {Alias})", jobId, job.Alias);
        await _queryService.DeleteCustomFlowJobAsync(jobId, ct);

        return Ok(new { success = true, jobId, alias = job.Alias });
    }

    /// <summary>
    /// Deletes all custom flow tracking jobs older than the specified number of days.
    /// </summary>
    [HttpDelete("custom-flow/cleanup")]
    public async Task<IActionResult> CleanupOldCustomFlowJobs(
        [FromQuery] int days = 30,
        CancellationToken ct = default)
    {
        _logger.LogWarning("Cleaning up custom flow jobs older than {Days} days", days);
        var count = await _queryService.DeleteOldCustomFlowJobsAsync(days, ct);

        return Ok(new
        {
            success = true,
            deletedCount = count,
            message = $"Deleted {count} custom flow jobs older than {days} days"
        });
    }
}
