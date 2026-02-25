using Microsoft.AspNetCore.Mvc;
using QubicExplorer.Api.Attributes;
using QubicExplorer.Api.Services;

namespace QubicExplorer.Api.Controllers;

[ApiController]
[Route("api/[controller]")]
public class EpochController : ControllerBase
{
    private readonly ClickHouseQueryService _queryService;
    private readonly BobProxyService _bobProxy;
    private readonly ComputorFlowService _flowService;
    private readonly AnalyticsCacheService _cache;
    private readonly ILogger<EpochController> _logger;

    public EpochController(
        ClickHouseQueryService queryService,
        BobProxyService bobProxy,
        ComputorFlowService flowService,
        AnalyticsCacheService cache,
        ILogger<EpochController> logger)
    {
        _queryService = queryService;
        _bobProxy = bobProxy;
        _flowService = flowService;
        _cache = cache;
        _logger = logger;
    }

    [HttpGet("countdown")]
    public async Task<IActionResult> GetEpochCountdown(CancellationToken ct = default)
    {
        var result = await _cache.GetOrSetAsync(
            "epoch:countdown",
            AnalyticsCacheService.EpochCountdownTtl,
            () => _queryService.GetEpochCountdownInfoAsync(ct));

        if (result == null)
            return NotFound();

        return Ok(result);
    }

    [HttpGet]
    public async Task<IActionResult> GetEpochs(
        [FromQuery] int limit = 50,
        CancellationToken ct = default)
    {
        if (limit < 1 || limit > 200) limit = 50;

        var result = await _queryService.GetEpochsAsync(limit, ct);
        return Ok(result);
    }

    [HttpGet("{epoch}")]
    public async Task<IActionResult> GetEpoch(uint epoch, CancellationToken ct = default)
    {
        var result = await _queryService.GetEpochStatsAsync(epoch, ct);
        if (result == null)
            return NotFound();

        return Ok(result);
    }

    [HttpGet("{epoch}/transfers-by-type")]
    public async Task<IActionResult> GetEpochTransfersByType(uint epoch, CancellationToken ct = default)
    {
        var result = await _queryService.GetEpochTransfersByTypeAsync(epoch, ct);
        return Ok(result);
    }

    [HttpGet("{epoch}/rewards")]
    public async Task<IActionResult> GetEpochRewards(uint epoch, CancellationToken ct = default)
    {
        var result = await _queryService.GetEpochRewardsAsync(epoch, ct);
        return Ok(result);
    }

    // =====================================================
    // EPOCH METADATA
    // =====================================================

    [HttpGet("{epoch}/meta")]
    public async Task<IActionResult> GetEpochMeta(uint epoch, CancellationToken ct = default)
    {
        var result = await _queryService.GetEpochMetaAsync(epoch, ct);
        if (result == null)
            return NotFound();

        return Ok(result);
    }

    [HttpGet("meta")]
    public async Task<IActionResult> GetAllEpochMeta(
        [FromQuery] int limit = 100,
        CancellationToken ct = default)
    {
        if (limit < 1) limit = 1;
        if (limit > 200) limit = 200;

        var result = await _queryService.GetAllEpochMetaAsync(limit, ct);
        return Ok(result);
    }

    [HttpGet("meta/current")]
    public async Task<IActionResult> GetCurrentEpochMeta(CancellationToken ct = default)
    {
        var currentEpoch = await _queryService.GetCurrentEpochAsync(ct);
        if (currentEpoch == null)
            return NotFound();

        var result = await _queryService.GetEpochMetaAsync(currentEpoch.Value, ct);
        if (result == null)
        {
            // If no metadata exists, return basic info from ticks
            return Ok(new { epoch = currentEpoch.Value, isComplete = false });
        }

        return Ok(result);
    }

    [HttpPost("{epoch}/meta")]
    [AdminApiKey]
    public async Task<IActionResult> UpsertEpochMeta(
        uint epoch,
        [FromBody] EpochMetaRequest request,
        CancellationToken ct = default)
    {
        var dto = new QubicExplorer.Shared.DTOs.EpochMetaDto(
            epoch,
            request.InitialTick,
            request.EndTick,
            request.EndTickStartLogId,
            request.EndTickEndLogId,
            request.EndTick > 0,
            DateTime.UtcNow
        );

        await _queryService.UpsertEpochMetaAsync(dto, ct);
        return Ok(new { success = true, epoch });
    }

    /// <summary>
    /// Fetches and inserts end-epoch logs for a specific epoch.
    /// Admin only - use this to refetch epoch end logs (emissions) if they were missed.
    /// </summary>
    [HttpPost("{epoch}/fetch-end-logs")]
    [AdminApiKey]
    public async Task<IActionResult> FetchEndEpochLogs(uint epoch, CancellationToken ct = default)
    {
        _logger.LogInformation("Manual fetch of end-epoch logs triggered for epoch {Epoch}", epoch);

        // Get epoch info from Bob
        var epochInfo = await _bobProxy.GetEpochInfoAsync(epoch, ct);
        if (epochInfo == null)
        {
            return BadRequest(new { error = $"Could not get epoch info from Bob for epoch {epoch}" });
        }

        // Check if epoch is complete (has end tick info)
        if (epochInfo.EndTickStartLogId == 0 || epochInfo.EndTickEndLogId == 0)
        {
            return BadRequest(new
            {
                error = $"Epoch {epoch} doesn't have complete end tick info yet",
                endTickStartLogId = epochInfo.EndTickStartLogId,
                endTickEndLogId = epochInfo.EndTickEndLogId
            });
        }

        // Check current state
        var maxLogId = await _queryService.GetMaxLogIdForEpochAsync(epoch, ct);

        _logger.LogInformation(
            "Epoch {Epoch} end tick log range: {Start} - {End}, current max log_id: {MaxLogId}",
            epoch, epochInfo.EndTickStartLogId, epochInfo.EndTickEndLogId, maxLogId);

        // If we already have all logs, verify and capture emissions
        if (maxLogId >= epochInfo.EndTickEndLogId)
        {
            // Check if emissions are captured
            var hasEmissions = await _queryService.IsEmissionImportedAsync(epoch, ct);
            if (!hasEmissions)
            {
                // Try to capture emissions
                var epochMeta = await _queryService.GetEpochMetaAsync(epoch, ct);
                if (epochMeta != null && epochMeta.EndTick > 0)
                {
                    if (await _flowService.EnsureComputorsImportedAsync(epoch, ct))
                    {
                        var computorList = await _queryService.GetComputorsAsync(epoch, ct);
                        if (computorList != null && computorList.Computors.Count > 0)
                        {
                            var computorAddresses = computorList.Computors.Select(c => c.Address).ToHashSet();
                            var addressToIndex = computorList.Computors.ToDictionary(c => c.Address, c => (int)c.Index);
                            var (count, total) = await _queryService.CaptureEmissionsForEpochAsync(
                                epoch, epochMeta.EndTick, computorAddresses, addressToIndex, ct);

                            return Ok(new
                            {
                                success = true,
                                message = "Logs already present, captured emissions",
                                epoch,
                                maxLogId,
                                expectedEndLogId = epochInfo.EndTickEndLogId,
                                emissionsCaptured = count,
                                totalEmission = total
                            });
                        }
                    }
                }
            }

            return Ok(new
            {
                success = true,
                message = "End epoch logs already present",
                epoch,
                maxLogId,
                expectedEndLogId = epochInfo.EndTickEndLogId,
                hasEmissions
            });
        }

        // Fetch end epoch logs from Bob
        var endLogs = await _bobProxy.GetEndEpochLogsAsync(epoch, ct);
        if (endLogs == null || endLogs.Count == 0)
        {
            return BadRequest(new { error = $"Could not fetch end epoch logs from Bob for epoch {epoch}" });
        }

        _logger.LogInformation("Received {Count} end epoch logs for epoch {Epoch}", endLogs.Count, epoch);

        // Insert the end epoch logs
        await _queryService.InsertEndEpochLogsAsync(epoch, endLogs, ct);
        _logger.LogInformation("Inserted {Count} end epoch logs for epoch {Epoch}", endLogs.Count, epoch);

        // Now capture emissions
        var meta = await _queryService.GetEpochMetaAsync(epoch, ct);
        ulong emissionTick = meta?.EndTick ?? 0;

        // If no end tick in meta, get it from the logs we just inserted
        if (emissionTick == 0)
        {
            emissionTick = endLogs.Max(l => l.Tick);
        }

        decimal totalEmission = 0;
        int emissionCount = 0;

        if (await _flowService.EnsureComputorsImportedAsync(epoch, ct))
        {
            var computorList = await _queryService.GetComputorsAsync(epoch, ct);
            if (computorList != null && computorList.Computors.Count > 0)
            {
                var computorAddresses = computorList.Computors.Select(c => c.Address).ToHashSet();
                var addressToIndex = computorList.Computors.ToDictionary(c => c.Address, c => (int)c.Index);
                (emissionCount, totalEmission) = await _queryService.CaptureEmissionsForEpochAsync(
                    epoch, emissionTick, computorAddresses, addressToIndex, ct);
            }
        }

        return Ok(new
        {
            success = true,
            message = "End epoch logs fetched and inserted",
            epoch,
            logsInserted = endLogs.Count,
            endTickStartLogId = epochInfo.EndTickStartLogId,
            endTickEndLogId = epochInfo.EndTickEndLogId,
            emissionsCaptured = emissionCount,
            totalEmission
        });
    }
    /// <summary>
    /// Backfills stored epoch stats for all completed epochs that don't have them yet.
    /// Run once after deploying the stat columns migration.
    /// </summary>
    [HttpPost("backfill-stats")]
    [AdminApiKey]
    public async Task<IActionResult> BackfillEpochStats(CancellationToken ct = default)
    {
        _logger.LogInformation("Backfill epoch stats triggered");

        var allMeta = await _queryService.GetAllEpochMetaAsync(500, ct);
        var toBackfill = allMeta.Where(m => m.IsComplete && m.TickCount == 0).ToList();

        if (toBackfill.Count == 0)
        {
            return Ok(new { success = true, message = "All complete epochs already have stored stats", backfilled = 0 });
        }

        var backfilled = 0;
        foreach (var meta in toBackfill.OrderBy(m => m.Epoch))
        {
            await _queryService.ComputeAndStoreEpochStatsAsync(meta.Epoch, ct);
            backfilled++;
        }

        _logger.LogInformation("Backfilled stats for {Count} epochs", backfilled);

        return Ok(new
        {
            success = true,
            message = $"Backfilled stats for {backfilled} epochs",
            backfilled,
            epochs = toBackfill.Select(m => m.Epoch).OrderBy(e => e).ToList()
        });
    }
}

public record EpochMetaRequest(
    ulong InitialTick,
    ulong EndTick = 0,
    ulong EndTickStartLogId = 0,
    ulong EndTickEndLogId = 0
);
