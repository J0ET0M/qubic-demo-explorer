using System.Text.Json.Serialization;
using Microsoft.AspNetCore.Mvc;
using QubicExplorer.Api.Services;

namespace QubicExplorer.Api.Controllers;

[ApiController]
[Route("api/[controller]")]
public class TicksController : ControllerBase
{
    private readonly ClickHouseQueryService _queryService;

    public TicksController(ClickHouseQueryService queryService)
    {
        _queryService = queryService;
    }

    [HttpGet]
    public async Task<IActionResult> GetTicks(
        [FromQuery] int page = 1,
        [FromQuery] int limit = 20,
        CancellationToken ct = default)
    {
        if (page < 1) page = 1;
        if (limit < 1 || limit > 100) limit = 20;

        var result = await _queryService.GetTicksAsync(page, limit, ct);
        return Ok(result);
    }

    [HttpGet("{tickNumber}")]
    public async Task<IActionResult> GetTick(ulong tickNumber, CancellationToken ct = default)
    {
        var result = await _queryService.GetTickByNumberAsync(tickNumber, ct);
        if (result != null) return Ok(result);

        // RPC-compatible: distinguish future / skipped ticks from true 404
        var availability = await _queryService.GetTickAvailabilityAsync(tickNumber, ct);
        if (tickNumber > availability.LatestTick)
            return BadRequest(RpcBadRequestEnvelope.NotYetReached(tickNumber, availability.LatestTick));
        if (availability.NextAvailable.HasValue)
            return BadRequest(RpcBadRequestEnvelope.Skipped(tickNumber, availability.NextAvailable.Value));
        return NotFound(new { error = "Tick not found" });
    }

    [HttpGet("{tickNumber}/transactions")]
    public async Task<IActionResult> GetTickTransactions(
        ulong tickNumber,
        [FromQuery] int page = 1,
        [FromQuery] int limit = 20,
        [FromQuery] string? address = null,
        [FromQuery] string? direction = null,
        [FromQuery] ulong? minAmount = null,
        [FromQuery] bool? executed = null,
        [FromQuery] int? inputType = null,
        [FromQuery] string? toAddress = null,
        [FromQuery] bool coreOnly = false,
        [FromQuery] bool detailed = false,
        [FromQuery] bool skipCount = false,
        CancellationToken ct = default)
    {
        if (page < 1) page = 1;
        if (limit < 1 || limit > 1024) limit = 20;

        // RPC-compatible semantics: if the tick is beyond what we've indexed, or the
        // tick was skipped (no row exists but later ticks do), return a BadRequest
        // envelope matching the RPC archive shape so the tx validator's shared
        // parser can handle both backends.
        var availability = await _queryService.GetTickAvailabilityAsync(tickNumber, ct);
        if (!availability.Exists)
        {
            if (tickNumber > availability.LatestTick)
            {
                return BadRequest(RpcBadRequestEnvelope.NotYetReached(tickNumber, availability.LatestTick));
            }
            if (availability.NextAvailable.HasValue)
            {
                return BadRequest(RpcBadRequestEnvelope.Skipped(tickNumber, availability.NextAvailable.Value));
            }
            return BadRequest(RpcBadRequestEnvelope.NotYetReached(tickNumber, availability.LatestTick));
        }

        var result = await _queryService.GetTransactionsByTickPagedAsync(tickNumber, page, limit, address, direction, minAmount, executed, inputType, toAddress, coreOnly, detailed, skipCount, ct);
        return Ok(result);
    }

    [HttpGet("empty")]
    public async Task<IActionResult> GetEmptyTicks(
        [FromQuery] ulong from,
        [FromQuery] ulong to,
        CancellationToken ct = default)
    {
        if (to <= from) return BadRequest("'to' must be greater than 'from'");
        if (to - from > 1_000_000) return BadRequest("Range too large (max 1,000,000 ticks)");

        var result = await _queryService.GetEmptyTicksInRangeAsync(from, to, ct);
        return Ok(result);
    }

    [HttpGet("{tickNumber}/logs")]
    public async Task<IActionResult> GetTickLogs(
        ulong tickNumber,
        [FromQuery] int page = 1,
        [FromQuery] int limit = 20,
        [FromQuery] string? fromAddress = null,
        [FromQuery] string? toAddress = null,
        [FromQuery] byte? type = null,
        [FromQuery] ulong? minAmount = null,
        CancellationToken ct = default)
    {
        if (page < 1) page = 1;
        // Clamp instead of resetting to default — sending limit=500 should give
        // you 500 (capped at the page-size ceiling), not silently drop to 20.
        if (limit < 1) limit = 20;
        if (limit > 1000) limit = 1000;

        var availability = await _queryService.GetTickAvailabilityAsync(tickNumber, ct);
        if (!availability.Exists)
        {
            if (tickNumber > availability.LatestTick)
                return BadRequest(RpcBadRequestEnvelope.NotYetReached(tickNumber, availability.LatestTick));
            if (availability.NextAvailable.HasValue)
                return BadRequest(RpcBadRequestEnvelope.Skipped(tickNumber, availability.NextAvailable.Value));
            return BadRequest(RpcBadRequestEnvelope.NotYetReached(tickNumber, availability.LatestTick));
        }

        var result = await _queryService.GetLogsByTickPagedAsync(tickNumber, page, limit, fromAddress, toAddress, type, minAmount, ct);
        return Ok(result);
    }
}

// Shape-compatible with the RPC archive's BadRequest envelope used by
// li.qubic.lib.Rpc.RpcBadRequestResponse. Do not rename field names.
internal sealed class RpcBadRequestEnvelope
{
    [JsonPropertyName("code")] public int Code { get; init; }
    [JsonPropertyName("message")] public string Message { get; init; } = string.Empty;
    [JsonPropertyName("details")] public List<RpcBadRequestEnvelopeDetail> Details { get; init; } = new();

    public static RpcBadRequestEnvelope Skipped(ulong requested, ulong nextAvailable) => new()
    {
        Code = 11,
        Message = $"tick {requested} was skipped by the system, next available tick is {nextAvailable}",
        Details = { new RpcBadRequestEnvelopeDetail { Type = "nextTickNumber", NextTickNumber = (long)nextAvailable } },
    };

    public static RpcBadRequestEnvelope NotYetReached(ulong requested, ulong lastProcessed) => new()
    {
        Code = 9,
        Message = $"requested tick {requested} is greater than last processed tick {lastProcessed}",
        Details = { new RpcBadRequestEnvelopeDetail { Type = "lastProcessedTick", LastProcessedTick = (long)lastProcessed } },
    };
}

internal sealed class RpcBadRequestEnvelopeDetail
{
    [JsonPropertyName("@type")] public string Type { get; init; } = string.Empty;
    [JsonPropertyName("nextTickNumber")] public long? NextTickNumber { get; init; }
    [JsonPropertyName("lastProcessedTick")] public long? LastProcessedTick { get; init; }
}
