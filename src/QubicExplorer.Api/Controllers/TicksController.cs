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
        if (result == null)
            return NotFound(new { error = "Tick not found" });

        return Ok(result);
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
        if (limit < 1 || limit > 100) limit = 20;

        var result = await _queryService.GetLogsByTickPagedAsync(tickNumber, page, limit, fromAddress, toAddress, type, minAmount, ct);
        return Ok(result);
    }
}
