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
        CancellationToken ct = default)
    {
        if (page < 1) page = 1;
        if (limit < 1 || limit > 100) limit = 20;

        var result = await _queryService.GetTransactionsByTickPagedAsync(tickNumber, page, limit, address, direction, minAmount, executed, ct);
        return Ok(result);
    }

    [HttpGet("{tickNumber}/logs")]
    public async Task<IActionResult> GetTickLogs(
        ulong tickNumber,
        [FromQuery] int page = 1,
        [FromQuery] int limit = 20,
        [FromQuery] string? address = null,
        [FromQuery] byte? type = null,
        [FromQuery] string? direction = null,
        [FromQuery] ulong? minAmount = null,
        CancellationToken ct = default)
    {
        if (page < 1) page = 1;
        if (limit < 1 || limit > 100) limit = 20;

        var result = await _queryService.GetLogsByTickPagedAsync(tickNumber, page, limit, address, type, direction, minAmount, ct);
        return Ok(result);
    }
}
