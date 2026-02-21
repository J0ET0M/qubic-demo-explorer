using Microsoft.AspNetCore.Mvc;
using QubicExplorer.Api.Services;

namespace QubicExplorer.Api.Controllers;

[ApiController]
[Route("api/[controller]")]
public class TransfersController : ControllerBase
{
    private readonly ClickHouseQueryService _queryService;

    public TransfersController(ClickHouseQueryService queryService)
    {
        _queryService = queryService;
    }

    /// <summary>
    /// Get paginated transfers (logs) with optional filters.
    /// </summary>
    /// <param name="page">Page number (1-based)</param>
    /// <param name="limit">Items per page (max 100)</param>
    /// <param name="address">Filter by address (source or dest)</param>
    /// <param name="type">Filter by single log type (0=QU_TRANSFER, 1=ASSET_ISSUANCE, etc.)</param>
    /// <param name="types">Filter by multiple log types, comma-separated (e.g., "0,1,2")</param>
    /// <param name="direction">Filter direction: "in" (receiver), "out" (sender), or both if not specified</param>
    /// <param name="minAmount">Minimum amount filter (useful to exclude zero/dust transfers)</param>
    /// <param name="epoch">Filter by epoch (enables partition pruning for faster queries)</param>
    [HttpGet]
    public async Task<IActionResult> GetTransfers(
        [FromQuery] int page = 1,
        [FromQuery] int limit = 20,
        [FromQuery] string? address = null,
        [FromQuery] byte? type = null,
        [FromQuery] string? types = null,
        [FromQuery] string? direction = null,
        [FromQuery] ulong? minAmount = null,
        [FromQuery] uint? epoch = null,
        CancellationToken ct = default)
    {
        if (page < 1) page = 1;
        if (limit < 1 || limit > 100) limit = 20;

        // Parse multiple log types if provided
        List<byte>? logTypes = null;
        if (!string.IsNullOrEmpty(types))
        {
            logTypes = types.Split(',')
                .Select(s => byte.TryParse(s.Trim(), out var b) ? b : (byte?)null)
                .Where(b => b.HasValue)
                .Select(b => b!.Value)
                .ToList();
        }

        var result = await _queryService.GetTransfersAsync(page, limit, address, type, direction, minAmount, logTypes, epoch, ct);
        return Ok(result);
    }
}
