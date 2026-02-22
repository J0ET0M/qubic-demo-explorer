using Microsoft.AspNetCore.Mvc;
using QubicExplorer.Api.Services;
using QubicExplorer.Shared.Constants;

namespace QubicExplorer.Api.Controllers;

[ApiController]
[Route("api/[controller]")]
public class TransactionsController : ControllerBase
{
    private readonly ClickHouseQueryService _queryService;

    public TransactionsController(ClickHouseQueryService queryService)
    {
        _queryService = queryService;
    }

    /// <summary>
    /// Get paginated transactions with optional filters.
    /// </summary>
    /// <param name="page">Page number (1-based)</param>
    /// <param name="limit">Items per page (max 100)</param>
    /// <param name="address">Filter by address (from or to)</param>
    /// <param name="direction">Filter direction: "from" (sender), "to" (receiver), or both if not specified</param>
    /// <param name="minAmount">Minimum amount filter (useful to exclude zero/dust transactions)</param>
    /// <param name="executed">Filter by execution status: true=executed only, false=failed only</param>
    /// <param name="inputType">Filter by input type (0=transfer, 1=vote counter, 2=mining solution, etc.)</param>
    /// <param name="toAddress">Filter by destination address (e.g. smart contract address)</param>
    [HttpGet]
    public async Task<IActionResult> GetTransactions(
        [FromQuery] int page = 1,
        [FromQuery] int limit = 20,
        [FromQuery] string? address = null,
        [FromQuery] string? direction = null,
        [FromQuery] ulong? minAmount = null,
        [FromQuery] bool? executed = null,
        [FromQuery] int? inputType = null,
        [FromQuery] string? toAddress = null,
        CancellationToken ct = default)
    {
        if (page < 1) page = 1;
        if (limit < 1 || limit > 100) limit = 20;

        var result = await _queryService.GetTransactionsAsync(page, limit, address, direction, minAmount, executed, inputType, toAddress, ct);
        return Ok(result);
    }

    [HttpGet("{hash}")]
    public async Task<IActionResult> GetTransaction(string hash, CancellationToken ct = default)
    {
        // Check if this is a special virtual transaction (smart contract lifecycle event)
        if (SpecialTransactionTypes.IsSpecialTransaction(hash))
        {
            var specialResult = await _queryService.GetSpecialTransactionAsync(hash, ct);
            if (specialResult == null)
                return NotFound(new { error = "Special transaction not found" });

            return Ok(specialResult);
        }

        var result = await _queryService.GetTransactionByHashAsync(hash, ct);
        if (result == null)
            return NotFound(new { error = "Transaction not found" });

        return Ok(result);
    }
}
