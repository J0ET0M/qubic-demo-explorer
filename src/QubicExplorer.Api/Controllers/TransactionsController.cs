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
    /// <param name="limit">Items per page. Default 20, max 100 normally. When at least one
    /// of <c>epoch</c>, <c>tickFrom</c>, or <c>tickTo</c> is set the cap is raised to 2000
    /// so bulk sync tools don't need 50k round-trips per epoch.</param>
    /// <param name="address">Filter by address (from or to)</param>
    /// <param name="direction">Filter direction: "from" (sender), "to" (receiver), or both if not specified</param>
    /// <param name="minAmount">Minimum amount filter (useful to exclude zero/dust transactions)</param>
    /// <param name="executed">Filter by execution status: true=executed only, false=failed only</param>
    /// <param name="inputType">Filter by input type (0=transfer, 1=vote counter, 2=mining solution, etc.)</param>
    /// <param name="toAddress">Filter by destination address (e.g. smart contract address)</param>
    /// <param name="epoch">Limit results to a single epoch (partition prune, essentially free).</param>
    /// <param name="tickFrom">Inclusive lower bound on tick_number — combine with tickTo for range/resumable bulk sync.</param>
    /// <param name="tickTo">Inclusive upper bound on tick_number.</param>
    /// <param name="sort">Order by tick_number: "asc" (ascending — useful for forward-streaming bulk sync) or "desc" (default — newest first).</param>
    /// <remarks>
    /// <para>
    /// Bulk-sync recipe (e.g. all solution txs in an epoch, input_type=2):
    /// </para>
    /// <code>
    /// GET /api/transactions?epoch=217&amp;inputType=2&amp;limit=2000&amp;page=1
    /// GET /api/transactions?epoch=217&amp;inputType=2&amp;limit=2000&amp;page=2
    /// ...
    /// </code>
    /// <para>
    /// Or cursor-style (avoids OFFSET cost on deep pages — the response is ordered
    /// by tick_number DESC, so step backwards through the range):
    /// </para>
    /// <code>
    /// GET /api/transactions?epoch=217&amp;inputType=2&amp;limit=2000           // newest 2000
    /// GET /api/transactions?epoch=217&amp;inputType=2&amp;limit=2000&amp;tickTo=&lt;lowest tick of prev response - 1&gt;
    /// ...
    /// </code>
    /// </remarks>
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
        [FromQuery] bool coreOnly = false,
        [FromQuery] bool detailed = false,
        [FromQuery] uint? epoch = null,
        [FromQuery] ulong? tickFrom = null,
        [FromQuery] ulong? tickTo = null,
        [FromQuery] string? sort = null,
        CancellationToken ct = default)
    {
        if (page < 1) page = 1;

        // Allow bigger pages when the client scopes by epoch / tick range —
        // those are partition-pruned and in-order on (tick_number, hash), so
        // returning 2000 rows is cheap. Without scoping, keep the conservative
        // 100 cap so a wildcard page=1&limit=10000 can't tank the DB.
        var scoped = epoch.HasValue || tickFrom.HasValue || tickTo.HasValue;
        var maxLimit = scoped ? 2000 : 100;
        if (limit < 1 || limit > maxLimit) limit = scoped ? 1000 : 20;

        var result = await _queryService.GetTransactionsAsync(
            page, limit, address, direction, minAmount, executed, inputType,
            toAddress, coreOnly, detailed,
            epoch, tickFrom, tickTo, sort, ct);
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
