using Microsoft.AspNetCore.Mvc;
using QubicExplorer.Api.Services;

namespace QubicExplorer.Api.Controllers;

[ApiController]
[Route("api/[controller]")]
public class AddressController : ControllerBase
{
    private readonly ClickHouseQueryService _queryService;
    private readonly BobProxyService _bobProxyService;

    public AddressController(ClickHouseQueryService queryService, BobProxyService bobProxyService)
    {
        _queryService = queryService;
        _bobProxyService = bobProxyService;
    }

    [HttpGet("{address}")]
    public async Task<IActionResult> GetAddress(string address, CancellationToken ct = default)
    {
        var result = await _queryService.GetAddressSummaryAsync(address, ct);

        // Get current balance and stats from Bob endpoint
        var liveData = await _bobProxyService.GetBalanceAsync(address, ct);
        if (liveData != null)
        {
            result = result with
            {
                Balance = liveData.Balance,
                IncomingAmount = liveData.IncomingAmount,
                OutgoingAmount = liveData.OutgoingAmount
            };
        }

        return Ok(result);
    }

    [HttpGet("{address}/transactions")]
    public async Task<IActionResult> GetAddressTransactions(
        string address,
        [FromQuery] int page = 1,
        [FromQuery] int limit = 20,
        [FromQuery] string? direction = null,
        [FromQuery] ulong? minAmount = null,
        [FromQuery] bool? executed = null,
        CancellationToken ct = default)
    {
        if (page < 1) page = 1;
        if (limit < 1 || limit > 100) limit = 20;

        var result = await _queryService.GetTransactionsAsync(page, limit, address, direction, minAmount, executed, ct);
        return Ok(result);
    }

    [HttpGet("{address}/transfers")]
    public async Task<IActionResult> GetAddressTransfers(
        string address,
        [FromQuery] int page = 1,
        [FromQuery] int limit = 20,
        [FromQuery] byte? type = null,
        [FromQuery] string? direction = null,
        [FromQuery] ulong? minAmount = null,
        CancellationToken ct = default)
    {
        if (page < 1) page = 1;
        if (limit < 1 || limit > 100) limit = 20;

        var result = await _queryService.GetTransfersAsync(page, limit, address, type, direction, minAmount, null, ct);
        return Ok(result);
    }

    [HttpGet("{address}/rewards")]
    public async Task<IActionResult> GetAddressRewards(
        string address,
        [FromQuery] int page = 1,
        [FromQuery] int limit = 20,
        CancellationToken ct = default)
    {
        if (page < 1) page = 1;
        if (limit < 1 || limit > 100) limit = 20;

        var result = await _queryService.GetContractRewardsAsync(address, page, limit, ct);
        return Ok(result);
    }

    [HttpGet("{address}/flow")]
    public async Task<IActionResult> GetAddressFlow(
        string address,
        [FromQuery] int limit = 10,
        CancellationToken ct = default)
    {
        if (limit < 1) limit = 1;
        if (limit > 50) limit = 50;

        var result = await _queryService.GetAddressFlowAsync(address, limit, ct);
        return Ok(result);
    }
}
