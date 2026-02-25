using Microsoft.AspNetCore.Mvc;
using QubicExplorer.Api.Services;

namespace QubicExplorer.Api.Controllers;

[ApiController]
[Route("api/[controller]")]
public class AddressController : ControllerBase
{
    private readonly ClickHouseQueryService _queryService;
    private readonly BobProxyService _bobProxyService;
    private readonly AnalyticsCacheService _cache;

    public AddressController(ClickHouseQueryService queryService, BobProxyService bobProxyService, AnalyticsCacheService cache)
    {
        _queryService = queryService;
        _bobProxyService = bobProxyService;
        _cache = cache;
    }

    [HttpGet("{address}")]
    public async Task<IActionResult> GetAddress(string address, CancellationToken ct = default)
    {
        // Parallel fetch: ClickHouse summary + live Bob balance
        var summaryTask = _cache.GetOrSetAsync(
            $"address:summary:{address}",
            AnalyticsCacheService.AddressSummaryTtl,
            () => _queryService.GetAddressSummaryAsync(address, ct));
        var liveTask = _bobProxyService.GetBalanceAsync(address, ct);
        await Task.WhenAll(summaryTask, liveTask);

        var result = summaryTask.Result;
        var liveData = liveTask.Result;
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

        var result = await _queryService.GetTransactionsAsync(page, limit, address, direction, minAmount, executed, ct: ct);
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
        [FromQuery] uint? epoch = null,
        CancellationToken ct = default)
    {
        if (page < 1) page = 1;
        if (limit < 1 || limit > 100) limit = 20;

        var result = await _queryService.GetTransfersAsync(page, limit, address, type, direction, minAmount, null, epoch, ct);
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

    [HttpGet("{address}/activity-range")]
    public async Task<IActionResult> GetAddressActivityRange(
        string address,
        CancellationToken ct = default)
    {
        var result = await _cache.GetOrSetAsync(
            $"address:activity-range:{address}",
            AnalyticsCacheService.AddressActivityRangeTtl,
            () => _queryService.GetAddressActivityRangeAsync(address, ct));
        return Ok(result);
    }

    [HttpGet("{address}/export")]
    public async Task<IActionResult> ExportAddressData(
        string address,
        [FromQuery] string format = "csv",
        [FromQuery] string type = "transfers",
        [FromQuery] uint? epoch = null,
        CancellationToken ct = default)
    {
        if (format != "csv")
            return BadRequest("Only CSV format is currently supported");

        var fileName = epoch.HasValue
            ? $"{address}-{type}-epoch{epoch}.csv"
            : $"{address}-{type}.csv";

        Response.ContentType = "text/csv";
        Response.Headers.Append("Content-Disposition", $"attachment; filename=\"{fileName}\"");

        await using var writer = new StreamWriter(Response.Body);
        await _queryService.StreamAddressTransfersAsCsvAsync(address, epoch, writer, ct);
        await writer.FlushAsync(ct);

        return new EmptyResult();
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

    [HttpGet("{address}/graph")]
    public async Task<IActionResult> GetAddressGraph(
        string address,
        [FromQuery] int hops = 1,
        [FromQuery] int limit = 20,
        CancellationToken ct = default)
    {
        if (hops < 1) hops = 1;
        if (hops > 2) hops = 2;
        if (limit < 1) limit = 1;
        if (limit > 50) limit = 50;

        var result = await _queryService.GetAddressGraphAsync(address, hops, limit, ct);
        return Ok(result);
    }

    /// <summary>
    /// Batch fetch multiple addresses (for portfolio view)
    /// </summary>
    [HttpPost("batch")]
    public async Task<IActionResult> GetAddressesBatch(
        [FromBody] BatchAddressRequest request,
        CancellationToken ct = default)
    {
        if (request.Addresses == null || request.Addresses.Count == 0)
            return BadRequest("No addresses provided");

        if (request.Addresses.Count > 20)
            return BadRequest("Maximum 20 addresses per batch request");

        var tasks = request.Addresses.Select(async addr =>
        {
            try
            {
                var summaryTask = _queryService.GetAddressSummaryAsync(addr, ct);
                var liveTask = _bobProxyService.GetBalanceAsync(addr, ct);
                await Task.WhenAll(summaryTask, liveTask);

                var summary = summaryTask.Result;
                var liveData = liveTask.Result;
                if (liveData != null)
                {
                    summary = summary with
                    {
                        Balance = liveData.Balance,
                        IncomingAmount = liveData.IncomingAmount,
                        OutgoingAmount = liveData.OutgoingAmount
                    };
                }
                return (object?)summary;
            }
            catch
            {
                return null;
            }
        });

        var results = (await Task.WhenAll(tasks))
            .Where(r => r != null)
            .ToList();

        return Ok(results);
    }
}

public record BatchAddressRequest(List<string> Addresses);
