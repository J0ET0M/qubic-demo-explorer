using Microsoft.AspNetCore.Mvc;
using QubicExplorer.Api.Services;
using QubicExplorer.Shared.DTOs;

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
        // Get balance from Bob (live network data) — no ClickHouse queries needed.
        // Bob provides balance, in/out amounts, and transfer counts directly.
        var liveData = await _bobProxyService.GetBalanceAsync(address, ct);
        if (liveData != null)
        {
            return Ok(new AddressDto(
                address,
                liveData.Balance,
                liveData.IncomingAmount,
                liveData.OutgoingAmount,
                liveData.NumberOfIncomingTransfers + liveData.NumberOfOutgoingTransfers,
                liveData.NumberOfIncomingTransfers + liveData.NumberOfOutgoingTransfers
            ));
        }

        // Fallback to ClickHouse if Bob is unavailable
        var result = await _cache.GetOrSetAsync(
            $"address:summary:{address}",
            AnalyticsCacheService.AddressSummaryTtl,
            () => _queryService.GetAddressSummaryAsync(address, ct));
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

        var result = await _cache.GetOrSetAsync(
            $"address:tx:{address}:{page}:{limit}:{direction}:{minAmount}:{executed}",
            AnalyticsCacheService.AddressSummaryTtl,
            () => _queryService.GetTransactionsAsync(page, limit, address, direction, minAmount, executed, ct: ct));
        return Ok(result);
    }

    [HttpGet("{address}/transfers")]
    public async Task<IActionResult> GetAddressTransfers(
        string address,
        [FromQuery] int page = 1,
        [FromQuery] int limit = 20,
        [FromQuery] byte? type = null,
        [FromQuery] string? fromAddress = null,
        [FromQuery] string? toAddress = null,
        [FromQuery] ulong? minAmount = null,
        [FromQuery] uint? epoch = null,
        CancellationToken ct = default)
    {
        if (page < 1) page = 1;
        if (limit < 1 || limit > 100) limit = 20;

        var result = await _cache.GetOrSetAsync(
            $"address:transfers:{address}:{page}:{limit}:{type}:{fromAddress}:{toAddress}:{minAmount}:{epoch}",
            AnalyticsCacheService.AddressSummaryTtl,
            () => _queryService.GetTransfersAsync(page, limit, address, type, minAmount, null, epoch, fromAddress, toAddress, ct));
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

        var result = await _cache.GetOrSetAsync(
            $"address:rewards:{address}:{page}:{limit}",
            AnalyticsCacheService.AddressSummaryTtl,
            () => _queryService.GetContractRewardsAsync(address, page, limit, ct));
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
            async () =>
            {
                // First-seen: cheap query from address_first_seen table (already fast)
                var firstSeenTask = _queryService.GetAddressFirstSeenAsync(address, ct);

                // Last-seen: use Bob's LatestIncoming/OutgoingTransferTick (no ClickHouse scan!)
                var bobData = await _bobProxyService.GetBalanceAsync(address, ct);

                var (firstTick, firstTimestamp, firstEpoch) = await firstSeenTask;

                if (bobData != null)
                {
                    var lastTick = Math.Max(bobData.LatestIncomingTransferTick, bobData.LatestOutgoingTransferTick);
                    if (lastTick > 0)
                    {
                        // Look up timestamp/epoch for the tick (single-row PK lookup, very fast)
                        var (lastTimestamp, lastEpoch) = await _queryService.GetTickTimestampAndEpochAsync(lastTick, ct);
                        return new AddressActivityRangeDto(
                            firstTick, firstTimestamp, firstEpoch,
                            lastTick, lastTimestamp, lastEpoch);
                    }
                }

                // Fallback: if Bob unavailable, use the expensive ClickHouse logs scan
                return await _queryService.GetAddressActivityRangeAsync(address, ct);
            });
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

    [HttpGet("{address}/ledger")]
    public async Task<IActionResult> GetAddressLedger(
        string address,
        [FromQuery] uint? epoch = null,
        CancellationToken ct = default)
    {
        var targetEpoch = epoch ?? await _queryService.GetCurrentEpochAsync(ct) ?? 0;
        if (targetEpoch == 0)
            return NotFound("No epoch data available");

        var result = await _cache.GetOrSetAsync(
            $"address:ledger:{address}:{targetEpoch}",
            AnalyticsCacheService.AddressSummaryTtl,
            () => _queryService.GetAddressLedgerAsync(address, targetEpoch, ct));
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

        var result = await _cache.GetOrSetAsync(
            $"address:flow:{address}:{limit}",
            AnalyticsCacheService.AddressSummaryTtl,
            () => _queryService.GetAddressFlowAsync(address, limit, ct));
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

        var result = await _cache.GetOrSetAsync(
            $"address:graph:{address}:{hops}:{limit}",
            AnalyticsCacheService.AddressSummaryTtl,
            () => _queryService.GetAddressGraphAsync(address, hops, limit, ct));
        return Ok(result);
    }

    /// <summary>
    /// Reserve (= QU balance) history for a smart-contract address. Snapshotted
    /// every ~10 min by the analytics service. Data older than 31 days is
    /// dropped by the table TTL.
    /// </summary>
    [HttpGet("{address}/reserve-history")]
    public async Task<IActionResult> GetReserveHistory(
        string address,
        [FromQuery] int days = 7,
        CancellationToken ct = default)
    {
        if (days < 1) days = 1;
        if (days > 31) days = 31;

        var result = await _queryService.GetContractReserveHistoryAsync(address, days, ct);
        return Ok(result);
    }

    /// <summary>
    /// Per-year tax report for a single address: monthly opening/closing balances,
    /// full transfer list with running balance, totals. Useful for tax filing.
    /// </summary>
    [HttpGet("{address}/tax-report")]
    public async Task<IActionResult> GetTaxReport(
        string address,
        [FromQuery] int year,
        [FromQuery] int maxTransfers = 20000,
        CancellationToken ct = default)
    {
        if (year < 2020 || year > DateTime.UtcNow.Year + 1)
            return BadRequest(new { error = "year must be between 2020 and next year" });
        if (maxTransfers < 100) maxTransfers = 100;
        if (maxTransfers > 100000) maxTransfers = 100000;

        var result = await _queryService.GetAddressTaxReportAsync(address, year, maxTransfers, ct);
        return Ok(result);
    }

    /// <summary>
    /// Same data as /tax-report but emitted as a CSV file. Ready to upload to
    /// most tax software or open in Excel.
    /// </summary>
    [HttpGet("{address}/tax-report.csv")]
    public async Task<IActionResult> GetTaxReportCsv(
        string address,
        [FromQuery] int year,
        CancellationToken ct = default)
    {
        if (year < 2020 || year > DateTime.UtcNow.Year + 1)
            return BadRequest(new { error = "year must be between 2020 and next year" });

        var report = await _queryService.GetAddressTaxReportAsync(address, year, 100000, ct);

        var sb = new System.Text.StringBuilder();
        // Header summary as commented rows (CSV-friendly)
        sb.AppendLine($"# Tax report — {address}");
        if (!string.IsNullOrEmpty(report.AddressLabel)) sb.AppendLine($"# Label: {report.AddressLabel}");
        sb.AppendLine($"# Year: {report.Year}");
        sb.AppendLine($"# Opening balance (QU): {report.OpeningBalance}");
        sb.AppendLine($"# Closing balance (QU): {report.ClosingBalance}");
        sb.AppendLine($"# Total in (QU): {report.TotalIn}   ({report.InboundCount} transfers)");
        sb.AppendLine($"# Total out (QU): {report.TotalOut}  ({report.OutboundCount} transfers)");
        sb.AppendLine($"# Net change (QU): {report.NetChange}");
        if (report.Truncated) sb.AppendLine($"# WARNING: transfer list truncated at {report.MaxTransfers} rows");
        sb.AppendLine();
        sb.AppendLine("timestamp_utc,tick,epoch,direction,amount_qu,counterparty,counterparty_label,running_balance_qu,tx_hash,log_type");
        foreach (var t in report.Transfers)
        {
            // CSV-escape: double-quote anything containing a comma or quote;
            // double internal quotes. Labels can contain commas.
            string esc(string? s) => s == null ? "" :
                (s.Contains(',') || s.Contains('"') || s.Contains('\n'))
                    ? "\"" + s.Replace("\"", "\"\"") + "\""
                    : s;
            sb.Append(t.Timestamp.ToString("yyyy-MM-ddTHH:mm:ss.fffZ")).Append(',');
            sb.Append(t.TickNumber).Append(',');
            sb.Append(t.Epoch).Append(',');
            sb.Append(t.Direction).Append(',');
            sb.Append(t.Amount).Append(',');
            sb.Append(esc(t.Counterparty)).Append(',');
            sb.Append(esc(t.CounterpartyLabel)).Append(',');
            sb.Append(t.RunningBalance).Append(',');
            sb.Append(esc(t.TxHash)).Append(',');
            sb.AppendLine(t.LogTypeName);
        }

        var filename = $"tax-report-{address[..6]}-{year}.csv";
        return File(System.Text.Encoding.UTF8.GetBytes(sb.ToString()), "text/csv", filename);
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
                var liveData = await _bobProxyService.GetBalanceAsync(addr, ct);
                if (liveData != null)
                {
                    return (object?)new AddressDto(
                        addr,
                        liveData.Balance,
                        liveData.IncomingAmount,
                        liveData.OutgoingAmount,
                        liveData.NumberOfIncomingTransfers + liveData.NumberOfOutgoingTransfers,
                        liveData.NumberOfIncomingTransfers + liveData.NumberOfOutgoingTransfers
                    );
                }
                return (object?)await _queryService.GetAddressSummaryAsync(addr, ct);
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
