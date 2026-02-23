using System.Text.RegularExpressions;
using Microsoft.AspNetCore.Mvc;
using QubicExplorer.Api.Services;
using QubicExplorer.Shared.DTOs;

namespace QubicExplorer.Api.Controllers;

/// <summary>
/// Public endpoints for custom flow tracking.
/// Users can create flow tracking jobs and view results by GUID.
/// </summary>
[ApiController]
[Route("api/custom-flow")]
public partial class CustomFlowController : ControllerBase
{
    private readonly ClickHouseQueryService _queryService;
    private readonly BobProxyService _bobProxy;
    private readonly ILogger<CustomFlowController> _logger;

    public CustomFlowController(
        ClickHouseQueryService queryService,
        BobProxyService bobProxy,
        ILogger<CustomFlowController> logger)
    {
        _queryService = queryService;
        _bobProxy = bobProxy;
        _logger = logger;
    }

    /// <summary>
    /// Creates a new custom flow tracking job.
    /// </summary>
    [HttpPost]
    public async Task<IActionResult> CreateFlowTracking(
        [FromBody] CreateCustomFlowRequest request,
        CancellationToken ct = default)
    {
        // Validate addresses
        if (request.Addresses == null || request.Addresses.Count == 0)
            return BadRequest(new { error = "At least one address is required" });

        if (request.Addresses.Count > 5)
            return BadRequest(new { error = "Maximum 5 addresses allowed" });

        var distinctAddresses = request.Addresses.Distinct().ToList();
        foreach (var addr in distinctAddresses)
        {
            if (string.IsNullOrEmpty(addr) || addr.Length != 60 || !QubicAddressRegex().IsMatch(addr))
                return BadRequest(new { error = $"Invalid Qubic address: {addr}" });
        }

        // Validate start tick
        if (request.StartTick == 0)
            return BadRequest(new { error = "StartTick must be greater than 0" });

        // Validate max hops
        var maxHops = request.MaxHops;
        if (maxHops == 0) maxHops = 10;
        if (maxHops > 20) maxHops = 20;

        // Resolve balances: use provided or fetch from Bob
        var balances = new List<ulong>();
        for (var i = 0; i < distinctAddresses.Count; i++)
        {
            if (request.Balances != null && i < request.Balances.Count && request.Balances[i] > 0)
            {
                balances.Add(request.Balances[i]);
            }
            else
            {
                // Fetch current balance from Bob
                var balance = await _bobProxy.GetBalanceAsync(distinctAddresses[i], ct);
                if (balance == null || balance.Balance == 0)
                {
                    return BadRequest(new { error = $"Could not fetch balance for {distinctAddresses[i]} (or balance is 0)" });
                }
                balances.Add(balance.Balance);
            }
        }

        var jobId = Guid.NewGuid().ToString();
        var alias = string.IsNullOrWhiteSpace(request.Alias) ? "" : request.Alias.Trim();
        if (alias.Length > 100) alias = alias[..100];

        await _queryService.CreateCustomFlowJobAsync(
            jobId, distinctAddresses, balances, request.StartTick, alias, maxHops, ct);

        _logger.LogInformation(
            "Created custom flow tracking job {JobId} (alias: {Alias}, addresses: {Count}, startTick: {StartTick})",
            jobId, alias, distinctAddresses.Count, request.StartTick);

        return Ok(new
        {
            jobId,
            alias,
            startTick = request.StartTick,
            addresses = distinctAddresses,
            balances,
            maxHops,
            status = "pending"
        });
    }

    /// <summary>
    /// Gets the status and metadata of a custom flow tracking job.
    /// </summary>
    [HttpGet("{jobId}")]
    public async Task<IActionResult> GetFlowTracking(string jobId, CancellationToken ct = default)
    {
        var job = await _queryService.GetCustomFlowJobAsync(jobId, ct);
        if (job == null)
            return NotFound(new { error = "Flow tracking job not found" });

        return Ok(job);
    }

    /// <summary>
    /// Gets Sankey visualization data for a custom flow tracking job.
    /// </summary>
    [HttpGet("{jobId}/visualization")]
    public async Task<IActionResult> GetVisualization(string jobId, CancellationToken ct = default)
    {
        var job = await _queryService.GetCustomFlowJobAsync(jobId, ct);
        if (job == null)
            return NotFound(new { error = "Flow tracking job not found" });

        var hops = await _queryService.GetCustomFlowHopsAsync(jobId, job.MaxHops, ct);

        // Build visualization (same pattern as MinerFlowController)
        var nodeMinDepth = new Dictionary<string, int>();
        var nodeTypes = new Dictionary<string, string>();
        var nodeLabels = new Dictionary<string, string?>();

        foreach (var hop in hops.Where(h => h.HopLevel == 1))
        {
            nodeMinDepth[hop.SourceAddress] = 0;
            nodeTypes[hop.SourceAddress] = "tracked";
            if (!string.IsNullOrEmpty(hop.SourceLabel))
                nodeLabels[hop.SourceAddress] = hop.SourceLabel;
        }

        foreach (var hop in hops.OrderBy(h => h.HopLevel))
        {
            if (!nodeMinDepth.ContainsKey(hop.SourceAddress))
            {
                nodeMinDepth[hop.SourceAddress] = hop.HopLevel - 1;
                nodeTypes[hop.SourceAddress] = "intermediary";
            }
            if (!string.IsNullOrEmpty(hop.SourceLabel) && !nodeLabels.ContainsKey(hop.SourceAddress))
                nodeLabels[hop.SourceAddress] = hop.SourceLabel;

            if (!nodeMinDepth.ContainsKey(hop.DestAddress))
            {
                nodeMinDepth[hop.DestAddress] = hop.HopLevel;
                nodeTypes[hop.DestAddress] = hop.DestType ?? "unknown";
            }
            else if (hop.HopLevel < nodeMinDepth[hop.DestAddress])
            {
                nodeMinDepth[hop.DestAddress] = hop.HopLevel;
            }
            if (!string.IsNullOrEmpty(hop.DestLabel) && !nodeLabels.ContainsKey(hop.DestAddress))
                nodeLabels[hop.DestAddress] = hop.DestLabel;
        }

        var nodes = new Dictionary<string, FlowVisualizationNodeDto>();
        foreach (var (address, depth) in nodeMinDepth)
        {
            nodes[address] = new FlowVisualizationNodeDto(
                Id: address,
                Address: address,
                Label: nodeLabels.GetValueOrDefault(address),
                Type: nodeTypes.GetValueOrDefault(address, "unknown"),
                TotalInflow: 0,
                TotalOutflow: 0,
                Depth: depth
            );
        }

        var links = new Dictionary<(string, string), (decimal Amount, uint Count)>();
        foreach (var hop in hops)
        {
            if (nodes.TryGetValue(hop.SourceAddress, out var srcNode))
                nodes[hop.SourceAddress] = srcNode with { TotalOutflow = srcNode.TotalOutflow + hop.Amount };
            if (nodes.TryGetValue(hop.DestAddress, out var dstNode))
                nodes[hop.DestAddress] = dstNode with { TotalInflow = dstNode.TotalInflow + hop.Amount };

            var linkKey = (hop.SourceAddress, hop.DestAddress);
            if (links.TryGetValue(linkKey, out var existing))
                links[linkKey] = (existing.Amount + hop.Amount, existing.Count + 1);
            else
                links[linkKey] = (hop.Amount, 1);
        }

        var vizLinks = links.Select(kvp => new FlowVisualizationLinkDto(
            SourceId: kvp.Key.Item1,
            TargetId: kvp.Key.Item2,
            Amount: kvp.Value.Amount,
            TransactionCount: kvp.Value.Count
        )).ToList();

        return Ok(new CustomFlowResultDto(
            Job: job,
            Nodes: nodes.Values.ToList(),
            Links: vizLinks,
            MaxDepth: hops.Count > 0 ? hops.Max(h => h.HopLevel) : 0,
            TotalTrackedVolume: hops.Where(h => h.HopLevel == 1).Sum(h => h.Amount)
        ));
    }

    /// <summary>
    /// Gets raw hop data for a custom flow tracking job.
    /// </summary>
    [HttpGet("{jobId}/hops")]
    public async Task<IActionResult> GetHops(
        string jobId,
        [FromQuery] int maxDepth = 10,
        CancellationToken ct = default)
    {
        var job = await _queryService.GetCustomFlowJobAsync(jobId, ct);
        if (job == null)
            return NotFound(new { error = "Flow tracking job not found" });

        if (maxDepth > 20) maxDepth = 20;
        var hops = await _queryService.GetCustomFlowHopsAsync(jobId, maxDepth, ct);

        return Ok(new { hops, totalHops = hops.Count });
    }

    /// <summary>
    /// Gets tracking state for all addresses in a custom flow job.
    /// </summary>
    [HttpGet("{jobId}/state")]
    public async Task<IActionResult> GetState(string jobId, CancellationToken ct = default)
    {
        var job = await _queryService.GetCustomFlowJobAsync(jobId, ct);
        if (job == null)
            return NotFound(new { error = "Flow tracking job not found" });

        var states = await _queryService.GetCustomFlowStatesAsync(jobId, ct);

        return Ok(new { states, totalStates = states.Count });
    }

    [GeneratedRegex("^[A-Z]{60}$")]
    private static partial Regex QubicAddressRegex();
}
