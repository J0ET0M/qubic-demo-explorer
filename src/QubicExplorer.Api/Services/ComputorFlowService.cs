using Qubic.Crypto;
using QubicExplorer.Shared.DTOs;

namespace QubicExplorer.Api.Services;

/// <summary>
/// Service for tracking and analyzing computor/miner flow.
/// Handles:
/// - Fetching and storing computor lists per epoch
/// - Tracking money flow from computors up to N hops
/// - Computing aggregated flow statistics
/// - Generating flow visualization data
/// </summary>
public class ComputorFlowService
{
    private readonly BobProxyService _bobProxy;
    private readonly ClickHouseQueryService _queryService;
    private readonly AddressLabelService _labelService;
    private readonly ILogger<ComputorFlowService> _logger;

    private const int MaxHops = 10;
    private const int ComputorCount = 676;

    // Qutil contract index - this SC is pass-through (distributes to multiple recipients)
    private const int QutilContractIndex = 4;

    // Qutil SendToManyV1 procedure ID
    private const int QutilSendToManyV1ProcedureId = 1;

    // Qutil SendToManyV1 payload: 25 addresses (32 bytes each) + 25 amounts (8 bytes each)
    private const int QutilAddressSize = 32;
    private const int QutilAmountSize = 8;
    private const int QutilMaxDestinations = 25;

    // Qubic crypto for address encoding
    private static readonly QubicCrypt QubicCrypt = new();

    public ComputorFlowService(
        BobProxyService bobProxy,
        ClickHouseQueryService queryService,
        AddressLabelService labelService,
        ILogger<ComputorFlowService> logger)
    {
        _bobProxy = bobProxy;
        _queryService = queryService;
        _labelService = labelService;
        _logger = logger;
    }

    /// <summary>
    /// Fetches and stores computors for an epoch if not already imported.
    /// Returns true if computors were fetched/available.
    /// </summary>
    public async Task<bool> EnsureComputorsImportedAsync(uint epoch, CancellationToken ct = default)
    {
        // Check if already imported
        if (await _queryService.IsComputorListImportedAsync(epoch, ct))
        {
            _logger.LogDebug("Computors for epoch {Epoch} already imported", epoch);
            return true;
        }

        // Fetch from RPC
        var result = await _bobProxy.GetComputorsAsync(epoch, ct);
        if (result == null || result.Computors.Count == 0)
        {
            _logger.LogWarning("Failed to fetch computors for epoch {Epoch}", epoch);
            return false;
        }

        // Clean addresses (remove trailing unicode character if present)
        var cleanedComputors = result.Computors
            .Select(addr => CleanAddress(addr))
            .ToList();

        // Store in database
        await _queryService.SaveComputorsAsync(epoch, cleanedComputors, ct);

        _logger.LogInformation("Imported {Count} computors for epoch {Epoch}", cleanedComputors.Count, epoch);
        return true;
    }

    /// <summary>
    /// Gets the list of computors for an epoch.
    /// </summary>
    public async Task<ComputorListDto?> GetComputorsAsync(uint epoch, CancellationToken ct = default)
    {
        await EnsureComputorsImportedAsync(epoch, ct);
        return await _queryService.GetComputorsAsync(epoch, ct);
    }

    /// <summary>
    /// Analyzes flow from computors within a tick range.
    /// Tracks money flow up to MaxHops and identifies flows to exchanges.
    /// Supports continuous tracking: not only starts from computors, but continues
    /// tracking intermediary addresses from previous windows until funds reach exchanges.
    /// </summary>
    public async Task<MinerFlowStatsDto?> AnalyzeFlowForWindowAsync(
        uint currentEpoch,
        uint emissionEpoch,
        ulong tickStart,
        ulong tickEnd,
        CancellationToken ct = default)
    {
        _logger.LogInformation(
            "Analyzing flow for epoch {Epoch} (emission from epoch {EmissionEpoch}), ticks {TickStart}-{TickEnd}",
            currentEpoch, emissionEpoch, tickStart, tickEnd);

        // Ensure computors are available for the emission epoch
        // Revenue distribution happens at the END of each epoch (in the last tick via logs/events)
        if (!await EnsureComputorsImportedAsync(emissionEpoch, ct))
        {
            _logger.LogWarning("Cannot analyze flow: computors not available for epoch {Epoch}", emissionEpoch);
            return null;
        }

        // Get computor addresses
        var computorList = await _queryService.GetComputorsAsync(emissionEpoch, ct);
        if (computorList == null || computorList.Computors.Count == 0)
        {
            _logger.LogWarning("No computors found for epoch {Epoch}", emissionEpoch);
            return null;
        }

        var computorAddresses = computorList.Computors.Select(c => c.Address).ToHashSet();

        // Get exchange addresses for classification
        var exchangeAddresses = await _queryService.GetAddressesByTypeAsync("exchange", ct);

        // Step 1: Get emission from computor_emissions table (captured at epoch end)
        // This is the reward distributed at the end of the emission epoch
        var emissionFromTable = await _queryService.GetTotalEmissionForEpochAsync(emissionEpoch, ct);

        // Step 1b: Calculate additional inflow to computors (not from zero address)
        // These are other transfers coming into computor addresses during the window
        var additionalInflow = await _queryService.CalculateInflowToAddressesAsync(
            computorAddresses, tickStart, tickEnd, ct);

        _logger.LogDebug(
            "Emission from table: {EmissionTable}, Additional inflow: {Additional}",
            emissionFromTable, additionalInflow.TotalAmount);

        // Step 2: Calculate outflow from computors (excludes transfers to zero address)
        var outflow = await _queryService.CalculateOutflowFromAddressesAsync(
            computorAddresses, tickStart, tickEnd, ct);

        // Step 3: Track flow through hops with continuous tracking
        // This tracks from computors AND from intermediary addresses from previous windows
        var flowAnalysis = await TrackFlowWithContinuousTrackingAsync(
            currentEpoch, emissionEpoch, computorAddresses, exchangeAddresses, tickStart, tickEnd, ct);

        // Step 4: Save flow hops to database for future queries
        if (flowAnalysis.Hops.Count > 0)
        {
            await _queryService.SaveFlowHopsAsync(flowAnalysis.Hops, ct);
        }

        // Step 5: Get snapshot timestamp
        var snapshotAt = await _queryService.GetTickTimestampAsync(tickEnd, ct) ?? DateTime.UtcNow;

        // Total inflow = emission from table + additional transfers from other addresses
        var totalInflow = emissionFromTable + additionalInflow.TotalAmount;

        // Build stats DTO
        var stats = new MinerFlowStatsDto(
            Epoch: currentEpoch,
            SnapshotAt: snapshotAt,
            TickStart: tickStart,
            TickEnd: tickEnd,
            EmissionEpoch: emissionEpoch,
            TotalEmission: emissionFromTable,
            ComputorCount: (ushort)computorAddresses.Count,
            TotalOutflow: outflow.TotalAmount,
            OutflowTxCount: outflow.TransactionCount,
            FlowToExchangeDirect: flowAnalysis.FlowToExchangeByHop.GetValueOrDefault(1, 0),
            FlowToExchange1Hop: flowAnalysis.FlowToExchangeByHop.GetValueOrDefault(2, 0),
            FlowToExchange2Hop: flowAnalysis.FlowToExchangeByHop.GetValueOrDefault(3, 0),
            FlowToExchange3Plus: flowAnalysis.FlowToExchangeByHop
                .Where(kvp => kvp.Key >= 4)
                .Sum(kvp => kvp.Value),
            FlowToExchangeTotal: flowAnalysis.TotalFlowToExchange,
            FlowToExchangeCount: flowAnalysis.ExchangeTransactionCount,
            FlowToOther: outflow.TotalAmount - flowAnalysis.TotalFlowToExchange,
            MinerNetPosition: totalInflow - outflow.TotalAmount,
            Hop1Volume: flowAnalysis.VolumeByHop.GetValueOrDefault(1, 0),
            Hop2Volume: flowAnalysis.VolumeByHop.GetValueOrDefault(2, 0),
            Hop3Volume: flowAnalysis.VolumeByHop.GetValueOrDefault(3, 0),
            Hop4PlusVolume: flowAnalysis.VolumeByHop
                .Where(kvp => kvp.Key >= 4)
                .Sum(kvp => kvp.Value)
        );

        // Save aggregated stats
        await _queryService.SaveMinerFlowStatsAsync(stats, ct);

        _logger.LogInformation(
            "Flow analysis complete: emission={Emission}, outflow={Outflow}, toExchange={ToExchange}",
            stats.TotalEmission, stats.TotalOutflow, stats.FlowToExchangeTotal);

        return stats;
    }

    /// <summary>
    /// Tracks money flow with continuous tracking across tick windows.
    /// Redesigned implementation that:
    /// 1. Uses logs table (log_type = 0) as the single source of truth
    /// 2. Tracks effective pending in real-time within each window
    /// 3. Uses log-based Qutil mapping instead of payload parsing
    /// 4. Always updates source sent amounts after processing transfers
    /// 5. Uses TransferProcessor class for clean, encapsulated logic
    /// </summary>
    private async Task<FlowAnalysisResult> TrackFlowWithContinuousTrackingAsync(
        uint currentEpoch,
        uint emissionEpoch,
        HashSet<string> computorAddresses,
        HashSet<string> exchangeAddresses,
        ulong tickStart,
        ulong tickEnd,
        CancellationToken ct)
    {
        var result = new FlowAnalysisResult();

        // Get smart contract addresses - these are sinks (except Qutil which is pass-through)
        await _labelService.EnsureFreshDataAsync();
        var smartContractInfos = _labelService.GetAddressesByType(AddressType.SmartContract);
        var smartContractAddresses = smartContractInfos.Select(sc => sc.Address).ToHashSet();

        // Find Qutil address by contract index
        var qutilAddress = smartContractInfos
            .FirstOrDefault(sc => sc.ContractIndex == QutilContractIndex)?.Address;

        if (qutilAddress != null)
        {
            _logger.LogDebug("Qutil address identified: {Address}", qutilAddress);
        }

        // Step 1: Initialize tracking state if this is the first window for this emission epoch
        if (!await _queryService.IsTrackingInitializedAsync(emissionEpoch, ct))
        {
            _logger.LogInformation("Initializing tracking state for emission epoch {Epoch}", emissionEpoch);

            // Get individual computor emissions
            var computorEmissions = await _queryService.GetComputorEmissionsAsync(emissionEpoch, ct);
            if (computorEmissions.Count > 0)
            {
                await _queryService.InitializeTrackingStateForComputorsAsync(emissionEpoch, computorEmissions, ct);
                _logger.LogInformation("Initialized tracking for {Count} computors with emissions", computorEmissions.Count);
            }
            else
            {
                _logger.LogWarning("No computor emissions found for epoch {Epoch}, cannot initialize tracking", emissionEpoch);
            }
        }

        // Step 2: Load all pending (non-complete) addresses to track
        var pendingAddresses = await _queryService.GetPendingTrackingAddressesAsync(emissionEpoch, ct);
        if (pendingAddresses.Count == 0)
        {
            _logger.LogDebug("No pending addresses to track for emission epoch {Epoch}", emissionEpoch);
            return result;
        }

        _logger.LogInformation(
            "Tracking {Count} pending address-origin pairs for emission epoch {Epoch} (computors: {Computors}, intermediaries: {Intermediaries})",
            pendingAddresses.Count,
            emissionEpoch,
            pendingAddresses.Count(a => a.AddressType == "computor"),
            pendingAddresses.Count(a => a.AddressType == "intermediary"));

        // Step 3: Build Qutil output mapping from logs (the authoritative source)
        var qutilMapping = new Dictionary<string, List<QutilOutput>>();
        if (qutilAddress != null)
        {
            qutilMapping = await _queryService.BuildQutilOutputMappingAsync(qutilAddress, tickStart, tickEnd, ct);
            _logger.LogDebug("Built Qutil mapping with {Count} tick entries", qutilMapping.Count);
        }

        // Step 4: Get all outgoing transfers from pending addresses (with log_id for ordering)
        var addressesToQuery = pendingAddresses.Select(a => a.Address).ToHashSet();
        var transfers = await _queryService.GetOutgoingTransfersWithLogIdAsync(addressesToQuery, tickStart, tickEnd, ct);

        _logger.LogDebug("Found {Count} outgoing transfers from pending addresses", transfers.Count);

        // Step 5: Create TransferProcessor and process all transfers
        var processor = new TransferProcessor(
            pendingAddresses,
            qutilMapping,
            exchangeAddresses,
            smartContractAddresses,
            computorAddresses,
            qutilAddress,
            currentEpoch,
            emissionEpoch,
            MaxHops,
            _logger
        );

        // Build a cache for address labels to avoid repeated DB queries
        var labelCache = new Dictionary<string, string?>();
        Func<string, string?> getLabelSync = (address) =>
        {
            if (!labelCache.ContainsKey(address))
            {
                // We'll populate this lazily - for now return null
                // The actual labels are populated below
                return null;
            }
            return labelCache[address];
        };

        // Pre-fetch labels for known exchange/smart contract destinations
        var potentialLabelAddresses = transfers
            .Select(t => t.DestAddress)
            .Where(a => exchangeAddresses.Contains(a) || smartContractAddresses.Contains(a))
            .Distinct()
            .ToList();

        // Also add Qutil output destinations
        foreach (var outputs in qutilMapping.Values)
        {
            foreach (var output in outputs)
            {
                if (exchangeAddresses.Contains(output.DestAddress) || smartContractAddresses.Contains(output.DestAddress))
                {
                    if (!potentialLabelAddresses.Contains(output.DestAddress))
                        potentialLabelAddresses.Add(output.DestAddress);
                }
            }
        }

        // Fetch all labels in parallel
        foreach (var addr in potentialLabelAddresses)
        {
            labelCache[addr] = await _queryService.GetAddressLabelAsync(addr, ct);
        }

        // Update the label function to use the cache
        getLabelSync = (address) => labelCache.GetValueOrDefault(address);

        // Process each transfer in order (they're already sorted by tick_number, log_id)
        foreach (var transfer in transfers)
        {
            processor.ProcessTransfer(transfer, getLabelSync);
        }

        // Step 6: Save tracking state updates
        var stateUpdates = processor.StateUpdates;
        if (stateUpdates.Count > 0)
        {
            // Build lookup for original tracking state
            var trackingState = pendingAddresses.ToDictionary(
                a => (a.Address, a.OriginAddress),
                a => a);

            await _queryService.UpdateTrackingStateAsync(emissionEpoch, tickEnd, stateUpdates.Values.ToList(), ct);
            _logger.LogInformation(
                "Updated tracking state: {Updates} address-origin pairs, {NewIntermediaries} new intermediary entries",
                stateUpdates.Count,
                stateUpdates.Values.Count(u => u.AddressType == "intermediary" && !trackingState.ContainsKey((u.Address, u.OriginAddress))));
        }

        // Build and return result
        return processor.BuildResult();
    }

    /// <summary>
    /// Tracks money flow from origin addresses through multiple hops using BFS.
    /// This is the original simple version that tracks within a single tick window.
    /// </summary>
    private async Task<FlowAnalysisResult> TrackFlowThroughHopsAsync(
        uint epoch,
        uint emissionEpoch,
        HashSet<string> originAddresses,
        HashSet<string> exchangeAddresses,
        ulong tickStart,
        ulong tickEnd,
        CancellationToken ct)
    {
        var result = new FlowAnalysisResult();
        var visited = new HashSet<string>(originAddresses); // Don't track back to origins
        var currentLevelAddresses = new HashSet<string>(originAddresses);

        for (int hop = 1; hop <= MaxHops && currentLevelAddresses.Count > 0; hop++)
        {
            // Get all outgoing transfers from current level addresses
            var transfers = await _queryService.GetOutgoingTransfersAsync(
                currentLevelAddresses, tickStart, tickEnd, ct);

            if (transfers.Count == 0)
            {
                _logger.LogDebug("No more transfers at hop {Hop}", hop);
                break;
            }

            var nextLevelAddresses = new HashSet<string>();
            decimal hopVolume = 0;
            decimal hopToExchange = 0;

            foreach (var transfer in transfers)
            {
                hopVolume += transfer.Amount;

                // Determine origin (the computor this flow originated from)
                var origin = originAddresses.Contains(transfer.SourceAddress)
                    ? transfer.SourceAddress
                    : transfer.SourceAddress; // For deeper hops, we'd need to track origin through the chain

                // Check if destination is an exchange
                var isExchange = exchangeAddresses.Contains(transfer.DestAddress);
                var destType = isExchange ? "exchange" : "";
                var destLabel = isExchange
                    ? await _queryService.GetAddressLabelAsync(transfer.DestAddress, ct)
                    : null;

                if (isExchange)
                {
                    hopToExchange += transfer.Amount;
                    result.ExchangeTransactionCount++;
                }

                // Record hop
                result.Hops.Add(new FlowHopRecord(
                    Epoch: epoch,
                    EmissionEpoch: emissionEpoch,
                    TickNumber: transfer.TickNumber,
                    Timestamp: transfer.Timestamp,
                    TxHash: transfer.TxHash,
                    SourceAddress: transfer.SourceAddress,
                    DestAddress: transfer.DestAddress,
                    Amount: transfer.Amount,
                    OriginAddress: origin,
                    OriginType: "computor",
                    HopLevel: (byte)hop,
                    DestType: destType,
                    DestLabel: destLabel ?? ""
                ));

                // Add destination to next level if not visited and not an exchange (terminal)
                if (!visited.Contains(transfer.DestAddress) && !isExchange)
                {
                    nextLevelAddresses.Add(transfer.DestAddress);
                    visited.Add(transfer.DestAddress);
                }
            }

            result.VolumeByHop[hop] = hopVolume;
            result.FlowToExchangeByHop[hop] = hopToExchange;
            result.TotalFlowToExchange += hopToExchange;

            _logger.LogDebug(
                "Hop {Hop}: {Count} transfers, volume={Volume}, toExchange={ToExchange}",
                hop, transfers.Count, hopVolume, hopToExchange);

            currentLevelAddresses = nextLevelAddresses;
        }

        return result;
    }

    /// <summary>
    /// Gets flow visualization data for Sankey diagram.
    /// </summary>
    public async Task<FlowVisualizationDto?> GetFlowVisualizationAsync(
        uint epoch,
        ulong tickStart,
        ulong tickEnd,
        int maxDepth = 5,
        CancellationToken ct = default)
    {
        // Get flow hops from database
        var hops = await _queryService.GetFlowHopsAsync(epoch, tickStart, tickEnd, maxDepth, ct);

        if (hops.Count == 0)
        {
            return null;
        }

        // First pass: determine the minimum depth for each node
        // A node's depth should be the minimum hop level at which it appears as a source
        // This ensures consistent column placement (nodes appear at their earliest position in the flow)
        var nodeMinDepth = new Dictionary<string, int>();
        var nodeTypes = new Dictionary<string, string>();
        var nodeLabels = new Dictionary<string, string?>();

        // First, identify all nodes that appear as sources at hop level 1 (these are computors)
        foreach (var hop in hops.Where(h => h.HopLevel == 1))
        {
            nodeMinDepth[hop.SourceAddress] = 0; // Computors are at depth 0
            nodeTypes[hop.SourceAddress] = "computor";
            if (!string.IsNullOrEmpty(hop.SourceLabel))
                nodeLabels[hop.SourceAddress] = hop.SourceLabel;
        }

        // Then process all hops to determine depths for other nodes
        // Sort by hop level to ensure we process lower levels first
        foreach (var hop in hops.OrderBy(h => h.HopLevel))
        {
            // For source nodes that aren't computors (appear as source in hop > 1)
            if (!nodeMinDepth.ContainsKey(hop.SourceAddress))
            {
                // This is an intermediary appearing as a source
                // Its depth should be hopLevel - 1 (it was a destination at the previous hop)
                nodeMinDepth[hop.SourceAddress] = hop.HopLevel - 1;
                nodeTypes[hop.SourceAddress] = "intermediary";
            }
            if (!string.IsNullOrEmpty(hop.SourceLabel) && !nodeLabels.ContainsKey(hop.SourceAddress))
                nodeLabels[hop.SourceAddress] = hop.SourceLabel;

            // For destination nodes
            if (!nodeMinDepth.ContainsKey(hop.DestAddress))
            {
                // Destination depth is the hop level
                nodeMinDepth[hop.DestAddress] = hop.HopLevel;
                nodeTypes[hop.DestAddress] = hop.DestType ?? "unknown";
            }
            else
            {
                // If node already exists, keep the minimum depth
                var existingDepth = nodeMinDepth[hop.DestAddress];
                if (hop.HopLevel < existingDepth)
                {
                    nodeMinDepth[hop.DestAddress] = hop.HopLevel;
                }
            }
            if (!string.IsNullOrEmpty(hop.DestLabel) && !nodeLabels.ContainsKey(hop.DestAddress))
                nodeLabels[hop.DestAddress] = hop.DestLabel;
        }

        // Build nodes and links
        var nodes = new Dictionary<string, FlowVisualizationNodeDto>();
        var links = new Dictionary<(string, string), (decimal Amount, uint Count)>();

        // Create all nodes with their determined depths
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

        // Process hops to update flows and links
        foreach (var hop in hops)
        {
            // Update flows
            var sourceNode = nodes[hop.SourceAddress];
            nodes[hop.SourceAddress] = sourceNode with { TotalOutflow = sourceNode.TotalOutflow + hop.Amount };

            var destNode = nodes[hop.DestAddress];
            nodes[hop.DestAddress] = destNode with { TotalInflow = destNode.TotalInflow + hop.Amount };

            // Link
            var linkKey = (hop.SourceAddress, hop.DestAddress);
            if (links.TryGetValue(linkKey, out var existing))
            {
                links[linkKey] = (existing.Amount + hop.Amount, existing.Count + 1);
            }
            else
            {
                links[linkKey] = (hop.Amount, 1);
            }
        }

        var visualizationLinks = links.Select(kvp => new FlowVisualizationLinkDto(
            SourceId: kvp.Key.Item1,
            TargetId: kvp.Key.Item2,
            Amount: kvp.Value.Amount,
            TransactionCount: kvp.Value.Count
        )).ToList();

        return new FlowVisualizationDto(
            Epoch: epoch,
            TickStart: tickStart,
            TickEnd: tickEnd,
            Nodes: nodes.Values.ToList(),
            Links: visualizationLinks,
            MaxDepth: hops.Max(h => h.HopLevel),
            TotalTrackedVolume: hops.Where(h => h.HopLevel == 1).Sum(h => h.Amount)
        );
    }

    /// <summary>
    /// Gets flow visualization data by emission epoch.
    /// This returns ALL hops tracked for a specific emission epoch across all tick windows.
    /// Use this for visualizing the complete flow from computor emission to exchanges.
    /// </summary>
    public async Task<FlowVisualizationDto?> GetFlowVisualizationByEmissionEpochAsync(
        uint emissionEpoch,
        int maxDepth = 10,
        CancellationToken ct = default)
    {
        // Get ALL flow hops for this emission epoch (across all tick windows)
        var hops = await _queryService.GetFlowHopsByEmissionEpochAsync(emissionEpoch, maxDepth, ct);

        if (hops.Count == 0)
        {
            return null;
        }

        // First pass: determine the minimum depth for each node
        // A node's depth should be the minimum hop level at which it appears as a source
        // This ensures consistent column placement (nodes appear at their earliest position in the flow)
        var nodeMinDepth = new Dictionary<string, int>();
        var nodeTypes = new Dictionary<string, string>();
        var nodeLabels = new Dictionary<string, string?>();

        // First, identify all nodes that appear as sources at hop level 1 (these are computors)
        foreach (var hop in hops.Where(h => h.HopLevel == 1))
        {
            nodeMinDepth[hop.SourceAddress] = 0; // Computors are at depth 0
            nodeTypes[hop.SourceAddress] = "computor";
            if (!string.IsNullOrEmpty(hop.SourceLabel))
                nodeLabels[hop.SourceAddress] = hop.SourceLabel;
        }

        // Then process all hops to determine depths for other nodes
        // Sort by hop level to ensure we process lower levels first
        foreach (var hop in hops.OrderBy(h => h.HopLevel))
        {
            // For source nodes that aren't computors (appear as source in hop > 1)
            if (!nodeMinDepth.ContainsKey(hop.SourceAddress))
            {
                // This is an intermediary appearing as a source
                // Its depth should be hopLevel - 1 (it was a destination at the previous hop)
                nodeMinDepth[hop.SourceAddress] = hop.HopLevel - 1;
                nodeTypes[hop.SourceAddress] = "intermediary";
            }
            if (!string.IsNullOrEmpty(hop.SourceLabel) && !nodeLabels.ContainsKey(hop.SourceAddress))
                nodeLabels[hop.SourceAddress] = hop.SourceLabel;

            // For destination nodes
            if (!nodeMinDepth.ContainsKey(hop.DestAddress))
            {
                // Destination depth is the hop level
                nodeMinDepth[hop.DestAddress] = hop.HopLevel;
                nodeTypes[hop.DestAddress] = hop.DestType ?? "unknown";
            }
            else
            {
                // If node already exists, keep the minimum depth
                // This ensures nodes appear at their earliest position in the flow
                var existingDepth = nodeMinDepth[hop.DestAddress];
                if (hop.HopLevel < existingDepth)
                {
                    nodeMinDepth[hop.DestAddress] = hop.HopLevel;
                }
            }
            if (!string.IsNullOrEmpty(hop.DestLabel) && !nodeLabels.ContainsKey(hop.DestAddress))
                nodeLabels[hop.DestAddress] = hop.DestLabel;
        }

        // Build nodes and links
        var nodes = new Dictionary<string, FlowVisualizationNodeDto>();
        var links = new Dictionary<(string, string), (decimal Amount, uint Count)>();

        // Create all nodes with their determined depths
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

        // Process hops to update flows and links
        foreach (var hop in hops)
        {
            // Update flows
            var sourceNode = nodes[hop.SourceAddress];
            nodes[hop.SourceAddress] = sourceNode with { TotalOutflow = sourceNode.TotalOutflow + hop.Amount };

            var destNode = nodes[hop.DestAddress];
            nodes[hop.DestAddress] = destNode with { TotalInflow = destNode.TotalInflow + hop.Amount };

            // Link
            var linkKey = (hop.SourceAddress, hop.DestAddress);
            if (links.TryGetValue(linkKey, out var existing))
            {
                links[linkKey] = (existing.Amount + hop.Amount, existing.Count + 1);
            }
            else
            {
                links[linkKey] = (hop.Amount, 1);
            }
        }

        var visualizationLinks = links.Select(kvp => new FlowVisualizationLinkDto(
            SourceId: kvp.Key.Item1,
            TargetId: kvp.Key.Item2,
            Amount: kvp.Value.Amount,
            TransactionCount: kvp.Value.Count
        )).ToList();

        // Get tick range from the hops
        var minTick = hops.Min(h => h.TickNumber);
        var maxTick = hops.Max(h => h.TickNumber);

        return new FlowVisualizationDto(
            Epoch: emissionEpoch,
            TickStart: minTick,
            TickEnd: maxTick,
            Nodes: nodes.Values.ToList(),
            Links: visualizationLinks,
            MaxDepth: hops.Max(h => h.HopLevel),
            TotalTrackedVolume: hops.Where(h => h.HopLevel == 1).Sum(h => h.Amount)
        );
    }

    /// <summary>
    /// Gets historical miner flow statistics.
    /// TotalEmissionTracked is calculated from emission_imports table (unique per emission epoch).
    /// </summary>
    public async Task<MinerFlowSummaryDto> GetMinerFlowHistoryAsync(
        int limit = 30,
        CancellationToken ct = default)
    {
        var history = await _queryService.GetMinerFlowStatsHistoryAsync(limit, ct);

        var latest = history.FirstOrDefault();

        // Get unique emission epochs from the history and sum their emissions from emission_imports
        // This avoids double-counting when multiple snapshots reference the same emission epoch
        var uniqueEmissionEpochs = history.Select(s => s.EmissionEpoch).Distinct().ToList();
        var totalEmission = await _queryService.GetTotalEmissionsForEpochsAsync(uniqueEmissionEpochs, ct);

        var totalToExchange = history.Sum(s => s.FlowToExchangeTotal);
        var avgPercent = totalEmission > 0 ? (totalToExchange / totalEmission) * 100 : 0;

        return new MinerFlowSummaryDto(
            Latest: latest,
            History: history,
            TotalEmissionTracked: totalEmission,
            TotalFlowToExchange: totalToExchange,
            AverageExchangeFlowPercent: avgPercent
        );
    }

    /// <summary>
    /// Validates flow conservation for an emission epoch.
    /// Checks that:
    /// 1. Computor received amounts match emission
    /// 2. No negative pending amounts exist
    /// 3. Total pending + terminal equals emission
    /// 4. Each hop level's sent equals next level's received
    /// </summary>
    public async Task<FlowValidationResult> ValidateFlowConservationAsync(
        uint emissionEpoch,
        CancellationToken ct = default)
    {
        var errors = new List<string>();
        var warnings = new List<string>();

        // Get total emission for the epoch
        var totalEmission = await _queryService.GetTotalEmissionForEpochAsync(emissionEpoch, ct);
        if (totalEmission == 0)
        {
            errors.Add($"No emission data found for epoch {emissionEpoch}");
            return new FlowValidationResult(
                EmissionEpoch: emissionEpoch,
                IsValid: false,
                TotalEmission: 0,
                ComputorReceivedTotal: 0,
                TotalPending: 0,
                TotalTerminal: 0,
                DiscrepancyAmount: 0,
                Errors: errors,
                Warnings: warnings
            );
        }

        // Get all tracking states
        var allStates = await _queryService.GetAllTrackingStatesAsync(emissionEpoch, ct);
        if (allStates.Count == 0)
        {
            errors.Add($"No tracking state found for epoch {emissionEpoch}");
            return new FlowValidationResult(
                EmissionEpoch: emissionEpoch,
                IsValid: false,
                TotalEmission: totalEmission,
                ComputorReceivedTotal: 0,
                TotalPending: 0,
                TotalTerminal: 0,
                DiscrepancyAmount: totalEmission,
                Errors: errors,
                Warnings: warnings
            );
        }

        // Check 1: Computor received amounts should sum to emission
        var computorStates = allStates.Where(s => s.HopLevel == 1 && s.AddressType == "computor").ToList();
        var computorReceivedTotal = computorStates.Sum(s => s.ReceivedAmount);

        if (Math.Abs(computorReceivedTotal - totalEmission) > 1) // Allow tiny rounding errors
        {
            var diff = totalEmission - computorReceivedTotal;
            if (Math.Abs(diff) > totalEmission * 0.01m) // More than 1% discrepancy
            {
                errors.Add($"Computor received ({computorReceivedTotal:N0}) does not match emission ({totalEmission:N0}). Difference: {diff:N0}");
            }
            else
            {
                warnings.Add($"Minor discrepancy: Computor received ({computorReceivedTotal:N0}) vs emission ({totalEmission:N0}). Difference: {diff:N0}");
            }
        }

        // Check 2: No negative pending amounts
        var negativeStates = allStates.Where(s => s.PendingAmount < 0).ToList();
        if (negativeStates.Count > 0)
        {
            errors.Add($"Found {negativeStates.Count} states with negative pending amounts");
            foreach (var state in negativeStates.Take(5))
            {
                errors.Add($"  - {state.Address} (origin: {state.OriginAddress}): pending = {state.PendingAmount:N0}");
            }
        }

        // Check 3: Total pending + terminal should equal emission
        var totalPending = allStates.Where(s => !s.IsComplete).Sum(s => s.PendingAmount);
        var terminalStates = allStates.Where(s => s.IsTerminal).ToList();
        var totalTerminal = terminalStates.Sum(s => s.ReceivedAmount);

        // Calculate how much has been "accounted for" (sent to terminals + still pending)
        // Note: This is complex because intermediaries can have pending amounts
        var discrepancy = computorReceivedTotal - (totalPending + totalTerminal);

        if (Math.Abs(discrepancy) > computorReceivedTotal * 0.01m && computorReceivedTotal > 0)
        {
            warnings.Add($"Discrepancy in flow conservation: pending ({totalPending:N0}) + terminal ({totalTerminal:N0}) = {totalPending + totalTerminal:N0} vs computor received ({computorReceivedTotal:N0})");
        }

        // Check 4: Validate per-hop-level consistency
        var statesByHopLevel = allStates.GroupBy(s => s.HopLevel).ToDictionary(g => g.Key, g => g.ToList());
        foreach (var (hopLevel, states) in statesByHopLevel.OrderBy(kvp => kvp.Key))
        {
            var totalReceivedAtLevel = states.Sum(s => s.ReceivedAmount);
            var totalSentAtLevel = states.Sum(s => s.SentAmount);
            var totalPendingAtLevel = states.Sum(s => s.PendingAmount);

            // received should equal sent + pending (approximately)
            var levelDiscrepancy = totalReceivedAtLevel - (totalSentAtLevel + totalPendingAtLevel);
            if (Math.Abs(levelDiscrepancy) > 1 && totalReceivedAtLevel > 0)
            {
                warnings.Add($"Hop {hopLevel}: received ({totalReceivedAtLevel:N0}) != sent ({totalSentAtLevel:N0}) + pending ({totalPendingAtLevel:N0}). Diff: {levelDiscrepancy:N0}");
            }
        }

        // Check for states that received more than they sent but aren't marked complete
        var incompleteWithNoSending = allStates
            .Where(s => !s.IsComplete && s.ReceivedAmount > 0 && s.SentAmount == 0 && !s.IsTerminal && s.AddressType != "computor")
            .ToList();

        if (incompleteWithNoSending.Count > 100)
        {
            warnings.Add($"Found {incompleteWithNoSending.Count} intermediaries that received funds but haven't sent any (may need more analysis windows)");
        }

        var isValid = errors.Count == 0;

        _logger.LogInformation(
            "Flow validation for epoch {Epoch}: Valid={Valid}, Emission={Emission}, ComputorReceived={Received}, Pending={Pending}, Terminal={Terminal}, Errors={Errors}, Warnings={Warnings}",
            emissionEpoch, isValid, totalEmission, computorReceivedTotal, totalPending, totalTerminal, errors.Count, warnings.Count);

        return new FlowValidationResult(
            EmissionEpoch: emissionEpoch,
            IsValid: isValid,
            TotalEmission: totalEmission,
            ComputorReceivedTotal: computorReceivedTotal,
            TotalPending: totalPending,
            TotalTerminal: totalTerminal,
            DiscrepancyAmount: discrepancy,
            Errors: errors,
            Warnings: warnings
        );
    }

    /// <summary>
    /// Cleans address by removing trailing unicode characters.
    /// The RPC response includes trailing \u0238 character.
    /// </summary>
    private static string CleanAddress(string address)
    {
        if (string.IsNullOrEmpty(address)) return address;

        // Remove any non-ASCII characters at the end
        var cleaned = address.TrimEnd();
        while (cleaned.Length > 0 && cleaned[^1] > 127)
        {
            cleaned = cleaned[..^1];
        }
        return cleaned;
    }

    // Internal types for flow analysis
    private class FlowAnalysisResult
    {
        public List<FlowHopRecord> Hops { get; } = new();
        public Dictionary<int, decimal> VolumeByHop { get; } = new();
        public Dictionary<int, decimal> FlowToExchangeByHop { get; } = new();
        public decimal TotalFlowToExchange { get; set; }
        public ulong ExchangeTransactionCount { get; set; }
    }

    public record FlowHopRecord(
        uint Epoch,
        uint EmissionEpoch,
        ulong TickNumber,
        DateTime Timestamp,
        string TxHash,
        string SourceAddress,
        string DestAddress,
        decimal Amount,
        string OriginAddress,
        string OriginType,
        byte HopLevel,
        string DestType,
        string DestLabel
    );

    public record TransferRecord(
        ulong TickNumber,
        DateTime Timestamp,
        string TxHash,
        string SourceAddress,
        string DestAddress,
        decimal Amount
    );

    public record FlowSummary(decimal TotalAmount, ulong TransactionCount);

    /// <summary>
    /// Represents a destination from a Qutil SendToManyV1 transaction
    /// </summary>
    public record QutilDestination(string Address, decimal Amount);

    /// <summary>
    /// Parses a Qutil SendToManyV1 input payload to extract destinations and amounts.
    /// Payload structure: 25 addresses (32 bytes each) followed by 25 amounts (8 bytes each, signed int64)
    /// Total: 25 * 32 + 25 * 8 = 800 + 200 = 1000 bytes
    /// </summary>
    /// <param name="inputDataHex">The hex-encoded input data from the transaction</param>
    /// <returns>List of destinations with non-zero amounts</returns>
    public static List<QutilDestination> ParseQutilSendToManyPayload(string? inputDataHex)
    {
        var result = new List<QutilDestination>();

        if (string.IsNullOrEmpty(inputDataHex))
            return result;

        try
        {
            // Remove 0x prefix if present
            var hex = inputDataHex.StartsWith("0x", StringComparison.OrdinalIgnoreCase)
                ? inputDataHex[2..]
                : inputDataHex;

            // Expected size: 25 * 32 + 25 * 8 = 1000 bytes = 2000 hex chars
            var expectedHexLength = QutilMaxDestinations * QutilAddressSize * 2 +
                                    QutilMaxDestinations * QutilAmountSize * 2;

            if (hex.Length < expectedHexLength)
            {
                // Payload too short, might be partial or different procedure
                return result;
            }

            var bytes = Convert.FromHexString(hex[..expectedHexLength]);

            // Parse addresses (first 800 bytes = 25 * 32)
            var addresses = new string[QutilMaxDestinations];
            for (var i = 0; i < QutilMaxDestinations; i++)
            {
                var offset = i * QutilAddressSize;
                var addressBytes = bytes[offset..(offset + QutilAddressSize)];
                addresses[i] = BytesToQubicAddress(addressBytes);
            }

            // Parse amounts (next 200 bytes = 25 * 8)
            var amountsOffset = QutilMaxDestinations * QutilAddressSize;
            for (var i = 0; i < QutilMaxDestinations; i++)
            {
                var offset = amountsOffset + i * QutilAmountSize;
                var amount = BitConverter.ToInt64(bytes, offset); // Little-endian signed int64

                // Skip zero amounts and zero addresses
                if (amount > 0 && !IsZeroAddress(addresses[i]))
                {
                    result.Add(new QutilDestination(addresses[i], amount));
                }
            }
        }
        catch (Exception)
        {
            // If parsing fails, return empty list - caller will fall back to log-based tracking
        }

        return result;
    }

    /// <summary>
    /// Converts a 32-byte public key to Qubic's base26 address format.
    /// Uses the Qubic.Crypto library for proper encoding.
    /// </summary>
    private static string BytesToQubicAddress(byte[] bytes)
    {
        if (bytes.Length != 32)
            return string.Empty;

        // Check if it's a zero address (all zeros = burn address = AAAA...AAAA)
        if (bytes.All(b => b == 0))
            return "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

        return QubicCrypt.GetIdentityFromPublicKey(bytes);
    }

    /// <summary>
    /// Checks if an address is the zero/burn address (all A's)
    /// </summary>
    private static bool IsZeroAddress(string address)
    {
        return string.IsNullOrEmpty(address) ||
               address.All(c => c == 'A') ||
               address == AddressLabelService.BurnAddress;
    }

    /// <summary>
    /// Output from Qutil's SendToMany operation, extracted from logs.
    /// </summary>
    public record QutilOutput(string DestAddress, decimal Amount, ulong TickNumber, string TxHash);

    /// <summary>
    /// Extended transfer record that includes log_id for deterministic ordering.
    /// </summary>
    public record TransferRecordWithLogId(
        ulong TickNumber,
        uint LogId,
        DateTime Timestamp,
        string TxHash,
        string SourceAddress,
        string DestAddress,
        decimal Amount
    );

    /// <summary>
    /// Processes transfers and maintains proper state tracking for flow analysis.
    /// This class encapsulates the core transfer processing logic to:
    /// 1. Track effective pending amounts in real-time (not stale values)
    /// 2. Always update source sent amounts after processing transfers
    /// 3. Handle Qutil pass-through transparently using log-based mapping
    /// 4. Properly attribute transfers to multiple origins proportionally
    /// </summary>
    private class TransferProcessor
    {
        private readonly Dictionary<(string Address, string Origin), FlowTrackingUpdateDto> _stateUpdates = new();
        private readonly List<FlowHopRecord> _hops = new();
        private readonly Dictionary<string, List<FlowTrackingStateDto>> _pendingByAddress;
        private readonly Dictionary<string, List<QutilOutput>> _qutilMappingByTick;
        private readonly HashSet<string> _exchangeAddresses;
        private readonly HashSet<string> _smartContractAddresses;
        private readonly HashSet<string> _computorAddresses;
        private readonly string? _qutilAddress;
        private readonly uint _currentEpoch;
        private readonly uint _emissionEpoch;
        private readonly int _maxHops;
        private readonly ILogger _logger;

        // Results
        public IReadOnlyList<FlowHopRecord> Hops => _hops;
        public IReadOnlyDictionary<(string Address, string Origin), FlowTrackingUpdateDto> StateUpdates => _stateUpdates;
        public Dictionary<int, decimal> VolumeByHop { get; } = new();
        public Dictionary<int, decimal> FlowToExchangeByHop { get; } = new();
        public decimal TotalFlowToExchange { get; private set; }
        public ulong ExchangeTransactionCount { get; private set; }

        public TransferProcessor(
            List<FlowTrackingStateDto> pendingStates,
            Dictionary<string, List<QutilOutput>> qutilMappingByTick,
            HashSet<string> exchangeAddresses,
            HashSet<string> smartContractAddresses,
            HashSet<string> computorAddresses,
            string? qutilAddress,
            uint currentEpoch,
            uint emissionEpoch,
            int maxHops,
            ILogger logger)
        {
            _qutilMappingByTick = qutilMappingByTick;
            _exchangeAddresses = exchangeAddresses;
            _smartContractAddresses = smartContractAddresses;
            _computorAddresses = computorAddresses;
            _qutilAddress = qutilAddress;
            _currentEpoch = currentEpoch;
            _emissionEpoch = emissionEpoch;
            _maxHops = maxHops;
            _logger = logger;

            // Group pending states by address
            _pendingByAddress = pendingStates
                .GroupBy(s => s.Address)
                .ToDictionary(g => g.Key, g => g.ToList());
        }

        /// <summary>
        /// Gets the effective pending amount for an address-origin pair.
        /// Checks _stateUpdates first (for in-flight changes), then falls back to original state.
        /// This is CRITICAL for correct proportional attribution when processing multiple
        /// transfers from the same source in a single window.
        /// </summary>
        public decimal GetEffectivePending(string address, string origin)
        {
            var key = (address, origin);
            if (_stateUpdates.TryGetValue(key, out var update))
            {
                return update.PendingAmount;
            }

            if (_pendingByAddress.TryGetValue(address, out var states))
            {
                var state = states.FirstOrDefault(s => s.OriginAddress == origin);
                if (state != null)
                {
                    return state.PendingAmount;
                }
            }

            return 0;
        }

        /// <summary>
        /// Gets all source states for an address that have positive effective pending.
        /// </summary>
        private List<(FlowTrackingStateDto State, decimal EffectivePending)> GetSourceStatesWithPending(string address)
        {
            if (!_pendingByAddress.TryGetValue(address, out var states))
                return new List<(FlowTrackingStateDto, decimal)>();

            var result = new List<(FlowTrackingStateDto, decimal)>();
            foreach (var state in states)
            {
                var pending = GetEffectivePending(address, state.OriginAddress);
                if (pending > 0)
                {
                    result.Add((state, pending));
                }
            }
            return result;
        }

        /// <summary>
        /// Processes a single transfer, handling all the complexity of:
        /// - Multi-origin proportional attribution
        /// - Qutil pass-through (log-based)
        /// - Source sent amount updates
        /// - Destination tracking state creation
        /// </summary>
        public void ProcessTransfer(TransferRecordWithLogId transfer, Func<string, string?> getLabelSync)
        {
            var sourceStates = GetSourceStatesWithPending(transfer.SourceAddress);
            if (sourceStates.Count == 0) return;

            var totalEffectivePending = sourceStates.Sum(s => s.EffectivePending);
            if (totalEffectivePending <= 0) return;

            var hopLevel = sourceStates[0].State.HopLevel;

            // Check if this is a Qutil transfer
            var isQutil = _qutilAddress != null && transfer.DestAddress == _qutilAddress;

            if (isQutil)
            {
                ProcessQutilTransfer(transfer, sourceStates, totalEffectivePending, hopLevel, getLabelSync);
            }
            else
            {
                ProcessDirectTransfer(transfer, sourceStates, totalEffectivePending, hopLevel, getLabelSync);
            }
        }

        /// <summary>
        /// Processes a direct transfer (not through Qutil).
        /// </summary>
        private void ProcessDirectTransfer(
            TransferRecordWithLogId transfer,
            List<(FlowTrackingStateDto State, decimal EffectivePending)> sourceStates,
            decimal totalEffectivePending,
            byte hopLevel,
            Func<string, string?> getLabelSync)
        {
            var isExchange = _exchangeAddresses.Contains(transfer.DestAddress);
            var isSmartContract = _smartContractAddresses.Contains(transfer.DestAddress);
            var isTerminal = isExchange || isSmartContract;

            var destType = isExchange ? "exchange" : (isSmartContract ? "smartcontract" : "intermediary");
            var destLabel = (isExchange || isSmartContract) ? getLabelSync(transfer.DestAddress) : null;

            foreach (var (sourceState, effectivePending) in sourceStates)
            {
                var proportion = effectivePending / totalEffectivePending;
                var attributedAmount = transfer.Amount * proportion;
                if (attributedAmount <= 0) continue;

                // Record the hop
                RecordHop(transfer, sourceState.OriginAddress, hopLevel, attributedAmount, destType, destLabel);

                // Update or create destination tracking state
                UpdateDestinationState(
                    transfer.DestAddress,
                    sourceState.OriginAddress,
                    attributedAmount,
                    (byte)(hopLevel + 1),
                    destType,
                    isTerminal);

                // Update source sent amount (CRITICAL - this was missing in some cases before)
                UpdateSourceSentAmount(sourceState, attributedAmount);

                // Track exchange flow
                if (isExchange)
                {
                    TrackExchangeFlow(hopLevel, attributedAmount);
                }
            }
        }

        /// <summary>
        /// Processes a Qutil transfer using log-based output mapping.
        /// Skips Qutil as a hop and directly attributes to the actual destinations.
        /// </summary>
        private void ProcessQutilTransfer(
            TransferRecordWithLogId transfer,
            List<(FlowTrackingStateDto State, decimal EffectivePending)> sourceStates,
            decimal totalEffectivePending,
            byte hopLevel,
            Func<string, string?> getLabelSync)
        {
            // Look up Qutil outputs for this tick
            var tickKey = $"{transfer.TickNumber}";
            if (!_qutilMappingByTick.TryGetValue(tickKey, out var qutilOutputs) || qutilOutputs.Count == 0)
            {
                _logger.LogDebug("No Qutil outputs found for tick {Tick}, transfer {TxHash}",
                    transfer.TickNumber, transfer.TxHash);
                return;
            }

            // For each Qutil output in the same tick, attribute proportionally to each source origin
            foreach (var qutilOutput in qutilOutputs)
            {
                if (IsZeroAddress(qutilOutput.DestAddress)) continue;
                if (qutilOutput.DestAddress == _qutilAddress) continue; // Skip self-transfers

                var isDestExchange = _exchangeAddresses.Contains(qutilOutput.DestAddress);
                var isDestSmartContract = _smartContractAddresses.Contains(qutilOutput.DestAddress);
                var isDestTerminal = isDestExchange || isDestSmartContract;

                var destType = isDestExchange ? "exchange" : (isDestSmartContract ? "smartcontract" : "intermediary");
                var destLabel = (isDestExchange || isDestSmartContract) ? getLabelSync(qutilOutput.DestAddress) : null;

                foreach (var (sourceState, effectivePending) in sourceStates)
                {
                    var proportion = effectivePending / totalEffectivePending;
                    var attributedAmount = qutilOutput.Amount * proportion;
                    if (attributedAmount <= 0) continue;

                    // Record hop directly from sender to Qutil destination (skip Qutil)
                    var hopRecord = new FlowHopRecord(
                        Epoch: _currentEpoch,
                        EmissionEpoch: _emissionEpoch,
                        TickNumber: transfer.TickNumber,
                        Timestamp: transfer.Timestamp,
                        TxHash: transfer.TxHash,
                        SourceAddress: transfer.SourceAddress,
                        DestAddress: qutilOutput.DestAddress,
                        Amount: attributedAmount,
                        OriginAddress: sourceState.OriginAddress,
                        OriginType: "computor",
                        HopLevel: hopLevel,
                        DestType: destType,
                        DestLabel: destLabel ?? ""
                    );
                    _hops.Add(hopRecord);

                    // Update volume tracking
                    if (!VolumeByHop.ContainsKey(hopLevel))
                        VolumeByHop[hopLevel] = 0;
                    VolumeByHop[hopLevel] += attributedAmount;

                    // Update destination state
                    UpdateDestinationState(
                        qutilOutput.DestAddress,
                        sourceState.OriginAddress,
                        attributedAmount,
                        (byte)(hopLevel + 1),
                        destType,
                        isDestTerminal);

                    // Track exchange flow
                    if (isDestExchange)
                    {
                        TrackExchangeFlow(hopLevel, attributedAmount);
                    }
                }
            }

            // CRITICAL: Update source sent amounts proportionally
            // We use the input transfer amount (what was sent to Qutil) for attribution
            foreach (var (sourceState, effectivePending) in sourceStates)
            {
                var proportion = effectivePending / totalEffectivePending;
                var attributedSent = transfer.Amount * proportion;
                UpdateSourceSentAmount(sourceState, attributedSent);
            }
        }

        /// <summary>
        /// Records a hop and updates volume tracking.
        /// </summary>
        private void RecordHop(
            TransferRecordWithLogId transfer,
            string originAddress,
            byte hopLevel,
            decimal amount,
            string destType,
            string? destLabel)
        {
            var hop = new FlowHopRecord(
                Epoch: _currentEpoch,
                EmissionEpoch: _emissionEpoch,
                TickNumber: transfer.TickNumber,
                Timestamp: transfer.Timestamp,
                TxHash: transfer.TxHash,
                SourceAddress: transfer.SourceAddress,
                DestAddress: transfer.DestAddress,
                Amount: amount,
                OriginAddress: originAddress,
                OriginType: "computor",
                HopLevel: hopLevel,
                DestType: destType,
                DestLabel: destLabel ?? ""
            );
            _hops.Add(hop);

            if (!VolumeByHop.ContainsKey(hopLevel))
                VolumeByHop[hopLevel] = 0;
            VolumeByHop[hopLevel] += amount;
        }

        /// <summary>
        /// Updates or creates destination tracking state.
        /// </summary>
        private void UpdateDestinationState(
            string destAddress,
            string originAddress,
            decimal amount,
            byte destHopLevel,
            string destType,
            bool isTerminal)
        {
            var destKey = (destAddress, originAddress);

            // Skip tracking back to computors
            if (_computorAddresses.Contains(destAddress)) return;

            // Skip if we've hit max hops and it's not terminal
            if (destHopLevel > _maxHops && !isTerminal) return;

            if (_stateUpdates.TryGetValue(destKey, out var existing))
            {
                // Update existing state
                _stateUpdates[destKey] = existing with
                {
                    ReceivedAmount = existing.ReceivedAmount + amount,
                    PendingAmount = isTerminal ? 0 : existing.PendingAmount + amount
                };
            }
            else if (_pendingByAddress.TryGetValue(destAddress, out var existingStates))
            {
                // Check if there's an existing state for this origin
                var existingState = existingStates.FirstOrDefault(s => s.OriginAddress == originAddress);
                if (existingState != null)
                {
                    // Create update from existing state
                    _stateUpdates[destKey] = new FlowTrackingUpdateDto(
                        Address: destAddress,
                        AddressType: existingState.AddressType,
                        OriginAddress: originAddress,
                        ReceivedAmount: existingState.ReceivedAmount + amount,
                        SentAmount: existingState.SentAmount,
                        PendingAmount: isTerminal ? 0 : existingState.PendingAmount + amount,
                        HopLevel: existingState.HopLevel,
                        IsTerminal: isTerminal,
                        IsComplete: isTerminal
                    );
                }
                else
                {
                    // New origin for existing address
                    _stateUpdates[destKey] = new FlowTrackingUpdateDto(
                        Address: destAddress,
                        AddressType: isTerminal ? destType : "intermediary",
                        OriginAddress: originAddress,
                        ReceivedAmount: amount,
                        SentAmount: 0,
                        PendingAmount: isTerminal ? 0 : amount,
                        HopLevel: destHopLevel,
                        IsTerminal: isTerminal,
                        IsComplete: isTerminal
                    );
                }
            }
            else
            {
                // Completely new address-origin pair
                _stateUpdates[destKey] = new FlowTrackingUpdateDto(
                    Address: destAddress,
                    AddressType: isTerminal ? destType : "intermediary",
                    OriginAddress: originAddress,
                    ReceivedAmount: amount,
                    SentAmount: 0,
                    PendingAmount: isTerminal ? 0 : amount,
                    HopLevel: destHopLevel,
                    IsTerminal: isTerminal,
                    IsComplete: isTerminal
                );
            }
        }

        /// <summary>
        /// Updates source sent amount and pending amount.
        /// This is CRITICAL - the old code sometimes missed this for Qutil transfers.
        /// </summary>
        private void UpdateSourceSentAmount(FlowTrackingStateDto sourceState, decimal sentAmount)
        {
            var sourceKey = (sourceState.Address, sourceState.OriginAddress);

            if (_stateUpdates.TryGetValue(sourceKey, out var existing))
            {
                var newPending = existing.PendingAmount - sentAmount;
                _stateUpdates[sourceKey] = existing with
                {
                    SentAmount = existing.SentAmount + sentAmount,
                    PendingAmount = Math.Max(0, newPending),
                    IsComplete = newPending <= 0
                };
            }
            else
            {
                var newPending = sourceState.PendingAmount - sentAmount;
                _stateUpdates[sourceKey] = new FlowTrackingUpdateDto(
                    Address: sourceState.Address,
                    AddressType: sourceState.AddressType,
                    OriginAddress: sourceState.OriginAddress,
                    ReceivedAmount: sourceState.ReceivedAmount,
                    SentAmount: sourceState.SentAmount + sentAmount,
                    PendingAmount: Math.Max(0, newPending),
                    HopLevel: sourceState.HopLevel,
                    IsTerminal: sourceState.IsTerminal,
                    IsComplete: newPending <= 0
                );
            }
        }

        /// <summary>
        /// Tracks flow to exchange for statistics.
        /// </summary>
        private void TrackExchangeFlow(int hopLevel, decimal amount)
        {
            if (!FlowToExchangeByHop.ContainsKey(hopLevel))
                FlowToExchangeByHop[hopLevel] = 0;
            FlowToExchangeByHop[hopLevel] += amount;
            TotalFlowToExchange += amount;
            ExchangeTransactionCount++;
        }

        /// <summary>
        /// Builds the flow analysis result from processed data.
        /// </summary>
        public FlowAnalysisResult BuildResult()
        {
            var result = new FlowAnalysisResult
            {
                TotalFlowToExchange = TotalFlowToExchange,
                ExchangeTransactionCount = ExchangeTransactionCount
            };
            result.Hops.AddRange(_hops);
            foreach (var kvp in VolumeByHop)
                result.VolumeByHop[kvp.Key] = kvp.Value;
            foreach (var kvp in FlowToExchangeByHop)
                result.FlowToExchangeByHop[kvp.Key] = kvp.Value;
            return result;
        }
    }
}
