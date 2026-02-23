using QubicExplorer.Shared.DTOs;
using QubicExplorer.Shared.Services;

namespace QubicExplorer.Analytics.Services;

/// <summary>
/// Processes custom flow tracking jobs registered by users.
/// Tracks outgoing transfers from user-specified addresses through intermediaries
/// until funds reach terminal destinations (exchanges, smart contracts).
/// </summary>
public class CustomFlowTrackingService
{
    private const int QutilContractIndex = 4;
    private const int MaxJobsPerCycle = 5;
    private const int TickWindowSize = 50000; // ~4h of ticks

    private readonly AnalyticsQueryService _queryService;
    private readonly AddressLabelService _labelService;
    private readonly ILogger<CustomFlowTrackingService> _logger;

    public CustomFlowTrackingService(
        AnalyticsQueryService queryService,
        AddressLabelService labelService,
        ILogger<CustomFlowTrackingService> logger)
    {
        _queryService = queryService;
        _labelService = labelService;
        _logger = logger;
    }

    /// <summary>
    /// Called by AnalyticsSnapshotService each cycle to process pending jobs.
    /// </summary>
    public async Task ProcessPendingJobsAsync(CancellationToken ct)
    {
        var jobs = await _queryService.GetPendingCustomFlowJobsAsync(MaxJobsPerCycle, ct);
        if (jobs.Count == 0) return;

        _logger.LogInformation("Processing {Count} custom flow tracking jobs", jobs.Count);

        // Get current network tick for upper bound
        var currentTick = await _queryService.GetCurrentTickAsync(ct);
        if (currentTick == null)
        {
            _logger.LogWarning("Could not get current tick, skipping custom flow processing");
            return;
        }

        foreach (var job in jobs)
        {
            if (ct.IsCancellationRequested) break;

            try
            {
                await ProcessJobAsync(job, currentTick.Value, ct);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing custom flow job {JobId}", job.JobId);
                await _queryService.UpdateCustomFlowJobStatusAsync(
                    job.JobId, "processing", job.LastProcessedTick,
                    job.TotalHopsRecorded, job.TotalTerminalAmount, job.TotalPendingAmount,
                    ex.Message, ct);
            }
        }
    }

    private async Task ProcessJobAsync(CustomFlowJobDto job, ulong currentNetworkTick, CancellationToken ct)
    {
        _logger.LogInformation(
            "Processing custom flow job {JobId} (alias: {Alias}, status: {Status}, lastTick: {LastTick})",
            job.JobId, job.Alias, job.Status, job.LastProcessedTick);

        // Determine tick window
        var tickStart = job.LastProcessedTick > 0 ? job.LastProcessedTick + 1 : job.StartTick;
        var tickEnd = Math.Min(tickStart + TickWindowSize - 1, currentNetworkTick);

        if (tickStart > currentNetworkTick)
        {
            _logger.LogDebug("Job {JobId} is caught up to current tick", job.JobId);
            return;
        }

        // Initialize tracking state if first run
        if (!await _queryService.IsCustomTrackingInitializedAsync(job.JobId, ct))
        {
            _logger.LogInformation("Initializing tracking state for job {JobId} with {Count} addresses",
                job.JobId, job.Addresses.Count);
            await _queryService.InitializeCustomTrackingStateAsync(
                job.JobId, job.Addresses, job.Balances, ct);
        }

        // Load exchange and SC addresses for terminal detection
        var exchangeAddresses = await _queryService.GetAddressesByTypeAsync("exchange", ct);
        var smartContractInfos = _labelService.GetAddressesByType(AddressType.SmartContract);
        var smartContractAddresses = smartContractInfos.Select(sc => sc.Address).ToHashSet();
        var qutilAddress = smartContractInfos
            .FirstOrDefault(sc => sc.ContractIndex == QutilContractIndex)?.Address;

        // Load pending addresses
        var pendingStates = await _queryService.GetCustomPendingAddressesAsync(job.JobId, ct);
        if (pendingStates.Count == 0)
        {
            _logger.LogInformation("Job {JobId} has no pending addresses — marking complete", job.JobId);
            await _queryService.UpdateCustomFlowJobStatusAsync(
                job.JobId, "complete", tickEnd, job.TotalHopsRecorded,
                job.TotalTerminalAmount, 0, null, ct);
            return;
        }

        // Build Qutil output mapping
        var qutilMapping = new Dictionary<string, List<ComputorFlowService.QutilOutput>>();
        if (qutilAddress != null)
        {
            qutilMapping = await _queryService.BuildQutilOutputMappingAsync(qutilAddress, tickStart, tickEnd, ct);
        }

        // Get outgoing transfers from pending addresses
        var addressesToQuery = pendingStates.Select(a => a.Address).ToHashSet();
        var transfers = await _queryService.GetOutgoingTransfersWithLogIdAsync(addressesToQuery, tickStart, tickEnd, ct);

        _logger.LogInformation(
            "Job {JobId}: processing {TransferCount} transfers from {AddressCount} pending addresses (ticks {TickStart}-{TickEnd})",
            job.JobId, transfers.Count, addressesToQuery.Count, tickStart, tickEnd);

        // Process transfers using the same logic as ComputorFlowService
        var processor = new CustomTransferProcessor(
            pendingStates,
            qutilMapping,
            exchangeAddresses,
            smartContractAddresses,
            job.Addresses.ToHashSet(),
            qutilAddress,
            job.MaxHops,
            _logger);

        foreach (var transfer in transfers)
        {
            processor.ProcessTransfer(transfer, addr => _labelService.GetLabel(addr));
        }

        // Save hops
        var hops = processor.Hops.Select(h => new AnalyticsQueryService.CustomFlowHopRecord(
            h.TickNumber, h.Timestamp, h.TxHash,
            h.SourceAddress, h.DestAddress, h.Amount,
            h.OriginAddress, h.HopLevel, h.DestType, h.DestLabel
        )).ToList();

        if (hops.Count > 0)
        {
            await _queryService.SaveCustomFlowHopsAsync(job.JobId, hops, ct);
        }

        // Save state updates
        var stateUpdates = processor.StateUpdates.Values.ToList();
        if (stateUpdates.Count > 0)
        {
            await _queryService.UpdateCustomTrackingStateAsync(job.JobId, tickEnd, stateUpdates, ct);
        }

        // Calculate totals
        var allStates = await _queryService.GetCustomPendingAddressesAsync(job.JobId, ct);
        var allStatesIncComplete = await _queryService.GetCustomFlowAllStatesAsync(job.JobId, ct);
        var totalTerminal = allStatesIncComplete.Where(s => s.IsTerminal).Sum(s => s.ReceivedAmount);
        var totalPending = allStatesIncComplete.Where(s => !s.IsComplete).Sum(s => s.PendingAmount);
        var totalHops = job.TotalHopsRecorded + (ulong)hops.Count;

        // Check if complete
        var isComplete = allStates.Count == 0; // No more pending
        var newStatus = isComplete ? "complete" : "processing";

        await _queryService.UpdateCustomFlowJobStatusAsync(
            job.JobId, newStatus, tickEnd, totalHops,
            totalTerminal, totalPending, null, ct);

        _logger.LogInformation(
            "Job {JobId}: {Status}, {HopCount} new hops, terminal={Terminal}, pending={Pending}",
            job.JobId, newStatus, hops.Count, totalTerminal, totalPending);
    }

    /// <summary>
    /// Gets visualization data for a custom flow job.
    /// </summary>
    public async Task<CustomFlowResultDto?> GetVisualizationAsync(string jobId, CancellationToken ct)
    {
        var job = await _queryService.GetCustomFlowJobAsync(jobId, ct);
        if (job == null) return null;

        var hops = await _queryService.GetCustomFlowHopsAsync(jobId, job.MaxHops, ct);

        // Build visualization using same pattern as MinerFlowController
        var nodeMinDepth = new Dictionary<string, int>();
        var nodeTypes = new Dictionary<string, string>();
        var nodeLabels = new Dictionary<string, string?>();

        // Identify tracked addresses (sources at hop level 1)
        foreach (var hop in hops.Where(h => h.HopLevel == 1))
        {
            nodeMinDepth[hop.SourceAddress] = 0;
            nodeTypes[hop.SourceAddress] = "tracked";
            if (!string.IsNullOrEmpty(hop.SourceLabel))
                nodeLabels[hop.SourceAddress] = hop.SourceLabel;
        }

        // Determine depths for all nodes
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

        // Build nodes
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

        // Build links
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

        var maxDepth = hops.Count > 0 ? hops.Max(h => h.HopLevel) : 0;
        var totalVolume = hops.Where(h => h.HopLevel == 1).Sum(h => h.Amount);

        return new CustomFlowResultDto(
            Job: job,
            Nodes: nodes.Values.ToList(),
            Links: vizLinks,
            MaxDepth: maxDepth,
            TotalTrackedVolume: totalVolume
        );
    }

    // =====================================================
    // TRANSFER PROCESSOR (simplified from ComputorFlowService)
    // =====================================================

    private record HopRecord(
        ulong TickNumber,
        DateTime Timestamp,
        string TxHash,
        string SourceAddress,
        string DestAddress,
        decimal Amount,
        string OriginAddress,
        byte HopLevel,
        string DestType,
        string DestLabel
    );

    private class CustomTransferProcessor
    {
        private readonly Dictionary<(string Address, string Origin), FlowTrackingUpdateDto> _stateUpdates = new();
        private readonly List<HopRecord> _hops = new();
        private readonly Dictionary<string, List<CustomFlowTrackingStateDto>> _pendingByAddress;
        private readonly Dictionary<string, List<ComputorFlowService.QutilOutput>> _qutilMappingByTick;
        private readonly HashSet<string> _exchangeAddresses;
        private readonly HashSet<string> _smartContractAddresses;
        private readonly HashSet<string> _trackedAddresses;
        private readonly string? _qutilAddress;
        private readonly byte _maxHops;
        private readonly ILogger _logger;

        public IReadOnlyList<HopRecord> Hops => _hops;
        public IReadOnlyDictionary<(string Address, string Origin), FlowTrackingUpdateDto> StateUpdates => _stateUpdates;

        public CustomTransferProcessor(
            List<CustomFlowTrackingStateDto> pendingStates,
            Dictionary<string, List<ComputorFlowService.QutilOutput>> qutilMapping,
            HashSet<string> exchangeAddresses,
            HashSet<string> smartContractAddresses,
            HashSet<string> trackedAddresses,
            string? qutilAddress,
            byte maxHops,
            ILogger logger)
        {
            _pendingByAddress = pendingStates
                .GroupBy(s => s.Address)
                .ToDictionary(g => g.Key, g => g.ToList());
            _qutilMappingByTick = qutilMapping;
            _exchangeAddresses = exchangeAddresses;
            _smartContractAddresses = smartContractAddresses;
            _trackedAddresses = trackedAddresses;
            _qutilAddress = qutilAddress;
            _maxHops = maxHops;
            _logger = logger;
        }

        public decimal GetEffectivePending(string address, string origin)
        {
            var key = (address, origin);
            if (_stateUpdates.TryGetValue(key, out var update))
                return update.PendingAmount;

            if (_pendingByAddress.TryGetValue(address, out var states))
            {
                var state = states.FirstOrDefault(s => s.OriginAddress == origin);
                if (state != null) return state.PendingAmount;
            }
            return 0;
        }

        private List<(CustomFlowTrackingStateDto State, decimal EffectivePending)> GetSourceStatesWithPending(string address)
        {
            var result = new List<(CustomFlowTrackingStateDto, decimal)>();
            if (!_pendingByAddress.TryGetValue(address, out var states)) return result;

            foreach (var state in states)
            {
                var pending = GetEffectivePending(address, state.OriginAddress);
                if (pending > 0)
                    result.Add((state, pending));
            }

            // Also check in-flight new states
            foreach (var kvp in _stateUpdates)
            {
                if (kvp.Key.Address == address && kvp.Value.PendingAmount > 0)
                {
                    if (!result.Any(r => r.Item1.OriginAddress == kvp.Key.Origin))
                    {
                        var synthState = new CustomFlowTrackingStateDto(
                            "", address, kvp.Value.AddressType, kvp.Key.Origin,
                            kvp.Value.ReceivedAmount, kvp.Value.SentAmount, kvp.Value.PendingAmount,
                            kvp.Value.HopLevel, 0, kvp.Value.IsTerminal, kvp.Value.IsComplete);
                        result.Add((synthState, kvp.Value.PendingAmount));
                    }
                }
            }

            return result;
        }

        public void ProcessTransfer(ComputorFlowService.TransferRecordWithLogId transfer, Func<string, string?> getLabel)
        {
            var sourceStates = GetSourceStatesWithPending(transfer.SourceAddress);
            if (sourceStates.Count == 0) return;

            var totalEffectivePending = sourceStates.Sum(s => s.EffectivePending);
            if (totalEffectivePending <= 0) return;

            var hopLevel = sourceStates[0].State.HopLevel;
            var isQutil = _qutilAddress != null && transfer.DestAddress == _qutilAddress;

            if (isQutil)
                ProcessQutilTransfer(transfer, sourceStates, totalEffectivePending, hopLevel, getLabel);
            else
                ProcessDirectTransfer(transfer, sourceStates, totalEffectivePending, hopLevel, getLabel);
        }

        private void ProcessDirectTransfer(
            ComputorFlowService.TransferRecordWithLogId transfer,
            List<(CustomFlowTrackingStateDto State, decimal EffectivePending)> sourceStates,
            decimal totalEffectivePending,
            byte hopLevel,
            Func<string, string?> getLabel)
        {
            var isExchange = _exchangeAddresses.Contains(transfer.DestAddress);
            var isSmartContract = _smartContractAddresses.Contains(transfer.DestAddress);
            var isTerminal = isExchange || isSmartContract;
            var destType = isExchange ? "exchange" : (isSmartContract ? "smartcontract" : "intermediary");
            var destLabel = (isExchange || isSmartContract) ? getLabel(transfer.DestAddress) : null;

            foreach (var (sourceState, effectivePending) in sourceStates)
            {
                var proportion = effectivePending / totalEffectivePending;
                var attributedAmount = transfer.Amount * proportion;
                if (attributedAmount <= 0) continue;

                _hops.Add(new HopRecord(
                    transfer.TickNumber, transfer.Timestamp, transfer.TxHash,
                    transfer.SourceAddress, transfer.DestAddress, attributedAmount,
                    sourceState.OriginAddress, hopLevel, destType, destLabel ?? ""));

                UpdateDestinationState(
                    transfer.DestAddress, sourceState.OriginAddress,
                    attributedAmount, (byte)(hopLevel + 1), destType, isTerminal);

                UpdateSourceSentAmount(sourceState, attributedAmount);
            }
        }

        private void ProcessQutilTransfer(
            ComputorFlowService.TransferRecordWithLogId transfer,
            List<(CustomFlowTrackingStateDto State, decimal EffectivePending)> sourceStates,
            decimal totalEffectivePending,
            byte hopLevel,
            Func<string, string?> getLabel)
        {
            var tickKey = transfer.TickNumber.ToString();
            if (!_qutilMappingByTick.TryGetValue(tickKey, out var qutilOutputs) || qutilOutputs.Count == 0)
                return;

            foreach (var output in qutilOutputs)
            {
                if (string.IsNullOrEmpty(output.DestAddress)) continue;
                if (output.DestAddress == _qutilAddress) continue;

                var isExchange = _exchangeAddresses.Contains(output.DestAddress);
                var isSC = _smartContractAddresses.Contains(output.DestAddress);
                var isTerminal = isExchange || isSC;
                var destType = isExchange ? "exchange" : (isSC ? "smartcontract" : "intermediary");
                var destLabel = (isExchange || isSC) ? getLabel(output.DestAddress) : null;

                foreach (var (sourceState, effectivePending) in sourceStates)
                {
                    var proportion = effectivePending / totalEffectivePending;
                    var attributedAmount = output.Amount * proportion;
                    if (attributedAmount <= 0) continue;

                    _hops.Add(new HopRecord(
                        transfer.TickNumber, transfer.Timestamp, transfer.TxHash,
                        transfer.SourceAddress, output.DestAddress, attributedAmount,
                        sourceState.OriginAddress, hopLevel, destType, destLabel ?? ""));

                    UpdateDestinationState(
                        output.DestAddress, sourceState.OriginAddress,
                        attributedAmount, (byte)(hopLevel + 1), destType, isTerminal);
                }
            }

            // Update source sent amounts using the input transfer amount
            foreach (var (sourceState, effectivePending) in sourceStates)
            {
                var proportion = effectivePending / totalEffectivePending;
                var attributedSent = transfer.Amount * proportion;
                UpdateSourceSentAmount(sourceState, attributedSent);
            }
        }

        private void UpdateDestinationState(
            string destAddress, string originAddress,
            decimal amount, byte destHopLevel, string destType, bool isTerminal)
        {
            // Don't track back to the original tracked addresses
            if (_trackedAddresses.Contains(destAddress)) return;

            // Skip beyond max hops if not terminal
            if (destHopLevel > _maxHops && !isTerminal) return;

            var key = (destAddress, originAddress);

            if (_stateUpdates.TryGetValue(key, out var existing))
            {
                _stateUpdates[key] = existing with
                {
                    ReceivedAmount = existing.ReceivedAmount + amount,
                    PendingAmount = isTerminal ? 0 : existing.PendingAmount + amount
                };
            }
            else if (_pendingByAddress.TryGetValue(destAddress, out var existingStates))
            {
                var existingState = existingStates.FirstOrDefault(s => s.OriginAddress == originAddress);
                if (existingState != null)
                {
                    _stateUpdates[key] = new FlowTrackingUpdateDto(
                        destAddress, existingState.AddressType, originAddress,
                        existingState.ReceivedAmount + amount,
                        existingState.SentAmount,
                        isTerminal ? 0 : existingState.PendingAmount + amount,
                        existingState.HopLevel, isTerminal, isTerminal);
                }
                else
                {
                    _stateUpdates[key] = new FlowTrackingUpdateDto(
                        destAddress, isTerminal ? destType : "intermediary", originAddress,
                        amount, 0, isTerminal ? 0 : amount,
                        destHopLevel, isTerminal, isTerminal);
                }
            }
            else
            {
                _stateUpdates[key] = new FlowTrackingUpdateDto(
                    destAddress, isTerminal ? destType : "intermediary", originAddress,
                    amount, 0, isTerminal ? 0 : amount,
                    destHopLevel, isTerminal, isTerminal);
            }
        }

        private void UpdateSourceSentAmount(CustomFlowTrackingStateDto sourceState, decimal sentAmount)
        {
            var key = (sourceState.Address, sourceState.OriginAddress);

            if (_stateUpdates.TryGetValue(key, out var existing))
            {
                var newPending = existing.PendingAmount - sentAmount;
                _stateUpdates[key] = existing with
                {
                    SentAmount = existing.SentAmount + sentAmount,
                    PendingAmount = Math.Max(0, newPending),
                    IsComplete = newPending <= 0
                };
            }
            else
            {
                var newPending = sourceState.PendingAmount - sentAmount;
                _stateUpdates[key] = new FlowTrackingUpdateDto(
                    sourceState.Address, sourceState.AddressType, sourceState.OriginAddress,
                    sourceState.ReceivedAmount,
                    sourceState.SentAmount + sentAmount,
                    Math.Max(0, newPending),
                    sourceState.HopLevel,
                    sourceState.IsTerminal,
                    newPending <= 0);
            }
        }
    }
}
