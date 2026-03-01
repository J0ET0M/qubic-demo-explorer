namespace QubicExplorer.Shared.DTOs;

/// <summary>
/// Top address by volume
/// </summary>
public record TopAddressDto(
    string Address,
    string? Label,
    string? Type,
    ulong SentVolume,
    ulong ReceivedVolume,
    ulong TotalVolume,
    uint SentCount,
    uint ReceivedCount,
    uint TotalCount
);

/// <summary>
/// Flow node representing a counterparty in address flow
/// </summary>
public record FlowNodeDto(
    string Address,
    string? Label,
    string? Type,
    ulong TotalAmount,
    uint TransactionCount
);

/// <summary>
/// Address transaction flow (inbound/outbound counterparties)
/// </summary>
public record AddressFlowDto(
    string Address,
    string? Label,
    string? Type,
    List<FlowNodeDto> Inbound,
    List<FlowNodeDto> Outbound
);

/// <summary>
/// Smart contract usage statistics
/// </summary>
public record SmartContractUsageDto(
    string Address,
    string Name,
    int? ContractIndex,
    ulong CallCount,
    ulong TotalAmount,
    uint UniqueCallers
);

// =====================================================
// RICH LIST DTOs
// =====================================================

/// <summary>
/// Single entry in the rich list
/// </summary>
public record RichListEntryDto(
    int Rank,
    string Address,
    string? Label,
    string? Type,
    decimal Balance,
    string BalanceFormatted,
    decimal PercentageOfSupply
);

/// <summary>
/// Rich list response with pagination
/// </summary>
public record RichListDto(
    List<RichListEntryDto> Entries,
    int Page,
    int Limit,
    ulong TotalCount,
    int TotalPages,
    decimal TotalBalance,
    uint SnapshotEpoch
);

// =====================================================
// SUPPLY DASHBOARD DTOs
// =====================================================

/// <summary>
/// Individual donation recipient in an epoch emission
/// </summary>
public record EmissionDonationDto(
    string Address,
    string? Label,
    decimal Amount
);

/// <summary>
/// Epoch emission data point with full breakdown:
/// 1T = ComputorEmission + ArbRevenue + DonationTotal
/// </summary>
public record EmissionDataPointDto(
    uint Epoch,
    decimal ComputorEmission,
    decimal ArbRevenue,
    List<EmissionDonationDto> Donations,
    decimal DonationTotal,
    int ComputorCount
);

/// <summary>
/// Burn data point (per 4-hour snapshot window)
/// </summary>
public record BurnDataPointDto(
    uint Epoch,
    DateTime SnapshotAt,
    ulong BurnAmount,
    ulong BurnCount
);

/// <summary>
/// Supply dashboard overview
/// </summary>
public record SupplyDashboardDto(
    decimal CirculatingSupply,
    decimal TotalEmitted,
    decimal TotalBurned,
    decimal LatestEpochEmission,
    uint EpochCount,
    uint SnapshotEpoch,
    decimal SupplyCap,
    decimal SupplyCapProgress,
    List<EmissionDataPointDto> EmissionHistory,
    List<BurnDataPointDto> BurnHistory
);

// =====================================================
// GLASSNODE-STYLE ANALYTICS DTOs
// =====================================================

/// <summary>
/// Active address trend data point
/// </summary>
public record ActiveAddressTrendDto(
    uint? Epoch,
    DateTime? Date,
    ulong UniqueSenders,
    ulong UniqueReceivers,
    ulong TotalActive
);

/// <summary>
/// New vs returning addresses per epoch
/// </summary>
public record NewVsReturningDto(
    uint Epoch,
    ulong NewAddresses,
    ulong ReturningAddresses,
    ulong TotalAddresses
);

/// <summary>
/// Exchange flow data point
/// </summary>
public record ExchangeFlowDataPointDto(
    uint Epoch,
    ulong InflowVolume,
    uint InflowCount,
    ulong OutflowVolume,
    uint OutflowCount,
    long NetFlow
);

/// <summary>
/// Exchange flows summary
/// </summary>
public record ExchangeFlowDto(
    List<ExchangeFlowDataPointDto> DataPoints,
    ulong TotalInflow,
    ulong TotalOutflow
);

/// <summary>
/// Address that sent funds to an exchange (or to a deposit address when depth=2)
/// </summary>
public record ExchangeSenderDto(
    string Address,
    string? Label,
    string? Type,
    decimal TotalVolume,
    string TotalVolumeFormatted,
    uint TransactionCount,
    uint EpochCount,
    List<string>? ViaDepositAddresses = null,
    int? ClusterId = null
);

/// <summary>
/// A link between two addresses suggesting they belong to the same entity
/// </summary>
public record ClusterLinkDto(
    string Address1,
    string Address2,
    string Reason,
    decimal Volume
);

/// <summary>
/// A cluster of addresses likely belonging to the same entity
/// </summary>
public record AddressClusterDto(
    int ClusterId,
    List<string> Addresses,
    List<ClusterLinkDto> Links,
    decimal TotalVolume,
    string TotalVolumeFormatted
);

/// <summary>
/// Response for exchange senders query
/// </summary>
public record ExchangeSendersDto(
    List<AddressClusterDto> Clusters,
    List<ExchangeSenderDto> Senders,
    uint EpochsQueried,
    decimal MinAmount
);

/// <summary>
/// Holder bracket for distribution analysis
/// </summary>
public record HolderBracketDto(
    string Name,
    ulong Count,
    decimal Balance,
    decimal PercentageOfSupply
);

/// <summary>
/// Holder distribution (whale analysis)
/// </summary>
public record HolderDistributionDto(
    List<HolderBracketDto> Brackets,
    ulong TotalHolders,
    decimal TotalBalance,
    ConcentrationMetricsDto? Concentration = null
);

/// <summary>
/// Concentration metrics showing wealth distribution
/// </summary>
public record ConcentrationMetricsDto(
    decimal Top10Balance,
    decimal Top10Percent,
    decimal Top50Balance,
    decimal Top50Percent,
    decimal Top100Balance,
    decimal Top100Percent
);

/// <summary>
/// Historical snapshot of holder distribution
/// </summary>
public record HolderDistributionHistoryDto(
    uint Epoch,
    DateTime SnapshotAt,
    ulong TickStart,
    ulong TickEnd,
    List<HolderBracketDto> Brackets,
    ulong TotalHolders,
    decimal TotalBalance,
    ConcentrationMetricsDto Concentration,
    string DataSource
);

/// <summary>
/// Extended holder distribution with history
/// </summary>
public record HolderDistributionExtendedDto(
    HolderDistributionDto Current,
    List<HolderDistributionHistoryDto> History
);

/// <summary>
/// Average transaction size trend
/// </summary>
public record AvgTxSizeTrendDto(
    uint? Epoch,
    DateTime? Date,
    ulong TxCount,
    decimal TotalVolume,
    decimal AvgTxSize,
    decimal MedianTxSize
);

// =====================================================
// NETWORK STATS HISTORY DTOs
// =====================================================

/// <summary>
/// Historical snapshot of network statistics for a specific time window
/// </summary>
public record NetworkStatsHistoryDto(
    uint Epoch,
    DateTime SnapshotAt,
    ulong TickStart,
    ulong TickEnd,
    ulong TotalTransactions,
    ulong TotalTransfers,
    decimal TotalVolume,
    ulong UniqueSenders,
    ulong UniqueReceivers,
    ulong TotalActiveAddresses,
    ulong NewAddresses,
    ulong ReturningAddresses,
    decimal ExchangeInflowVolume,
    ulong ExchangeInflowCount,
    decimal ExchangeOutflowVolume,
    ulong ExchangeOutflowCount,
    decimal ExchangeNetFlow,
    ulong ScCallCount,
    ulong ScUniqueCallers,
    double AvgTxSize,
    double MedianTxSize,
    ulong NewUsers100MPlus,
    ulong NewUsers1BPlus,
    ulong NewUsers10BPlus
);

/// <summary>
/// Extended network stats with current and historical data
/// </summary>
public record NetworkStatsExtendedDto(
    NetworkStatsHistoryDto? Current,
    List<NetworkStatsHistoryDto> History
);

// =====================================================
// BURN STATS HISTORY DTOs
// =====================================================

/// <summary>
/// Historical snapshot of burn statistics for a specific time window
/// </summary>
public record BurnStatsHistoryDto(
    uint Epoch,
    DateTime SnapshotAt,
    ulong TickStart,
    ulong TickEnd,
    ulong TotalBurned,
    ulong BurnCount,
    ulong BurnAmount,
    ulong DustBurnCount,
    ulong DustBurned,
    ulong TransferBurnCount,
    ulong TransferBurned,
    ulong UniqueBurners,
    ulong LargestBurn,
    ulong CumulativeBurned
);

/// <summary>
/// Extended burn stats with current and historical data
/// </summary>
public record BurnStatsExtendedDto(
    BurnStatsHistoryDto? Current,
    List<BurnStatsHistoryDto> History,
    ulong AllTimeTotalBurned
);

// =====================================================
// EPOCH COUNTDOWN DTOs
// =====================================================

/// <summary>
/// Epoch countdown data for displaying time remaining in current epoch
/// </summary>
public record EpochCountdownDto(
    uint CurrentEpoch,
    DateTime CurrentEpochStart,
    double AverageEpochDurationMs,
    DateTime EstimatedEpochEnd,
    ulong CurrentTick
);

// =====================================================
// EPOCH METADATA DTOs
// =====================================================

/// <summary>
/// Epoch metadata containing tick ranges and boundaries
/// </summary>
public record EpochMetaDto(
    uint Epoch,
    ulong InitialTick,
    ulong EndTick,
    ulong EndTickStartLogId,
    ulong EndTickEndLogId,
    bool IsComplete,
    DateTime UpdatedAt,
    ulong TickCount = 0,
    ulong TxCount = 0,
    decimal TotalVolume = 0,
    ulong ActiveAddresses = 0,
    ulong TransferCount = 0,
    decimal QuTransferred = 0
);

// =====================================================
// COMPUTOR/MINER FLOW TRACKING DTOs
// =====================================================

/// <summary>
/// Computor information for a specific epoch
/// </summary>
public record ComputorDto(
    uint Epoch,
    string Address,
    ushort Index,
    string? Label
);

/// <summary>
/// Computor list for an epoch
/// </summary>
public record ComputorListDto(
    uint Epoch,
    List<ComputorDto> Computors,
    int Count,
    DateTime? ImportedAt
);

/// <summary>
/// Individual flow hop in the money trail
/// </summary>
public record FlowHopDto(
    uint Epoch,
    ulong TickNumber,
    DateTime Timestamp,
    string TxHash,
    string SourceAddress,
    string? SourceLabel,
    string? SourceType,
    string DestAddress,
    string? DestLabel,
    string? DestType,
    decimal Amount,
    string OriginAddress,
    string OriginType,
    byte HopLevel
);

/// <summary>
/// Aggregated flow statistics for a snapshot window
/// </summary>
public record MinerFlowStatsDto(
    uint Epoch,
    DateTime SnapshotAt,
    ulong TickStart,
    ulong TickEnd,
    uint EmissionEpoch,
    decimal TotalEmission,
    ushort ComputorCount,
    decimal TotalOutflow,
    ulong OutflowTxCount,
    decimal FlowToExchangeDirect,
    decimal FlowToExchange1Hop,
    decimal FlowToExchange2Hop,
    decimal FlowToExchange3Plus,
    decimal FlowToExchangeTotal,
    ulong FlowToExchangeCount,
    decimal FlowToOther,
    decimal MinerNetPosition,
    decimal Hop1Volume,
    decimal Hop2Volume,
    decimal Hop3Volume,
    decimal Hop4PlusVolume
);

/// <summary>
/// Summary of miner flow statistics with history
/// </summary>
public record MinerFlowSummaryDto(
    MinerFlowStatsDto? Latest,
    List<MinerFlowStatsDto> History,
    decimal TotalEmissionTracked,
    decimal TotalFlowToExchange,
    decimal AverageExchangeFlowPercent
);

/// <summary>
/// Flow visualization node for Sankey/flow diagrams
/// </summary>
public record FlowVisualizationNodeDto(
    string Id,
    string Address,
    string? Label,
    string Type,
    decimal TotalInflow,
    decimal TotalOutflow,
    int Depth
);

/// <summary>
/// Flow visualization link between nodes
/// </summary>
public record FlowVisualizationLinkDto(
    string SourceId,
    string TargetId,
    decimal Amount,
    uint TransactionCount
);

/// <summary>
/// Complete flow visualization data for rendering Sankey diagrams
/// </summary>
public record FlowVisualizationDto(
    uint Epoch,
    ulong TickStart,
    ulong TickEnd,
    List<FlowVisualizationNodeDto> Nodes,
    List<FlowVisualizationLinkDto> Links,
    int MaxDepth,
    decimal TotalTrackedVolume
);

/// <summary>
/// Address to track for flow analysis (extensible for pools, miners, etc.)
/// </summary>
public record FlowTrackingAddressDto(
    string Address,
    string AddressType,
    uint Epoch,
    string? Label,
    bool Enabled
);

// =====================================================
// COMPUTOR EMISSIONS DTOs
// =====================================================

/// <summary>
/// Emission received by a specific computor at the end of an epoch
/// </summary>
public record ComputorEmissionDto(
    uint Epoch,
    ushort ComputorIndex,
    string Address,
    string? Label,
    decimal EmissionAmount,
    ulong EmissionTick,
    DateTime EmissionTimestamp
);

/// <summary>
/// Summary of emissions for an epoch
/// </summary>
public record EmissionSummaryDto(
    uint Epoch,
    ushort ComputorCount,
    decimal TotalEmission,
    ulong EmissionTick,
    DateTime ImportedAt
);

/// <summary>
/// Full emission data for an epoch with all computor emissions
/// </summary>
public record EpochEmissionDto(
    uint Epoch,
    ushort ComputorCount,
    decimal TotalEmission,
    ulong EmissionTick,
    DateTime? ImportedAt,
    List<ComputorEmissionDto> Emissions
);

// =====================================================
// FLOW TRACKING STATE DTOs
// =====================================================

/// <summary>
/// Represents the tracking state of an address for flow analysis.
/// Used to continue tracking across tick windows until funds reach exchanges.
/// </summary>
public record FlowTrackingStateDto(
    uint EmissionEpoch,
    string Address,
    string AddressType,      // 'computor' or 'intermediary'
    string OriginAddress,    // Original computor this flow came from
    decimal ReceivedAmount,  // Total amount received
    decimal SentAmount,      // Total amount sent out
    decimal PendingAmount,   // Amount still to trace (received - sent)
    byte HopLevel,           // Current hop level (1 = computor)
    ulong LastTick,          // Last tick processed for this address
    bool IsTerminal,         // True if this is an exchange
    bool IsComplete          // True if all funds have been traced
);

/// <summary>
/// Update to apply to tracking state after processing a tick window.
/// </summary>
public record FlowTrackingUpdateDto(
    string Address,
    string AddressType,
    string OriginAddress,
    decimal ReceivedAmount,
    decimal SentAmount,
    decimal PendingAmount,
    byte HopLevel,
    bool IsTerminal,
    bool IsComplete
);

// =====================================================
// WHALE ALERT DTOs
// =====================================================

/// <summary>
/// Large transfer alert entry
/// </summary>
public record WhaleAlertDto(
    ulong TickNumber,
    uint Epoch,
    string TxHash,
    string SourceAddress,
    string? SourceLabel,
    string? SourceType,
    string DestAddress,
    string? DestLabel,
    string? DestType,
    decimal Amount,
    string AmountFormatted,
    DateTime Timestamp
);

// =====================================================
// TRANSACTION GRAPH DTOs
// =====================================================

/// <summary>
/// Graph node representing an address
/// </summary>
public record GraphNodeDto(
    string Address,
    string? Label,
    string? Type,
    decimal TotalVolume,
    int Depth
);

/// <summary>
/// Graph link between two addresses
/// </summary>
public record GraphLinkDto(
    string Source,
    string Target,
    decimal Amount,
    uint TxCount
);

/// <summary>
/// Transaction graph for visualization
/// </summary>
public record TransactionGraphDto(
    List<GraphNodeDto> Nodes,
    List<GraphLinkDto> Links
);

// =====================================================
// QEARN STATS DTOs
// =====================================================

/// <summary>
/// Qearn statistics per epoch: burns, deposits (inputs), payouts (outputs)
/// Yield = TotalOutput - TotalInput + TotalBurned
/// </summary>
public record QearnEpochStatsDto(
    uint Epoch,
    ulong TotalBurned,
    ulong BurnCount,
    ulong TotalInput,
    ulong InputCount,
    ulong TotalOutput,
    ulong OutputCount,
    ulong UniqueLockers,
    ulong UniqueUnlockers
);

/// <summary>
/// Qearn overall statistics with per-epoch breakdown
/// </summary>
public record QearnStatsDto(
    List<QearnEpochStatsDto> Epochs,
    ulong AllTimeTotalBurned,
    ulong AllTimeTotalInput,
    ulong AllTimeTotalOutput
);

/// <summary>
/// Result of flow validation/sanity check for an emission epoch.
/// </summary>
public record FlowValidationResult(
    uint EmissionEpoch,
    bool IsValid,
    decimal TotalEmission,
    decimal ComputorReceivedTotal,
    decimal TotalPending,
    decimal TotalTerminal,
    decimal DiscrepancyAmount,
    List<string> Errors,
    List<string> Warnings
);

// =====================================================
// CCF (Computor Controlled Fund) DTOs
// =====================================================

/// <summary>
/// CCF one-time transfer record (persisted from contract polling).
/// </summary>
public record CcfTransferDto(
    string Destination,
    string Url,
    long Amount,
    uint Tick,
    uint Epoch,
    bool Success
);

/// <summary>
/// CCF regular/subscription payment record (persisted from contract polling).
/// </summary>
public record CcfRegularPaymentDto(
    string Destination,
    string Url,
    long Amount,
    uint Tick,
    uint Epoch,
    int PeriodIndex,
    bool Success
);

/// <summary>
/// CCF proposal with voting results.
/// </summary>
public record CcfProposalDto(
    ushort ProposalIndex,
    string ProposerAddress,
    string Url,
    ushort ProposalType,
    uint ProposalTick,
    ushort Epoch,
    /// <summary>Transfer destination (for transfer proposals).</summary>
    string? TransferDestination,
    /// <summary>Transfer amount (for transfer proposals).</summary>
    long? TransferAmount,
    /// <summary>Total votes authorized (676 computors).</summary>
    int TotalVotesAuthorized,
    /// <summary>Total votes actually cast.</summary>
    int TotalVotesCast,
    /// <summary>Votes for option 0 (No).</summary>
    int NoVotes,
    /// <summary>Votes for option 1 (Yes).</summary>
    int YesVotes,
    bool Passed,
    bool IsActive
);

/// <summary>
/// CCF active subscription info.
/// </summary>
public record CcfSubscriptionDto(
    string Destination,
    string Url,
    long AmountPerPeriod,
    int NumberOfPeriods,
    int CurrentPeriod,
    byte WeeksPerPeriod,
    uint StartEpoch
);

/// <summary>
/// CCF spending aggregated per epoch.
/// </summary>
public record CcfEpochSpendingDto(
    uint Epoch,
    long TotalSpent,
    int TransferCount
);

/// <summary>
/// Full CCF dashboard stats combining persisted transfers, live proposals, and subscriptions.
/// </summary>
public record CcfStatsDto(
    List<CcfProposalDto> ActiveProposals,
    List<CcfProposalDto> PastProposals,
    List<CcfTransferDto> Transfers,
    List<CcfRegularPaymentDto> RegularPayments,
    List<CcfSubscriptionDto> ActiveSubscriptions,
    long TotalSpent,
    int TotalTransferCount,
    uint ProposalFee,
    List<CcfEpochSpendingDto> SpendingByEpoch
);
