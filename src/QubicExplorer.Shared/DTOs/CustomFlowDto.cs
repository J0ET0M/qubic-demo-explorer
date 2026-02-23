namespace QubicExplorer.Shared.DTOs;

/// <summary>
/// Request to create a new custom flow tracking job.
/// </summary>
public record CreateCustomFlowRequest(
    List<string> Addresses,
    ulong StartTick,
    List<ulong>? Balances = null,
    string? Alias = null,
    byte MaxHops = 10
);

/// <summary>
/// Custom flow tracking job metadata.
/// </summary>
public record CustomFlowJobDto(
    string JobId,
    string Alias,
    ulong StartTick,
    List<string> Addresses,
    List<ulong> Balances,
    byte MaxHops,
    string Status,
    ulong LastProcessedTick,
    ulong TotalHopsRecorded,
    decimal TotalTerminalAmount,
    decimal TotalPendingAmount,
    string? ErrorMessage,
    DateTime CreatedAt,
    DateTime UpdatedAt
);

/// <summary>
/// Hop record in a custom flow tracking.
/// </summary>
public record CustomFlowHopDto(
    string JobId,
    ulong TickNumber,
    DateTime Timestamp,
    string TxHash,
    string SourceAddress,
    string? SourceLabel,
    string DestAddress,
    string? DestLabel,
    string? DestType,
    decimal Amount,
    string OriginAddress,
    byte HopLevel
);

/// <summary>
/// Custom flow visualization result.
/// </summary>
public record CustomFlowResultDto(
    CustomFlowJobDto Job,
    List<FlowVisualizationNodeDto> Nodes,
    List<FlowVisualizationLinkDto> Links,
    int MaxDepth,
    decimal TotalTrackedVolume
);

/// <summary>
/// Custom flow tracking state for an individual address-origin pair.
/// </summary>
public record CustomFlowTrackingStateDto(
    string JobId,
    string Address,
    string AddressType,
    string OriginAddress,
    decimal ReceivedAmount,
    decimal SentAmount,
    decimal PendingAmount,
    byte HopLevel,
    ulong LastTick,
    bool IsTerminal,
    bool IsComplete
);
