namespace QubicExplorer.Shared.DTOs;

public record EpochStatsDto(
    uint Epoch,
    ulong TickCount,
    ulong EmptyTickCount,
    ulong FirstTick,
    ulong LastTick,
    DateTime StartTime,
    DateTime EndTime,
    ulong TxCount,
    decimal TotalVolume,
    ulong UniqueSenders,
    ulong UniqueReceivers,
    ulong ActiveAddresses,
    ulong TransferCount,
    decimal QuTransferred,
    ulong AssetTransferCount,
    // Puzzle-mining solutions submitted in the epoch (input_type=2 to burn).
    ulong SolutionCount = 0
);

public record EpochTransferByTypeDto(
    uint Epoch,
    byte LogType,
    string LogTypeName,
    ulong Count,
    decimal TotalAmount
);

public record EpochSummaryDto(
    uint Epoch,
    ulong TickCount,
    ulong EmptyTickCount,
    ulong TxCount,
    decimal TotalVolume,
    ulong ActiveAddresses,
    DateTime StartTime,
    DateTime EndTime,
    ulong FirstTick,
    ulong LastTick
);
