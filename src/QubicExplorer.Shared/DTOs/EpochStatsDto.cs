namespace QubicExplorer.Shared.DTOs;

public record EpochStatsDto(
    uint Epoch,
    ulong TickCount,
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
    ulong AssetTransferCount
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
    ulong TxCount,
    decimal TotalVolume,
    ulong ActiveAddresses,
    DateTime StartTime,
    DateTime EndTime,
    ulong FirstTick,
    ulong LastTick
);
