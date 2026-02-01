namespace QubicExplorer.Shared.DTOs;

public record LogDto(
    ulong TickNumber,
    uint LogId,
    byte LogType,
    string LogTypeName,
    string? TxHash,
    string? SourceAddress,
    string? DestAddress,
    ulong Amount,
    string? AssetName,
    DateTime Timestamp
);

public record TransferDto(
    ulong TickNumber,
    uint Epoch,
    uint LogId,
    byte LogType,
    string LogTypeName,
    string? TxHash,
    string SourceAddress,
    string DestAddress,
    ulong Amount,
    string? AssetName,
    DateTime Timestamp
);
