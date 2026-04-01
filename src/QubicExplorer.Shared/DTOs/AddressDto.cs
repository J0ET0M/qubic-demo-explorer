namespace QubicExplorer.Shared.DTOs;

public record AddressDto(
    string Address,
    ulong Balance,
    ulong IncomingAmount,
    ulong OutgoingAmount,
    uint TxCount,
    uint TransferCount
);

public record AddressBalanceDto(
    string Address,
    ulong Balance,
    ulong IncomingAmount,
    ulong OutgoingAmount,
    uint NumberOfIncomingTransfers,
    uint NumberOfOutgoingTransfers,
    uint LatestIncomingTransferTick,
    uint LatestOutgoingTransferTick
);

public record AddressActivityRangeDto(
    ulong? FirstTick,
    DateTime? FirstTimestamp,
    uint? FirstEpoch,
    ulong? LastTick,
    DateTime? LastTimestamp,
    uint? LastEpoch
);

public record AddressLedgerDto(
    string Address,
    uint Epoch,
    long OpeningBalance,
    long ClosingBalance,
    List<LedgerEntryDto> Entries
);

public record LedgerEntryDto(
    ulong TickNumber,
    DateTime Timestamp,
    string? TxHash,
    byte LogType,
    string LogTypeName,
    string? CounterpartyAddress,
    string Direction, // "in" or "out"
    long Amount,
    long RunningBalance
);
