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
