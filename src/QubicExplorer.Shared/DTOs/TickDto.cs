namespace QubicExplorer.Shared.DTOs;

public record TickDto(
    ulong TickNumber,
    uint Epoch,
    DateTime Timestamp,
    uint TxCount,
    uint LogCount,
    bool IsEmpty
);

public record TickDetailDto(
    ulong TickNumber,
    uint Epoch,
    DateTime Timestamp,
    uint TxCount,
    uint LogCount,
    bool IsEmpty,
    List<TransactionDto> Transactions
);
