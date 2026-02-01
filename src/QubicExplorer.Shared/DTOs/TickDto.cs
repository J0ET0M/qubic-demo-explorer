namespace QubicExplorer.Shared.DTOs;

public record TickDto(
    ulong TickNumber,
    uint Epoch,
    DateTime Timestamp,
    uint TxCount,
    uint LogCount
);

public record TickDetailDto(
    ulong TickNumber,
    uint Epoch,
    DateTime Timestamp,
    uint TxCount,
    uint LogCount,
    List<TransactionDto> Transactions
);
