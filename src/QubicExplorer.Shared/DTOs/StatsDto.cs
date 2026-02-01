namespace QubicExplorer.Shared.DTOs;

public record NetworkStatsDto(
    ulong LatestTick,
    uint CurrentEpoch,
    ulong TotalTransactions,
    ulong TotalTransfers,
    ulong TotalVolume,
    DateTime LastUpdated
);

public record ChartDataPointDto(
    DateTime Date,
    ulong TxCount,
    ulong Volume
);

public record HourlyActivityDto(
    DateTime Hour,
    ulong TxCount,
    ulong Volume,
    uint UniqueSenders,
    uint UniqueReceivers
);
