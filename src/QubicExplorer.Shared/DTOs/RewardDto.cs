namespace QubicExplorer.Shared.DTOs;

public record RewardDistributionDto(
    uint Epoch,
    string ContractAddress,
    string? ContractName,
    ulong TickNumber,
    ulong TotalAmount,
    decimal AmountPerShare,
    uint TransferCount,
    DateTime Timestamp
);

public record EpochRewardSummaryDto(
    uint Epoch,
    List<RewardDistributionDto> Distributions,
    ulong TotalRewardsDistributed
);

public record ContractRewardHistoryDto(
    string ContractAddress,
    string? ContractName,
    List<RewardDistributionDto> Distributions,
    ulong TotalAllTimeDistributed,
    int Page,
    int Limit,
    long TotalCount,
    int TotalPages,
    bool HasNextPage,
    bool HasPreviousPage
);
