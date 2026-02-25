namespace QubicExplorer.Shared.DTOs;

public record AssetDto(
    string AssetName,
    string IssuerAddress,
    ulong TotalSupply,
    ulong TickNumber
);

public record AssetHolderDto(
    string Address,
    ulong Balance
);

/// <summary>
/// Asset summary for the asset list page
/// </summary>
public record AssetSummaryDto(
    string AssetName,
    string IssuerAddress,
    string? IssuerLabel,
    int NumberOfDecimalPlaces,
    long TotalSupply,
    int HolderCount
);

/// <summary>
/// Detailed asset holder with ownership and possession
/// </summary>
public record AssetHolderDetailDto(
    string Address,
    string? Label,
    string? Type,
    long OwnedShares,
    long PossessedShares
);

/// <summary>
/// Full asset detail with top holders
/// </summary>
public record AssetDetailDto(
    string AssetName,
    string IssuerAddress,
    string? IssuerLabel,
    int NumberOfDecimalPlaces,
    long TotalSupply,
    int HolderCount,
    uint SnapshotEpoch,
    List<AssetHolderDetailDto> TopHolders
);

/// <summary>
/// Paginated asset holders response
/// </summary>
public record AssetHoldersPageDto(
    List<AssetHolderDetailDto> Holders,
    int Page,
    int Limit,
    int TotalCount,
    int TotalPages
);

/// <summary>
/// Universe import result
/// </summary>
public record UniverseImportResultDto(
    uint Epoch,
    bool Success,
    ulong IssuanceCount,
    ulong OwnershipCount,
    ulong PossessionCount,
    ulong FileSize,
    string? Error = null
);
