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
