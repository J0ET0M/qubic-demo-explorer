namespace QubicExplorer.Shared.DTOs;

public record SearchResultDto(
    SearchResultType Type,
    string Value,
    string? DisplayName
);

public enum SearchResultType
{
    Tick,
    Transaction,
    Address,
    Asset
}

public record SearchResponse(
    string Query,
    List<SearchResultDto> Results
);
