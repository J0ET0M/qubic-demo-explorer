namespace QubicExplorer.Shared.DTOs;

public record PaginatedResponse<T>(
    List<T> Items,
    int Page,
    int Limit,
    long TotalCount,
    int TotalPages
)
{
    public bool HasNextPage => Page < TotalPages;
    public bool HasPreviousPage => Page > 1;
}
