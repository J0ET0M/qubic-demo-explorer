using QubicExplorer.Shared.DTOs;

namespace QubicExplorer.Api.Services;

/// <summary>
/// Lightweight service for computor list management in the API.
/// Full flow analysis has moved to the Analytics service.
/// </summary>
public class ComputorFlowService
{
    private readonly BobProxyService _bobProxy;
    private readonly ClickHouseQueryService _queryService;
    private readonly ILogger<ComputorFlowService> _logger;

    public ComputorFlowService(
        BobProxyService bobProxy,
        ClickHouseQueryService queryService,
        ILogger<ComputorFlowService> logger)
    {
        _bobProxy = bobProxy;
        _queryService = queryService;
        _logger = logger;
    }

    /// <summary>
    /// Ensures computors for the given epoch are imported from RPC into ClickHouse.
    /// </summary>
    public async Task<bool> EnsureComputorsImportedAsync(uint epoch, CancellationToken ct = default)
    {
        if (await _queryService.IsComputorListImportedAsync(epoch, ct))
        {
            _logger.LogDebug("Computors for epoch {Epoch} already imported", epoch);
            return true;
        }

        var result = await _bobProxy.GetComputorsAsync(epoch, ct);
        if (result == null || result.Computors.Count == 0)
        {
            _logger.LogWarning("Failed to fetch computors for epoch {Epoch}", epoch);
            return false;
        }

        var cleanedComputors = result.Computors
            .Select(addr => CleanAddress(addr))
            .ToList();

        await _queryService.SaveComputorsAsync(epoch, cleanedComputors, ct);

        _logger.LogInformation("Imported {Count} computors for epoch {Epoch}", cleanedComputors.Count, epoch);
        return true;
    }

    /// <summary>
    /// Gets the list of computors for an epoch, importing if needed.
    /// </summary>
    public async Task<ComputorListDto?> GetComputorsAsync(uint epoch, CancellationToken ct = default)
    {
        await EnsureComputorsImportedAsync(epoch, ct);
        return await _queryService.GetComputorsAsync(epoch, ct);
    }

    /// <summary>
    /// Cleans address by removing trailing unicode characters from RPC responses.
    /// </summary>
    private static string CleanAddress(string address)
    {
        if (string.IsNullOrEmpty(address)) return address;
        return new string(address.Where(c => c >= 'A' && c <= 'Z').ToArray());
    }
}
