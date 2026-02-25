using Microsoft.AspNetCore.Mvc;
using QubicExplorer.Api.Services;

namespace QubicExplorer.Api.Controllers;

[ApiController]
[Route("api/[controller]")]
public class AssetsController : ControllerBase
{
    private readonly ClickHouseQueryService _queryService;
    private readonly AnalyticsCacheService _cache;

    public AssetsController(ClickHouseQueryService queryService, AnalyticsCacheService cache)
    {
        _queryService = queryService;
        _cache = cache;
    }

    /// <summary>
    /// Get all assets from the latest universe snapshot
    /// </summary>
    [HttpGet]
    public async Task<IActionResult> GetAssets(CancellationToken ct = default)
    {
        var result = await _cache.GetOrSetAsync(
            "assets:list",
            AnalyticsCacheService.AssetListTtl,
            () => _queryService.GetAssetsAsync(ct));
        return Ok(result);
    }

    /// <summary>
    /// Get detailed info for a specific asset
    /// </summary>
    [HttpGet("{name}")]
    public async Task<IActionResult> GetAsset(
        string name,
        [FromQuery] string? issuer = null,
        CancellationToken ct = default)
    {
        var result = await _cache.GetOrSetAsync(
            $"assets:detail:{name}:{issuer ?? "any"}",
            AnalyticsCacheService.AssetDetailTtl,
            () => _queryService.GetAssetDetailAsync(name, issuer, ct));

        if (result == null)
            return NotFound(new { error = $"Asset '{name}' not found" });

        return Ok(result);
    }

    /// <summary>
    /// Get paginated holders for a specific asset
    /// </summary>
    [HttpGet("{name}/holders")]
    public async Task<IActionResult> GetAssetHolders(
        string name,
        [FromQuery] string? issuer = null,
        [FromQuery] int page = 1,
        [FromQuery] int limit = 50,
        CancellationToken ct = default)
    {
        if (page < 1) page = 1;
        if (limit < 1) limit = 1;
        if (limit > 100) limit = 100;

        var result = await _cache.GetOrSetAsync(
            $"assets:holders:{name}:{issuer ?? "any"}:{page}:{limit}",
            AnalyticsCacheService.AssetDetailTtl,
            () => _queryService.GetAssetHoldersAsync(name, issuer, page, limit, ct));
        return Ok(result);
    }
}
