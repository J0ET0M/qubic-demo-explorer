using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;
using QubicExplorer.Api.Attributes;
using QubicExplorer.Api.Services;
using QubicExplorer.Shared.Configuration;

namespace QubicExplorer.Api.Controllers;

[ApiController]
[Route("api/[controller]")]
public class SpectrumController : ControllerBase
{
    private readonly SpectrumImportService _spectrumService;
    private readonly UniverseImportService _universeService;
    private readonly ILogger<SpectrumController> _logger;

    public SpectrumController(
        SpectrumImportService spectrumService,
        UniverseImportService universeService,
        ILogger<SpectrumController> logger)
    {
        _spectrumService = spectrumService;
        _universeService = universeService;
        _logger = logger;
    }

    /// <summary>
    /// Get the status of spectrum imports
    /// </summary>
    [HttpGet("status")]
    public async Task<IActionResult> GetStatus(CancellationToken ct)
    {
        var latestEpoch = await _spectrumService.GetLatestImportedEpochAsync(ct);
        return Ok(new
        {
            latestImportedEpoch = latestEpoch,
            hasImports = latestEpoch.HasValue
        });
    }

    /// <summary>
    /// Check if a specific epoch has been imported
    /// </summary>
    [HttpGet("{epoch}/status")]
    public async Task<IActionResult> GetEpochStatus(uint epoch, CancellationToken ct)
    {
        var isImported = await _spectrumService.IsEpochImportedAsync(epoch, ct);
        return Ok(new
        {
            epoch,
            imported = isImported
        });
    }

    /// <summary>
    /// Import spectrum file for a specific epoch
    /// </summary>
    [HttpPost("{epoch}/import")]
    [AdminApiKey]
    public async Task<IActionResult> ImportEpoch(uint epoch, CancellationToken ct)
    {
        _logger.LogInformation("Received request to import spectrum for epoch {Epoch}", epoch);

        // Check if already imported
        var alreadyImported = await _spectrumService.IsEpochImportedAsync(epoch, ct);
        if (alreadyImported)
        {
            return Ok(new
            {
                success = true,
                message = $"Epoch {epoch} was already imported",
                alreadyImported = true
            });
        }

        var result = await _spectrumService.ImportEpochAsync(epoch, ct);

        if (result.Success)
        {
            return Ok(new
            {
                success = true,
                epoch = result.Epoch,
                addressCount = result.AddressCount,
                totalBalance = result.TotalBalance,
                fileSize = result.FileSize
            });
        }

        return BadRequest(new
        {
            success = false,
            epoch = result.Epoch,
            error = result.Error
        });
    }

    /// <summary>
    /// Import the latest available spectrum file
    /// </summary>
    [HttpPost("import-latest")]
    [AdminApiKey]
    public IActionResult ImportLatest()
    {
        // This would require knowing the current epoch from the network
        // For now, return a helpful message
        return BadRequest(new
        {
            success = false,
            error = "Please specify an epoch to import using POST /api/spectrum/{epoch}/import"
        });
    }

    // =====================================================
    // UNIVERSE IMPORTS
    // =====================================================

    /// <summary>
    /// Import universe file for a specific epoch
    /// </summary>
    [HttpPost("{epoch}/import-universe")]
    [AdminApiKey]
    public async Task<IActionResult> ImportUniverse(uint epoch, CancellationToken ct)
    {
        _logger.LogInformation("Received request to import universe for epoch {Epoch}", epoch);

        var alreadyImported = await _universeService.IsEpochImportedAsync(epoch, ct);
        if (alreadyImported)
        {
            return Ok(new
            {
                success = true,
                message = $"Universe for epoch {epoch} was already imported",
                alreadyImported = true
            });
        }

        var result = await _universeService.ImportEpochAsync(epoch, ct);

        if (result.Success)
        {
            return Ok(new
            {
                success = true,
                epoch = result.Epoch,
                issuanceCount = result.IssuanceCount,
                ownershipCount = result.OwnershipCount,
                possessionCount = result.PossessionCount,
                fileSize = result.FileSize
            });
        }

        return BadRequest(new
        {
            success = false,
            epoch = result.Epoch,
            error = result.Error
        });
    }

    /// <summary>
    /// Get the status of universe imports
    /// </summary>
    [HttpGet("universe/status")]
    public async Task<IActionResult> GetUniverseStatus(CancellationToken ct)
    {
        await using var cmd = new ClickHouse.Client.ADO.ClickHouseConnection(
            HttpContext.RequestServices.GetRequiredService<IOptions<ClickHouseOptions>>().Value.ConnectionString);
        await cmd.OpenAsync(ct);

        await using var query = cmd.CreateCommand();
        query.CommandText = @"
            SELECT epoch, issuance_count, ownership_count, possession_count, imported_at
            FROM universe_imports
            ORDER BY epoch DESC
            LIMIT 10";

        var imports = new List<object>();
        await using var reader = await query.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            imports.Add(new
            {
                epoch = Convert.ToUInt32(reader.GetValue(0)),
                issuanceCount = Convert.ToUInt64(reader.GetValue(1)),
                ownershipCount = Convert.ToUInt64(reader.GetValue(2)),
                possessionCount = Convert.ToUInt64(reader.GetValue(3)),
                importedAt = reader.GetDateTime(4)
            });
        }

        return Ok(new
        {
            imports,
            hasImports = imports.Count > 0
        });
    }
}
