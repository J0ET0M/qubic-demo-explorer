using Microsoft.AspNetCore.Mvc;
using QubicExplorer.Api.Attributes;
using QubicExplorer.Api.Services;

namespace QubicExplorer.Api.Controllers;

[ApiController]
[Route("api/[controller]")]
public class SpectrumController : ControllerBase
{
    private readonly SpectrumImportService _spectrumService;
    private readonly ILogger<SpectrumController> _logger;

    public SpectrumController(
        SpectrumImportService spectrumService,
        ILogger<SpectrumController> logger)
    {
        _spectrumService = spectrumService;
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
}
