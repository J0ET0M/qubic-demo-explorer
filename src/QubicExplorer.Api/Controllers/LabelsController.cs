using Microsoft.AspNetCore.Mvc;
using QubicExplorer.Api.Services;

namespace QubicExplorer.Api.Controllers;

[ApiController]
[Route("api/[controller]")]
public class LabelsController : ControllerBase
{
    private readonly AddressLabelService _labelService;

    public LabelsController(AddressLabelService labelService)
    {
        _labelService = labelService;
    }

    [HttpGet("{address}")]
    public async Task<IActionResult> GetLabel(string address)
    {
        await _labelService.EnsureFreshDataAsync();
        var info = _labelService.GetAddressInfo(address);

        if (info == null)
            return Ok(new { address, label = (string?)null, type = "unknown" });

        return Ok(new
        {
            address,
            label = info.Label,
            type = info.Type.ToString().ToLowerInvariant(),
            contractIndex = info.ContractIndex,
            website = info.Website
        });
    }

    [HttpPost("batch")]
    public async Task<IActionResult> GetLabels([FromBody] string[] addresses)
    {
        if (addresses == null || addresses.Length == 0)
            return BadRequest("Addresses array is required");

        if (addresses.Length > 100)
            return BadRequest("Maximum 100 addresses allowed per request");

        await _labelService.EnsureFreshDataAsync();
        var labels = _labelService.GetLabelsForAddresses(addresses);

        var result = addresses.Select(addr =>
        {
            labels.TryGetValue(addr, out var info);
            return new
            {
                address = addr,
                label = info?.Label,
                type = info?.Type.ToString().ToLowerInvariant() ?? "unknown",
                contractIndex = info?.ContractIndex,
                website = info?.Website
            };
        });

        return Ok(result);
    }

    [HttpGet("stats")]
    public IActionResult GetStats()
    {
        return Ok(new
        {
            totalLabels = _labelService.LabelCount,
            byType = _labelService.GetTypeCounts()
        });
    }

    [HttpGet]
    public async Task<IActionResult> GetAllAddresses([FromQuery] string? type = null)
    {
        await _labelService.EnsureFreshDataAsync();
        var addresses = _labelService.GetAllAddresses(type);
        return Ok(addresses);
    }

    [HttpPost("refresh")]
    public async Task<IActionResult> RefreshLabels()
    {
        await _labelService.RefreshLabelsAsync();
        return Ok(new
        {
            totalLabels = _labelService.LabelCount,
            message = "Labels refreshed successfully"
        });
    }

    [HttpGet("procedure/{contractAddress}/{inputType:int}")]
    public async Task<IActionResult> GetProcedureName(string contractAddress, int inputType)
    {
        await _labelService.EnsureFreshDataAsync();
        var procedureName = _labelService.GetProcedureName(contractAddress, inputType);

        return Ok(new
        {
            contractAddress,
            inputType,
            procedureName
        });
    }
}
