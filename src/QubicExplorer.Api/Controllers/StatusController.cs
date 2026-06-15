using Microsoft.AspNetCore.Mvc;
using QubicExplorer.Api.Services;

namespace QubicExplorer.Api.Controllers;

[ApiController]
[Route("api/[controller]")]
public class StatusController : ControllerBase
{
    private readonly ClickHouseQueryService _queryService;
    private readonly SpectrumImportService _spectrumService;

    public StatusController(
        ClickHouseQueryService queryService,
        SpectrumImportService spectrumService)
    {
        _queryService = queryService;
        _spectrumService = spectrumService;
    }

    /// <summary>
    /// Indexer status / frontier. Clients should bound their analytics queries
    /// by <c>safeTick</c> (tick-level) or <c>safeEpoch</c> (epoch-level) so they
    /// never read partially-indexed data.
    /// </summary>
    /// <remarks>
    /// Response fields:
    /// <list type="bullet">
    ///   <item><c>headTick</c>: highest tick the indexer has touched (from the ticks table).</item>
    ///   <item><c>safeTick</c>: highest tick at which the cross-check service has verified the row against Bob's RPC and refetched it if needed. Every tick at or below this is guaranteed consistent (correct <c>is_empty</c> flag, all tx/log rows present). Clients should bound classification queries by this value.</item>
    ///   <item><c>headEpoch</c>: most recent epoch we have any data for. Equal to the live epoch.</item>
    ///   <item><c>safeEpoch</c>: most recent epoch where everything is in — all ticks indexed and spectrum (balance_snapshots) imported. Typically <c>headEpoch - 1</c> once the previous epoch's spectrum has been imported.</item>
    ///   <item><c>spectrum.latestImportedEpoch</c>: latest epoch for which the spectrum file has been imported.</item>
    ///   <item><c>crossCheck.lastTick</c>: raw cross-check watermark from <c>indexer_state</c>.</item>
    ///   <item><c>crossCheck.lagTicks</c>: <c>headTick - safeTick</c> — number of ticks not yet verified.</item>
    ///   <item><c>tables.{ticks,transactions,logs}</c>: per-table heads for diagnostics.</item>
    ///   <item><c>asOf</c>: server UTC timestamp at which these values were computed.</item>
    /// </list>
    /// </remarks>
    [HttpGet]
    public async Task<IActionResult> GetStatus(CancellationToken ct)
    {
        var frontier = await _queryService.GetIndexerFrontierAsync(ct);
        var latestSpectrum = await _spectrumService.GetLatestImportedEpochAsync(ct);

        // safeEpoch = the highest epoch where (a) ticks are written and (b)
        // spectrum has been imported. The current head epoch is in-flight, so
        // it's never safe even if spectrum was somehow available.
        uint safeEpoch = 0;
        if (frontier.HeadEpoch > 0)
        {
            var ceiling = frontier.HeadEpoch - 1u;
            safeEpoch = Math.Min(ceiling, latestSpectrum ?? 0u);
        }

        return Ok(new
        {
            headTick = frontier.HeadTick,
            safeTick = frontier.SafeTick,
            headEpoch = frontier.HeadEpoch,
            safeEpoch,
            spectrum = new
            {
                latestImportedEpoch = latestSpectrum
            },
            crossCheck = new
            {
                lastTick = frontier.CrossCheckLastTick,
                lagTicks = frontier.HeadTick > frontier.SafeTick
                    ? frontier.HeadTick - frontier.SafeTick
                    : 0UL
            },
            tables = new
            {
                ticks = frontier.HeadTick,
                transactions = frontier.HeadTransactionTick,
                logs = frontier.HeadLogTick
            },
            asOf = DateTime.UtcNow
        });
    }
}
