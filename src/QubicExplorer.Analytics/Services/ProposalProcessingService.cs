using Microsoft.Extensions.Options;
using QubicExplorer.Analytics.Configuration;

namespace QubicExplorer.Analytics.Services;

/// <summary>
/// Coordinator for proposal ingestion + result tallying. Runs on its own loop
/// so it can't block (or be blocked by) the general analytics snapshot pass.
///
/// Each cycle:
///   1. Drain SetProposal + Vote txs into <c>proposals</c> / <c>proposal_votes</c>
///   2. For every closed epoch without a <c>proposal_results</c> row, tally + verify.
///
/// Toggle via <see cref="AnalyticsOptions.EnableProposals"/>.
/// </summary>
public class ProposalProcessingService : BackgroundService
{
    private readonly AnalyticsQueryService _queryService;
    private readonly ProposalIndexingService _indexer;
    private readonly ProposalResultsService _results;
    private readonly IOptions<AnalyticsOptions> _options;
    private readonly ILogger<ProposalProcessingService> _logger;

    private static readonly TimeSpan CycleInterval = TimeSpan.FromSeconds(60);

    public ProposalProcessingService(
        AnalyticsQueryService queryService,
        ProposalIndexingService indexer,
        ProposalResultsService results,
        IOptions<AnalyticsOptions> options,
        ILogger<ProposalProcessingService> logger)
    {
        _queryService = queryService;
        _indexer = indexer;
        _results = results;
        _options = options;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_options.Value.EnableProposals)
        {
            _logger.LogInformation("ProposalProcessingService disabled via config");
            return;
        }

        _logger.LogInformation("ProposalProcessingService starting (cycle interval: {Interval}s)",
            CycleInterval.TotalSeconds);

        // Initial warm-up delay so the rest of the host is ready.
        await Task.Delay(TimeSpan.FromSeconds(45), stoppingToken);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await RunCycleAsync(stoppingToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _logger.LogError(ex, "Error in ProposalProcessingService cycle");
            }

            try { await Task.Delay(CycleInterval, stoppingToken); }
            catch (OperationCanceledException) { break; }
        }
    }

    private async Task RunCycleAsync(CancellationToken ct)
    {
        var currentEpoch = await _queryService.GetCurrentEpochAsync(ct);
        if (currentEpoch == null)
        {
            _logger.LogWarning("Could not get current epoch, skipping proposals cycle");
            return;
        }

        // 1. Drain ingestion
        var totalRows = 0;
        var passes = 0;
        while (!ct.IsCancellationRequested)
        {
            var (rows, hasMore) = await _indexer.ProcessAsync(currentEpoch.Value, ct);
            totalRows += rows;
            passes++;
            if (!hasMore) break;
            await Task.Delay(500, ct);
        }
        if (totalRows > 0)
            _logger.LogInformation(
                "Proposals: ingested {Rows} rows across {Passes} pass(es)", totalRows, passes);

        // 2. Tally closed epochs
        var tallied = 0;
        while (!ct.IsCancellationRequested && await _results.ProcessNextEpochAsync(currentEpoch.Value, ct))
        {
            tallied++;
            await Task.Delay(200, ct);
        }
        if (tallied > 0)
            _logger.LogInformation("Proposals: tallied {Count} closed epoch(s)", tallied);
    }
}
