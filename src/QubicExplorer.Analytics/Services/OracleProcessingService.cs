using Microsoft.Extensions.Options;
using QubicExplorer.Analytics.Configuration;

namespace QubicExplorer.Analytics.Services;

/// <summary>
/// Dedicated background service for oracle event ingestion, long-term aggregation,
/// and knowledge-proof verification.
///
/// Previously this ran as a step inside <see cref="AnalyticsSnapshotService"/>'s
/// single sequential pass. That coupling was a liability: KP verification drains its
/// whole backlog inline (500 queries/batch, crypto-heavy), so a large backlog would
/// monopolise the pass and starve every other analytics step — holder distribution,
/// network stats, and most importantly computor revenue — which then never refreshed
/// for the current epoch. Running it on its own loop here fully decouples that work,
/// so heavy oracle processing can never again block the snapshot pass.
///
/// Toggle via <see cref="AnalyticsOptions.EnableOracleEvents"/>.
/// </summary>
public class OracleProcessingService : BackgroundService
{
    private readonly AnalyticsQueryService _queryService;
    private readonly OracleEventService _eventService;
    private readonly OracleAggregateService _aggregateService;
    private readonly OracleKpVerificationService _kpVerificationService;
    private readonly IOptions<AnalyticsOptions> _options;
    private readonly ILogger<OracleProcessingService> _logger;

    /// <summary>Idle wait between drain cycles once the backlog is caught up.</summary>
    private static readonly TimeSpan CycleInterval = TimeSpan.FromSeconds(30);

    public OracleProcessingService(
        AnalyticsQueryService queryService,
        OracleEventService eventService,
        OracleAggregateService aggregateService,
        OracleKpVerificationService kpVerificationService,
        IOptions<AnalyticsOptions> options,
        ILogger<OracleProcessingService> logger)
    {
        _queryService = queryService;
        _eventService = eventService;
        _aggregateService = aggregateService;
        _kpVerificationService = kpVerificationService;
        _options = options;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_options.Value.EnableOracleEvents)
        {
            _logger.LogInformation("OracleProcessingService disabled via config");
            return;
        }

        _logger.LogInformation("OracleProcessingService starting (cycle interval: {Interval}s)",
            CycleInterval.TotalSeconds);

        // Initial delay so the rest of the host can warm up.
        await Task.Delay(TimeSpan.FromSeconds(30), stoppingToken);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await ProcessOracleEventsAsync(stoppingToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _logger.LogError(ex, "Error in OracleProcessingService cycle");
            }

            try { await Task.Delay(CycleInterval, stoppingToken); }
            catch (OperationCanceledException) { break; }
        }
    }

    /// <summary>
    /// One drain cycle: raw events → epoch aggregates → KP verification. Each stage
    /// loops until its backlog is empty (the inner services page in fixed batches).
    /// </summary>
    private async Task ProcessOracleEventsAsync(CancellationToken ct)
    {
        var currentEpoch = await _queryService.GetCurrentEpochAsync(ct);
        if (currentEpoch == null)
        {
            _logger.LogWarning("Could not get current epoch, skipping oracle processing cycle");
            return;
        }

        // 1. Drain raw events
        var totalEvents = 0;
        var passes = 0;
        while (!ct.IsCancellationRequested)
        {
            var (events, hasMore) = await _eventService.ProcessAsync(currentEpoch.Value, ct);
            totalEvents += events;
            passes++;
            if (!hasMore) break;
            await Task.Delay(500, ct);
        }
        if (totalEvents > 0)
            _logger.LogInformation(
                "Oracle events: persisted {Events} events across {Passes} pass(es)",
                totalEvents, passes);

        // 2. Build aggregates for any completed epoch we haven't aggregated yet
        var epochsAggregated = 0;
        while (!ct.IsCancellationRequested && await _aggregateService.ProcessNextEpochAsync(currentEpoch.Value, ct))
        {
            epochsAggregated++;
            await Task.Delay(100, ct);
        }
        if (epochsAggregated > 0)
            _logger.LogInformation("Oracle aggregates: built {Count} epoch(s)", epochsAggregated);

        // 3. KP-verify any newly-revealed queries. Cheating commits (right
        // digest, wrong knowledge proof) get persisted to oracle_kp_forgeries.
        // Skipped when EnableOracleKpVerification = false — the K12 work can
        // be turned off independently of event ingestion + aggregation above.
        if (!_options.Value.EnableOracleKpVerification)
            return;

        var totalKpVerified = 0;
        var totalForgeries = 0;
        var kpPasses = 0;
        while (!ct.IsCancellationRequested)
        {
            var (verified, forgeries, hasMore) = await _kpVerificationService.ProcessAsync(currentEpoch.Value, ct);
            totalKpVerified += verified;
            totalForgeries += forgeries;
            kpPasses++;
            if (!hasMore) break;
            await Task.Delay(200, ct);
        }
        if (totalKpVerified > 0)
            _logger.LogInformation(
                "Oracle KP: verified {V} queries across {P} pass(es), found {F} forgery attempt(s)",
                totalKpVerified, kpPasses, totalForgeries);
    }
}
