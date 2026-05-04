using Microsoft.Extensions.Options;
using QubicExplorer.Analytics.Configuration;

namespace QubicExplorer.Analytics.Services;

/// <summary>
/// Background service that periodically saves analytics snapshots.
/// Creates snapshots every 4 hours to track historical trends for:
/// - Holder distribution (whale analysis, concentration metrics)
/// - Network stats (transactions, active addresses, exchange flows, SC usage)
/// - Burn stats (QU burned via BurnQubic SC, dust burns, direct transfers)
/// - Miner flow (computor emission tracking through multiple hops)
///
/// On startup, catches up on any missed snapshots by creating all windows
/// from the last snapshot until the current time.
///
/// Each analytics feature can be individually disabled via AnalyticsOptions.
/// </summary>
public class AnalyticsSnapshotService : BackgroundService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<AnalyticsSnapshotService> _logger;
    private readonly AnalyticsOptions _options;
    private readonly TimeSpan _snapshotInterval = TimeSpan.FromHours(4);

    public AnalyticsSnapshotService(
        IServiceProvider serviceProvider,
        IOptions<AnalyticsOptions> options,
        ILogger<AnalyticsSnapshotService> logger)
    {
        _serviceProvider = serviceProvider;
        _options = options.Value;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("AnalyticsSnapshotService started (interval: {Interval}h)",
            _snapshotInterval.TotalHours);

        // Wait for other services to initialize
        await Task.Delay(TimeSpan.FromSeconds(30), stoppingToken);

        // Catch up on any missed snapshots at startup
        await CatchUpOnMissedSnapshotsAsync(stoppingToken);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                // Get current epoch from the database
                using var scope = _serviceProvider.CreateScope();
                var queryService = scope.ServiceProvider.GetRequiredService<AnalyticsQueryService>();
                var currentEpoch = await queryService.GetCurrentEpochAsync(stoppingToken);

                if (currentEpoch == null)
                {
                    _logger.LogWarning("Could not get current epoch from database, skipping snapshot check");
                }
                else
                {
                    _logger.LogInformation("=== Analytics pass starting (epoch {Epoch}) ===", currentEpoch.Value);

                    await RunStepAsync("Holder distribution", _options.EnableHolderDistribution, async () =>
                    {
                        while (!stoppingToken.IsCancellationRequested &&
                               await CreateHolderDistributionSnapshotAsync(queryService, currentEpoch.Value, stoppingToken))
                            await Task.Delay(100, stoppingToken);
                    });

                    await RunStepAsync("Network stats", _options.EnableNetworkStats, async () =>
                    {
                        while (!stoppingToken.IsCancellationRequested &&
                               await CreateNetworkStatsSnapshotAsync(queryService, currentEpoch.Value, stoppingToken))
                            await Task.Delay(100, stoppingToken);
                    });

                    await RunStepAsync("Burn stats", _options.EnableBurnStats, async () =>
                    {
                        while (!stoppingToken.IsCancellationRequested &&
                               await CreateBurnStatsSnapshotAsync(queryService, currentEpoch.Value, stoppingToken))
                            await Task.Delay(100, stoppingToken);
                    });

                    await RunStepAsync("Miner flow", _options.EnableMinerFlow, async () =>
                    {
                        var flowService = scope.ServiceProvider.GetRequiredService<ComputorFlowService>();
                        while (!stoppingToken.IsCancellationRequested &&
                               await CreateMinerFlowSnapshotAsync(queryService, flowService, currentEpoch.Value, stoppingToken))
                            await Task.Delay(100, stoppingToken);
                    });

                    await RunStepAsync("Qearn epoch stats", _options.EnableQearn,
                        () => CreateQearnEpochStatsAsync(queryService, currentEpoch.Value, stoppingToken));

                    await RunStepAsync("CCF transfers", _options.EnableCcf, () =>
                    {
                        var bobProxy = scope.ServiceProvider.GetRequiredService<BobProxyService>();
                        return PersistCcfTransfersAsync(queryService, bobProxy, currentEpoch.Value, stoppingToken);
                    });

                    await RunStepAsync("Computor revenue", _options.EnableComputorRevenue,
                        () => ComputeComputorRevenueAsync(scope, currentEpoch.Value, stoppingToken));

                    await RunStepAsync("Tick votes", _options.EnableTickVotes,
                        () => PersistTickVotesAsync(scope, currentEpoch.Value, stoppingToken));

                    await RunStepAsync("Reward distributions", _options.EnableRewardDistributions,
                        () => PersistRewardDistributionsAsync(scope, currentEpoch.Value, stoppingToken));

                    await RunStepAsync("Execution fee reports", _options.EnableExecutionFees,
                        () => PersistExecutionFeeReportsAsync(scope, currentEpoch.Value, stoppingToken));

                    await RunStepAsync("Custom flow jobs", _options.EnableCustomFlowJobs, () =>
                    {
                        var customFlowService = scope.ServiceProvider.GetRequiredService<CustomFlowTrackingService>();
                        return customFlowService.ProcessPendingJobsAsync(stoppingToken);
                    });

                    _logger.LogInformation("=== Analytics pass complete ===");
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in analytics snapshot service");
            }

            // Check every 5 minutes
            await Task.Delay(TimeSpan.FromMinutes(5), stoppingToken);
        }
    }

    private async Task CatchUpOnMissedSnapshotsAsync(CancellationToken ct)
    {
        _logger.LogInformation("Checking for missed analytics snapshots...");

        try
        {
            using var scope = _serviceProvider.CreateScope();
            var queryService = scope.ServiceProvider.GetRequiredService<AnalyticsQueryService>();

            var currentEpoch = await queryService.GetCurrentEpochAsync(ct);
            if (currentEpoch == null)
            {
                _logger.LogWarning("Could not get current epoch, skipping catch-up");
                return;
            }

            var holderSnapshotsCreated = 0;
            await RunStepAsync("Catch-up: Holder distribution", _options.EnableHolderDistribution, async () =>
            {
                while (!ct.IsCancellationRequested)
                {
                    var created = await CreateHolderDistributionSnapshotAsync(queryService, currentEpoch.Value, ct);
                    if (!created) break;
                    holderSnapshotsCreated++;
                    await Task.Delay(100, ct);
                }
            });

            var networkSnapshotsCreated = 0;
            await RunStepAsync("Catch-up: Network stats", _options.EnableNetworkStats, async () =>
            {
                while (!ct.IsCancellationRequested)
                {
                    var created = await CreateNetworkStatsSnapshotAsync(queryService, currentEpoch.Value, ct);
                    if (!created) break;
                    networkSnapshotsCreated++;
                    await Task.Delay(100, ct);
                }
            });

            var burnSnapshotsCreated = 0;
            await RunStepAsync("Catch-up: Burn stats", _options.EnableBurnStats, async () =>
            {
                while (!ct.IsCancellationRequested)
                {
                    var created = await CreateBurnStatsSnapshotAsync(queryService, currentEpoch.Value, ct);
                    if (!created) break;
                    burnSnapshotsCreated++;
                    await Task.Delay(100, ct);
                }
            });

            var minerFlowSnapshotsCreated = 0;
            await RunStepAsync("Catch-up: Miner flow", _options.EnableMinerFlow, async () =>
            {
                var flowService = scope.ServiceProvider.GetRequiredService<ComputorFlowService>();
                while (!ct.IsCancellationRequested)
                {
                    var created = await CreateMinerFlowSnapshotAsync(queryService, flowService, currentEpoch.Value, ct);
                    if (!created) break;
                    minerFlowSnapshotsCreated++;
                    await Task.Delay(100, ct);
                }
            });

            await RunStepAsync("Catch-up: Qearn epoch stats", _options.EnableQearn,
                () => CreateQearnEpochStatsAsync(queryService, currentEpoch.Value, ct));

            await RunStepAsync("Catch-up: Custom flow jobs", _options.EnableCustomFlowJobs, () =>
            {
                var customFlowService = scope.ServiceProvider.GetRequiredService<CustomFlowTrackingService>();
                return customFlowService.ProcessPendingJobsAsync(ct);
            });

            _logger.LogInformation(
                "Catch-up complete. Snapshots created: holder={H} network={N} burn={B} minerFlow={M}",
                holderSnapshotsCreated, networkSnapshotsCreated, burnSnapshotsCreated, minerFlowSnapshotsCreated);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during analytics snapshot catch-up");
        }
    }

    private async Task<bool> CreateHolderDistributionSnapshotAsync(
        AnalyticsQueryService queryService, uint currentEpoch, CancellationToken ct)
    {
        try
        {
            var lastTickEnd = await queryService.GetLastHolderDistributionSnapshotTickEndAsync(currentEpoch, ct);

            ulong tickStart;
            DateTime windowStartTime;

            if (lastTickEnd == 0)
            {
                var firstTick = await queryService.GetFirstTickAsync(ct);
                if (firstTick == null)
                {
                    _logger.LogDebug("No ticks found in database, skipping holder distribution snapshot");
                    return false;
                }
                tickStart = firstTick.Value.TickNumber;
                windowStartTime = firstTick.Value.Timestamp;
                _logger.LogInformation("Starting holder snapshot from first tick {Tick} at {Time}", tickStart, windowStartTime);
            }
            else
            {
                var nextTick = await queryService.GetNextTickAfterAsync(lastTickEnd, ct);
                if (nextTick == null)
                {
                    _logger.LogDebug("No tick found after {Tick}, skipping holder snapshot", lastTickEnd);
                    return false;
                }
                tickStart = nextTick.Value.TickNumber;
                var lastTickTimestamp = await queryService.GetTickTimestampAsync(lastTickEnd, ct);
                windowStartTime = lastTickTimestamp ?? nextTick.Value.Timestamp;
            }

            var windowEndTime = windowStartTime.AddHours(4);

            var currentTick = await queryService.GetCurrentTickAsync(ct);
            if (currentTick == null)
            {
                _logger.LogDebug("Could not get current tick, skipping holder distribution snapshot");
                return false;
            }

            var currentTickTimestamp = await queryService.GetTickTimestampAsync(currentTick.Value, ct);
            if (currentTickTimestamp == null || currentTickTimestamp.Value < windowEndTime)
            {
                _logger.LogDebug("Not enough data for 4h holder window yet. Current tick time: {Current}, need: {Needed}",
                    currentTickTimestamp?.ToString("yyyy-MM-dd HH:mm:ss") ?? "unknown",
                    windowEndTime.ToString("yyyy-MM-dd HH:mm:ss"));
                return false;
            }

            var tickEnd = await queryService.GetTickAtTimestampAsync(windowEndTime, ct);
            if (tickEnd == null || tickEnd.Value <= tickStart)
            {
                _logger.LogDebug("Could not determine tick end for holder window ending at {Time}", windowEndTime);
                return false;
            }

            _logger.LogInformation("Creating holder snapshot for 4h window: ticks {TickStart}-{TickEnd} ({StartTime} to {EndTime})",
                tickStart, tickEnd.Value,
                windowStartTime.ToString("yyyy-MM-dd HH:mm:ss"),
                windowEndTime.ToString("yyyy-MM-dd HH:mm:ss"));

            await queryService.SaveHolderDistributionSnapshotAsync(currentEpoch, tickStart, tickEnd.Value, ct);

            _logger.LogInformation(
                "Successfully saved holder distribution snapshot for epoch {Epoch} (ticks {TickStart}-{TickEnd})",
                currentEpoch, tickStart, tickEnd.Value);

            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to save holder distribution snapshot for epoch {Epoch}", currentEpoch);
            return false;
        }
    }

    private async Task<bool> CreateNetworkStatsSnapshotAsync(
        AnalyticsQueryService queryService, uint currentEpoch, CancellationToken ct)
    {
        try
        {
            var lastTickEnd = await queryService.GetLastNetworkStatsSnapshotTickEndAsync(currentEpoch, ct);

            ulong tickStart;
            DateTime windowStartTime;

            if (lastTickEnd == 0)
            {
                var firstTick = await queryService.GetFirstTickAsync(ct);
                if (firstTick == null)
                {
                    _logger.LogDebug("No ticks found in database, skipping network stats snapshot");
                    return false;
                }
                tickStart = firstTick.Value.TickNumber;
                windowStartTime = firstTick.Value.Timestamp;
                _logger.LogInformation("Starting network stats from first tick {Tick} at {Time}", tickStart, windowStartTime);
            }
            else
            {
                var nextTick = await queryService.GetNextTickAfterAsync(lastTickEnd, ct);
                if (nextTick == null)
                {
                    _logger.LogDebug("No tick found after {Tick}, skipping network stats snapshot", lastTickEnd);
                    return false;
                }
                tickStart = nextTick.Value.TickNumber;
                var lastTickTimestamp = await queryService.GetTickTimestampAsync(lastTickEnd, ct);
                windowStartTime = lastTickTimestamp ?? nextTick.Value.Timestamp;
            }

            var windowEndTime = windowStartTime.AddHours(4);

            var currentTick = await queryService.GetCurrentTickAsync(ct);
            if (currentTick == null)
            {
                _logger.LogDebug("Could not get current tick, skipping network stats snapshot");
                return false;
            }

            var currentTickTimestamp = await queryService.GetTickTimestampAsync(currentTick.Value, ct);
            if (currentTickTimestamp == null || currentTickTimestamp.Value < windowEndTime)
            {
                _logger.LogDebug("Not enough data for 4h window yet. Current tick time: {Current}, need: {Needed}",
                    currentTickTimestamp?.ToString("yyyy-MM-dd HH:mm:ss") ?? "unknown",
                    windowEndTime.ToString("yyyy-MM-dd HH:mm:ss"));
                return false;
            }

            var tickEnd = await queryService.GetTickAtTimestampAsync(windowEndTime, ct);
            if (tickEnd == null || tickEnd.Value <= tickStart)
            {
                _logger.LogDebug("Could not determine tick end for window ending at {Time}", windowEndTime);
                return false;
            }

            _logger.LogInformation("Creating network stats snapshot for 4h window: ticks {TickStart}-{TickEnd} ({StartTime} to {EndTime})",
                tickStart, tickEnd.Value,
                windowStartTime.ToString("yyyy-MM-dd HH:mm:ss"),
                windowEndTime.ToString("yyyy-MM-dd HH:mm:ss"));

            await queryService.SaveNetworkStatsSnapshotAsync(currentEpoch, tickStart, tickEnd.Value, ct);

            _logger.LogInformation(
                "Successfully saved network stats snapshot for epoch {Epoch} (ticks {TickStart}-{TickEnd})",
                currentEpoch, tickStart, tickEnd.Value);

            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to save network stats snapshot for epoch {Epoch}", currentEpoch);
            return false;
        }
    }

    private async Task<bool> CreateMinerFlowSnapshotAsync(
        AnalyticsQueryService queryService,
        ComputorFlowService flowService,
        uint currentEpoch,
        CancellationToken ct)
    {
        try
        {
            if (currentEpoch == 0)
            {
                _logger.LogDebug("Current epoch is 0, no previous epoch to track miner flow from");
                return false;
            }
            var emissionEpoch = currentEpoch - 1;

            var lastTickEnd = await queryService.GetLastMinerFlowSnapshotTickEndAsync(currentEpoch, ct);

            ulong tickStart;
            DateTime windowStartTime;

            if (lastTickEnd == 0)
            {
                var firstTick = await queryService.GetFirstTickAsync(ct);
                if (firstTick == null)
                {
                    _logger.LogDebug("No ticks found in database, skipping miner flow snapshot");
                    return false;
                }
                tickStart = firstTick.Value.TickNumber;
                windowStartTime = firstTick.Value.Timestamp;
                _logger.LogInformation("Starting miner flow from first tick {Tick} at {Time}", tickStart, windowStartTime);
            }
            else
            {
                var nextTick = await queryService.GetNextTickAfterAsync(lastTickEnd, ct);
                if (nextTick == null)
                {
                    _logger.LogDebug("No tick found after {Tick}, skipping miner flow snapshot", lastTickEnd);
                    return false;
                }
                tickStart = nextTick.Value.TickNumber;
                var lastTickTimestamp = await queryService.GetTickTimestampAsync(lastTickEnd, ct);
                windowStartTime = lastTickTimestamp ?? nextTick.Value.Timestamp;
            }

            var windowEndTime = windowStartTime.AddHours(4);

            var currentTick = await queryService.GetCurrentTickAsync(ct);
            if (currentTick == null)
            {
                _logger.LogDebug("Could not get current tick, skipping miner flow snapshot");
                return false;
            }

            var currentTickTimestamp = await queryService.GetTickTimestampAsync(currentTick.Value, ct);
            if (currentTickTimestamp == null || currentTickTimestamp.Value < windowEndTime)
            {
                _logger.LogDebug("Not enough data for 4h miner flow window yet. Current tick time: {Current}, need: {Needed}",
                    currentTickTimestamp?.ToString("yyyy-MM-dd HH:mm:ss") ?? "unknown",
                    windowEndTime.ToString("yyyy-MM-dd HH:mm:ss"));
                return false;
            }

            var tickEnd = await queryService.GetTickAtTimestampAsync(windowEndTime, ct);
            if (tickEnd == null || tickEnd.Value <= tickStart)
            {
                _logger.LogDebug("Could not determine tick end for miner flow window ending at {Time}", windowEndTime);
                return false;
            }

            _logger.LogInformation(
                "Creating miner flow snapshot for epoch {Epoch} (emission from {EmissionEpoch}): ticks {TickStart}-{TickEnd} ({StartTime} to {EndTime})",
                currentEpoch, emissionEpoch, tickStart, tickEnd.Value,
                windowStartTime.ToString("yyyy-MM-dd HH:mm:ss"),
                windowEndTime.ToString("yyyy-MM-dd HH:mm:ss"));

            var stats = await flowService.AnalyzeFlowForWindowAsync(
                currentEpoch, emissionEpoch, tickStart, tickEnd.Value, ct);

            if (stats == null)
            {
                _logger.LogWarning("Miner flow analysis returned null for epoch {Epoch}", currentEpoch);
                return false;
            }

            _logger.LogInformation(
                "Successfully saved miner flow snapshot for epoch {Epoch}: emission={Emission}, outflow={Outflow}, toExchange={ToExchange}",
                currentEpoch, stats.TotalEmission, stats.TotalOutflow, stats.FlowToExchangeTotal);

            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to save miner flow snapshot for epoch {Epoch}", currentEpoch);
            return false;
        }
    }

    /// <summary>
    /// Compute Qearn per-epoch stats for all completed epochs that are missing.
    /// Qearn was active from epoch 138 onwards. Stats are immutable once an epoch ends.
    /// </summary>
    private async Task CreateQearnEpochStatsAsync(
        AnalyticsQueryService queryService, uint currentEpoch, CancellationToken ct)
    {
        const uint qearnInitialEpoch = 138;
        if (currentEpoch <= qearnInitialEpoch) return;

        try
        {
            var persisted = await queryService.GetPersistedQearnEpochsAsync(ct);

            // Build the list of epochs we still need to compute, so we know
            // total work upfront and can log meaningful progress.
            var todo = new List<uint>();
            for (var epoch = qearnInitialEpoch; epoch < currentEpoch; epoch++)
            {
                if (!persisted.Contains(epoch)) todo.Add(epoch);
            }

            if (todo.Count == 0) return;

            _logger.LogInformation("Qearn: {Count} epoch(s) pending (range {First}..{Last})",
                todo.Count, todo[0], todo[^1]);

            var sw = System.Diagnostics.Stopwatch.StartNew();
            var created = 0;
            var idx = 0;

            foreach (var epoch in todo)
            {
                if (ct.IsCancellationRequested) break;
                idx++;

                var saved = await queryService.SaveQearnEpochStatsAsync(epoch, ct);
                if (saved) created++;

                // Progress every 10 epochs (or last one)
                if (idx % 10 == 0 || idx == todo.Count)
                {
                    var rate = idx / Math.Max(sw.Elapsed.TotalSeconds, 0.001);
                    var remaining = (todo.Count - idx) / Math.Max(rate, 0.001);
                    _logger.LogInformation(
                        "Qearn progress: {Done}/{Total} epochs (epoch {Epoch}, {Rate:F1}/s, ETA {Eta:mm\\:ss})",
                        idx, todo.Count, epoch, rate, TimeSpan.FromSeconds(remaining));
                }

                await Task.Delay(50, ct);
            }

            _logger.LogInformation("Qearn: computed stats for {Count}/{Total} epochs in {Elapsed}",
                created, todo.Count, sw.Elapsed);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error computing Qearn epoch stats");
        }
    }

    private async Task<bool> CreateBurnStatsSnapshotAsync(
        AnalyticsQueryService queryService, uint currentEpoch, CancellationToken ct)
    {
        try
        {
            var lastTickEnd = await queryService.GetLastBurnStatsSnapshotTickEndAsync(currentEpoch, ct);
            _logger.LogInformation("Burn stats: lastTickEnd={LastTickEnd} for epoch {Epoch}", lastTickEnd, currentEpoch);

            ulong tickStart;
            DateTime windowStartTime;

            if (lastTickEnd == 0)
            {
                var firstTick = await queryService.GetFirstTickAsync(ct);
                if (firstTick == null)
                {
                    _logger.LogInformation("No ticks found in database, skipping burn stats snapshot");
                    return false;
                }
                tickStart = firstTick.Value.TickNumber;
                windowStartTime = firstTick.Value.Timestamp;
                _logger.LogInformation("Starting burn stats from first tick {Tick} at {Time}", tickStart, windowStartTime);
            }
            else
            {
                var nextTick = await queryService.GetNextTickAfterAsync(lastTickEnd, ct);
                if (nextTick == null)
                {
                    _logger.LogInformation("No tick found after {Tick}, skipping burn stats snapshot", lastTickEnd);
                    return false;
                }
                tickStart = nextTick.Value.TickNumber;
                var lastTickTimestamp = await queryService.GetTickTimestampAsync(lastTickEnd, ct);
                windowStartTime = lastTickTimestamp ?? nextTick.Value.Timestamp;
                _logger.LogInformation("Burn stats: continuing from tick {Tick}, window start {Time}", tickStart, windowStartTime);
            }

            var windowEndTime = windowStartTime.AddHours(4);

            var currentTick = await queryService.GetCurrentTickAsync(ct);
            if (currentTick == null)
            {
                _logger.LogInformation("Could not get current tick, skipping burn stats snapshot");
                return false;
            }

            var currentTickTimestamp = await queryService.GetTickTimestampAsync(currentTick.Value, ct);
            if (currentTickTimestamp == null || currentTickTimestamp.Value < windowEndTime)
            {
                _logger.LogInformation("Not enough data for 4h burn stats window yet. Current: {Current}, need: {Needed}",
                    currentTickTimestamp?.ToString("yyyy-MM-dd HH:mm:ss") ?? "unknown",
                    windowEndTime.ToString("yyyy-MM-dd HH:mm:ss"));
                return false;
            }

            var tickEnd = await queryService.GetTickAtTimestampAsync(windowEndTime, ct);
            if (tickEnd == null || tickEnd.Value <= tickStart)
            {
                _logger.LogInformation("Could not determine tick end for burn stats window ending at {Time}", windowEndTime);
                return false;
            }

            _logger.LogInformation("Creating burn stats snapshot for 4h window: ticks {TickStart}-{TickEnd} ({StartTime} to {EndTime})",
                tickStart, tickEnd.Value,
                windowStartTime.ToString("yyyy-MM-dd HH:mm:ss"),
                windowEndTime.ToString("yyyy-MM-dd HH:mm:ss"));

            await queryService.SaveBurnStatsSnapshotAsync(currentEpoch, tickStart, tickEnd.Value, ct);

            _logger.LogInformation(
                "Successfully saved burn stats snapshot for epoch {Epoch} (ticks {TickStart}-{TickEnd})",
                currentEpoch, tickStart, tickEnd.Value);

            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to save burn stats snapshot for epoch {Epoch}", currentEpoch);
            return false;
        }
    }

    /// <summary>
    /// Poll the CCF contract for latest transfers and regular payments, persist any new entries.
    /// </summary>
    private async Task PersistCcfTransfersAsync(
        AnalyticsQueryService queryService, BobProxyService bobProxy,
        uint currentEpoch, CancellationToken ct)
    {
        try
        {
            // Build a tick → epoch mapping from the ticks table
            await queryService.GetTickEpochMapAsync(ct);
            uint TickToEpoch(uint tick)
            {
                var ep = queryService.GetEpochForTick(tick);
                return ep > 0 ? ep : currentEpoch;
            }

            // Poll one-time transfers
            var transfers = await bobProxy.GetCcfLatestTransfersAsync(ct);
            if (transfers.Count > 0)
            {
                var persisted = await queryService.PersistCcfTransfersAsync(transfers, TickToEpoch, ct);
                if (persisted > 0)
                    _logger.LogInformation("CCF: persisted {Count} new one-time transfers", persisted);
            }

            // Poll regular payments
            var payments = await bobProxy.GetCcfRegularPaymentsAsync(ct);
            if (payments.Count > 0)
            {
                var persisted = await queryService.PersistCcfRegularPaymentsAsync(payments, TickToEpoch, ct);
                if (persisted > 0)
                    _logger.LogInformation("CCF: persisted {Count} new regular payments", persisted);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error persisting CCF transfers");
        }
    }

    private async Task ComputeComputorRevenueAsync(IServiceScope scope, uint currentEpoch, CancellationToken ct)
    {
        try
        {
            var revenueService = scope.ServiceProvider.GetRequiredService<ComputorRevenueService>();
            await revenueService.ComputeAndPersistAsync(currentEpoch, ct);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error computing computor revenue for epoch {Epoch}", currentEpoch);
        }
    }

    private async Task PersistTickVotesAsync(IServiceScope scope, uint currentEpoch, CancellationToken ct)
    {
        try
        {
            var voteService = scope.ServiceProvider.GetRequiredService<TickVotePersistenceService>();
            var windowsProcessed = 0;
            while (!ct.IsCancellationRequested && await voteService.ProcessNextWindowAsync(currentEpoch, ct))
            {
                windowsProcessed++;
                await Task.Delay(50, ct);
            }

            if (windowsProcessed > 0)
                _logger.LogInformation("Persisted {Count} tick vote windows for epoch {Epoch}", windowsProcessed, currentEpoch);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error persisting tick votes for epoch {Epoch}", currentEpoch);
        }
    }

    private async Task PersistRewardDistributionsAsync(IServiceScope scope, uint currentEpoch, CancellationToken ct)
    {
        try
        {
            var rewardService = scope.ServiceProvider.GetRequiredService<RewardDistributionPersistenceService>();
            var epochsProcessed = 0;
            while (!ct.IsCancellationRequested && await rewardService.ProcessNextEpochAsync(currentEpoch, ct))
            {
                epochsProcessed++;
                await Task.Delay(100, ct);
            }

            if (epochsProcessed > 0)
                _logger.LogInformation("Persisted reward distributions for {Count} epoch(s)", epochsProcessed);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error persisting reward distributions");
        }
    }

    /// <summary>
    /// Wraps a step with start/finish/duration log lines and a skipped message
    /// when the toggle is off. Errors inside the step bubble up to the caller's
    /// existing try/catch.
    /// </summary>
    private async Task RunStepAsync(string name, bool enabled, Func<Task> work)
    {
        if (!enabled)
        {
            _logger.LogInformation("[{Step}] skipped (disabled)", name);
            return;
        }

        var sw = System.Diagnostics.Stopwatch.StartNew();
        _logger.LogInformation("[{Step}] starting...", name);
        try
        {
            await work();
            _logger.LogInformation("[{Step}] finished in {Elapsed}", name, sw.Elapsed);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "[{Step}] failed after {Elapsed}", name, sw.Elapsed);
        }
    }

    private async Task PersistExecutionFeeReportsAsync(IServiceScope scope, uint currentEpoch, CancellationToken ct)
    {
        try
        {
            var feeService = scope.ServiceProvider.GetRequiredService<ExecutionFeeReportService>();
            var totalTicks = 0;
            var passes = 0;

            // Drain pending work in this analytics step instead of waiting for the next 5min loop.
            // Each pass is capped at 2M rows so we yield periodically and don't starve other work.
            while (!ct.IsCancellationRequested)
            {
                var (ticks, hasMore) = await feeService.ProcessAsync(currentEpoch, ct);
                totalTicks += ticks;
                passes++;

                if (!hasMore) break;

                // Short delay between passes so other queries can interleave.
                await Task.Delay(500, ct);
            }

            if (totalTicks > 0)
                _logger.LogInformation(
                    "Execution fee reports: processed {Ticks} phase tick(s) across {Passes} pass(es)",
                    totalTicks, passes);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error persisting execution fee reports");
        }
    }
}
