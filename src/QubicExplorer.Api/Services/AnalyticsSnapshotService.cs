namespace QubicExplorer.Api.Services;

/// <summary>
/// Background service that periodically saves analytics snapshots.
/// Creates snapshots every 4 hours to track historical trends for:
/// - Holder distribution (whale analysis, concentration metrics)
/// - Network stats (transactions, active addresses, exchange flows, SC usage)
///
/// On startup, catches up on any missed snapshots by creating all windows
/// from the last snapshot until the current time.
/// </summary>
public class AnalyticsSnapshotService : BackgroundService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<AnalyticsSnapshotService> _logger;
    private readonly TimeSpan _snapshotInterval = TimeSpan.FromHours(4);

    public AnalyticsSnapshotService(
        IServiceProvider serviceProvider,
        ILogger<AnalyticsSnapshotService> logger)
    {
        _serviceProvider = serviceProvider;
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
                var queryService = scope.ServiceProvider.GetRequiredService<ClickHouseQueryService>();
                var currentEpoch = await queryService.GetCurrentEpochAsync(stoppingToken);

                if (currentEpoch == null)
                {
                    _logger.LogWarning("Could not get current epoch from database, skipping snapshot check");
                }
                else
                {
                    // Create all pending snapshots (loop until no more can be created)
                    while (!stoppingToken.IsCancellationRequested &&
                           await CreateHolderDistributionSnapshotAsync(queryService, currentEpoch.Value, stoppingToken))
                    {
                        await Task.Delay(100, stoppingToken);
                    }

                    while (!stoppingToken.IsCancellationRequested &&
                           await CreateNetworkStatsSnapshotAsync(queryService, currentEpoch.Value, stoppingToken))
                    {
                        await Task.Delay(100, stoppingToken);
                    }

                    while (!stoppingToken.IsCancellationRequested &&
                           await CreateBurnStatsSnapshotAsync(queryService, currentEpoch.Value, stoppingToken))
                    {
                        await Task.Delay(100, stoppingToken);
                    }

                    // Create miner flow snapshots
                    var flowService = scope.ServiceProvider.GetRequiredService<ComputorFlowService>();
                    while (!stoppingToken.IsCancellationRequested &&
                           await CreateMinerFlowSnapshotAsync(queryService, flowService, currentEpoch.Value, stoppingToken))
                    {
                        await Task.Delay(100, stoppingToken);
                    }
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

    /// <summary>
    /// Catches up on any missed snapshots by creating all 4-hour windows
    /// from the last snapshot until the current time.
    /// </summary>
    private async Task CatchUpOnMissedSnapshotsAsync(CancellationToken ct)
    {
        _logger.LogInformation("Checking for missed analytics snapshots...");

        try
        {
            using var scope = _serviceProvider.CreateScope();
            var queryService = scope.ServiceProvider.GetRequiredService<ClickHouseQueryService>();

            var currentEpoch = await queryService.GetCurrentEpochAsync(ct);
            if (currentEpoch == null)
            {
                _logger.LogWarning("Could not get current epoch, skipping catch-up");
                return;
            }

            // Catch up on holder distribution snapshots
            var holderSnapshotsCreated = 0;
            while (!ct.IsCancellationRequested)
            {
                var created = await CreateHolderDistributionSnapshotAsync(queryService, currentEpoch.Value, ct);
                if (!created) break;
                holderSnapshotsCreated++;

                // Small delay to avoid overwhelming the database
                await Task.Delay(100, ct);
            }

            if (holderSnapshotsCreated > 0)
            {
                _logger.LogInformation("Created {Count} holder distribution snapshots during catch-up", holderSnapshotsCreated);
            }

            // Catch up on network stats snapshots
            var networkSnapshotsCreated = 0;
            while (!ct.IsCancellationRequested)
            {
                var created = await CreateNetworkStatsSnapshotAsync(queryService, currentEpoch.Value, ct);
                if (!created) break;
                networkSnapshotsCreated++;

                // Small delay to avoid overwhelming the database
                await Task.Delay(100, ct);
            }

            if (networkSnapshotsCreated > 0)
            {
                _logger.LogInformation("Created {Count} network stats snapshots during catch-up", networkSnapshotsCreated);
            }

            // Catch up on burn stats snapshots
            var burnSnapshotsCreated = 0;
            while (!ct.IsCancellationRequested)
            {
                var created = await CreateBurnStatsSnapshotAsync(queryService, currentEpoch.Value, ct);
                if (!created) break;
                burnSnapshotsCreated++;

                await Task.Delay(100, ct);
            }

            if (burnSnapshotsCreated > 0)
            {
                _logger.LogInformation("Created {Count} burn stats snapshots during catch-up", burnSnapshotsCreated);
            }

            // Catch up on miner flow snapshots
            var flowService = scope.ServiceProvider.GetRequiredService<ComputorFlowService>();
            var minerFlowSnapshotsCreated = 0;
            while (!ct.IsCancellationRequested)
            {
                var created = await CreateMinerFlowSnapshotAsync(queryService, flowService, currentEpoch.Value, ct);
                if (!created) break;
                minerFlowSnapshotsCreated++;

                // Small delay to avoid overwhelming the database
                await Task.Delay(100, ct);
            }

            if (minerFlowSnapshotsCreated > 0)
            {
                _logger.LogInformation("Created {Count} miner flow snapshots during catch-up", minerFlowSnapshotsCreated);
            }

            if (holderSnapshotsCreated == 0 && networkSnapshotsCreated == 0 && burnSnapshotsCreated == 0 && minerFlowSnapshotsCreated == 0)
            {
                _logger.LogInformation("Analytics snapshots are up to date");
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during analytics snapshot catch-up");
        }
    }

    /// <summary>
    /// Creates a holder distribution snapshot for the next 4-hour window.
    /// Returns true if a snapshot was created, false if there's not enough data yet.
    /// </summary>
    private async Task<bool> CreateHolderDistributionSnapshotAsync(
        ClickHouseQueryService queryService, uint currentEpoch, CancellationToken ct)
    {
        try
        {
            // Get the last snapshot's tick_end to determine the starting point
            var lastTickEnd = await queryService.GetLastHolderDistributionSnapshotTickEndAsync(currentEpoch, ct);

            ulong tickStart;
            DateTime windowStartTime;

            if (lastTickEnd == 0)
            {
                // No previous snapshot - start from the first tick in the database
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
                // Find the next tick after the last snapshot's end (tick numbers have gaps)
                var nextTick = await queryService.GetNextTickAfterAsync(lastTickEnd, ct);
                if (nextTick == null)
                {
                    _logger.LogDebug("No tick found after {Tick}, skipping holder snapshot", lastTickEnd);
                    return false;
                }
                tickStart = nextTick.Value.TickNumber;
                var startTimestamp = (DateTime?)nextTick.Value.Timestamp;
                windowStartTime = startTimestamp.Value;
            }

            // Calculate the window end time (start + 4 hours)
            var windowEndTime = windowStartTime.AddHours(4);

            // Check if we have enough data for a full 4-hour window
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

            // Get the tick at the window end time
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

    /// <summary>
    /// Creates a network stats snapshot for the next 4-hour window.
    /// Returns true if a snapshot was created, false if there's not enough data yet.
    /// </summary>
    private async Task<bool> CreateNetworkStatsSnapshotAsync(
        ClickHouseQueryService queryService, uint currentEpoch, CancellationToken ct)
    {
        try
        {
            // Get the last snapshot's tick_end to determine the starting point
            var lastTickEnd = await queryService.GetLastNetworkStatsSnapshotTickEndAsync(currentEpoch, ct);

            ulong tickStart;
            DateTime windowStartTime;

            if (lastTickEnd == 0)
            {
                // No previous snapshot - start from the first tick in the database
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
                // Find the next tick after the last snapshot's end (tick numbers have gaps)
                var nextTick = await queryService.GetNextTickAfterAsync(lastTickEnd, ct);
                if (nextTick == null)
                {
                    _logger.LogDebug("No tick found after {Tick}, skipping network stats snapshot", lastTickEnd);
                    return false;
                }
                tickStart = nextTick.Value.TickNumber;
                var startTimestamp = (DateTime?)nextTick.Value.Timestamp;
                windowStartTime = startTimestamp.Value;
            }

            // Calculate the window end time (start + 4 hours)
            var windowEndTime = windowStartTime.AddHours(4);

            // Check if we have enough data for a full 4-hour window
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

            // Get the tick at the window end time
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

    /// <summary>
    /// Creates a miner flow snapshot for the next 4-hour window.
    /// Tracks money flow from computors (who receive epoch emission) through multiple hops.
    /// Revenue distribution happens at the END of each epoch (in the last tick).
    /// We track computors from the current epoch receiving their rewards.
    /// Returns true if a snapshot was created, false if there's not enough data yet.
    /// </summary>
    private async Task<bool> CreateMinerFlowSnapshotAsync(
        ClickHouseQueryService queryService,
        ComputorFlowService flowService,
        uint currentEpoch,
        CancellationToken ct)
    {
        try
        {
            // We are in epoch N+1 (currentEpoch).
            // Computors from epoch N received their emission at the END of epoch N (last tick).
            // So we track computors from the PREVIOUS epoch (N = currentEpoch - 1).
            // Their outflows during epoch N+1 represent the money flow we want to track.
            if (currentEpoch == 0)
            {
                _logger.LogDebug("Current epoch is 0, no previous epoch to track miner flow from");
                return false;
            }
            var emissionEpoch = currentEpoch - 1;

            // Get the last miner flow snapshot's tick_end to determine the starting point
            var lastTickEnd = await queryService.GetLastMinerFlowSnapshotTickEndAsync(currentEpoch, ct);

            ulong tickStart;
            DateTime windowStartTime;

            if (lastTickEnd == 0)
            {
                // No previous snapshot - start from the first tick in the database
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
                // Find the next tick after the last snapshot's end (tick numbers have gaps)
                var nextTick = await queryService.GetNextTickAfterAsync(lastTickEnd, ct);
                if (nextTick == null)
                {
                    _logger.LogDebug("No tick found after {Tick}, skipping miner flow snapshot", lastTickEnd);
                    return false;
                }
                tickStart = nextTick.Value.TickNumber;
                var startTimestamp = (DateTime?)nextTick.Value.Timestamp;
                windowStartTime = startTimestamp.Value;
            }

            // Calculate the window end time (start + 4 hours)
            var windowEndTime = windowStartTime.AddHours(4);

            // Check if we have enough data for a full 4-hour window
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

            // Get the tick at the window end time
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

            // Run the flow analysis
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
    /// Creates a burn stats snapshot for the next 4-hour window.
    /// Returns true if a snapshot was created, false if there's not enough data yet.
    /// </summary>
    private async Task<bool> CreateBurnStatsSnapshotAsync(
        ClickHouseQueryService queryService, uint currentEpoch, CancellationToken ct)
    {
        try
        {
            var lastTickEnd = await queryService.GetLastBurnStatsSnapshotTickEndAsync(currentEpoch, ct);

            ulong tickStart;
            DateTime windowStartTime;

            if (lastTickEnd == 0)
            {
                var firstTick = await queryService.GetFirstTickAsync(ct);
                if (firstTick == null)
                {
                    _logger.LogDebug("No ticks found in database, skipping burn stats snapshot");
                    return false;
                }
                tickStart = firstTick.Value.TickNumber;
                windowStartTime = firstTick.Value.Timestamp;
                _logger.LogInformation("Starting burn stats from first tick {Tick} at {Time}", tickStart, windowStartTime);
            }
            else
            {
                // Find the next tick after the last snapshot's end (tick numbers have gaps)
                var nextTick = await queryService.GetNextTickAfterAsync(lastTickEnd, ct);
                if (nextTick == null)
                {
                    _logger.LogDebug("No tick found after {Tick}, skipping burn stats snapshot", lastTickEnd);
                    return false;
                }
                tickStart = nextTick.Value.TickNumber;
                var startTimestamp = (DateTime?)nextTick.Value.Timestamp;
                windowStartTime = startTimestamp.Value;
            }

            var windowEndTime = windowStartTime.AddHours(4);

            var currentTick = await queryService.GetCurrentTickAsync(ct);
            if (currentTick == null)
            {
                _logger.LogDebug("Could not get current tick, skipping burn stats snapshot");
                return false;
            }

            var currentTickTimestamp = await queryService.GetTickTimestampAsync(currentTick.Value, ct);
            if (currentTickTimestamp == null || currentTickTimestamp.Value < windowEndTime)
            {
                _logger.LogDebug("Not enough data for 4h burn stats window yet. Current tick time: {Current}, need: {Needed}",
                    currentTickTimestamp?.ToString("yyyy-MM-dd HH:mm:ss") ?? "unknown",
                    windowEndTime.ToString("yyyy-MM-dd HH:mm:ss"));
                return false;
            }

            var tickEnd = await queryService.GetTickAtTimestampAsync(windowEndTime, ct);
            if (tickEnd == null || tickEnd.Value <= tickStart)
            {
                _logger.LogDebug("Could not determine tick end for burn stats window ending at {Time}", windowEndTime);
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
}
