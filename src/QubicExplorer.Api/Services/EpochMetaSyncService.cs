using QubicExplorer.Shared.DTOs;

namespace QubicExplorer.Api.Services;

/// <summary>
/// Background service that monitors epoch changes and syncs epoch metadata from Bob.
/// When a new epoch is detected:
/// - Fetches complete info for the previous epoch (now finalized)
/// - Fetches current info for the new epoch (ongoing)
/// </summary>
public class EpochMetaSyncService : BackgroundService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly BobProxyService _bobProxy;
    private readonly ILogger<EpochMetaSyncService> _logger;
    private uint? _lastKnownEpoch;

    public EpochMetaSyncService(
        IServiceProvider serviceProvider,
        BobProxyService bobProxy,
        ILogger<EpochMetaSyncService> logger)
    {
        _serviceProvider = serviceProvider;
        _bobProxy = bobProxy;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("EpochMetaSyncService started");

        // Wait for other services to initialize
        await Task.Delay(TimeSpan.FromSeconds(10), stoppingToken);

        // Initialize by syncing current epoch
        await InitializeCurrentEpochAsync(stoppingToken);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await CheckAndSyncEpochAsync(stoppingToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in epoch meta sync service");
            }

            // Check every minute
            await Task.Delay(TimeSpan.FromMinutes(1), stoppingToken);
        }
    }

    private async Task InitializeCurrentEpochAsync(CancellationToken ct)
    {
        try
        {
            // Get current epoch from database
            using var scope = _serviceProvider.CreateScope();
            var queryService = scope.ServiceProvider.GetRequiredService<ClickHouseQueryService>();
            var currentEpoch = await queryService.GetCurrentEpochAsync(ct);

            if (currentEpoch == null)
            {
                _logger.LogWarning("No current epoch found in database during initialization");
                return;
            }

            _lastKnownEpoch = currentEpoch.Value;
            _logger.LogInformation("Initialized with current epoch {Epoch}", _lastKnownEpoch);

            // Check if we already have valid epoch metadata
            var existingMeta = await queryService.GetEpochMetaAsync(currentEpoch.Value, ct);
            if (existingMeta != null && existingMeta.InitialTick > 0)
            {
                _logger.LogInformation("Epoch {Epoch} already has valid metadata (initialTick={InitialTick})",
                    currentEpoch.Value, existingMeta.InitialTick);
            }
            else
            {
                _logger.LogInformation("Epoch {Epoch} needs metadata sync (existing={Exists}, initialTick={InitialTick})",
                    currentEpoch.Value, existingMeta != null, existingMeta?.InitialTick ?? 0);
            }

            // Sync current and previous epochs
            await SyncEpochFromBobAsync(currentEpoch.Value, queryService, ct);
            if (currentEpoch.Value > 0)
            {
                await SyncEpochFromBobAsync(currentEpoch.Value - 1, queryService, ct);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to initialize current epoch");
        }
    }

    private async Task CheckAndSyncEpochAsync(CancellationToken ct)
    {
        using var scope = _serviceProvider.CreateScope();
        var queryService = scope.ServiceProvider.GetRequiredService<ClickHouseQueryService>();

        // Get current epoch from database (uses ticks table as source of truth)
        var currentEpoch = await queryService.GetCurrentEpochAsync(ct);
        if (currentEpoch == null)
        {
            _logger.LogDebug("No current epoch found in database");
            return;
        }

        // Check if epoch changed
        if (_lastKnownEpoch.HasValue && currentEpoch.Value > _lastKnownEpoch.Value)
        {
            _logger.LogInformation("Epoch change detected: {OldEpoch} -> {NewEpoch}",
                _lastKnownEpoch.Value, currentEpoch.Value);

            // Sync the previous epoch (now complete)
            await SyncEpochFromBobAsync(_lastKnownEpoch.Value, queryService, ct);

            // Sync the new current epoch
            await SyncEpochFromBobAsync(currentEpoch.Value, queryService, ct);
        }
        else if (!_lastKnownEpoch.HasValue)
        {
            // First run, sync current epoch
            await SyncEpochFromBobAsync(currentEpoch.Value, queryService, ct);
        }

        _lastKnownEpoch = currentEpoch.Value;
    }

    private async Task SyncEpochFromBobAsync(uint epoch, ClickHouseQueryService queryService, CancellationToken ct)
    {
        try
        {
            _logger.LogInformation("Fetching epoch info from Bob for epoch {Epoch}", epoch);
            var epochInfo = await _bobProxy.GetEpochInfoAsync(epoch, ct);
            if (epochInfo == null)
            {
                _logger.LogWarning("Could not get epoch info from Bob for epoch {Epoch} (returned null)", epoch);
                return;
            }

            _logger.LogInformation("Bob returned epoch {Epoch}: initialTick={InitialTick}, endTick={EndTick}, finalTick={FinalTick}, endTickStartLogId={EndTickStartLogId}, endTickEndLogId={EndTickEndLogId}",
                epochInfo.Epoch, epochInfo.InitialTick, epochInfo.EndTick, epochInfo.FinalTick,
                epochInfo.EndTickStartLogId, epochInfo.EndTickEndLogId);

            // Validate we got meaningful data
            if (epochInfo.InitialTick == 0)
            {
                _logger.LogWarning("Bob returned initialTick=0 for epoch {Epoch}, skipping upsert", epoch);
                return;
            }

            // Determine if epoch is complete
            // An epoch is complete if it has an end tick (finalTick or endTick > 0)
            var endTick = epochInfo.EndTick > 0 ? epochInfo.EndTick : epochInfo.FinalTick;
            var isComplete = endTick > 0 && endTick > epochInfo.InitialTick;

            var dto = new EpochMetaDto(
                epochInfo.Epoch,
                epochInfo.InitialTick,
                endTick,
                epochInfo.EndTickStartLogId,
                epochInfo.EndTickEndLogId,
                isComplete,
                DateTime.UtcNow
            );

            await queryService.UpsertEpochMetaAsync(dto, ct);
            _logger.LogInformation("Synced epoch {Epoch} metadata from Bob (initialTick={InitialTick}, endTick={EndTick}, complete={IsComplete})",
                epoch, epochInfo.InitialTick, endTick, isComplete);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to sync epoch {Epoch} from Bob", epoch);
        }
    }
}
