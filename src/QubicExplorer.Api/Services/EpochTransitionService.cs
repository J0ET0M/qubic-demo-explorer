using QubicExplorer.Shared.Constants;
using QubicExplorer.Shared.DTOs;

namespace QubicExplorer.Api.Services;

/// <summary>
/// Background service that validates epoch transitions.
/// When a new epoch is detected, it verifies that:
/// 1. We have all logs up to endTickStartLogId - 1
/// 2. Fetches end-epoch logs from Bob if missing
/// 3. Verifies the END_EPOCH log (type 255) with correct raw data exists
/// </summary>
public class EpochTransitionService : BackgroundService
{
    private const ulong END_EPOCH_OP_CODE = LogTypes.CustomMessageOpEndEpoch;

    private readonly IServiceProvider _serviceProvider;
    private readonly BobProxyService _bobProxy;
    private readonly ILogger<EpochTransitionService> _logger;
    private uint? _lastKnownEpoch;
    private bool _hasCriticalError;
    private uint? _criticalErrorEpoch;

    public EpochTransitionService(
        IServiceProvider serviceProvider,
        BobProxyService bobProxy,
        ILogger<EpochTransitionService> logger)
    {
        _serviceProvider = serviceProvider;
        _bobProxy = bobProxy;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("EpochTransitionService started");

        // Wait for other services to initialize
        await Task.Delay(TimeSpan.FromSeconds(15), stoppingToken);

        // First run: validate previous epoch on startup
        bool isFirstRun = true;

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await CheckEpochTransitionAsync(stoppingToken, isFirstRun);
                isFirstRun = false;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in epoch transition service");
            }

            // If we have a critical error, retry every 30 minutes
            // Otherwise, check every minute
            var delay = _hasCriticalError
                ? TimeSpan.FromMinutes(30)
                : TimeSpan.FromMinutes(1);

            await Task.Delay(delay, stoppingToken);
        }
    }

    private async Task CheckEpochTransitionAsync(CancellationToken ct, bool validatePreviousEpoch = false)
    {
        using var scope = _serviceProvider.CreateScope();
        var queryService = scope.ServiceProvider.GetRequiredService<ClickHouseQueryService>();

        // Get current epoch from database
        var currentEpoch = await queryService.GetCurrentEpochAsync(ct);
        if (currentEpoch == null)
        {
            _logger.LogDebug("No current epoch found in database");
            return;
        }

        // If we have a critical error for a specific epoch, try to resolve it
        if (_hasCriticalError && _criticalErrorEpoch.HasValue)
        {
            _logger.LogWarning("Retrying epoch transition validation for epoch {Epoch} (critical error state)",
                _criticalErrorEpoch.Value);
            await ValidateEpochEndAsync(_criticalErrorEpoch.Value, queryService, ct);
            _lastKnownEpoch = currentEpoch.Value;
            return;
        }

        // On startup, validate the previous epoch
        if (validatePreviousEpoch && currentEpoch.Value > 0)
        {
            var previousEpoch = currentEpoch.Value - 1;
            _logger.LogInformation("Startup sanity check: validating previous epoch {Epoch}", previousEpoch);
            await ValidateEpochEndAsync(previousEpoch, queryService, ct);
            _lastKnownEpoch = currentEpoch.Value;
            return;
        }

        // Detect epoch change during normal operation
        if (_lastKnownEpoch.HasValue && currentEpoch.Value > _lastKnownEpoch.Value)
        {
            _logger.LogInformation("Epoch transition detected: {OldEpoch} -> {NewEpoch}",
                _lastKnownEpoch.Value, currentEpoch.Value);

            // Validate the previous epoch (now complete)
            await ValidateEpochEndAsync(_lastKnownEpoch.Value, queryService, ct);
        }

        _lastKnownEpoch = currentEpoch.Value;
    }

    private async Task ValidateEpochEndAsync(uint epoch, ClickHouseQueryService queryService, CancellationToken ct)
    {
        _logger.LogInformation("Validating epoch end for epoch {Epoch}", epoch);

        // Get epoch info from Bob
        var epochInfo = await _bobProxy.GetEpochInfoAsync(epoch, ct);
        if (epochInfo == null)
        {
            _logger.LogWarning("Could not get epoch info from Bob for epoch {Epoch}", epoch);
            SetCriticalError(epoch, "Could not fetch epoch info from Bob");
            return;
        }

        // Check if epoch is complete (has end tick info)
        if (epochInfo.EndTickStartLogId == 0 || epochInfo.EndTickEndLogId == 0)
        {
            _logger.LogWarning("Epoch {Epoch} doesn't have complete end tick info yet (endTickStartLogId={Start}, endTickEndLogId={End})",
                epoch, epochInfo.EndTickStartLogId, epochInfo.EndTickEndLogId);
            SetCriticalError(epoch, "Epoch doesn't have complete end tick info");
            return;
        }

        _logger.LogInformation("Epoch {Epoch} end tick log range: {Start} - {End}",
            epoch, epochInfo.EndTickStartLogId, epochInfo.EndTickEndLogId);

        // Get the highest log_id in our database for this epoch
        var maxLogId = await queryService.GetMaxLogIdForEpochAsync(epoch, ct);
        _logger.LogInformation("Current max log_id in database for epoch {Epoch}: {MaxLogId}", epoch, maxLogId);

        var expectedMaxBeforeEndLogs = epochInfo.EndTickStartLogId - 1;

        // Case 1: We have more logs than expected - might already be complete
        if (maxLogId >= epochInfo.EndTickEndLogId)
        {
            _logger.LogInformation("Database already has all logs including end-epoch logs for epoch {Epoch}", epoch);
            await VerifyEndEpochLogAsync(epoch, epochInfo.EndTickStartLogId, epochInfo.EndTickEndLogId, queryService, ct);
            return;
        }

        // Case 2: We're missing some logs before the end tick
        if (maxLogId < expectedMaxBeforeEndLogs)
        {
            _logger.LogCritical(
                "CRITICAL: Missing logs before end tick for epoch {Epoch}. Max log_id: {MaxLogId}, expected: {Expected} (endTickStartLogId - 1)",
                epoch, maxLogId, expectedMaxBeforeEndLogs);
            SetCriticalError(epoch, $"Missing logs before end tick. Have up to {maxLogId}, need up to {expectedMaxBeforeEndLogs}");
            return;
        }

        // Case 3: We have all logs up to endTickStartLogId - 1, need to fetch end epoch logs
        if (maxLogId == expectedMaxBeforeEndLogs || (maxLogId >= expectedMaxBeforeEndLogs && maxLogId < epochInfo.EndTickEndLogId))
        {
            _logger.LogInformation("Fetching end epoch logs for epoch {Epoch} (logs {Start} to {End})",
                epoch, epochInfo.EndTickStartLogId, epochInfo.EndTickEndLogId);

            var endLogs = await _bobProxy.GetEndEpochLogsAsync(epoch, ct);
            if (endLogs == null || endLogs.Count == 0)
            {
                _logger.LogCritical("CRITICAL: Could not fetch end epoch logs from Bob for epoch {Epoch}", epoch);
                SetCriticalError(epoch, "Could not fetch end epoch logs from Bob");
                return;
            }

            _logger.LogInformation("Received {Count} end epoch logs for epoch {Epoch}", endLogs.Count, epoch);

            // Insert the end epoch logs
            await queryService.InsertEndEpochLogsAsync(epoch, endLogs, ct);
            _logger.LogInformation("Inserted {Count} end epoch logs for epoch {Epoch}", endLogs.Count, epoch);

            // Verify the END_EPOCH log exists
            await VerifyEndEpochLogAsync(epoch, epochInfo.EndTickStartLogId, epochInfo.EndTickEndLogId, queryService, ct);
        }
    }

    private async Task VerifyEndEpochLogAsync(
        uint epoch,
        ulong startLogId,
        ulong endLogId,
        ClickHouseQueryService queryService,
        CancellationToken ct)
    {
        // Look for log type 255 (CUSTOM_MESSAGE) with END_EPOCH raw data
        var hasEndEpochLog = await queryService.HasEndEpochLogAsync(epoch, startLogId, endLogId, END_EPOCH_OP_CODE, ct);

        if (!hasEndEpochLog)
        {
            _logger.LogCritical(
                "CRITICAL: END_EPOCH log (type 255 with OP code {OpCode}) not found for epoch {Epoch} in log range {Start}-{End}",
                END_EPOCH_OP_CODE, epoch, startLogId, endLogId);
            SetCriticalError(epoch, $"END_EPOCH log not found in log range {startLogId}-{endLogId}");
            return;
        }

        _logger.LogInformation("✓ Epoch {Epoch} transition validated successfully - END_EPOCH log found", epoch);
        ClearCriticalError();

        // Update epoch_meta to mark as complete
        var epochMeta = await queryService.GetEpochMetaAsync(epoch, ct);
        if (epochMeta != null && !epochMeta.IsComplete)
        {
            var updatedMeta = new EpochMetaDto(
                epoch,
                epochMeta.InitialTick,
                epochMeta.EndTick,
                epochMeta.EndTickStartLogId,
                epochMeta.EndTickEndLogId,
                true, // Mark as complete
                DateTime.UtcNow
            );
            await queryService.UpsertEpochMetaAsync(updatedMeta, ct);
            _logger.LogInformation("Marked epoch {Epoch} as complete in epoch_meta", epoch);

            // Compute and store final epoch stats (immutable after this)
            await queryService.ComputeAndStoreEpochStatsAsync(epoch, ct);

            // Capture emissions for this epoch
            await CaptureEmissionsAsync(epoch, epochMeta.EndTick, queryService, ct);
        }
        else if (epochMeta != null && epochMeta.IsComplete)
        {
            // Check if emissions already captured
            if (!await queryService.IsEmissionImportedAsync(epoch, ct))
            {
                await CaptureEmissionsAsync(epoch, epochMeta.EndTick, queryService, ct);
            }
        }
    }

    /// <summary>
    /// Captures emissions from the end-epoch tick for all computors.
    /// Emissions are transfers FROM zero address TO computor addresses.
    /// </summary>
    private async Task CaptureEmissionsAsync(uint epoch, ulong endTick, ClickHouseQueryService queryService, CancellationToken ct)
    {
        try
        {
            // Check if already captured
            if (await queryService.IsEmissionImportedAsync(epoch, ct))
            {
                _logger.LogDebug("Emissions already captured for epoch {Epoch}", epoch);
                return;
            }

            // Get computor flow service
            using var scope = _serviceProvider.CreateScope();
            var flowService = scope.ServiceProvider.GetRequiredService<ComputorFlowService>();

            // Ensure computors are imported for this epoch
            if (!await flowService.EnsureComputorsImportedAsync(epoch, ct))
            {
                _logger.LogWarning("Could not import computors for epoch {Epoch}, skipping emission capture", epoch);
                return;
            }

            // Get computor addresses and build index map
            var computorList = await queryService.GetComputorsAsync(epoch, ct);
            if (computorList == null || computorList.Computors.Count == 0)
            {
                _logger.LogWarning("No computors found for epoch {Epoch}, skipping emission capture", epoch);
                return;
            }

            var computorAddresses = computorList.Computors.Select(c => c.Address).ToHashSet();
            var addressToIndex = computorList.Computors.ToDictionary(c => c.Address, c => (int)c.Index);

            // Capture emissions
            var (count, total) = await queryService.CaptureEmissionsForEpochAsync(
                epoch, endTick, computorAddresses, addressToIndex, ct);

            if (count > 0)
            {
                _logger.LogInformation(
                    "✓ Captured emissions for epoch {Epoch}: {Count} computors received {Total} total",
                    epoch, count, total);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to capture emissions for epoch {Epoch}", epoch);
        }
    }

    private void SetCriticalError(uint epoch, string message)
    {
        _hasCriticalError = true;
        _criticalErrorEpoch = epoch;
        _logger.LogCritical("Epoch transition critical error for epoch {Epoch}: {Message}. Will retry in 30 minutes.",
            epoch, message);
    }

    private void ClearCriticalError()
    {
        if (_hasCriticalError)
        {
            _logger.LogInformation("Critical error state cleared");
        }
        _hasCriticalError = false;
        _criticalErrorEpoch = null;
    }
}
