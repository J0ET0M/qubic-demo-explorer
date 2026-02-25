namespace QubicExplorer.Api.Services;

/// <summary>
/// Background service that automatically imports spectrum and universe files
/// for completed epochs. Checks periodically for epochs that haven't been imported
/// yet and triggers imports in order.
/// </summary>
public class AutoImportService : BackgroundService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<AutoImportService> _logger;

    // Check every 5 minutes for new epochs to import
    private static readonly TimeSpan CheckInterval = TimeSpan.FromMinutes(5);

    // On error, back off for 15 minutes
    private static readonly TimeSpan ErrorBackoff = TimeSpan.FromMinutes(15);

    // Maximum number of epochs to import in a single check cycle
    private const int MaxEpochsPerCycle = 5;

    public AutoImportService(
        IServiceProvider serviceProvider,
        ILogger<AutoImportService> logger)
    {
        _serviceProvider = serviceProvider;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("AutoImportService started — will check for missing spectrum/universe imports every {Interval}",
            CheckInterval);

        // Wait for other services to initialize (schema, connections, etc.)
        await Task.Delay(TimeSpan.FromSeconds(30), stoppingToken);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await CheckAndImportAsync(stoppingToken);
                await Task.Delay(CheckInterval, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in AutoImportService check cycle");
                await Task.Delay(ErrorBackoff, stoppingToken);
            }
        }

        _logger.LogInformation("AutoImportService stopped");
    }

    private async Task CheckAndImportAsync(CancellationToken ct)
    {
        using var scope = _serviceProvider.CreateScope();
        var queryService = scope.ServiceProvider.GetRequiredService<ClickHouseQueryService>();
        var spectrumService = scope.ServiceProvider.GetRequiredService<SpectrumImportService>();
        var universeService = scope.ServiceProvider.GetRequiredService<UniverseImportService>();

        // Get the current epoch from the database (the epoch we're currently in)
        var currentEpoch = await queryService.GetCurrentEpochAsync(ct);
        if (currentEpoch == null || currentEpoch.Value < 2)
        {
            _logger.LogDebug("No current epoch found or epoch too low, skipping import check");
            return;
        }

        // We only import completed epochs (everything before the current epoch).
        // The spectrum/universe files represent the state at the START of an epoch,
        // so the file for epoch N is available once epoch N has started (i.e., epoch N-1 is complete).
        var latestCompletedEpoch = currentEpoch.Value;

        // Find epochs that need spectrum import
        var epochsToImport = await FindMissingImportsAsync(
            spectrumService, universeService, latestCompletedEpoch, ct);

        if (epochsToImport.Count == 0)
        {
            _logger.LogDebug("All completed epochs are already imported");
            return;
        }

        _logger.LogInformation("Found {Count} epochs needing import: {Epochs}",
            epochsToImport.Count,
            string.Join(", ", epochsToImport.Take(10)));

        // Import in order, limited per cycle to avoid overwhelming the system
        var imported = 0;
        foreach (var epoch in epochsToImport.Take(MaxEpochsPerCycle))
        {
            ct.ThrowIfCancellationRequested();

            var (spectrumNeeded, universeNeeded) = await CheckEpochNeedsAsync(
                epoch, spectrumService, universeService, ct);

            if (spectrumNeeded)
            {
                await ImportSpectrumAsync(epoch, spectrumService, ct);
            }

            if (universeNeeded)
            {
                await ImportUniverseAsync(epoch, universeService, ct);
            }

            imported++;
        }

        if (imported > 0)
        {
            _logger.LogInformation("Auto-import cycle complete: processed {Count} epochs", imported);
        }
    }

    private async Task<List<uint>> FindMissingImportsAsync(
        SpectrumImportService spectrumService,
        UniverseImportService universeService,
        uint latestCompletedEpoch,
        CancellationToken ct)
    {
        var missing = new List<uint>();

        // Check the last N epochs for missing imports (don't go too far back on first run)
        // On first startup, this will import recent epochs. Old epochs can be imported manually.
        const uint lookbackEpochs = 10;
        var startEpoch = latestCompletedEpoch > lookbackEpochs
            ? latestCompletedEpoch - lookbackEpochs + 1
            : 1;

        for (var epoch = startEpoch; epoch <= latestCompletedEpoch; epoch++)
        {
            var spectrumImported = await spectrumService.IsEpochImportedAsync(epoch, ct);
            var universeImported = await universeService.IsEpochImportedAsync(epoch, ct);

            if (!spectrumImported || !universeImported)
            {
                missing.Add(epoch);
            }
        }

        return missing;
    }

    private static async Task<(bool spectrumNeeded, bool universeNeeded)> CheckEpochNeedsAsync(
        uint epoch,
        SpectrumImportService spectrumService,
        UniverseImportService universeService,
        CancellationToken ct)
    {
        var spectrumImported = await spectrumService.IsEpochImportedAsync(epoch, ct);
        var universeImported = await universeService.IsEpochImportedAsync(epoch, ct);
        return (!spectrumImported, !universeImported);
    }

    private async Task ImportSpectrumAsync(
        uint epoch,
        SpectrumImportService spectrumService,
        CancellationToken ct)
    {
        _logger.LogInformation("Auto-importing spectrum for epoch {Epoch}", epoch);

        try
        {
            var result = await spectrumService.ImportEpochAsync(epoch, ct);
            if (result.Success)
            {
                _logger.LogInformation(
                    "Auto-imported spectrum for epoch {Epoch}: {Count} addresses, total balance {Balance}",
                    epoch, result.AddressCount, result.TotalBalance);
            }
            else
            {
                _logger.LogWarning(
                    "Failed to auto-import spectrum for epoch {Epoch}: {Error}",
                    epoch, result.Error);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Exception during auto-import of spectrum for epoch {Epoch}", epoch);
        }
    }

    private async Task ImportUniverseAsync(
        uint epoch,
        UniverseImportService universeService,
        CancellationToken ct)
    {
        _logger.LogInformation("Auto-importing universe for epoch {Epoch}", epoch);

        try
        {
            var result = await universeService.ImportEpochAsync(epoch, ct);
            if (result.Success)
            {
                _logger.LogInformation(
                    "Auto-imported universe for epoch {Epoch}: {Issuances} issuances, {Ownerships} ownerships, {Possessions} possessions",
                    epoch, result.IssuanceCount, result.OwnershipCount, result.PossessionCount);
            }
            else
            {
                _logger.LogWarning(
                    "Failed to auto-import universe for epoch {Epoch}: {Error}",
                    epoch, result.Error);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Exception during auto-import of universe for epoch {Epoch}", epoch);
        }
    }
}
