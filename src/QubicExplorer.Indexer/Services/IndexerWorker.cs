using Microsoft.Extensions.Options;
using QubicExplorer.Indexer.Configuration;

namespace QubicExplorer.Indexer.Services;

public class IndexerWorker : BackgroundService
{
    private readonly ILogger<IndexerWorker> _logger;
    private readonly BobConnectionService _bobConnection;
    private readonly ClickHouseWriterService _clickHouseWriter;
    private readonly IndexerOptions _options;

    public IndexerWorker(
        ILogger<IndexerWorker> logger,
        BobConnectionService bobConnection,
        ClickHouseWriterService clickHouseWriter,
        IOptions<IndexerOptions> options)
    {
        _logger = logger;
        _bobConnection = bobConnection;
        _clickHouseWriter = clickHouseWriter;
        _options = options.Value;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Indexer worker starting...");

        try
        {
            // Initialize ClickHouse connection
            await _clickHouseWriter.InitializeAsync(stoppingToken);

            // Determine start tick
            var startTick = await DetermineStartTickAsync(stoppingToken);
            _logger.LogInformation("Starting indexing from tick {StartTick}", startTick);

            // Start processing in background
            var processingTask = ProcessTicksAsync(stoppingToken);

            // Connect and subscribe to Bob node
            var connectionTask = _bobConnection.ConnectAndSubscribeAsync(startTick, stoppingToken);

            // Wait for either to complete (or fail)
            await Task.WhenAny(processingTask, connectionTask);

            // If we get here, flush remaining data
            await _clickHouseWriter.FlushBatchesAsync(stoppingToken);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            _logger.LogInformation("Indexer worker stopping...");
        }
        catch (Exception ex)
        {
            _logger.LogCritical(ex, "Indexer worker failed");
            throw;
        }
    }

    private async Task<long> DetermineStartTickAsync(CancellationToken cancellationToken)
    {
        if (_options.StartFromLatest)
        {
            _logger.LogInformation("Configured to start from latest tick");
            return -1; // Bob node interprets this as latest
        }

        if (_options.ResumeFromLastTick)
        {
            var lastTick = await _clickHouseWriter.GetLastIndexedTickAsync(cancellationToken);
            if (lastTick.HasValue)
            {
                _logger.LogInformation("Resuming from last indexed tick: {LastTick}", lastTick.Value);
                return lastTick.Value + 1;
            }

            _logger.LogWarning(
                "ResumeFromLastTick is enabled but no last tick found in database. " +
                "Falling back to StartTick={StartTick}. This will re-index from the beginning!",
                _options.StartTick);
        }

        _logger.LogInformation("Starting from configured tick: {StartTick}", _options.StartTick);
        return _options.StartTick;
    }

    private async Task ProcessTicksAsync(CancellationToken stoppingToken)
    {
        var ticksProcessed = 0L;
        var lastLogTime = DateTime.UtcNow;

        await foreach (var tickData in _bobConnection.TickReader.ReadAllAsync(stoppingToken))
        {
            await _clickHouseWriter.WriteTickDataAsync(tickData, stoppingToken);
            ticksProcessed++;

            // Log progress periodically
            if ((DateTime.UtcNow - lastLogTime).TotalSeconds >= 10)
            {
                _logger.LogInformation(
                    "Processed {Count} ticks, current: {Tick}, catch-up: {IsCatchUp}",
                    ticksProcessed, tickData.Tick, tickData.IsCatchUp);
                lastLogTime = DateTime.UtcNow;
            }

            // When transitioning from catch-up to real-time, flush
            if (!tickData.IsCatchUp)
            {
                await _clickHouseWriter.FlushBatchesAsync(stoppingToken);
            }
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Stopping indexer worker...");

        try
        {
            await _clickHouseWriter.FlushBatchesAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error flushing during shutdown");
        }

        await base.StopAsync(cancellationToken);
    }
}
