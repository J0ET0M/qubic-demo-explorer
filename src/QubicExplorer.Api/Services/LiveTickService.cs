using Microsoft.AspNetCore.SignalR;
using Qubic.Bob;
using QubicExplorer.Api.Hubs;

namespace QubicExplorer.Api.Services;

public class LiveTickService : BackgroundService
{
    private readonly IHubContext<LiveUpdatesHub> _hubContext;
    private readonly BobWebSocketClient _bobClient;
    private readonly ILogger<LiveTickService> _logger;
    private ulong _lastBroadcastTick; // Track last broadcast tick to avoid duplicates

    public LiveTickService(
        IHubContext<LiveUpdatesHub> hubContext,
        BobWebSocketClient bobClient,
        ILogger<LiveTickService> logger)
    {
        _hubContext = hubContext;
        _bobClient = bobClient;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Wait for the shared BobWebSocketClient to be connected
        while (_bobClient.State != BobConnectionState.Connected && !stoppingToken.IsCancellationRequested)
        {
            await Task.Delay(1000, stoppingToken);
        }

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await SubscribeAndProcessAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "NewTicks subscription error, resubscribing in 5 seconds...");
                await Task.Delay(5000, stoppingToken);
            }
        }
    }

    private async Task SubscribeAndProcessAsync(CancellationToken ct)
    {
        _logger.LogInformation("Subscribing to newTicks via BobWebSocketClient");

        using var subscription = await _bobClient.SubscribeNewTicksAsync(ct);

        _logger.LogInformation("Subscribed to newTicks: {SubscriptionId}", subscription.ServerSubscriptionId);

        await foreach (var tick in subscription.WithCancellation(ct))
        {
            var tickNumber = (ulong)tick.TickNumber;

            // Skip if we've already broadcast this tick (deduplication)
            // Each tick can have ~451 computor votes, we only want to broadcast once
            if (tickNumber <= _lastBroadcastTick)
            {
                _logger.LogDebug("Skipping already broadcast tick: {TickNumber}", tickNumber);
                continue;
            }

            var tickData = new
            {
                tickNumber,
                epoch = (uint)tick.Epoch,
                txCount = (uint)tick.TransactionCount,
                timestamp = DateTime.UtcNow
            };

            _lastBroadcastTick = tickNumber;

            _logger.LogDebug("Broadcasting new tick: {TickNumber} (epoch {Epoch}, {TxCount} txs)",
                tickData.tickNumber, tickData.epoch, tickData.txCount);

            // Broadcast to all subscribed clients
            await _hubContext.SendNewTick(tickData);
        }
    }
}
