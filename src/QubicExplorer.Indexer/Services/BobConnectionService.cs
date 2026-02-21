using System.Threading.Channels;
using Microsoft.Extensions.Options;
using Qubic.Bob;
using Qubic.Bob.Models;
using QubicExplorer.Indexer.Configuration;
using QubicExplorer.Indexer.Models;

namespace QubicExplorer.Indexer.Services;

/// <summary>
/// Connects to Bob via BobWebSocketClient, subscribes to the tickStream,
/// and writes tick data into a bounded channel for the IndexerWorker to consume.
/// </summary>
public class BobConnectionService : IDisposable
{
    private readonly ILogger<BobConnectionService> _logger;
    private readonly BobOptions _bobOptions;
    private readonly IndexerOptions _indexerOptions;
    private readonly Channel<TickStreamData> _tickChannel;
    private BobWebSocketClient? _bobClient;
    private bool _disposed;
    private long _lastProcessedTick;
    private CancellationTokenSource? _disconnectCts;
    private bool _loggedSampleNotification;

    public ChannelReader<TickStreamData> TickReader => _tickChannel.Reader;

    public BobConnectionService(
        ILogger<BobConnectionService> logger,
        IOptions<BobOptions> bobOptions,
        IOptions<IndexerOptions> indexerOptions)
    {
        _logger = logger;
        _bobOptions = bobOptions.Value;
        _indexerOptions = indexerOptions.Value;
        _tickChannel = Channel.CreateBounded<TickStreamData>(new BoundedChannelOptions(10000)
        {
            FullMode = BoundedChannelFullMode.Wait
        });
    }

    public async Task ConnectAndSubscribeAsync(long startTick, CancellationToken cancellationToken)
    {
        _lastProcessedTick = startTick;

        var effectiveNodes = _bobOptions.GetEffectiveNodes();

        var options = new BobWebSocketOptions
        {
            Nodes = effectiveNodes.ToArray(),
            ReconnectDelay = TimeSpan.FromMilliseconds(_bobOptions.ReconnectDelayMs),
            MaxReconnectDelay = TimeSpan.FromMilliseconds(_bobOptions.MaxReconnectDelayMs),
            OnConnectionEvent = e =>
            {
                switch (e.Type)
                {
                    case BobConnectionEventType.Connected:
                        _logger.LogInformation("Connected to Bob node: {Url}", e.NodeUrl);
                        break;
                    case BobConnectionEventType.Disconnected:
                        _logger.LogWarning("Disconnected from Bob node: {Url} - {Message}", e.NodeUrl, e.Message);
                        // Cancel the subscription iterator so we can resubscribe
                        _disconnectCts?.Cancel();
                        break;
                    case BobConnectionEventType.Reconnecting:
                        _logger.LogInformation("Reconnecting to Bob: {Message}", e.Message);
                        break;
                    case BobConnectionEventType.NodeRecovered:
                        _logger.LogInformation("Bob node recovered: {Url}", e.NodeUrl);
                        break;
                }
            }
        };

        _bobClient = new BobWebSocketClient(options);

        _logger.LogInformation("Connecting to Bob nodes: {Nodes}", string.Join(", ", effectiveNodes));
        await _bobClient.ConnectAsync(cancellationToken);
        _logger.LogInformation("Connected to Bob node: {ActiveNode}", _bobClient.ActiveNodeUrl);

        while (!cancellationToken.IsCancellationRequested)
        {
            // Create a new CTS for this subscription attempt - cancelled on disconnect
            _disconnectCts?.Dispose();
            _disconnectCts = new CancellationTokenSource();
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _disconnectCts.Token);

            try
            {
                // On reconnect, resume from last processed tick + 1 (not the original startTick)
                var resumeTick = _lastProcessedTick > startTick ? _lastProcessedTick + 1 : startTick;

                var tickStreamOptions = new TickStreamOptions
                {
                    StartTick = resumeTick >= 0 ? (uint)resumeTick : null,
                    SkipEmptyTicks = _indexerOptions.SkipEmptyTicks,
                    IncludeInputData = _indexerOptions.IncludeInputData
                };

                _logger.LogInformation("Subscribing to tickStream from tick {StartTick}", resumeTick);

                using var subscription = await _bobClient.SubscribeTickStreamAsync(tickStreamOptions, cancellationToken);

                _logger.LogInformation("Subscribed to tickStream: {SubscriptionId}", subscription.ServerSubscriptionId);

                await foreach (var notification in subscription.WithCancellation(linkedCts.Token))
                {
                    // Debug: log first non-empty tick notification to diagnose data issues
                    if (!_loggedSampleNotification && notification.TxCountTotal > 0)
                    {
                        _loggedSampleNotification = true;
                        _logger.LogWarning(
                            "SAMPLE non-empty tick {Tick}: TxCountTotal={TxTotal}, TxCountFiltered={TxFiltered}, " +
                            "LogCountTotal={LogTotal}, LogCountFiltered={LogFiltered}, " +
                            "Transactions={TxState} (count={TxCount}), Logs={LogState} (count={LogCount})",
                            notification.Tick, notification.TxCountTotal, notification.TxCountFiltered,
                            notification.LogCountTotal, notification.LogCountFiltered,
                            notification.Transactions == null ? "null" : "present", notification.Transactions?.Count ?? 0,
                            notification.Logs == null ? "null" : "present", notification.Logs?.Count ?? 0);

                        // Log first transaction details if available
                        if (notification.Transactions is { Count: > 0 })
                        {
                            var tx = notification.Transactions[0];
                            _logger.LogWarning(
                                "SAMPLE tx[0]: Hash={Hash}, From={From}, To={To}, Amount={Amount}, Executed={Executed}",
                                tx.Hash, tx.From, tx.To, tx.Amount, tx.Executed);
                        }
                    }

                    var tickData = MapToTickStreamData(notification);
                    await _tickChannel.Writer.WriteAsync(tickData, cancellationToken);

                    // Track last processed tick for reconnection resume
                    _lastProcessedTick = (long)tickData.Tick;

                    if (tickData.IsCatchUp && tickData.Tick % 1000 == 0)
                    {
                        _logger.LogInformation("Catch-up progress: tick {Tick}", tickData.Tick);
                    }
                }

                // If the foreach completes, the subscription ended — resubscribe
                _logger.LogWarning("TickStream subscription ended, resubscribing...");
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                _logger.LogInformation("Connection cancelled");
                break;
            }
            catch (OperationCanceledException) when (_disconnectCts.IsCancellationRequested)
            {
                // Disconnected - wait for reconnection then resubscribe
                _logger.LogInformation("Subscription interrupted by disconnect, waiting for reconnection...");
                await WaitForReconnectionAsync(cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "TickStream error, resubscribing in 5s...");
                await Task.Delay(5000, cancellationToken);
            }
        }
    }

    /// <summary>
    /// Waits briefly for the BobWebSocketClient to reconnect after a disconnect.
    /// The client handles reconnection internally; we just need to give it time
    /// before attempting to resubscribe.
    /// </summary>
    private async Task WaitForReconnectionAsync(CancellationToken cancellationToken)
    {
        // Give the client time to reconnect before resubscribing
        // The actual reconnection is handled by BobWebSocketClient internally
        await Task.Delay(_bobOptions.ReconnectDelayMs, cancellationToken);
    }

    /// <summary>
    /// Maps a Qubic.Bob TickStreamNotification to the explorer's TickStreamData model.
    /// </summary>
    private TickStreamData MapToTickStreamData(TickStreamNotification notification)
    {
        // Debug: detect when Bob reports transactions/logs but we have none deserialized
        if (notification.TxCountTotal > 0 && (notification.Transactions == null || notification.Transactions.Count == 0))
        {
            _logger.LogWarning(
                "Tick {Tick}: Bob reports TxCountTotal={TxCount} but Transactions is {State}",
                notification.Tick, notification.TxCountTotal,
                notification.Transactions == null ? "null" : "empty");
        }
        if (notification.LogCountTotal > 0 && (notification.Logs == null || notification.Logs.Count == 0))
        {
            _logger.LogWarning(
                "Tick {Tick}: Bob reports LogCountTotal={LogCount} but Logs is {State}",
                notification.Tick, notification.LogCountTotal,
                notification.Logs == null ? "null" : "empty");
        }

        return new TickStreamData
        {
            Epoch = notification.Epoch,
            Tick = notification.Tick,
            IsCatchUp = notification.IsCatchUp,
            Timestamp = notification.Timestamp,
            TxCountFiltered = notification.TxCountFiltered,
            TxCountTotal = notification.TxCountTotal,
            LogCountFiltered = notification.LogCountFiltered,
            LogCountTotal = notification.LogCountTotal,
            Transactions = notification.Transactions?.Select(tx => new BobTransaction
            {
                Hash = tx.Hash,
                From = tx.From,
                To = tx.To,
                Amount = (ulong)tx.GetAmount(),
                InputType = tx.InputType,
                InputData = tx.InputData,
                Executed = tx.Executed,
                LogIdFrom = (int)tx.LogIdFrom,
                LogIdLength = tx.LogIdLength
            }).ToList(),
            Logs = notification.Logs?.Select(log => new BobLog
            {
                Ok = log.Ok,
                Tick = log.Tick,
                Epoch = log.Epoch,
                LogId = (uint)log.LogId,
                LogType = (byte)log.LogType,
                LogTypeName = log.LogTypeName,
                LogDigest = log.LogDigest,
                BodySize = log.BodySize,
                Timestamp = log.Timestamp,
                TxHash = log.TxHash,
                Body = log.Body
            }).ToList()
        };
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        _disconnectCts?.Cancel();
        _disconnectCts?.Dispose();
        _bobClient?.Dispose();
        _tickChannel.Writer.Complete();
    }
}
