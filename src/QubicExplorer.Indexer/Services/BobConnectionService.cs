using System.Threading.Channels;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Qubic.Bob;
using Qubic.Bob.Models;
using QubicExplorer.Shared.Configuration;
using IndexerOptions = QubicExplorer.Indexer.Configuration.IndexerOptions;
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
    private readonly IHostApplicationLifetime _hostLifetime;
    private readonly Channel<TickStreamData> _tickChannel;
    private BobWebSocketClient? _bobClient;
    private bool _disposed;
    private long _lastProcessedTick;
    private CancellationTokenSource? _disconnectCts;
    // Set by MonitorTipsAsync when it detects a fresher node; consumed and
    // cleared by the hard-reset path to make that node the next connection target.
    private volatile string? _preferredNodeUrl;
    private static readonly HttpClient _tipProbeHttp = new() { Timeout = TimeSpan.FromSeconds(5) };
    public ChannelReader<TickStreamData> TickReader => _tickChannel.Reader;

    public BobConnectionService(
        ILogger<BobConnectionService> logger,
        IOptions<BobOptions> bobOptions,
        IOptions<IndexerOptions> indexerOptions,
        IHostApplicationLifetime hostLifetime)
    {
        _logger = logger;
        _bobOptions = bobOptions.Value;
        _indexerOptions = indexerOptions.Value;
        _hostLifetime = hostLifetime;
        _tickChannel = Channel.CreateBounded<TickStreamData>(new BoundedChannelOptions(10000)
        {
            FullMode = BoundedChannelFullMode.Wait
        });
    }

    public async Task ConnectAndSubscribeAsync(long startTick, CancellationToken cancellationToken)
    {
        _lastProcessedTick = startTick;

        var allNodes = _bobOptions.GetEffectiveNodes().ToArray();
        if (allNodes.Length == 0)
            throw new InvalidOperationException("No Bob nodes configured");

        // Rotation offset — incremented every time a node fails so the next attempt
        // starts with a different node. The client itself takes a list, but on
        // Dispose+recreate it always starts at index 0, so we rotate the list
        // ourselves to force trying the next node.
        int rotation = 0;

        BobWebSocketOptions BuildOptions(string[] nodes) => new()
        {
            Nodes = nodes,
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

        string[] RotatedNodes()
        {
            var n = ((rotation % allNodes.Length) + allNodes.Length) % allNodes.Length;
            return allNodes.Skip(n).Concat(allNodes.Take(n)).ToArray();
        }

        var rotatedNodes = RotatedNodes();
        var options = BuildOptions(rotatedNodes);
        _bobClient = new BobWebSocketClient(options);

        _logger.LogInformation("Configured Bob nodes: {Nodes}", string.Join(", ", allNodes));

        // Loop the initial connect with a hard timeout so a single hanging node
        // doesn't block us forever. On timeout we rotate to the next node and retry.
        while (!cancellationToken.IsCancellationRequested)
        {
            var attemptUrl = rotatedNodes[0];
            _logger.LogInformation("Attempting Bob connection: {Url}", attemptUrl);

            var connectTimeout = TimeSpan.FromSeconds(_bobOptions.ConnectTimeoutSeconds);
            try
            {
                using var connectTimeoutCts = new CancellationTokenSource(connectTimeout);
                using var linkedConnectCts = CancellationTokenSource.CreateLinkedTokenSource(
                    cancellationToken, connectTimeoutCts.Token);

                await _bobClient.ConnectAsync(linkedConnectCts.Token);
                _logger.LogInformation("Connected to Bob node: {ActiveNode}", _bobClient.ActiveNodeUrl);
                break;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
            catch (OperationCanceledException)
            {
                rotation++;
                rotatedNodes = RotatedNodes();
                var nextUrl = rotatedNodes[0];
                _logger.LogWarning(
                    "Bob node {Failed} timed out after {Timeout}s, rotating to {Next}",
                    attemptUrl, connectTimeout.TotalSeconds, nextUrl);
                try { _bobClient.Dispose(); } catch { }
                options = BuildOptions(rotatedNodes);
                _bobClient = new BobWebSocketClient(options);
                await Task.Delay(2000, cancellationToken);
            }
            catch (Exception ex)
            {
                rotation++;
                rotatedNodes = RotatedNodes();
                var nextUrl = rotatedNodes[0];
                // Log full exception detail incl. inner — TLS / DNS / WebSocket errors
                // come through here and are otherwise lost.
                _logger.LogError(ex,
                    "Bob node {Failed} connect failed: {ExType}: {Msg}{Inner}. Rotating to {Next} in 5s",
                    attemptUrl, ex.GetType().Name, ex.Message,
                    ex.InnerException is { } ie ? $" -> {ie.GetType().Name}: {ie.Message}" : "",
                    nextUrl);
                try { _bobClient.Dispose(); } catch { }
                options = BuildOptions(rotatedNodes);
                _bobClient = new BobWebSocketClient(options);
                await Task.Delay(5000, cancellationToken);
            }
        }

        // Start the tip monitor — runs alongside the main subscription loop and
        // signals us to rotate if a fresher node is found among the configured pool.
        _ = Task.Run(() => MonitorTipsAsync(allNodes, cancellationToken), cancellationToken);

        int consecutiveFailures = 0;
        const int maxFailuresBeforeHardReset = 3;
        // After this many consecutive hard-resets fail too, give up and exit
        // the process. Docker compose's restart policy then brings us back —
        // empirically that's the only thing that recovers some socket/state
        // wedges (e.g. SocketsHttpHandler pool in a bad state). 5 × 5s sleep
        // = at least 25s of fruitless retries before we bail.
        const int maxHardResetsBeforeExit = 5;
        var consecutiveHardResetFailures = 0;

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

                using var subscription = await _bobClient!.SubscribeTickStreamAsync(tickStreamOptions, cancellationToken);

                _logger.LogInformation("Subscribed to tickStream: {SubscriptionId}", subscription.ServerSubscriptionId);
                consecutiveFailures = 0; // success — reset counter

                await foreach (var notification in subscription.WithCancellation(linkedCts.Token))
                {
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
                consecutiveFailures++;
                await WaitForReconnectionAsync(cancellationToken);
            }
            catch (InvalidOperationException) when (_bobClient?.State != BobConnectionState.Connected)
            {
                // WebSocket not connected yet — wait for internal reconnection
                _logger.LogWarning("WebSocket not connected, waiting for reconnection...");
                consecutiveFailures++;
                if (_bobClient != null)
                {
                    using var waitCts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
                    using var linkedWaitCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, waitCts.Token);
                    try
                    {
                        await _bobClient.WaitForConnectionAsync(cancellationToken: linkedWaitCts.Token);
                    }
                    catch (OperationCanceledException) when (waitCts.IsCancellationRequested)
                    {
                        // timed out — fall through to hard-reset check below
                    }
                }
                else
                {
                    await Task.Delay(5000, cancellationToken);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "TickStream error, resubscribing in 5s...");
                consecutiveFailures++;
                await Task.Delay(5000, cancellationToken);
            }

            // Hard-reset: if reconnection keeps failing OR the tip monitor flagged
            // a fresher node, dispose the client and connect via the preferred /
            // next-in-rotation node.
            var preferred = Interlocked.Exchange(ref _preferredNodeUrl, null);
            if (consecutiveFailures >= maxFailuresBeforeHardReset || preferred != null)
            {
                string nextUrl;
                if (preferred != null)
                {
                    // Put the preferred (fresher) node first, keep the rest as the fallback chain.
                    rotatedNodes = new[] { preferred }
                        .Concat(allNodes.Where(n => !string.Equals(n, preferred, StringComparison.OrdinalIgnoreCase)))
                        .ToArray();
                    nextUrl = preferred;
                    _logger.LogInformation(
                        "Bob client rotating to fresher tip-monitored node: {Next}", nextUrl);
                }
                else
                {
                    rotation++;
                    rotatedNodes = RotatedNodes();
                    nextUrl = rotatedNodes[0];
                    _logger.LogWarning(
                        "Bob client stuck after {Count} failures, hard-resetting and rotating to {Next}",
                        consecutiveFailures, nextUrl);
                }
                try { _bobClient?.Dispose(); } catch { }
                // Give the OS a moment to reclaim sockets — disposing a
                // WebSocket-backed HttpClient is asynchronous in practice and
                // a brand-new instance can otherwise inherit the bad pool.
                await Task.Delay(2000, cancellationToken);
                options = BuildOptions(rotatedNodes);
                _bobClient = new BobWebSocketClient(options);
                consecutiveFailures = 0;

                try
                {
                    // 30s — must be enough to probe every configured node (TLS +
                    // WebSocket upgrade) on a cold start. 10s wasn't.
                    using var connectTimeoutCts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
                    using var linkedConnectCts = CancellationTokenSource.CreateLinkedTokenSource(
                        cancellationToken, connectTimeoutCts.Token);
                    await _bobClient.ConnectAsync(linkedConnectCts.Token);
                    _logger.LogInformation("Bob client reconnected after hard-reset: {Node}", _bobClient.ActiveNodeUrl);
                    consecutiveHardResetFailures = 0;
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    consecutiveHardResetFailures++;
                    _logger.LogError(ex,
                        "Hard-reset connect to {Url} failed ({Count}/{Max}), retrying in 5s",
                        nextUrl, consecutiveHardResetFailures, maxHardResetsBeforeExit);

                    if (consecutiveHardResetFailures >= maxHardResetsBeforeExit)
                    {
                        _logger.LogCritical(
                            "Bob client failed to recover after {Count} hard-resets; exiting process so the orchestrator can restart us",
                            consecutiveHardResetFailures);
                        _hostLifetime.StopApplication();
                        // Throw so we leave this loop immediately rather than
                        // continue racing with the host shutdown.
                        throw;
                    }

                    await Task.Delay(5000, cancellationToken);
                }
            }
        }
    }

    /// <summary>
    /// Waits for the BobWebSocketClient to reconnect after a disconnect.
    /// </summary>
    private async Task WaitForReconnectionAsync(CancellationToken cancellationToken)
    {
        if (_bobClient != null)
        {
            _logger.LogInformation("Waiting for Bob to reconnect...");
            var connected = await _bobClient.WaitForConnectionAsync(
                TimeSpan.FromSeconds(60), cancellationToken);
            if (!connected)
                _logger.LogWarning("Reconnection timed out after 60s, will retry subscription...");
        }
        else
        {
            await Task.Delay(_bobOptions.ReconnectDelayMs, cancellationToken);
        }
    }

    /// <summary>
    /// Maps a Qubic.Bob TickStreamNotification to the explorer's TickStreamData model.
    /// Drops transactions in "pending" state (Bob 1.4.0+) — these are not yet
    /// log-verified and Bob will re-emit the tick once verification completes.
    /// </summary>
    private TickStreamData MapToTickStreamData(TickStreamNotification notification)
    {
        List<BobTransaction>? mappedTxs = null;
        int droppedPending = 0;
        if (notification.Transactions != null)
        {
            mappedTxs = new List<BobTransaction>(notification.Transactions.Count);
            foreach (var tx in notification.Transactions)
            {
                // Bob 1.4.0+: executed can be null when tick hasn't been log-verified yet.
                // Per Bob docs this is terminal for this delivery — skip and wait for re-emit.
                if (tx.Executed == null)
                {
                    droppedPending++;
                    continue;
                }

                mappedTxs.Add(new BobTransaction
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
                });
            }

            if (droppedPending > 0)
            {
                _logger.LogWarning(
                    "Tick {Tick}: dropped {Count} pending tx(s) — will be re-emitted by Bob after log verification",
                    notification.Tick, droppedPending);
            }
        }

        return new TickStreamData
        {
            Epoch = notification.Epoch,
            Tick = notification.Tick,
            HasNoTickData = notification.HasNoTickData,
            IsSkipped = notification.IsSkipped,
            IsCatchUp = notification.IsCatchUp,
            Timestamp = notification.Timestamp,
            TxCountFiltered = notification.TxCountFiltered,
            TxCountTotal = notification.TxCountTotal,
            LogCountFiltered = notification.LogCountFiltered,
            LogCountTotal = notification.LogCountTotal,
            Transactions = mappedTxs,
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

    /// <summary>
    /// Periodically polls every configured Bob node for its current tick. If
    /// another node is ahead of the one we're connected to by more than
    /// <see cref="BobOptions.TipMonitorLagThreshold"/> ticks, sets
    /// <see cref="_preferredNodeUrl"/> and cancels the current subscription so
    /// the main loop's hard-reset path rotates to it.
    /// </summary>
    private async Task MonitorTipsAsync(string[] allNodes, CancellationToken cancellationToken)
    {
        if (!_bobOptions.EnableTipMonitor) return;
        if (allNodes.Length < 2) return; // nothing to switch to

        var interval = TimeSpan.FromSeconds(Math.Max(10, _bobOptions.TipMonitorIntervalSeconds));
        var lagThreshold = Math.Max(10, _bobOptions.TipMonitorLagThreshold);
        _logger.LogInformation(
            "Bob tip monitor running every {Interval}s, switch threshold {Threshold} ticks",
            interval.TotalSeconds, lagThreshold);

        while (!cancellationToken.IsCancellationRequested)
        {
            try { await Task.Delay(interval, cancellationToken); }
            catch (OperationCanceledException) { return; }

            var activeNode = _bobClient?.ActiveNodeUrl;
            if (string.IsNullOrEmpty(activeNode)) continue;

            // Probe all nodes in parallel. Failures are silent — they just don't
            // contribute a candidate this round.
            var tips = await Task.WhenAll(allNodes.Select(async node =>
            {
                try
                {
                    var t = await ProbeNodeTickAsync(node, cancellationToken);
                    return (node, tick: (uint?)t);
                }
                catch { return (node, tick: (uint?)null); }
            }));

            var valid = tips.Where(x => x.tick.HasValue)
                .Select(x => (x.node, tick: x.tick!.Value))
                .ToList();
            if (valid.Count == 0) continue;

            var bestNode = valid.OrderByDescending(x => x.tick).First();
            var activeTip = valid.FirstOrDefault(x =>
                string.Equals(x.node, activeNode, StringComparison.OrdinalIgnoreCase));

            if (activeTip.node == null) continue; // active not in pool (rotated mid-flight)

            var lag = (long)bestNode.tick - activeTip.tick;
            if (lag > lagThreshold && !string.Equals(bestNode.node, activeNode, StringComparison.OrdinalIgnoreCase))
            {
                _logger.LogInformation(
                    "Tip monitor: active {Active}@{ActiveTick} is {Lag} ticks behind {Best}@{BestTick}; rotating",
                    activeNode, activeTip.tick, lag, bestNode.node, bestNode.tick);
                _preferredNodeUrl = bestNode.node;
                // Break out of the current subscription so the main loop hits
                // the hard-reset path immediately rather than waiting for the
                // normal failure threshold.
                try { _disconnectCts?.Cancel(); } catch { }
            }
        }
    }

    /// <summary>
    /// JSON-RPC HTTP probe of a single node for its current tick number.
    /// Converts ws:// → http:// (and wss:// → https://) for the probe URL.
    /// </summary>
    private static async Task<uint> ProbeNodeTickAsync(string nodeUrl, CancellationToken ct)
    {
        var httpUrl =
            nodeUrl.StartsWith("ws://", StringComparison.OrdinalIgnoreCase) ? "http://" + nodeUrl[5..]
            : nodeUrl.StartsWith("wss://", StringComparison.OrdinalIgnoreCase) ? "https://" + nodeUrl[6..]
            : nodeUrl;

        using var probeCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        probeCts.CancelAfter(TimeSpan.FromSeconds(5));

        var body = "{\"jsonrpc\":\"2.0\",\"method\":\"qubic_getTickNumber\",\"id\":1,\"params\":[]}";
        using var req = new HttpRequestMessage(HttpMethod.Post, httpUrl);
        req.Content = new StringContent(body, System.Text.Encoding.UTF8, "application/json");
        using var resp = await _tipProbeHttp.SendAsync(req, probeCts.Token);
        resp.EnsureSuccessStatusCode();

        var json = await resp.Content.ReadAsStringAsync(probeCts.Token);
        using var doc = System.Text.Json.JsonDocument.Parse(json);
        var result = doc.RootElement.GetProperty("result");
        return result.ValueKind == System.Text.Json.JsonValueKind.Number
            ? result.GetUInt32()
            : uint.Parse(result.GetString() ?? "0");
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
