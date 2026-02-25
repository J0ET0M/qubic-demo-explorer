using ClickHouse.Client.ADO;
using Microsoft.Extensions.Options;
using QubicExplorer.Shared.Configuration;
using QubicExplorer.Shared.Services;

namespace QubicExplorer.Api.Services;

/// <summary>
/// Background service that monitors addresses with active push subscriptions
/// and sends notifications when new transfers are detected.
/// </summary>
public class AddressMonitorService : BackgroundService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<AddressMonitorService> _logger;

    // Check every 30 seconds for new transfers
    private static readonly TimeSpan CheckInterval = TimeSpan.FromSeconds(30);

    // Track the last processed tick per address to detect new transfers
    private readonly Dictionary<string, ulong> _lastProcessedTick = new();

    public AddressMonitorService(
        IServiceProvider serviceProvider,
        ILogger<AddressMonitorService> logger)
    {
        _serviceProvider = serviceProvider;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("AddressMonitorService started");

        // Wait for services to initialize
        await Task.Delay(TimeSpan.FromSeconds(20), stoppingToken);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await CheckForNewTransfersAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in address monitor cycle");
            }

            await Task.Delay(CheckInterval, stoppingToken);
        }

        _logger.LogInformation("AddressMonitorService stopped");
    }

    private async Task CheckForNewTransfersAsync(CancellationToken ct)
    {
        using var scope = _serviceProvider.CreateScope();
        var pushService = scope.ServiceProvider.GetRequiredService<WebPushService>();
        var queryService = scope.ServiceProvider.GetRequiredService<ClickHouseQueryService>();
        var labelService = scope.ServiceProvider.GetRequiredService<AddressLabelService>();

        // Get all unique addresses being watched
        var watchedAddresses = await GetWatchedAddressesAsync(pushService, ct);
        if (watchedAddresses.Count == 0) return;

        _logger.LogDebug("Monitoring {Count} addresses for push notifications", watchedAddresses.Count);

        foreach (var address in watchedAddresses)
        {
            ct.ThrowIfCancellationRequested();

            try
            {
                await CheckAddressTransfersAsync(address, pushService, queryService, labelService, ct);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Error checking transfers for {Address}", address);
            }
        }
    }

    private async Task<HashSet<string>> GetWatchedAddressesAsync(
        WebPushService pushService, CancellationToken ct)
    {
        // Query all subscriptions and collect unique addresses
        using var scope = _serviceProvider.CreateScope();
        var chOptions = scope.ServiceProvider.GetRequiredService<IOptions<ClickHouseOptions>>().Value;
        using var connection = new ClickHouseConnection(chOptions.ConnectionString);
        await connection.OpenAsync(ct);

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT DISTINCT arrayJoin(addresses) AS addr FROM push_subscriptions FINAL";

        var addresses = new HashSet<string>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            addresses.Add(reader.GetString(0));
        }
        return addresses;
    }

    private async Task CheckAddressTransfersAsync(
        string address,
        WebPushService pushService,
        ClickHouseQueryService queryService,
        AddressLabelService labelService,
        CancellationToken ct)
    {
        // Get latest transfers for this address (QU transfers only, log_type=0)
        var transfers = await queryService.GetTransfersAsync(
            page: 1, limit: 5, address: address, logType: 0, ct: ct);

        if (transfers.Items.Count == 0) return;

        var latestTick = transfers.Items.Max(t => t.TickNumber);

        // Initialize baseline on first check
        if (!_lastProcessedTick.TryGetValue(address, out var lastTick))
        {
            _lastProcessedTick[address] = latestTick;
            return;
        }

        // No new transfers
        if (latestTick <= lastTick) return;

        _lastProcessedTick[address] = latestTick;

        // Get subscriptions for this address
        var subscriptions = await pushService.GetSubscriptionsForAddressAsync(address, ct);
        if (subscriptions.Count == 0) return;

        // Process new transfers
        var newTransfers = transfers.Items
            .Where(t => t.TickNumber > lastTick)
            .ToList();

        foreach (var transfer in newTransfers)
        {
            var isIncoming = transfer.DestAddress == address;
            var eventType = isIncoming ? "incoming" : "outgoing";
            var amount = transfer.Amount;
            var tickNumber = transfer.TickNumber;

            foreach (var sub in subscriptions)
            {
                // Check if subscription wants this event type
                var isLarge = amount >= sub.LargeTransferThreshold;
                var wantsEvent = sub.Events.Contains(eventType) ||
                                 (isLarge && sub.Events.Contains("large_transfer"));

                if (!wantsEvent) continue;

                // Deduplication check
                if (await pushService.WasNotificationSentAsync(sub.SubscriptionId, address, tickNumber, ct))
                    continue;

                // Build notification
                var counterparty = isIncoming ? transfer.SourceAddress : transfer.DestAddress;
                var counterDisplay = labelService.GetLabel(counterparty) ?? TruncateAddress(counterparty);
                var addrDisplay = labelService.GetLabel(address) ?? TruncateAddress(address);

                var title = isLarge ? "Large Transfer Detected" :
                            isIncoming ? "Incoming Transfer" : "Outgoing Transfer";

                var body = isIncoming
                    ? $"{FormatAmount(amount)} QU received by {addrDisplay} from {counterDisplay}"
                    : $"{FormatAmount(amount)} QU sent from {addrDisplay} to {counterDisplay}";

                var url = $"/address/{address}";

                var sent = await pushService.SendNotificationAsync(sub, title, body, url, ct);
                if (sent)
                {
                    await pushService.RecordNotificationAsync(
                        sub.SubscriptionId, address, tickNumber, eventType, amount, ct);
                }
            }
        }
    }

    private static string FormatAmount(ulong amount)
    {
        if (amount >= 1_000_000_000_000) return (amount / 1_000_000_000_000.0).ToString("F1") + "T";
        if (amount >= 1_000_000_000) return (amount / 1_000_000_000.0).ToString("F1") + "B";
        if (amount >= 1_000_000) return (amount / 1_000_000.0).ToString("F1") + "M";
        if (amount >= 1_000) return (amount / 1_000.0).ToString("F1") + "K";
        return amount.ToString("N0");
    }

    private static string TruncateAddress(string addr)
        => addr.Length > 16 ? addr[..6] + "..." + addr[^6..] : addr;
}
