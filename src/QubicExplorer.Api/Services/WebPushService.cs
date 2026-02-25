using System.Security.Cryptography;
using System.Text.Json;
using ClickHouse.Client.ADO;
using Microsoft.Extensions.Options;
using QubicExplorer.Shared.Configuration;
using WebPush;

namespace QubicExplorer.Api.Services;

/// <summary>
/// Manages Web Push subscriptions and sends push notifications to clients.
/// Uses VAPID (Voluntary Application Server Identification) for authentication.
/// </summary>
public class WebPushService : IDisposable
{
    private readonly ClickHouseConnection _connection;
    private readonly VapidDetails _vapidDetails;
    private readonly WebPushClient _pushClient;
    private readonly ILogger<WebPushService> _logger;
    private bool _disposed;

    public string PublicKey => _vapidDetails.PublicKey;

    public WebPushService(
        IOptions<ClickHouseOptions> chOptions,
        IOptions<VapidOptions> vapidOptions,
        ILogger<WebPushService> logger)
    {
        _logger = logger;
        _connection = new ClickHouseConnection(chOptions.Value.ConnectionString);
        _connection.Open();

        var vapid = vapidOptions.Value;

        // Auto-generate VAPID keys if not configured
        if (string.IsNullOrEmpty(vapid.PublicKey) || string.IsNullOrEmpty(vapid.PrivateKey))
        {
            var keys = VapidHelper.GenerateVapidKeys();
            _logger.LogWarning(
                "VAPID keys not configured — generated ephemeral keys. " +
                "Set Vapid:PublicKey and Vapid:PrivateKey in config for persistent push notifications. " +
                "Public: {PublicKey}, Private: {PrivateKey}",
                keys.PublicKey, keys.PrivateKey);
            vapid.PublicKey = keys.PublicKey;
            vapid.PrivateKey = keys.PrivateKey;
        }

        _vapidDetails = new VapidDetails(vapid.Subject, vapid.PublicKey, vapid.PrivateKey);
        _pushClient = new WebPushClient();
    }

    /// <summary>
    /// Save or update a push subscription with watched addresses and event preferences.
    /// </summary>
    public async Task SaveSubscriptionAsync(
        string subscriptionId,
        string endpoint,
        string p256dh,
        string auth,
        string[] addresses,
        string[] events,
        ulong largeTransferThreshold,
        CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();
        var addrArray = "[" + string.Join(",", addresses.Select(a => $"'{a}'")) + "]";
        var evtArray = "[" + string.Join(",", events.Select(e => $"'{e}'")) + "]";
        cmd.CommandText = $@"
            INSERT INTO push_subscriptions
            (subscription_id, endpoint, p256dh, auth, addresses, events, large_transfer_threshold)
            VALUES
            ('{EscapeSql(subscriptionId)}', '{EscapeSql(endpoint)}', '{EscapeSql(p256dh)}',
             '{EscapeSql(auth)}', {addrArray}, {evtArray}, {largeTransferThreshold})";
        await cmd.ExecuteNonQueryAsync(ct);

        _logger.LogInformation("Saved push subscription {Id} watching {Count} addresses",
            subscriptionId, addresses.Length);
    }

    /// <summary>
    /// Remove a push subscription.
    /// </summary>
    public async Task RemoveSubscriptionAsync(string subscriptionId, CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $"ALTER TABLE push_subscriptions DELETE WHERE subscription_id = '{EscapeSql(subscriptionId)}'";
        await cmd.ExecuteNonQueryAsync(ct);
    }

    /// <summary>
    /// Get all subscriptions watching a specific address.
    /// </summary>
    public async Task<List<PushSubscriptionRecord>> GetSubscriptionsForAddressAsync(
        string address, CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT subscription_id, endpoint, p256dh, auth, events, large_transfer_threshold
            FROM push_subscriptions FINAL
            WHERE has(addresses, '{EscapeSql(address)}')";

        var results = new List<PushSubscriptionRecord>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            results.Add(new PushSubscriptionRecord(
                SubscriptionId: reader.GetString(0),
                Endpoint: reader.GetString(1),
                P256dh: reader.GetString(2),
                Auth: reader.GetString(3),
                Events: ((string[])reader.GetValue(4)).ToList(),
                LargeTransferThreshold: Convert.ToUInt64(reader.GetValue(5))
            ));
        }
        return results;
    }

    /// <summary>
    /// Send a push notification to a subscription.
    /// </summary>
    public async Task<bool> SendNotificationAsync(
        PushSubscriptionRecord sub,
        string title,
        string body,
        string? url = null,
        CancellationToken ct = default)
    {
        try
        {
            var subscription = new PushSubscription(sub.Endpoint, sub.P256dh, sub.Auth);
            var payload = JsonSerializer.Serialize(new
            {
                title,
                body,
                url,
                timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
            });

            await _pushClient.SendNotificationAsync(subscription, payload, _vapidDetails, ct);
            return true;
        }
        catch (WebPushException ex) when (ex.StatusCode == System.Net.HttpStatusCode.Gone ||
                                           ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            // Subscription expired or invalid — remove it
            _logger.LogInformation("Removing expired push subscription {Id}", sub.SubscriptionId);
            await RemoveSubscriptionAsync(sub.SubscriptionId, ct);
            return false;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to send push notification to {Id}", sub.SubscriptionId);
            return false;
        }
    }

    /// <summary>
    /// Check if a notification was already sent for this event (deduplication).
    /// </summary>
    public async Task<bool> WasNotificationSentAsync(
        string subscriptionId, string address, ulong tickNumber,
        CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT count() FROM notification_log
            WHERE subscription_id = '{EscapeSql(subscriptionId)}'
              AND address = '{EscapeSql(address)}'
              AND tick_number = {tickNumber}";
        var count = Convert.ToInt64(await cmd.ExecuteScalarAsync(ct));
        return count > 0;
    }

    /// <summary>
    /// Record that a notification was sent.
    /// </summary>
    public async Task RecordNotificationAsync(
        string subscriptionId, string address, ulong tickNumber,
        string eventType, ulong amount,
        CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            INSERT INTO notification_log
            (subscription_id, address, tick_number, event_type, amount)
            VALUES
            ('{EscapeSql(subscriptionId)}', '{EscapeSql(address)}', {tickNumber}, '{eventType}', {amount})";
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static string EscapeSql(string value)
        => value.Replace("'", "\\'");

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _connection.Dispose();
    }
}

public record PushSubscriptionRecord(
    string SubscriptionId,
    string Endpoint,
    string P256dh,
    string Auth,
    List<string> Events,
    ulong LargeTransferThreshold
);
