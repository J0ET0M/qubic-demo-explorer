using Microsoft.AspNetCore.Mvc;
using QubicExplorer.Api.Services;

namespace QubicExplorer.Api.Controllers;

[ApiController]
[Route("api/notifications")]
public class NotificationController : ControllerBase
{
    private readonly WebPushService _pushService;

    public NotificationController(WebPushService pushService)
    {
        _pushService = pushService;
    }

    /// <summary>
    /// Get the VAPID public key for push subscription.
    /// </summary>
    [HttpGet("vapid-key")]
    public IActionResult GetVapidKey()
    {
        return Ok(new { publicKey = _pushService.PublicKey });
    }

    /// <summary>
    /// Subscribe to push notifications for the given addresses.
    /// </summary>
    [HttpPost("subscribe")]
    public async Task<IActionResult> Subscribe(
        [FromBody] PushSubscribeRequest request,
        CancellationToken ct)
    {
        if (request.Subscription == null || string.IsNullOrEmpty(request.Subscription.Endpoint))
            return BadRequest("Invalid subscription");

        if (request.Addresses == null || request.Addresses.Length == 0)
            return BadRequest("At least one address is required");

        if (request.Addresses.Length > 20)
            return BadRequest("Maximum 20 addresses per subscription");

        // Generate a subscription ID from the endpoint hash
        var subscriptionId = GenerateSubscriptionId(request.Subscription.Endpoint);

        await _pushService.SaveSubscriptionAsync(
            subscriptionId,
            request.Subscription.Endpoint,
            request.Subscription.Keys.P256dh,
            request.Subscription.Keys.Auth,
            request.Addresses,
            request.Events ?? ["incoming", "outgoing", "large_transfer"],
            request.LargeTransferThreshold > 0 ? request.LargeTransferThreshold : 1_000_000_000,
            request.BalanceMinThreshold,
            request.BalanceMaxThreshold,
            ct);

        return Ok(new { subscriptionId });
    }

    /// <summary>
    /// Unsubscribe from push notifications.
    /// </summary>
    [HttpPost("unsubscribe")]
    public async Task<IActionResult> Unsubscribe(
        [FromBody] PushUnsubscribeRequest request,
        CancellationToken ct)
    {
        if (string.IsNullOrEmpty(request.Endpoint))
            return BadRequest("Endpoint is required");

        var subscriptionId = GenerateSubscriptionId(request.Endpoint);
        await _pushService.RemoveSubscriptionAsync(subscriptionId, ct);

        return Ok(new { removed = true });
    }

    private static string GenerateSubscriptionId(string endpoint)
    {
        var hash = System.Security.Cryptography.SHA256.HashData(
            System.Text.Encoding.UTF8.GetBytes(endpoint));
        return Convert.ToHexString(hash)[..32].ToLowerInvariant();
    }
}

public record PushSubscribeRequest(
    PushSubscriptionData Subscription,
    string[] Addresses,
    string[]? Events = null,
    ulong LargeTransferThreshold = 1_000_000_000,
    ulong BalanceMinThreshold = 0,
    ulong BalanceMaxThreshold = 0
);

public record PushSubscriptionData(
    string Endpoint,
    PushSubscriptionKeys Keys
);

public record PushSubscriptionKeys(
    string P256dh,
    string Auth
);

public record PushUnsubscribeRequest(string Endpoint);
