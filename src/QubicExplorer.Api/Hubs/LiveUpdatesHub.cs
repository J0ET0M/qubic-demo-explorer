using Microsoft.AspNetCore.SignalR;

namespace QubicExplorer.Api.Hubs;

public class LiveUpdatesHub : Hub
{
    private readonly ILogger<LiveUpdatesHub> _logger;

    public LiveUpdatesHub(ILogger<LiveUpdatesHub> logger)
    {
        _logger = logger;
    }

    public async Task SubscribeToTicks()
    {
        await Groups.AddToGroupAsync(Context.ConnectionId, "ticks");
        _logger.LogDebug("Client {ConnectionId} subscribed to ticks", Context.ConnectionId);
    }

    public async Task UnsubscribeFromTicks()
    {
        await Groups.RemoveFromGroupAsync(Context.ConnectionId, "ticks");
        _logger.LogDebug("Client {ConnectionId} unsubscribed from ticks", Context.ConnectionId);
    }

    public async Task SubscribeToAddress(string address)
    {
        await Groups.AddToGroupAsync(Context.ConnectionId, $"address:{address}");
        _logger.LogDebug("Client {ConnectionId} subscribed to address {Address}", Context.ConnectionId, address);
    }

    public async Task UnsubscribeFromAddress(string address)
    {
        await Groups.RemoveFromGroupAsync(Context.ConnectionId, $"address:{address}");
        _logger.LogDebug("Client {ConnectionId} unsubscribed from address {Address}", Context.ConnectionId, address);
    }

    public override async Task OnConnectedAsync()
    {
        _logger.LogDebug("Client connected: {ConnectionId}", Context.ConnectionId);
        await base.OnConnectedAsync();
    }

    public override async Task OnDisconnectedAsync(Exception? exception)
    {
        _logger.LogDebug("Client disconnected: {ConnectionId}", Context.ConnectionId);
        await base.OnDisconnectedAsync(exception);
    }
}

// Extension methods for sending notifications
public static class LiveUpdatesHubExtensions
{
    public static async Task SendNewTick(this IHubContext<LiveUpdatesHub> hubContext, object tickData)
    {
        await hubContext.Clients.Group("ticks").SendAsync("OnNewTick", tickData);
    }

    public static async Task SendNewTransaction(this IHubContext<LiveUpdatesHub> hubContext, object txData)
    {
        await hubContext.Clients.Group("ticks").SendAsync("OnNewTransaction", txData);
    }

    public static async Task SendAddressUpdate(this IHubContext<LiveUpdatesHub> hubContext, string address, object data)
    {
        await hubContext.Clients.Group($"address:{address}").SendAsync("OnAddressUpdate", data);
    }
}
