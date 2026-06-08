namespace QubicExplorer.Shared.Configuration;

public class ClickHouseOptions
{
    public const string SectionName = "ClickHouse";

    public string Host { get; set; } = "localhost";
    public int Port { get; set; } = 8123;
    public string Database { get; set; } = "qubic";
    public string? Username { get; set; }
    public string? Password { get; set; }

    public string ConnectionString =>
        $"Host={Host};Port={Port};Database={Database}" +
        (string.IsNullOrEmpty(Username) ? "" : $";Username={Username}") +
        (string.IsNullOrEmpty(Password) ? "" : $";Password={Password}");

    /// <summary>
    /// Connection string without database — used for initial schema creation.
    /// </summary>
    public string ServerConnectionString =>
        $"Host={Host};Port={Port}" +
        (string.IsNullOrEmpty(Username) ? "" : $";Username={Username}") +
        (string.IsNullOrEmpty(Password) ? "" : $";Password={Password}");
}

public class BobOptions
{
    public const string SectionName = "Bob";

    /// <summary>
    /// List of Bob node base URLs for multi-node failover.
    /// BobWebSocketClient derives WebSocket URLs automatically.
    /// NOTE: do not give this a default list — .NET config binding appends to
    /// existing list instances rather than replacing them. Use GetEffectiveNodes()
    /// for the fallback.
    /// </summary>
    public List<string> Nodes { get; set; } = [];

    /// <summary>
    /// Returns the configured nodes, or the default if none configured.
    /// </summary>
    public IReadOnlyList<string> GetEffectiveNodes() =>
        Nodes.Count > 0 ? Nodes : ["https://bobnet.qubic.li"];

    public int ReconnectDelayMs { get; set; } = 5000;
    public int MaxReconnectDelayMs { get; set; } = 60000;

    /// <summary>
    /// How long to wait for a single connect attempt before rotating to the next node.
    /// Bumped from 10s default — TLS + WebSocket upgrade through some proxies can take longer.
    /// </summary>
    public int ConnectTimeoutSeconds { get; set; } = 30;

    /// <summary>
    /// Periodically probe all configured Bob nodes for their current tick. If
    /// another node is significantly ahead of the one we're connected to, switch.
    /// Useful when the active node falls behind without explicitly disconnecting.
    /// </summary>
    public bool EnableTipMonitor { get; set; } = true;

    /// <summary>Interval (seconds) between tip-height probes across all nodes.</summary>
    public int TipMonitorIntervalSeconds { get; set; } = 60;

    /// <summary>
    /// Switch to a fresher Bob node when its tick is at least this many ticks
    /// ahead of our active node's tick. Below this we ignore the gap (avoids
    /// thrashing during normal sync jitter — Bob nodes are within ~10 ticks of
    /// each other in steady state).
    /// </summary>
    public int TipMonitorLagThreshold { get; set; } = 200;
}

public class AddressLabelOptions
{
    public const string SectionName = "AddressLabels";
    public string BundleUrl { get; set; } = "https://static.qubic.org/v1/general/data/bundle.min.json";
}

public class VapidOptions
{
    public const string SectionName = "Vapid";
    public string Subject { get; set; } = "mailto:admin@qubic.li";
    public string PublicKey { get; set; } = "";
    public string PrivateKey { get; set; } = "";
}
