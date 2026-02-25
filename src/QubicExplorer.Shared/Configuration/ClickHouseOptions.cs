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
    /// </summary>
    public List<string> Nodes { get; set; } = ["https://bob02.qubic.li"];

    /// <summary>
    /// Returns the configured nodes, or the default if none configured.
    /// </summary>
    public IReadOnlyList<string> GetEffectiveNodes() =>
        Nodes.Count > 0 ? Nodes : ["https://bob02.qubic.li"];

    public int ReconnectDelayMs { get; set; } = 5000;
    public int MaxReconnectDelayMs { get; set; } = 60000;
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
