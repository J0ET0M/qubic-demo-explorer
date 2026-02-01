namespace QubicExplorer.Api.Configuration;

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
}

public class AddressLabelOptions
{
    public const string SectionName = "AddressLabels";
    public string BundleUrl { get; set; } = "https://static.qubic.org/v1/general/data/bundle.min.json";
}
