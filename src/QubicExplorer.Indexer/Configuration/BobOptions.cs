namespace QubicExplorer.Indexer.Configuration;

public class BobOptions
{
    public const string SectionName = "Bob";

    /// <summary>
    /// List of Bob node base URLs for multi-node failover.
    /// BobWebSocketClient derives WebSocket URLs automatically.
    /// </summary>
    public List<string> Nodes { get; set; } = ["https://bob02.qubic.li"];

    public int ReconnectDelayMs { get; set; } = 5000;
    public int MaxReconnectDelayMs { get; set; } = 60000;
}
