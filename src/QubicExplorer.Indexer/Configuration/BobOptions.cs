namespace QubicExplorer.Indexer.Configuration;

public class BobOptions
{
    public const string SectionName = "Bob";

    /// <summary>
    /// List of Bob node base URLs for multi-node failover.
    /// BobWebSocketClient derives WebSocket URLs automatically.
    /// Default is applied in code if list is empty after configuration binding.
    /// </summary>
    public List<string> Nodes { get; set; } = [];

    /// <summary>
    /// Returns the configured nodes, or the default if none configured.
    /// </summary>
    public IReadOnlyList<string> GetEffectiveNodes() =>
        Nodes.Count > 0 ? Nodes : ["https://bob02.qubic.li"];

    public int ReconnectDelayMs { get; set; } = 5000;
    public int MaxReconnectDelayMs { get; set; } = 60000;
}
