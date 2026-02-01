namespace QubicExplorer.Shared.Models;

public class Asset
{
    public string AssetName { get; set; } = string.Empty;
    public string IssuerAddress { get; set; } = string.Empty;
    public ulong TickNumber { get; set; }
    public ulong TotalSupply { get; set; }
    public DateTime CreatedAt { get; set; }
}
