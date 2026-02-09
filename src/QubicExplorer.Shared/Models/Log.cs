namespace QubicExplorer.Shared.Models;

public class Log
{
    public ulong TickNumber { get; set; }
    public uint Epoch { get; set; }
    public uint LogId { get; set; }
    public byte LogType { get; set; }
    public string? TxHash { get; set; }
    public ushort InputType { get; set; }
    public string? SourceAddress { get; set; }
    public string? DestAddress { get; set; }
    public ulong Amount { get; set; }
    public string? AssetName { get; set; }
    public string? RawData { get; set; }
    public DateTime Timestamp { get; set; }
    public DateTime CreatedAt { get; set; }
}
