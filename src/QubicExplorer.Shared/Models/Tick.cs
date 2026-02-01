namespace QubicExplorer.Shared.Models;

public class Tick
{
    public ulong TickNumber { get; set; }
    public uint Epoch { get; set; }
    public DateTime Timestamp { get; set; }
    public uint TxCount { get; set; }
    public uint TxCountFiltered { get; set; }
    public uint LogCount { get; set; }
    public uint LogCountFiltered { get; set; }
    public DateTime CreatedAt { get; set; }
}
