namespace QubicExplorer.Shared.Models;

public class Transaction
{
    public string Hash { get; set; } = string.Empty;
    public ulong TickNumber { get; set; }
    public uint Epoch { get; set; }
    public string FromAddress { get; set; } = string.Empty;
    public string ToAddress { get; set; } = string.Empty;
    public ulong Amount { get; set; }
    public ushort InputType { get; set; }
    public string? InputData { get; set; }
    public bool Executed { get; set; }
    public int LogIdFrom { get; set; }
    public ushort LogIdLength { get; set; }
    public DateTime Timestamp { get; set; }
    public DateTime CreatedAt { get; set; }
}
