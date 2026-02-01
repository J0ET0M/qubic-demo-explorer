namespace QubicExplorer.Indexer.Configuration;

public class IndexerOptions
{
    public const string SectionName = "Indexer";

    public long StartTick { get; set; } = 0;
    public bool StartFromLatest { get; set; } = false;
    public bool ResumeFromLastTick { get; set; } = true;
    public int BatchSize { get; set; } = 1000;
    public int FlushIntervalMs { get; set; } = 1000;
    public bool IncludeInputData { get; set; } = true;
    public bool SkipEmptyTicks { get; set; } = false;
    public List<string> TxFilters { get; set; } = new();
    public List<string> LogFilters { get; set; } = new();
}
