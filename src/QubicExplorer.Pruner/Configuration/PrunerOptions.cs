namespace QubicExplorer.Pruner.Configuration;

public class PrunerOptions
{
    public const string SectionName = "Pruner";

    /// <summary>
    /// How often to check for prunable data (in minutes). Default: 60.
    /// </summary>
    public int IntervalMinutes { get; set; } = 60;

    /// <summary>
    /// Log what would be deleted without actually deleting. Default: true (safe by default).
    /// </summary>
    public bool DryRun { get; set; } = true;

    /// <summary>
    /// Pruning rules for transactions.
    /// </summary>
    public List<PruneRule> Rules { get; set; } = [];
}

public class PruneRule
{
    /// <summary>
    /// Unique name for this rule (used for state tracking and logging).
    /// </summary>
    public string Name { get; set; } = string.Empty;

    // ── Transaction conditions ──────────────────────────────────────

    /// <summary>Filter by destination address.</summary>
    public string? DestId { get; set; }

    /// <summary>Filter by source address.</summary>
    public string? SourceId { get; set; }

    /// <summary>Filter by input type.</summary>
    public int? InputType { get; set; }

    /// <summary>Filter by exact amount.</summary>
    public long? Amount { get; set; }

    /// <summary>Filter by executed status.</summary>
    public bool? Executed { get; set; }

    // ── Log-only conditions ─────────────────────────────────────────

    /// <summary>
    /// Filter by log type (e.g. 7 for CONTRACT_DEBUG_MESSAGE).
    /// When set without transaction conditions, this becomes a log-only rule.
    /// </summary>
    public int? LogType { get; set; }

    // ── Retention ───────────────────────────────────────────────────

    /// <summary>
    /// Keep data from the last N days. Prune older rows based on timestamp.
    /// If both KeepDays and KeepEpochs are set, whichever retains more data wins.
    /// </summary>
    public int? KeepDays { get; set; }

    /// <summary>
    /// Keep data from the last N epochs. Prune rows from older epochs.
    /// If both KeepDays and KeepEpochs are set, whichever retains more data wins.
    /// </summary>
    public int? KeepEpochs { get; set; }

    /// <summary>
    /// When pruning transactions, also prune their associated logs (matched by tx_hash).
    /// Default: true.
    /// </summary>
    public bool PruneLogs { get; set; } = true;

    /// <summary>
    /// Whether this is a log-only rule (no transaction conditions, only LogType).
    /// </summary>
    public bool IsLogOnly => LogType.HasValue
        && !DestId.HasValue() && !SourceId.HasValue()
        && !InputType.HasValue && !Amount.HasValue && !Executed.HasValue;
}

internal static class StringExtensions
{
    public static bool HasValue(this string? s) => !string.IsNullOrEmpty(s);
}
