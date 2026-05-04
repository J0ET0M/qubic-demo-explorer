namespace QubicExplorer.Analytics.Configuration;

/// <summary>
/// Toggles for individual analytics features. Disable any of these to skip
/// the corresponding work in the analytics loop without rebuilding.
///
/// Bind from the "Analytics" config section. All defaults are true so the
/// service runs all analytics out of the box.
/// </summary>
public class AnalyticsOptions
{
    public const string SectionName = "Analytics";

    public bool EnableHolderDistribution { get; set; } = true;
    public bool EnableNetworkStats { get; set; } = true;
    public bool EnableBurnStats { get; set; } = true;
    public bool EnableMinerFlow { get; set; } = true;
    public bool EnableQearn { get; set; } = true;
    public bool EnableCcf { get; set; } = true;
    public bool EnableComputorRevenue { get; set; } = true;
    public bool EnableTickVotes { get; set; } = true;
    public bool EnableRewardDistributions { get; set; } = true;
    public bool EnableExecutionFees { get; set; } = true;
    public bool EnableCustomFlowJobs { get; set; } = true;
}
