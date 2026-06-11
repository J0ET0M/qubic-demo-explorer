namespace QubicExplorer.Shared.Services;

/// <summary>
/// Deterministic epoch ↔ wall-clock mapping. Every Qubic epoch starts on
/// Wednesday 12:00 UTC and lasts exactly 7 days. Given one reference epoch we
/// can compute any other epoch's start without consulting the DB — useful when
/// indexer-supplied timestamps are unreliable or missing (e.g. epoch_meta
/// hasn't been backfilled, or ticks carry sentinel values).
///
/// To shift the reference (after a future epoch reshuffle, if it happens),
/// update only the two consts below.
/// </summary>
public static class EpochCalendar
{
    /// <summary>Epoch whose start date is encoded in <see cref="ReferenceStartUtc"/>.</summary>
    public const uint ReferenceEpoch = 202;

    /// <summary>Wall-clock start of <see cref="ReferenceEpoch"/> — Wednesday 12:00 UTC.</summary>
    public static readonly DateTime ReferenceStartUtc =
        new DateTime(2026, 2, 25, 12, 0, 0, DateTimeKind.Utc);

    public static readonly TimeSpan EpochDuration = TimeSpan.FromDays(7);

    /// <summary>Returns the UTC start time of the given epoch.</summary>
    public static DateTime GetEpochStart(uint epoch)
    {
        var offsetDays = ((long)epoch - ReferenceEpoch) * 7L;
        return ReferenceStartUtc.AddDays(offsetDays);
    }

    /// <summary>Returns the UTC midpoint of the given epoch (start + 3.5 days).</summary>
    public static DateTime GetEpochMidpoint(uint epoch) =>
        GetEpochStart(epoch).AddDays(3.5);

    /// <summary>
    /// Epoch containing the given instant. The epoch whose start is the
    /// most recent Wednesday 12 UTC at-or-before <paramref name="utc"/>.
    /// </summary>
    public static uint GetEpochAt(DateTime utc)
    {
        var weeks = (utc - ReferenceStartUtc).TotalDays / 7.0;
        var n = (long)Math.Floor(weeks) + ReferenceEpoch;
        return (uint)Math.Max(1, n);
    }

    /// <summary>
    /// Returns the inclusive epoch range whose midpoint falls in the given
    /// calendar year. Using midpoint (instead of start) puts boundary epochs
    /// in the year they predominantly cover — an epoch starting Dec 31 12 UTC
    /// has 6.5 of its 7 days in the new year, so it ends up bucketed there.
    /// </summary>
    public static (uint First, uint Last) EpochsForYear(int year)
    {
        // Linear interpolation to find the approximate first epoch, then walk
        // up/down a few steps for an exact answer (only ~52 epochs per year).
        var yearStart = new DateTime(year, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var approxFirst = (uint)Math.Max(
            1,
            (long)Math.Floor((yearStart - ReferenceStartUtc).TotalDays / 7.0) + ReferenceEpoch - 1);

        // Walk forward while midpoint is still before the requested year.
        var first = approxFirst;
        while (GetEpochMidpoint(first).Year < year) first++;
        // Walk back if we overshot (rare with the -1 cushion above).
        while (first > 1 && GetEpochMidpoint(first - 1).Year == year) first--;

        // Walk forward until midpoint leaves the year.
        var last = first;
        while (GetEpochMidpoint(last + 1).Year == year) last++;

        return (first, last);
    }
}
