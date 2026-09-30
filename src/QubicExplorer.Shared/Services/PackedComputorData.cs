using Qubic.Core;

namespace QubicExplorer.Shared.Services;

/// <summary>
/// Bit-packed 10-bit-per-computor byte layout shared by Qubic's on-chain
/// per-tick counters:
///   - VoteCounter (inputType 1, legacy)
///   - CustomMiningShareCounter (inputType 8, legacy)
///   - DogeMiningShare (inputType 11, current)
///
/// Each of these carries a fixed <c>848</c>-byte packed array followed by a
/// 32-byte dataLock — see <c>QubicConstants.PackedComputorInputSize</c> (880).
/// The C++ definition is <c>extract10Bit()</c> in
/// <c>mining/mining.h</c> / <c>network_messages/vote_counter.h</c>.
///
/// Every 4 values occupy 5 bytes; bits are packed MSB→LSB within each byte:
///   byte0 = data[i + (i &gt;&gt; 2)]
///   byte1 = data[i + (i &gt;&gt; 2) + 1]
///   lastBit0 = 8 - (i &amp; 3) * 2
///   firstBit1 = 10 - lastBit0
///   value = ((byte0 &amp; ((1 &lt;&lt; lastBit0) - 1)) &lt;&lt; firstBit1) | (byte1 &gt;&gt; (8 - firstBit1))
///
/// Historically the explorer carried 4 near-identical private copies of this
/// routine (TransactionInputParser, ComputorRevenueService, TickVotePersistenceService,
/// ClickHouseQueryService's on-the-fly revenue recompute). This class is the
/// single source of truth — new call sites should use it directly rather than
/// re-inline the bit math.
/// </summary>
public static class PackedComputorData
{
    /// <summary>
    /// Extract a single 10-bit value at <paramref name="idx"/> (0..675) from
    /// the 848-byte packed buffer. Returns 0 on out-of-bounds so callers don't
    /// need to guard truncated packets — validation is a separate concern.
    /// </summary>
    public static uint Extract10Bit(ReadOnlySpan<byte> data, int idx)
    {
        int byteOffset = idx + (idx >> 2);
        if (byteOffset + 1 >= data.Length) return 0;
        uint byte0 = data[byteOffset];
        uint byte1 = data[byteOffset + 1];
        int lastBit0 = 8 - (idx & 3) * 2;
        int firstBit1 = 10 - lastBit0;
        uint res = (byte0 & (uint)((1 << lastBit0) - 1)) << firstBit1;
        res |= byte1 >> (8 - firstBit1);
        return res;
    }

    /// <summary>Bulk-extract <paramref name="count"/> 10-bit values.</summary>
    public static ushort[] ExtractAll(ReadOnlySpan<byte> data, int count)
    {
        var values = new ushort[count];
        for (int i = 0; i < count; i++) values[i] = (ushort)Extract10Bit(data, i);
        return values;
    }

    /// <summary>
    /// Convenience overload matching legacy call sites — 676 computors by default.
    /// </summary>
    public static ushort[] ExtractAll(ReadOnlySpan<byte> data) =>
        ExtractAll(data, QubicConstants.NumberOfComputors);

    /// <summary>
    /// Semantic validation of a packed packet's shape.
    ///
    /// For vote counters:
    ///   1. Sum of all votes must be at least <c>(676 - 1) * 451 = 304,425</c>
    ///      — a computor with fewer votes than quorum can't have been leader.
    ///   2. The reporter's own slot must be zero (a computor doesn't count itself).
    ///
    /// For mining/DOGE share counters: only rule 2 applies. Same self-slot check.
    ///
    /// Returns true if the packet passes.
    /// </summary>
    public static bool Validate(ReadOnlySpan<byte> data, int reporterComputorIdx, bool isVoteCounter)
    {
        var N = QubicConstants.NumberOfComputors;
        if (isVoteCounter)
        {
            ulong sum = 0;
            var values = new uint[N];
            for (int i = 0; i < N; i++)
            {
                values[i] = Extract10Bit(data, i);
                sum += values[i];
            }
            if (sum < (ulong)(N - 1) * (ulong)QubicConstants.Quorum) return false;
            if (reporterComputorIdx >= 0 && reporterComputorIdx < N && values[reporterComputorIdx] != 0)
                return false;
        }
        else
        {
            if (reporterComputorIdx >= 0 && reporterComputorIdx < N)
            {
                var ownValue = Extract10Bit(data, reporterComputorIdx);
                if (ownValue != 0) return false;
            }
        }
        return true;
    }
}
