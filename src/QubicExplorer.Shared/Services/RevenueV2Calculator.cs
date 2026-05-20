namespace QubicExplorer.Shared.Services;

/// <summary>
/// Revenue V2 — additive bonus formula from qubic core PR #829, with XMR removed
/// in PR #844. Faithful C# port of qubic/src/revenue.h:computeRevenueV2 and the
/// matching qli-manager implementation (li.qubic.data/RevenueCalc/RevenueV2Calculator.cs).
///
/// Formula:
///   revenue = IPC × M × (S² + B·E) / (S·(S+B)·S)
///   where  S = 1024,  B = 256,
///          M = (17·txFactor + 3·oracleFactor) / 20  ∈ [0, S]
///          E = miningFactor (DOGE)                  ∈ [0, S]
///          IPC = ISSUANCE_RATE / NUMBER_OF_COMPUTORS
///
/// Vote no longer feeds revenue under V2 (kept only as a metric).
/// </summary>
public static class RevenueV2Calculator
{
    public const int N = OracleRevenueFactor.N;       // 676
    public const ulong S = OracleRevenueFactor.S;     // 1024
    public const ulong B = 256;
    public const ulong W_TX = 17;
    public const ulong W_ORACLE = 3;
    public const ulong W_SUM = 20;
    public const ulong DIVISOR = S * (S + B) * S;     // 1024 × 1280 × 1024
    public const long IPC = 1_000_000_000_000L / 676; // ISSUANCE_RATE / N (= 1,479,289,940)

    public const int REVENUE_HALF_WINDOW = N - 1;                       // 675
    public const int REVENUE_WINDOW_SIZE = 2 * REVENUE_HALF_WINDOW + 1; // 1351

    /// <summary>The default epoch from which the V2 formula is active. Mirrors qubic core PR #829.</summary>
    public const uint DefaultV2FromEpoch = 209;

    /// <summary>
    /// Input type for the DOGE shares packet (10-bit packed CustomMiningSharesCounter format).
    /// PR #844 removed XMR (input_type 8) and DOGE took over as input_type 11.
    /// Matches qli-manager's li.qubic.micro.revenueIngest/Sources/ExplorerRevenueTickDataSource.cs.
    /// </summary>
    public const ushort DogeSharesInputType = 11;

    public class Result
    {
        public required ulong[] SlidingWindowTxScoreFull;
        public required ulong[] SlidingWindowTxScoreLagged;
        public required ulong[] TxFactor;       // [0..S]
        public required ulong[] OracleFactor;   // [0..S]
        public required ulong[] MiningFactor;   // [0..S], DOGE only
        public required ulong[] CombinedMandatoryFactor; // M
        public required long[]  Revenue;        // V2 revenue per computor
    }

    /// <summary>
    /// Compute V2 revenue using the gTxRevenuePoints table that corresponds to
    /// <paramref name="epoch"/> (legacy 1025-entry for epoch &lt; 214,
    /// extended 4097-entry from PR #881 for epoch ≥ 214).
    /// </summary>
    public static Result Compute(
        long initialTick,
        ushort[] perTickTxCount,
        ulong[] oracleScore,
        ulong[] dogeScore,
        uint epoch)
        => Compute(
            initialTick, perTickTxCount, oracleScore, dogeScore,
            QubicProtocolParams.GetTxRevenuePoints(epoch));

    /// <summary>
    /// Compute V2 revenue from per-tick TX counts and per-computor input scores.
    /// </summary>
    /// <param name="initialTick">First tick of the epoch.</param>
    /// <param name="perTickTxCount">Indexed by (tick - initialTick). Length = totalTicks observed so far.</param>
    /// <param name="oracleScore">Per-computor oracle revenue points (676). Pass all-zero to fall back to "all full" oracle factor.</param>
    /// <param name="dogeScore">Per-computor DOGE share counts (676).</param>
    /// <param name="txRevenuePoints">Lookup table from revenue.h (gTxRevenuePoints, length 1025 or 4097).</param>
    public static Result Compute(
        long initialTick,
        ushort[] perTickTxCount,
        ulong[] oracleScore,
        ulong[] dogeScore,
        ReadOnlySpan<ushort> txRevenuePoints)
    {
        if (oracleScore.Length != N) throw new ArgumentException($"oracleScore must be length {N}");
        if (dogeScore.Length != N) throw new ArgumentException($"dogeScore must be length {N}");

        var totalTicks = perTickTxCount.Length;
        var result = new Result
        {
            SlidingWindowTxScoreFull = new ulong[N],
            SlidingWindowTxScoreLagged = new ulong[N],
            TxFactor = new ulong[N],
            OracleFactor = new ulong[N],
            MiningFactor = new ulong[N],
            CombinedMandatoryFactor = new ulong[N],
            Revenue = new long[N],
        };

        // Defensive: matches qubic core. Without enough ticks the circular window is invalid.
        if (totalTicks < REVENUE_WINDOW_SIZE)
            return result;

        // 1) Per-tick log score from gTxRevenuePoints lookup.
        var maxLutIndex = txRevenuePoints.Length - 1;
        var perTickLogScore = new uint[totalTicks];
        for (int t = 0; t < totalTicks; t++)
        {
            int txCount = perTickTxCount[t];
            if (txCount > maxLutIndex) txCount = maxLutIndex;
            perTickLogScore[t] = txRevenuePoints[txCount];
        }

        // 2) Sliding window TX score (canonical full-epoch variant).
        ComputeSlidingWindowTxScore(initialTick, perTickLogScore, totalTicks, result.SlidingWindowTxScoreFull);

        // 2b) Lagged variant — only ticks whose ±675 window is fully observed contribute.
        ComputeSlidingWindowTxScoreLagged(initialTick, perTickLogScore, totalTicks, result.SlidingWindowTxScoreLagged);

        // 3) TX factor [0..S], global 2/3 quorum.
        ComputeRevFactor(result.SlidingWindowTxScoreFull, S, result.TxFactor, N, GlobalQuorumRank(N));

        // 4) Oracle factor — fall back to "all full" if no oracle data available.
        bool anyOracle = false;
        for (int i = 0; i < N; i++) if (oracleScore[i] != 0) { anyOracle = true; break; }
        if (anyOracle)
        {
            ComputeRevFactor(oracleScore, S, result.OracleFactor, N, GlobalQuorumRank(N));
        }
        else
        {
            for (int i = 0; i < N; i++) result.OracleFactor[i] = S;
        }

        // 5) Mining factor — DOGE-only, global 2/3 quorum.
        ComputeRevFactor(dogeScore, S, result.MiningFactor, N, GlobalQuorumRank(N));

        // 6) Final formula.
        for (int i = 0; i < N; i++)
        {
            ulong tx = result.TxFactor[i];
            ulong oracle = result.OracleFactor[i];
            ulong E = result.MiningFactor[i];

            ulong M = (W_TX * tx + W_ORACLE * oracle) / W_SUM;
            result.CombinedMandatoryFactor[i] = M;

            ulong numerator = M * (S * S + B * E);
            result.Revenue[i] = (long)((ulong)IPC * numerator / DIVISOR);
        }

        return result;
    }

    /// <summary>
    /// Sliding window — full epoch variant. Mirrors qubic/src/revenue.h:computeRevenueV2 (circular block).
    /// </summary>
    public static void ComputeSlidingWindowTxScore(
        long initialTick, uint[] perTickLogScore, int totalTicks, ulong[] outScore)
    {
        Array.Clear(outScore);
        if (totalTicks <= 0) return;

        ulong windowSum = 0;
        for (int i = -REVENUE_HALF_WINDOW; i <= REVENUE_HALF_WINDOW; i++)
        {
            int idx = ((i % totalTicks) + totalTicks) % totalTicks;
            windowSum += perTickLogScore[idx];
        }

        for (int t = 0; t < totalTicks; t++)
        {
            ulong windowVal = windowSum == 0 ? 1 : windowSum;
            ulong txScore = (ulong)perTickLogScore[t] * S * (ulong)REVENUE_WINDOW_SIZE / windowVal;

            int computorIndex = (int)(((initialTick + t) % N + N) % N);
            outScore[computorIndex] += txScore;

            int leavingIdx = ((t + totalTicks - REVENUE_HALF_WINDOW) % totalTicks + totalTicks) % totalTicks;
            int enteringIdx = (t + REVENUE_HALF_WINDOW + 1) % totalTicks;
            windowSum = windowSum - perTickLogScore[leavingIdx] + perTickLogScore[enteringIdx];
        }
    }

    /// <summary>
    /// Sliding window — lagged variant. Only ticks whose full ±675 window lies within the
    /// observed range contribute. Edge ticks (first 675 and last 675) get zero contribution.
    /// Should converge to the full variant once the epoch is complete.
    /// </summary>
    public static void ComputeSlidingWindowTxScoreLagged(
        long initialTick, uint[] perTickLogScore, int totalTicks, ulong[] outScore)
    {
        Array.Clear(outScore);
        if (totalTicks < REVENUE_WINDOW_SIZE) return;

        ulong windowSum = 0;
        for (int i = 0; i < REVENUE_WINDOW_SIZE; i++)
            windowSum += perTickLogScore[i];

        for (int t = REVENUE_HALF_WINDOW; t < totalTicks - REVENUE_HALF_WINDOW; t++)
        {
            ulong windowVal = windowSum == 0 ? 1 : windowSum;
            ulong txScore = (ulong)perTickLogScore[t] * S * (ulong)REVENUE_WINDOW_SIZE / windowVal;

            int computorIndex = (int)(((initialTick + t) % N + N) % N);
            outScore[computorIndex] += txScore;

            int next = t + REVENUE_HALF_WINDOW + 1;
            int leaving = t - REVENUE_HALF_WINDOW;
            if (next < totalTicks)
                windowSum = windowSum - perTickLogScore[leaving] + perTickLogScore[next];
        }
    }

    /// <summary>
    /// computeRevFactor — mirrors qubic/src/revenue.h.
    /// For each entry: 0 if score is 0, S if score >= quorumScore (or quorumScore == 0),
    /// otherwise proportional. quorumScore = score at rank quorumRank when sorted DESC (1-indexed).
    /// </summary>
    public static void ComputeRevFactor(
        ulong[] score, ulong scalingThreshold, ulong[] outFactor, int count, int quorumRank)
    {
        if (count == 0) return;
        if (quorumRank < 1) quorumRank = 1;
        if (quorumRank > count) quorumRank = count;

        var sorted = new ulong[count];
        Array.Copy(score, sorted, count);
        Array.Sort(sorted);
        Array.Reverse(sorted);
        ulong quorumScore = sorted[quorumRank - 1];

        for (int i = 0; i < count; i++)
        {
            ulong s = score[i];
            if (s == 0) outFactor[i] = 0;
            else if (quorumScore == 0 || s >= quorumScore) outFactor[i] = scalingThreshold;
            else outFactor[i] = scalingThreshold * s / quorumScore;
        }
    }

    /// <summary>2/3 quorum rank, ceil. (groupSize*2+2)/3 — matches the C++ rank derivation.</summary>
    public static int GlobalQuorumRank(int groupSize) => (groupSize * 2 + 2) / 3;
}
