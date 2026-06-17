namespace QubicExplorer.Shared.Services;

/// <summary>
/// Multi-dimension revenue — the formula qubic core pays from epoch 218
/// (qubic v1.296.0, commit 392887b "Enable multi dimension revenue"). Faithful C#
/// port of qubic/src/revenue.h:computeMultiDimRevenue / finalizeTickScore /
/// revenueOnTick / scoreEpochEdgeTicks.
///
/// Unlike V2 (additive bonus over a 1-D log-TX sliding window) this is FULLY
/// MULTIPLICATIVE and its TX score is an asymmetric-L2 deficit measured over a
/// per-tick observation vector across every transaction source/destination category:
///
///   txFactor   = computeRevFactor(txScore, S)                         // L2 multi-dim TX
///   oracleF    = computeRevFactor(oracleScore, S) | all-S valve
///   dogeF      = computeRevFactor(dogeScore,   S) | all-S valve
///   revenue[i] = IPC × txFactor[i] × oracleF[i] × ISqrt(dogeF[i]·S) / S³
///
/// The gTxRevenuePoints log LUT is NOT used here (multi-dim uses raw per-dim counts).
/// computeRevFactor / the 2/3 quorum rank are shared with <see cref="RevenueV2Calculator"/>.
/// </summary>
public static class MultiDimRevenueCalculator
{
    public const int N = OracleRevenueFactor.N;     // 676 (NUMBER_OF_COMPUTORS)
    public const ulong S = OracleRevenueFactor.S;   // 1024 (REVENUE_SCALE)
    public const int H = N - 1;                     // 675 (REVENUE_HALF_WINDOW)
    public const int W = 2 * H + 1;                 // 1351 (REVENUE_WINDOW_SIZE)
    public const int DOGE_K = 2;                    // REVENUE_DOGE_K (was 4 pre-v1.296.0)
    public const long IPC = 1_000_000_000_000L / N; // ISSUANCE_RATE / N = 1,479,289,940

    /// <summary>
    /// Production contract count at v1.296.0 — counts contractDescriptions[] in
    /// qubic/src/contract_core/contract_def.h:390-419 (index 0 "" … 28 GGWP). The 4
    /// TESTEXA-D contracts are test-only (INCLUDE_CONTRACT_TEST_EXAMPLES is commented
    /// out in production qubic.cpp:3), so they are NOT counted. Bump if core adds a
    /// contract (core bumps protocol version when it does).
    /// </summary>
    public const int CONTRACT_DIMS = 29;            // REVENUE_CONTRACT_DIMS = contractCount

    /// <summary>NUMBER_OF_COMPUTORS + contractCount + 1 = 706.</summary>
    public const int REVENUE_TX_DIM = N + CONTRACT_DIMS + 1; // 706
    /// <summary>The "transfer/other" dimension (last). = 705.</summary>
    public const int TRANSFER_DIM = REVENUE_TX_DIM - 1;

    /// <summary>Epoch from which multi-dim becomes the active/paid formula (qubic v1.296.0).</summary>
    public const uint DefaultMultiDimFromEpoch = 218;

    public class Result
    {
        public required ulong[] TxScore;        // [N] accumulated asymmetric-L2 tick scores per leader
        public required ulong[] TxFactor;       // [N] in [0,S]
        public required ulong[] OracleFactor;   // [N] in [0,S]
        public required ulong[] DogeFactor;     // [N] in [0,S] (pre-root)
        public required ulong[] DogeRootScaled; // [N] in [0,S] (ISqrt(dogeFactor·S))
        public required long[]  Revenue;        // [N] final multi-dim revenue
    }

    /// <summary>
    /// Compute multi-dimension revenue.
    /// </summary>
    /// <param name="initialTick">First tick of the epoch.</param>
    /// <param name="obsByTick">
    /// Sparse per-tick observation vectors indexed by (tick - initialTick); length = totalTicks.
    /// Each entry maps dimIndex → count for the non-zero dims of that tick
    /// (dims [0,676)=source-computor, [676,705)=contract, 705=transfer). Gap/empty ticks
    /// contribute an empty dictionary.
    /// </param>
    /// <param name="oracleScore">Per-computor oracle revenue points (676). All-zero → "all full" valve.</param>
    /// <param name="dogeScore">Per-computor DOGE share counts (676). All-zero → "all full" valve.</param>
    public static Result Compute(
        long initialTick,
        IReadOnlyList<IReadOnlyDictionary<int, ushort>> obsByTick,
        ulong[] oracleScore,
        ulong[] dogeScore,
        uint epoch)
    {
        if (oracleScore.Length != N) throw new ArgumentException($"oracleScore must be length {N}");
        if (dogeScore.Length != N) throw new ArgumentException($"dogeScore must be length {N}");

        var result = new Result
        {
            TxScore = new ulong[N],
            TxFactor = new ulong[N],
            OracleFactor = new ulong[N],
            DogeFactor = new ulong[N],
            DogeRootScaled = new ulong[N],
            Revenue = new long[N],
        };

        int totalTicks = obsByTick.Count;

        // Defensive: matches qubic core. Without a full window the circular slide is invalid;
        // core static_asserts MAX_NUMBER_OF_TICKS_PER_EPOCH >= REVENUE_WINDOW_SIZE.
        if (totalTicks < W)
            return result;

        // --- Asymmetric-L2 TX score over the circular sliding window. -----------------
        // Offline we have the whole epoch, so we slide the circular window directly — this is
        // mathematically identical to core's ring/head + scoreEpochEdgeTicks machinery, which only
        // exists because the node streams one tick at a time and cannot allocate.
        var windowSum = new ulong[REVENUE_TX_DIM];

        // Initial window for center c=0: circular indices [-H, +H].
        for (int i = -H; i <= H; i++)
        {
            int idx = ((i % totalTicks) + totalTicks) % totalTicks;
            foreach (var kv in obsByTick[idx])
                windowSum[kv.Key] += kv.Value;
        }

        for (int c = 0; c < totalTicks; c++)
        {
            FinalizeTickScore(initialTick, c, obsByTick[c], windowSum, result.TxScore);

            // Slide the window by one (skip after the last center tick).
            if (c + 1 < totalTicks)
            {
                int leaving = ((c - H) % totalTicks + totalTicks) % totalTicks;
                int entering = (c + H + 1) % totalTicks;
                foreach (var kv in obsByTick[leaving]) windowSum[kv.Key] -= kv.Value;
                foreach (var kv in obsByTick[entering]) windowSum[kv.Key] += kv.Value;
            }
        }

        // --- Factors + final multiplicative revenue (computeMultiDimRevenue). ---------
        int quorumRank = RevenueV2Calculator.GlobalQuorumRank(N);
        RevenueV2Calculator.ComputeRevFactor(result.TxScore, S, result.TxFactor, N, quorumRank);

        if (HasActivity(oracleScore))
            RevenueV2Calculator.ComputeRevFactor(oracleScore, S, result.OracleFactor, N, quorumRank);
        else
            Array.Fill(result.OracleFactor, S);

        if (HasActivity(dogeScore))
            RevenueV2Calculator.ComputeRevFactor(dogeScore, S, result.DogeFactor, N, quorumRank);
        else
            Array.Fill(result.DogeFactor, S);

        ulong sPowKm1 = IPow(S, DOGE_K - 1); // = 1024
        for (int i = 0; i < N; i++)
        {
            result.DogeRootScaled[i] = ISqrt(result.DogeFactor[i] * sPowKm1); // irootK64<2>(dogeF·S)
            ulong num = (ulong)IPC * result.TxFactor[i] * result.OracleFactor[i] * result.DogeRootScaled[i];
            result.Revenue[i] = (long)(num / (S * S * S));
        }

        return result;
    }

    /// <summary>
    /// Port of revenue.h:finalizeTickScore. With every dimension multiplier m = 1, only dims with a
    /// non-zero windowSum contribute (a dim with ws==0 adds 0 to both sums; and obs_c[d] &gt; 0 ⇒
    /// ws[d] &gt; 0 because the window contains tick c). Iterating the dense windowSum is exact.
    /// </summary>
    private static void FinalizeTickScore(
        long initialTick, int c, IReadOnlyDictionary<int, ushort> obsC, ulong[] windowSum, ulong[] txScore)
    {
        ulong sumDef = 0, sumCap = 0;
        for (int d = 0; d < REVENUE_TX_DIM; d++)
        {
            ulong ws = windowSum[d];
            if (ws == 0) continue;
            ulong wo = (ulong)W * (obsC.TryGetValue(d, out var v) ? v : (ushort)0);
            ulong deficit = wo >= ws ? 0UL : ws - wo;
            sumDef += deficit * deficit;
            sumCap += ws * ws;
        }

        ulong tickScore;
        if (sumCap == 0)
            tickScore = S;
        else
            tickScore = S - (S * ISqrt(sumDef)) / ISqrt(sumCap);

        int leader = (int)(((initialTick + c) % N + N) % N);
        txScore[leader] += tickScore;
    }

    private static bool HasActivity(ulong[] scores)
    {
        for (int i = 0; i < scores.Length; i++)
            if (scores[i] != 0) return true;
        return false;
    }

    // ---- Integer math ports (math_lib.h). .NET has no exact u64 root, and Math.Sqrt(double)
    //      loses precision above 2^53 while sumCap reaches ~2.2e16 — so port the integer algorithms.

    /// <summary>Exact floor(sqrt(n)) over the full u64 range — port of math_lib::irootK64&lt;2&gt;.</summary>
    public static ulong ISqrt(ulong n)
    {
        if (n < 2) return n;
        // Seed: 2^ceil(bitLength/2).
        int seedShift = (BitLength(n) + 1) / 2;
        ulong x = 1UL << seedShift;
        // Condition-driven Newton (non-increasing).
        ulong t = (x + n / x) >> 1;
        int iter = 0;
        while (t < x && iter < 100) { x = t; t = (x + n / x) >> 1; iter++; }
        // Exact floor correction using the overflow-safe x > n/x form (never x*x).
        while (x > n / x) x--;
        while (x < ulong.MaxValue && (x + 1) <= n / (x + 1)) x++;
        return x;
    }

    /// <summary>Saturating integer power — port of math_lib::ipow.</summary>
    public static ulong IPow(ulong v, int e)
    {
        ulong result = 1;
        while (e > 0)
        {
            if ((e & 1) != 0)
            {
                if (v != 0 && result > ulong.MaxValue / v) return ulong.MaxValue;
                result *= v;
            }
            e >>= 1;
            if (e > 0)
            {
                if (v != 0 && v > ulong.MaxValue / v) return ulong.MaxValue;
                v *= v;
            }
        }
        return result;
    }

    private static int BitLength(ulong x) =>
        x == 0 ? 0 : 64 - System.Numerics.BitOperations.LeadingZeroCount(x);
}
