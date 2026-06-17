using QubicExplorer.Shared.Services;

namespace QubicExplorer.Indexer.Tests;

/// <summary>
/// Self-contained unit tests for the multi-dimension revenue calculator (qubic v1.296.0).
/// No database/ClickHouse access — exercises the algorithm directly with synthetic observation
/// streams. Mirrors qubic/src/revenue.h:computeMultiDimRevenue / finalizeTickScore and
/// test/revenue.cpp, and is the cross-repo twin of qli-manager's MultiDimRevenueTest.
/// </summary>
public class MultiDimRevenueTests
{
    private const int N = MultiDimRevenueCalculator.N;
    private const ulong S = MultiDimRevenueCalculator.S;
    private const int W = MultiDimRevenueCalculator.W;
    private const int TRANSFER = MultiDimRevenueCalculator.TRANSFER_DIM;
    private static readonly long IPC = MultiDimRevenueCalculator.IPC;

    private static ulong[] Zero(int n) => new ulong[n];
    private static ulong[] Constant(int n, ulong v) { var a = new ulong[n]; Array.Fill(a, v); return a; }

    // ---------------- Integer math parity (math_lib.h) ----------------

    [Fact]
    public void ISqrt_ExactFloor_AcrossRange()
    {
        Assert.Equal(0UL, MultiDimRevenueCalculator.ISqrt(0));
        Assert.Equal(1UL, MultiDimRevenueCalculator.ISqrt(1));
        Assert.Equal(S, MultiDimRevenueCalculator.ISqrt(S * S));            // 1024
        Assert.Equal(4294967295UL, MultiDimRevenueCalculator.ISqrt(ulong.MaxValue));
        for (ulong i = 1; i < 5000; i++)
        {
            Assert.Equal(i, MultiDimRevenueCalculator.ISqrt(i * i));
            Assert.Equal(i - 1, MultiDimRevenueCalculator.ISqrt(i * i - 1));
        }

        // Randomized floor property: r^2 <= n < (r+1)^2 up to where sumCap can reach (~2.2e16).
        var rnd = new Random(12345);
        for (int k = 0; k < 2000; k++)
        {
            ulong n = (ulong)rnd.NextInt64(0, (long)1e16);
            ulong r = MultiDimRevenueCalculator.ISqrt(n);
            Assert.True(r * r <= n && (r + 1) * (r + 1) > n, $"isqrt floor broke at {n}");
        }
    }

    [Fact]
    public void IPow_Saturates()
    {
        Assert.Equal(1UL, MultiDimRevenueCalculator.IPow(1234, 0));
        Assert.Equal(1024UL, MultiDimRevenueCalculator.IPow(S, 1));
        Assert.Equal(S * S * S, MultiDimRevenueCalculator.IPow(S, 3));
        Assert.Equal(ulong.MaxValue, MultiDimRevenueCalculator.IPow(1UL << 33, 3)); // overflow → saturate
    }

    // ---------------- Calculator ----------------

    [Fact]
    public void Compute_TooFewTicks_ReturnsAllZero()
    {
        var stream = MakeStream(W - 1, _ => new Dictionary<int, ushort>());
        var r = MultiDimRevenueCalculator.Compute(0, stream, Zero(N), Zero(N), 218);
        Assert.All(r.Revenue, x => Assert.Equal(0L, x));
        Assert.All(r.TxScore, x => Assert.Equal(0UL, x));
    }

    [Fact]
    public void Compute_NoActivity_FullRevenue()
    {
        // No observations anywhere → no deficit on any dim → sumCap==0 → every tick scores the
        // full REVENUE_SCALE (revenue.h:587-589, finalizeTickScore). With no oracle/doge either,
        // all three valves give S, so every computor receives the full per-computor issuance IPC.
        // (Zero activity is "everyone perfect", NOT "everyone zero" — the latter is the
        // insufficient-window case, see Compute_TooFewTicks_ReturnsAllZero.)
        var stream = MakeStream(W + 100, _ => new Dictionary<int, ushort>());
        var r = MultiDimRevenueCalculator.Compute(0, stream, Zero(N), Zero(N), 218);
        Assert.All(r.TxFactor, x => Assert.Equal(S, x));
        Assert.All(r.OracleFactor, x => Assert.Equal(S, x));
        Assert.All(r.DogeFactor, x => Assert.Equal(S, x));
        Assert.All(r.Revenue, x => Assert.Equal(IPC, x));
    }

    [Fact]
    public void Compute_RevenueWithinBounds()
    {
        var rnd = new Random(99);
        int totalTicks = W + 300;
        var stream = MakeStream(totalTicks, t =>
        {
            var d = new Dictionary<int, ushort>();
            d[t % 50] = (ushort)rnd.Next(0, 50);          // some source-computor dims
            d[TRANSFER] = (ushort)rnd.Next(0, 100);       // transfer dim
            return d;
        });
        var oracle = new ulong[N];
        var doge = new ulong[N];
        for (int i = 0; i < N; i++) { oracle[i] = (ulong)rnd.Next(0, 1000); doge[i] = (ulong)rnd.Next(0, 1000); }

        var r = MultiDimRevenueCalculator.Compute(0, stream, oracle, doge, 218);

        long total = 0;
        foreach (var rev in r.Revenue)
        {
            Assert.True(rev >= 0 && rev <= IPC, $"revenue {rev} out of [0,{IPC}]");
            total += rev;
        }
        Assert.True(total <= 1_000_000_000_000L, $"total {total} exceeds issuance rate");
        Assert.True(total > 0);
    }

    [Fact]
    public void Compute_MonotonicInDoge()
    {
        int totalTicks = W + 100;
        // Uniform observations so txFactor is identical across computors.
        var stream = MakeStream(totalTicks, _ => new Dictionary<int, ushort> { [TRANSFER] = 10 });
        var oracle = Constant(N, 500);

        var dogeLow = Constant(N, 500);
        var dogeHigh = Constant(N, 500);
        dogeHigh[3] = 1000; // bump one computor

        var rLow = MultiDimRevenueCalculator.Compute(0, stream, oracle, dogeLow, 218);
        var rHigh = MultiDimRevenueCalculator.Compute(0, stream, oracle, dogeHigh, 218);

        Assert.True(rHigh.Revenue[3] >= rLow.Revenue[3], "more DOGE must not reduce revenue");
    }

    [Fact]
    public void Compute_TxScore_MatchesBruteForceWindow()
    {
        // Non-uniform stream so deficits are non-trivial; verify the streamed circular window
        // reproduces an independent O(T·W·dims) centered-window reference exactly.
        int totalTicks = W + 137;
        long initialTick = 5;
        var stream = MakeStream(totalTicks, t =>
        {
            var d = new Dictionary<int, ushort>();
            d[t % 7] = (ushort)(t % 13);    // source-computor dims 0..6, varying
            d[676] = (ushort)(t % 5);       // contract 0
            d[TRANSFER] = (ushort)(t % 17); // transfer
            return d;
        });

        var actual = MultiDimRevenueCalculator.Compute(
            initialTick, stream, Zero(N), Zero(N), 218).TxScore;
        var expected = BruteForceTxScore(initialTick, stream);

        for (int i = 0; i < N; i++)
            Assert.Equal(expected[i], actual[i]);
    }

    // ---------------- Helpers ----------------

    private static List<Dictionary<int, ushort>> MakeStream(int totalTicks, Func<int, Dictionary<int, ushort>> gen)
    {
        var list = new List<Dictionary<int, ushort>>(totalTicks);
        for (int t = 0; t < totalTicks; t++) list.Add(gen(t));
        return list;
    }

    // Independent reference: for each center tick, sum the circular ±H window over all dims,
    // then apply the asymmetric-L2 deficit (m = 1) using the same integer sqrt.
    private static ulong[] BruteForceTxScore(long initialTick, List<Dictionary<int, ushort>> stream)
    {
        int T = stream.Count;
        int dimCount = MultiDimRevenueCalculator.REVENUE_TX_DIM;
        var txScore = new ulong[N];
        var ws = new ulong[dimCount];

        for (int c = 0; c < T; c++)
        {
            Array.Clear(ws);
            for (int off = -MultiDimRevenueCalculator.H; off <= MultiDimRevenueCalculator.H; off++)
            {
                int idx = (int)((((long)c + off) % T + T) % T);
                foreach (var kv in stream[idx]) ws[kv.Key] += kv.Value;
            }

            ulong sumDef = 0, sumCap = 0;
            var obsC = stream[c];
            for (int d = 0; d < dimCount; d++)
            {
                ulong w = ws[d];
                if (w == 0) continue;
                ulong wo = (ulong)W * (obsC.TryGetValue(d, out var v) ? v : (ushort)0);
                ulong deficit = wo >= w ? 0UL : w - wo;
                sumDef += deficit * deficit;
                sumCap += w * w;
            }
            ulong tickScore = sumCap == 0
                ? S
                : S - (S * MultiDimRevenueCalculator.ISqrt(sumDef)) / MultiDimRevenueCalculator.ISqrt(sumCap);
            int leader = (int)(((initialTick + c) % N + N) % N);
            txScore[leader] += tickScore;
        }
        return txScore;
    }
}
