namespace QubicExplorer.Shared.Services;

/// <summary>
/// Oracle revenue factor — port of qubic core's V2 ComputeRevFactor (revenue.h).
/// Same algorithm as li.qubic.data/RevenueCalc/RevenueV2Calculator.ComputeRevFactor in qli-manager.
///
/// Given each computor's oracle revenue points (the count of queries where their
/// commit landed in-quorum), returns a factor in [0..S=1024] used as input to the
/// V2 revenue formula:
///
///     revenue = IPC × M × (S² + B·E) / (S·(S+B)·S)
///       where M = (17·txFactor + 3·oracleFactor) / 20
///
/// Algorithm:
///   - 0 if score is 0
///   - S if score is at-or-above the rank-451 score (sorted DESC across all 676 computors)
///   - S × score / quorumScore otherwise
///   - If quorumScore is 0 (i.e. fewer than 451 non-zero entries), every non-zero
///     score gets S — matches the core fallback.
/// </summary>
public static class OracleRevenueFactor
{
    public const int N = 676;
    public const ulong S = 1024;

    /// <summary>2/3 quorum rank, ceil. (676*2+2)/3 = 451 — matches qubic core.</summary>
    public const int QuorumRank = (N * 2 + 2) / 3;

    /// <summary>Compute the rank-451 score from a 676-element score array.</summary>
    public static ulong ComputeQuorumScore(ulong[] scores)
    {
        if (scores.Length != N) throw new ArgumentException($"scores must have length {N}", nameof(scores));
        var sorted = (ulong[])scores.Clone();
        Array.Sort(sorted);
        Array.Reverse(sorted);
        return sorted[QuorumRank - 1];
    }

    /// <summary>Compute factor for a single computor given the global quorum score.</summary>
    public static ulong ComputeFactor(ulong score, ulong quorumScore)
    {
        if (score == 0) return 0;
        if (quorumScore == 0 || score >= quorumScore) return S;
        return S * score / quorumScore;
    }

    /// <summary>Compute factors for all 676 computors. scores must be length 676 (pad missing with 0).</summary>
    public static ulong[] ComputeFactors(ulong[] scores)
    {
        var quorumScore = ComputeQuorumScore(scores);
        var factors = new ulong[N];
        for (int i = 0; i < N; i++) factors[i] = ComputeFactor(scores[i], quorumScore);
        return factors;
    }
}
