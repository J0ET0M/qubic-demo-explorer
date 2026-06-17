namespace QubicExplorer.Shared.Services;

/// <summary>
/// Maps a transaction's (from, to) identities to a multi-dimension revenue observation dimension,
/// for a single epoch. Mirrors qubic core's per-tick categorization (qubic.cpp:3167-3192):
///
///   dest == zero/null  AND source is a computor  → dim = sourceComputorIndex  [0,676)
///   dest == zero/null  AND source not a computor → dim = TRANSFER_DIM (705)
///   dest is a contract                           → dim = 676 + contractIndex  [676,705)
///   else (user→user / other)                     → dim = TRANSFER_DIM (705)
///
/// Contract detection is done purely from the Qubic identity string, with no crypto dependency:
/// a contract public key is [contractIndex as 8-byte LE] followed by 24 zero bytes, so in the
/// 60-char identity the last 24 pubkey bytes encode to 42 trailing 'A's (chars [14,56)) and the
/// first 14 chars are the little-endian base-26 encoding of contractIndex. (chars [56,60) are the
/// per-key checksum.) This is exactly isPublicKeyOfContract: low8 &lt; contractCount AND high 24
/// bytes zero. The zero/null address itself also matches this pattern (index 0), so callers MUST
/// check the null address first — which DimFor does, matching core's isZero()-first order.
/// </summary>
public sealed class RevenueDimensionMap
{
    public const int N = MultiDimRevenueCalculator.N;                       // 676
    public const int CONTRACT_DIMS = MultiDimRevenueCalculator.CONTRACT_DIMS; // 29
    public const int TRANSFER_DIM = MultiDimRevenueCalculator.TRANSFER_DIM;   // 705

    public const string NullAddress = AddressLabelService.BurnAddress;

    private readonly Dictionary<string, int> _computorIdx;

    public RevenueDimensionMap(IReadOnlyList<string> computorAddresses)
    {
        if (computorAddresses.Count != N)
            throw new ArgumentException($"expected {N} computor addresses, got {computorAddresses.Count}");
        _computorIdx = new Dictionary<string, int>(N, StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < N; i++) _computorIdx[computorAddresses[i]] = i;
    }

    /// <summary>Returns the observation dimension for one transaction's (from, to) identities.</summary>
    public int DimFor(string fromAddress, string toAddress)
    {
        // dest == zero/null (checked first, matching core's isZero() order).
        if (string.Equals(toAddress, NullAddress, StringComparison.OrdinalIgnoreCase))
            return _computorIdx.TryGetValue(fromAddress, out var src) ? src : TRANSFER_DIM;

        // dest is a contract → [676, 705)
        if (TryGetContractIndex(toAddress, out int contractIndex))
            return N + contractIndex;

        // user→user / other
        return TRANSFER_DIM;
    }

    /// <summary>
    /// True iff <paramref name="identity"/> is a contract address with index &lt; CONTRACT_DIMS,
    /// i.e. chars [14,56) are all 'A' (high 24 pubkey bytes zero) and the index decoded from
    /// chars [0,14) is &lt; CONTRACT_DIMS. Does not depend on the checksum.
    /// </summary>
    public static bool TryGetContractIndex(string identity, out int contractIndex)
    {
        contractIndex = -1;
        if (string.IsNullOrEmpty(identity) || identity.Length != 60)
            return false;

        // High 24 pubkey bytes must be zero → identity chars [14,56) all 'A'.
        for (int i = 14; i < 56; i++)
            if (identity[i] != 'A') return false;

        // Decode the low uint64 (= contract index) from chars [0,14): little-endian base-26.
        ulong value = 0;
        ulong place = 1;
        for (int i = 0; i < 14; i++)
        {
            char ch = identity[i];
            if (ch < 'A' || ch > 'Z') return false;
            value += (ulong)(ch - 'A') * place;
            place *= 26;
        }

        if (value >= (ulong)CONTRACT_DIMS) return false;
        contractIndex = (int)value;
        return true;
    }
}
