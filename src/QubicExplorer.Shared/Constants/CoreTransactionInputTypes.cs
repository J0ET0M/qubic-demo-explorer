namespace QubicExplorer.Shared.Constants;

/// <summary>
/// Input type constants for special transactions sent to the burn/null address.
/// These match the C++ core definitions in qubic.cpp processTransaction().
/// </summary>
public static class CoreTransactionInputTypes
{
    /// <summary>Vote counter data from tick leader (vote_counter.h)</summary>
    public const ushort VoteCounter = 1;

    /// <summary>Mining solution submission (mining.h)</summary>
    public const ushort MiningSolution = 2;

    /// <summary>File header for distributed file storage (files.h) — stub</summary>
    public const ushort FileHeader = 3;

    /// <summary>File fragment for distributed file storage (files.h) — stub</summary>
    public const ushort FileFragment = 4;

    /// <summary>File trailer for distributed file storage (files.h) — stub</summary>
    public const ushort FileTrailer = 5;

    /// <summary>Oracle reply commit from oracle provider (oracle_transactions.h)</summary>
    public const ushort OracleReplyCommit = 6;

    /// <summary>Oracle reply reveal from oracle provider (oracle_transactions.h)</summary>
    public const ushort OracleReplyReveal = 7;

    /// <summary>Custom mining share counter from computor (mining.h)</summary>
    public const ushort CustomMiningShareCounter = 8;

    /// <summary>Execution fee report from computor (execution_fees.h)</summary>
    public const ushort ExecutionFeeReport = 9;

    /// <summary>Oracle user query (oracle_transactions.h)</summary>
    public const ushort OracleUserQuery = 10;

    /// <summary>Minimum input data sizes (bytes) for each type, excluding signature.</summary>
    public static int GetMinInputSize(ushort inputType) => inputType switch
    {
        VoteCounter => 880,           // 848 packed votes + 32 dataLock
        MiningSolution => 64,          // 32 miningSeed + 32 nonce
        FileHeader => 24,              // 8 fileSize + 8 numberOfFragments + 8 fileFormat
        FileFragment => 40,            // 8 fragmentIndex + 32 prevDigest
        FileTrailer => 56,             // 8 fileSize + 8 numberOfFragments + 8 fileFormat + 32 lastDigest
        OracleReplyCommit => 72,       // 8 queryId + 32 replyDigest + 32 proof (per item)
        OracleReplyReveal => 8,        // 8 queryId
        CustomMiningShareCounter => 880, // 848 packed scores + 32 dataLock
        ExecutionFeeReport => 8,       // 4 phaseNumber + 4 numEntries
        OracleUserQuery => 8,          // 4 oracleInterfaceIndex + 4 timeoutMs
        _ => 0
    };

    public static string GetName(ushort inputType) => inputType switch
    {
        VoteCounter => "VOTE_COUNTER",
        MiningSolution => "MINING_SOLUTION",
        FileHeader => "FILE_HEADER",
        FileFragment => "FILE_FRAGMENT",
        FileTrailer => "FILE_TRAILER",
        OracleReplyCommit => "ORACLE_REPLY_COMMIT",
        OracleReplyReveal => "ORACLE_REPLY_REVEAL",
        CustomMiningShareCounter => "CUSTOM_MINING_SHARE_COUNTER",
        ExecutionFeeReport => "EXECUTION_FEE_REPORT",
        OracleUserQuery => "ORACLE_USER_QUERY",
        _ => $"UNKNOWN_{inputType}"
    };

    public static string GetDisplayName(ushort inputType) => inputType switch
    {
        VoteCounter => "Vote Counter",
        MiningSolution => "Mining Solution",
        FileHeader => "File Header",
        FileFragment => "File Fragment",
        FileTrailer => "File Trailer",
        OracleReplyCommit => "Oracle Reply Commit",
        OracleReplyReveal => "Oracle Reply Reveal",
        CustomMiningShareCounter => "Custom Mining Share Counter",
        ExecutionFeeReport => "Execution Fee Report",
        OracleUserQuery => "Oracle User Query",
        _ => $"Unknown ({inputType})"
    };

    /// <summary>
    /// Whether this input type is a known special burn-address transaction.
    /// </summary>
    public static bool IsKnownType(ushort inputType) =>
        inputType >= VoteCounter && inputType <= OracleUserQuery;

    /// <summary>
    /// Whether this input type is a protocol-level transaction (max priority in mempool).
    /// </summary>
    public static bool IsProtocolTransaction(ushort inputType) => inputType switch
    {
        VoteCounter or CustomMiningShareCounter or ExecutionFeeReport => true,
        _ => false
    };
}
