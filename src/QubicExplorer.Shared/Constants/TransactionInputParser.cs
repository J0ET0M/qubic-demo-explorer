using System.Buffers.Binary;
using System.Text;
using Qubic.Core;

namespace QubicExplorer.Shared.Constants;

/// <summary>
/// Parses raw hex-encoded transaction inputData into structured objects
/// based on the inputType. InputData from Bob API is a hex string of the
/// binary payload (excluding the signature suffix).
/// </summary>
public static class TransactionInputParser
{
    /// <summary>
    /// Parse hex-encoded inputData into a typed result based on inputType.
    /// Returns null if inputData is empty/invalid or inputType is unknown.
    /// </summary>
    public static ParsedInputData? Parse(ushort inputType, string? inputData)
    {
        if (string.IsNullOrEmpty(inputData))
            return null;

        byte[] data;
        try
        {
            data = Convert.FromHexString(inputData);
        }
        catch
        {
            return null;
        }

        if (data.Length < CoreTransactionInputTypes.GetMinInputSize(inputType))
            return null;

        return inputType switch
        {
            CoreTransactionInputTypes.VoteCounter => ParseVoteCounter(data),
            CoreTransactionInputTypes.MiningSolution => ParseMiningSolution(data),
            CoreTransactionInputTypes.FileHeader => ParseFileHeader(data),
            CoreTransactionInputTypes.FileFragment => ParseFileFragment(data),
            CoreTransactionInputTypes.FileTrailer => ParseFileTrailer(data),
            CoreTransactionInputTypes.OracleReplyCommit => ParseOracleReplyCommit(data),
            CoreTransactionInputTypes.OracleReplyReveal => ParseOracleReplyReveal(data),
            CoreTransactionInputTypes.CustomMiningShareCounter => ParseCustomMiningShareCounter(data),
            CoreTransactionInputTypes.ExecutionFeeReport => ParseExecutionFeeReport(data),
            CoreTransactionInputTypes.OracleUserQuery => ParseOracleUserQuery(data),
            _ => null
        };
    }

    // =========================================================================
    // Type 1: Vote Counter (880 bytes = 848 packed + 32 dataLock)
    // =========================================================================
    private static ParsedInputData ParseVoteCounter(byte[] data)
    {
        const int packedSize = 848;
        var votes = Extract10BitValues(data.AsSpan(0, packedSize), LogTypes.NumberOfComputors);
        var dataLock = ToHexString(data, packedSize, 32);

        return new VoteCounterInputData(
            Votes: votes,
            DataLock: dataLock,
            TotalVotes: votes.Sum(v => (long)v),
            NonZeroCount: votes.Count(v => v > 0));
    }

    // =========================================================================
    // Type 2: Mining Solution (64+ bytes)
    // =========================================================================
    private static ParsedInputData ParseMiningSolution(byte[] data)
    {
        var miningSeed = ToHexString(data, 0, 32);
        var nonce = ToHexString(data, 32, 32);

        return new MiningSolutionInputData(
            MiningSeed: miningSeed,
            Nonce: nonce);
    }

    // =========================================================================
    // Type 3: File Header (24 bytes)
    // =========================================================================
    private static ParsedInputData ParseFileHeader(byte[] data)
    {
        var fileSize = BinaryPrimitives.ReadUInt64LittleEndian(data.AsSpan(0));
        var numberOfFragments = BinaryPrimitives.ReadUInt64LittleEndian(data.AsSpan(8));
        var fileFormat = Encoding.ASCII.GetString(data, 16, 8).TrimEnd('\0');

        return new FileHeaderInputData(
            FileSize: fileSize,
            NumberOfFragments: numberOfFragments,
            FileFormat: fileFormat);
    }

    // =========================================================================
    // Type 4: File Fragment (40+ bytes)
    // =========================================================================
    private static ParsedInputData ParseFileFragment(byte[] data)
    {
        var fragmentIndex = BinaryPrimitives.ReadUInt64LittleEndian(data.AsSpan(0));
        var prevDigest = ToHexString(data, 8, 32);
        var payloadSize = data.Length - 40;

        return new FileFragmentInputData(
            FragmentIndex: fragmentIndex,
            PrevFileFragmentTransactionDigest: prevDigest,
            PayloadSize: payloadSize);
    }

    // =========================================================================
    // Type 5: File Trailer (56 bytes)
    // =========================================================================
    private static ParsedInputData ParseFileTrailer(byte[] data)
    {
        var fileSize = BinaryPrimitives.ReadUInt64LittleEndian(data.AsSpan(0));
        var numberOfFragments = BinaryPrimitives.ReadUInt64LittleEndian(data.AsSpan(8));
        var fileFormat = Encoding.ASCII.GetString(data, 16, 8).TrimEnd('\0');
        var lastDigest = ToHexString(data, 24, 32);

        return new FileTrailerInputData(
            FileSize: fileSize,
            NumberOfFragments: numberOfFragments,
            FileFormat: fileFormat,
            LastFileFragmentTransactionDigest: lastDigest);
    }

    // =========================================================================
    // Type 6: Oracle Reply Commit (n × 72 bytes)
    // =========================================================================
    private static ParsedInputData ParseOracleReplyCommit(byte[] data)
    {
        const int itemSize = 72; // 8 + 32 + 32
        var itemCount = data.Length / itemSize;
        var items = new List<OracleReplyCommitItem>(itemCount);

        for (var i = 0; i < itemCount; i++)
        {
            var offset = i * itemSize;
            items.Add(new OracleReplyCommitItem(
                QueryId: BinaryPrimitives.ReadUInt64LittleEndian(data.AsSpan(offset)),
                ReplyDigest: ToHexString(data, offset + 8, 32),
                ReplyKnowledgeProof: ToHexString(data, offset + 40, 32)));
        }

        return new OracleReplyCommitInputData(Items: items);
    }

    // =========================================================================
    // Type 7: Oracle Reply Reveal (8+ bytes)
    // =========================================================================
    private static ParsedInputData ParseOracleReplyReveal(byte[] data)
    {
        var queryId = BinaryPrimitives.ReadUInt64LittleEndian(data.AsSpan(0));
        var replyDataSize = data.Length - 8;
        var replyData = replyDataSize > 0 ? ToHexString(data, 8, replyDataSize) : null;

        return new OracleReplyRevealInputData(
            QueryId: queryId,
            ReplyDataHex: replyData,
            ReplyDataSize: replyDataSize);
    }

    // =========================================================================
    // Type 8: Custom Mining Share Counter (880 bytes)
    // =========================================================================
    private static ParsedInputData ParseCustomMiningShareCounter(byte[] data)
    {
        const int packedSize = 848;
        var scores = Extract10BitValues(data.AsSpan(0, packedSize), LogTypes.NumberOfComputors);
        var dataLock = ToHexString(data, packedSize, 32);

        return new CustomMiningShareCounterInputData(
            Scores: scores,
            DataLock: dataLock,
            TotalScore: scores.Sum(s => (long)s),
            NonZeroCount: scores.Count(s => s > 0));
    }

    // =========================================================================
    // Type 9: Execution Fee Report (8+ bytes, variable)
    // =========================================================================
    private static ParsedInputData ParseExecutionFeeReport(byte[] data)
    {
        var phaseNumber = BinaryPrimitives.ReadUInt32LittleEndian(data.AsSpan(0));
        var numEntries = BinaryPrimitives.ReadUInt32LittleEndian(data.AsSpan(4));

        var entries = new List<ExecutionFeeEntry>();
        if (numEntries > 0 && data.Length >= 8 + numEntries * 4)
        {
            // contractIndices: numEntries × u32
            var indicesOffset = 8;
            // alignment padding: 4 bytes if numEntries is odd
            var alignmentPadding = (numEntries % 2 == 1) ? 4 : 0;
            var feesOffset = indicesOffset + (int)(numEntries * 4) + alignmentPadding;

            for (var i = 0; i < numEntries; i++)
            {
                var contractIndex = BinaryPrimitives.ReadUInt32LittleEndian(
                    data.AsSpan(indicesOffset + i * 4));

                ulong fee = 0;
                var feePos = feesOffset + i * 8;
                if (feePos + 8 <= data.Length)
                {
                    fee = BinaryPrimitives.ReadUInt64LittleEndian(data.AsSpan(feePos));
                }

                entries.Add(new ExecutionFeeEntry(
                    ContractIndex: contractIndex,
                    ExecutionFee: fee));
            }
        }

        // dataLock is at the end (32 bytes before signature)
        string? dataLock = null;
        var expectedPayloadEnd = 8 + (int)(numEntries * 4) +
                                  ((numEntries % 2 == 1) ? 4 : 0) +
                                  (int)(numEntries * 8);
        if (data.Length >= expectedPayloadEnd + 32)
        {
            dataLock = ToHexString(data, expectedPayloadEnd, 32);
        }

        return new ExecutionFeeReportInputData(
            PhaseNumber: phaseNumber,
            NumEntries: numEntries,
            Entries: entries,
            DataLock: dataLock);
    }

    // =========================================================================
    // Type 10: Oracle User Query (8+ bytes)
    // =========================================================================
    private static ParsedInputData ParseOracleUserQuery(byte[] data)
    {
        var oracleInterfaceIndex = BinaryPrimitives.ReadUInt32LittleEndian(data.AsSpan(0));
        var timeoutMilliseconds = BinaryPrimitives.ReadUInt32LittleEndian(data.AsSpan(4));
        var queryDataSize = data.Length - 8;
        var queryData = queryDataSize > 0 ? ToHexString(data, 8, queryDataSize) : null;

        return new OracleUserQueryInputData(
            OracleInterfaceIndex: oracleInterfaceIndex,
            TimeoutMilliseconds: timeoutMilliseconds,
            QueryDataHex: queryData,
            QueryDataSize: queryDataSize);
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    /// <summary>
    /// Extract 10-bit packed values from a byte buffer (used by vote counter and mining shares).
    /// Each value occupies 10 bits, packed in little-endian bit order.
    /// </summary>
    private static ushort[] Extract10BitValues(ReadOnlySpan<byte> data, int count)
    {
        var values = new ushort[count];
        for (var i = 0; i < count; i++)
        {
            var bitOffset = i * 10;
            var byteOffset = bitOffset / 8;
            var bitShift = bitOffset % 8;

            // Read 2-3 bytes spanning this 10-bit value
            uint raw = data[byteOffset];
            if (byteOffset + 1 < data.Length)
                raw |= (uint)data[byteOffset + 1] << 8;
            if (byteOffset + 2 < data.Length)
                raw |= (uint)data[byteOffset + 2] << 16;

            values[i] = (ushort)((raw >> bitShift) & 0x3FF);
        }
        return values;
    }

    private static string ToHexString(byte[] data, int offset, int length)
    {
        if (offset + length > data.Length)
            length = data.Length - offset;
        return Convert.ToHexString(data, offset, length).ToLowerInvariant();
    }
}

// =========================================================================
// Parsed input data types
// =========================================================================

/// <summary>Base type for all parsed input data.</summary>
public abstract record ParsedInputData
{
    public abstract string TypeName { get; }
}

public record VoteCounterInputData(
    ushort[] Votes,
    string DataLock,
    long TotalVotes,
    int NonZeroCount
) : ParsedInputData
{
    public override string TypeName => "VOTE_COUNTER";
}

public record MiningSolutionInputData(
    string MiningSeed,
    string Nonce
) : ParsedInputData
{
    public override string TypeName => "MINING_SOLUTION";
}

public record FileHeaderInputData(
    ulong FileSize,
    ulong NumberOfFragments,
    string FileFormat
) : ParsedInputData
{
    public override string TypeName => "FILE_HEADER";
}

public record FileFragmentInputData(
    ulong FragmentIndex,
    string PrevFileFragmentTransactionDigest,
    int PayloadSize
) : ParsedInputData
{
    public override string TypeName => "FILE_FRAGMENT";
}

public record FileTrailerInputData(
    ulong FileSize,
    ulong NumberOfFragments,
    string FileFormat,
    string LastFileFragmentTransactionDigest
) : ParsedInputData
{
    public override string TypeName => "FILE_TRAILER";
}

public record OracleReplyCommitItem(
    ulong QueryId,
    string ReplyDigest,
    string ReplyKnowledgeProof
);

public record OracleReplyCommitInputData(
    List<OracleReplyCommitItem> Items
) : ParsedInputData
{
    public override string TypeName => "ORACLE_REPLY_COMMIT";
}

public record OracleReplyRevealInputData(
    ulong QueryId,
    string? ReplyDataHex,
    int ReplyDataSize
) : ParsedInputData
{
    public override string TypeName => "ORACLE_REPLY_REVEAL";
}

public record CustomMiningShareCounterInputData(
    ushort[] Scores,
    string DataLock,
    long TotalScore,
    int NonZeroCount
) : ParsedInputData
{
    public override string TypeName => "CUSTOM_MINING_SHARE_COUNTER";
}

public record ExecutionFeeEntry(
    uint ContractIndex,
    ulong ExecutionFee
);

public record ExecutionFeeReportInputData(
    uint PhaseNumber,
    uint NumEntries,
    List<ExecutionFeeEntry> Entries,
    string? DataLock
) : ParsedInputData
{
    public override string TypeName => "EXECUTION_FEE_REPORT";
}

public record OracleUserQueryInputData(
    uint OracleInterfaceIndex,
    uint TimeoutMilliseconds,
    string? QueryDataHex,
    int QueryDataSize
) : ParsedInputData
{
    public override string TypeName => "ORACLE_USER_QUERY";
}
