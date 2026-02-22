using System.Buffers.Binary;
using System.Text;
using System.Text.Json.Serialization;
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

        // Strip 0x prefix if present
        var hex = inputData.StartsWith("0x", StringComparison.OrdinalIgnoreCase)
            ? inputData[2..]
            : inputData;

        byte[] data;
        try
        {
            data = Convert.FromHexString(hex);
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

        var interfaceName = oracleInterfaceIndex switch
        {
            0 => "Price",
            1 => "Mock",
            _ => null
        };

        List<OracleQueryField>? parsedFields = null;
        try
        {
            parsedFields = oracleInterfaceIndex switch
            {
                0 => ParsePriceQuery(data.AsSpan(8)),
                1 => ParseMockQuery(data.AsSpan(8)),
                _ => null
            };
        }
        catch
        {
            // Silently fall back to raw hex if parsing fails
        }

        return new OracleUserQueryInputData(
            OracleInterfaceIndex: oracleInterfaceIndex,
            OracleInterfaceName: interfaceName,
            TimeoutMilliseconds: timeoutMilliseconds,
            QueryDataHex: queryData,
            QueryDataSize: queryDataSize,
            ParsedQueryFields: parsedFields);
    }

    // Price oracle query: oracle (32B id/ascii) + timestamp (8B DateAndTime) + currency1 (32B id/ascii) + currency2 (32B id/ascii)
    private static List<OracleQueryField> ParsePriceQuery(ReadOnlySpan<byte> queryData)
    {
        if (queryData.Length < 104) return [];

        var fields = new List<OracleQueryField>();

        var oracle = ReadIdAsAscii(queryData.Slice(0, 32));
        fields.Add(new OracleQueryField("Oracle", oracle, "text"));

        var timestampValue = BinaryPrimitives.ReadUInt64LittleEndian(queryData.Slice(32, 8));
        fields.Add(new OracleQueryField("Timestamp", FormatDateAndTime(timestampValue), "DateAndTime"));

        var currency1 = ReadIdAsAscii(queryData.Slice(40, 32));
        fields.Add(new OracleQueryField("Currency 1", currency1, "text"));

        var currency2 = ReadIdAsAscii(queryData.Slice(72, 32));
        fields.Add(new OracleQueryField("Currency 2", currency2, "text"));

        return fields;
    }

    // Mock oracle query: value (8B uint64)
    private static List<OracleQueryField> ParseMockQuery(ReadOnlySpan<byte> queryData)
    {
        if (queryData.Length < 8) return [];

        var value = BinaryPrimitives.ReadUInt64LittleEndian(queryData.Slice(0, 8));
        return [new OracleQueryField("Value", value.ToString(), "uint64")];
    }

    /// <summary>
    /// Reads a 32-byte Qubic id as a null-terminated ASCII string.
    /// Used for oracle fields like oracle name, currency names, etc. where the
    /// id type stores text data (Ch namespace characters are plain ASCII values).
    /// </summary>
    private static string ReadIdAsAscii(ReadOnlySpan<byte> data)
    {
        var end = data.IndexOf((byte)0);
        var textBytes = end >= 0 ? data.Slice(0, end) : data;
        return Encoding.ASCII.GetString(textBytes);
    }

    /// <summary>
    /// Decodes a Qubic DateAndTime packed uint64 into a human-readable string.
    /// Bit layout: year(18) | month(4) | day(5) | hour(5) | minute(6) | second(6) | millisec(10) | microsec(10)
    /// </summary>
    private static string FormatDateAndTime(ulong value)
    {
        var year = (int)(value >> 46) & 0x3FFFF;
        var month = (int)(value >> 42) & 0xF;
        var day = (int)(value >> 37) & 0x1F;
        var hour = (int)(value >> 32) & 0x1F;
        var minute = (int)(value >> 26) & 0x3F;
        var second = (int)(value >> 20) & 0x3F;
        var millisec = (int)(value >> 10) & 0x3FF;

        if (value == 0) return "0 (unset)";
        return $"{year:D4}-{month:D2}-{day:D2} {hour:D2}:{minute:D2}:{second:D2}.{millisec:D3}";
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
[JsonPolymorphic(TypeDiscriminatorPropertyName = "$type")]
[JsonDerivedType(typeof(VoteCounterInputData), "VOTE_COUNTER")]
[JsonDerivedType(typeof(MiningSolutionInputData), "MINING_SOLUTION")]
[JsonDerivedType(typeof(FileHeaderInputData), "FILE_HEADER")]
[JsonDerivedType(typeof(FileFragmentInputData), "FILE_FRAGMENT")]
[JsonDerivedType(typeof(FileTrailerInputData), "FILE_TRAILER")]
[JsonDerivedType(typeof(OracleReplyCommitInputData), "ORACLE_REPLY_COMMIT")]
[JsonDerivedType(typeof(OracleReplyRevealInputData), "ORACLE_REPLY_REVEAL")]
[JsonDerivedType(typeof(CustomMiningShareCounterInputData), "CUSTOM_MINING_SHARE_COUNTER")]
[JsonDerivedType(typeof(ExecutionFeeReportInputData), "EXECUTION_FEE_REPORT")]
[JsonDerivedType(typeof(OracleUserQueryInputData), "ORACLE_USER_QUERY")]
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

public record OracleQueryField(
    string Name,
    string Value,
    string Type
);

public record OracleUserQueryInputData(
    uint OracleInterfaceIndex,
    string? OracleInterfaceName,
    uint TimeoutMilliseconds,
    string? QueryDataHex,
    int QueryDataSize,
    List<OracleQueryField>? ParsedQueryFields
) : ParsedInputData
{
    public override string TypeName => "ORACLE_USER_QUERY";
}
