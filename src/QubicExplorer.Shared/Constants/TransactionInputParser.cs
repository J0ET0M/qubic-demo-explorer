using System.Buffers.Binary;
using System.Text;
using System.Text.Json.Serialization;
using Qubic.Core;
using Qubic.Crypto;

namespace QubicExplorer.Shared.Constants;

/// <summary>
/// Parses raw hex-encoded transaction inputData into structured objects
/// based on the inputType. InputData from Bob API is a hex string of the
/// binary payload (excluding the signature suffix).
/// </summary>
public static class TransactionInputParser
{
    // -------------------------------------------------------------------------
    // Local constants for input types that are not (yet) in Qubic.Core NuGet.
    // When these appear in a future Qubic.Core release, remove them here and
    // reference the NuGet constants instead. See docs/updating-transaction-decoders.md.
    // -------------------------------------------------------------------------

    /// <summary>DOGE / custom-mining shares (packed 10-bit × 676 + dataLock). C++: mining/mining.h DOGE_MINING_SHARE_COUNTER_INPUT_TYPE.</summary>
    public const ushort DogeMiningShare = 11;
    /// <summary>Ant-colony bpp9000 mining solution. C++: mining/mining.h ANT_COLONY_MINING_SOLUTION_INPUT_TYPE.</summary>
    public const ushort AntColonyMiningSolution = 12;
    /// <summary>Off-chain contract auth signatures. C++: oc_core/oc_transactions.h.</summary>
    public const ushort OcAuthSignature = 13;

    private static readonly QubicCrypt Crypt = new();

    /// <summary>
    /// True when the input type is a known core-protocol tx (destination = burn
    /// OR — in the case of mining solutions — a computor identity). Used by
    /// the API's decode gate so mining txs get decoded regardless of destination.
    /// Contract-scoped procedure input_types are NOT covered here (they overlap
    /// numerically with core types — dispatch must gate on to_address separately).
    /// </summary>
    public static bool IsCoreProtocolType(ushort inputType) =>
        CoreTransactionInputTypes.IsKnownType(inputType)   // 1..10
        || inputType == DogeMiningShare
        || inputType == AntColonyMiningSolution
        || inputType == OcAuthSignature;

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

        // Minimum-size guard. NuGet's GetMinInputSize returns 0 for unknown types
        // so we fall back to a local table for the input types added post-1.279.
        var minSize = CoreTransactionInputTypes.GetMinInputSize(inputType);
        if (minSize == 0)
        {
            minSize = inputType switch
            {
                DogeMiningShare => 880,          // 848 packed + 32 dataLock
                AntColonyMiningSolution => 48,   // 4+4+4+4+32
                OcAuthSignature => 4,            // 2 itemCount + 2 pad (items follow)
                _ => 0,
            };
        }
        if (data.Length < minSize) return null;

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
            DogeMiningShare => ParseDogeMiningShare(data),
            AntColonyMiningSolution => ParseAntColonyMiningSolution(data),
            OcAuthSignature => ParseOcAuthSignature(data),
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
    // Type 2: Mining Solution (72 bytes)
    //   0..31  miningSeed (m256i)
    //   32..63 nonce (m256i)  — nonce[0]=algoType, nonce[1]=L, nonce[2]=K (bpp9000)
    //   64..67 score (uint)
    //   68..71 reserved (uint)
    // Source: mining/mining.h
    // =========================================================================
    private static ParsedInputData ParseMiningSolution(byte[] data)
    {
        var miningSeed = ToHexString(data, 0, 32);
        var nonce = ToHexString(data, 32, 32);
        var algoTypeByte = data[32];   // nonce[0]
        var lParam = data[33];         // nonce[1] — meaningful for bpp9000 only
        var kParam = data[34];         // nonce[2] — meaningful for bpp9000 only
        uint? score = null;
        if (data.Length >= 72)
        {
            score = BinaryPrimitives.ReadUInt32LittleEndian(data.AsSpan(64, 4));
        }
        return new MiningSolutionInputData(
            MiningSeed: miningSeed,
            Nonce: nonce,
            AlgoType: algoTypeByte,
            AlgoTypeName: AlgoTypeName(algoTypeByte),
            LParam: lParam,
            KParam: kParam,
            Score: score);
    }

    /// <summary>
    /// Mining algorithm codes carried in MiningSolution.nonce[0].
    /// Mirrors qubic core mining/score_common.h AlgoType enum.
    /// </summary>
    private static string AlgoTypeName(byte algoType) => algoType switch
    {
        0 => "Classic",
        1 => "Bpp9000",
        _ => $"Unknown({algoType})",
    };

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

        // Reveal carries no interface index; infer it from the reply size
        var interfaceName = replyDataSize switch
        {
            440 => "EvmLogRead",
            288 => "QubicLogRead",
            _ => null
        };

        // Null when the bytes do not fit the inferred interface
        var parsedFields = replyDataSize switch
        {
            440 => ParseEvmLogReadReply(data.AsSpan(8)),
            288 => ParseQubicLogReadReply(data.AsSpan(8)),
            _ => null
        };

        return new OracleReplyRevealInputData(
            QueryId: queryId,
            ReplyDataHex: replyData,
            ReplyDataSize: replyDataSize,
            OracleInterfaceName: parsedFields != null ? interfaceName : null,
            ParsedReplyFields: parsedFields);
    }

    // EvmLogRead reply: code (8B) + address (32B) + topicCount (8B) + topics (4×32B) + dataLen (8B) + data (256B)
    private static List<OracleQueryField>? ParseEvmLogReadReply(ReadOnlySpan<byte> reply)
    {
        var code = BinaryPrimitives.ReadUInt64LittleEndian(reply);
        var codeName = code switch
        {
            0 => "SUCCESS",
            1 => "BAD_QUERY",
            2 => "CHAIN_UNSUPPORTED",
            3 => "TX_NOT_FOUND",
            4 => "TX_NOT_FINALIZED",
            5 => "LOG_INDEX_OUT_OF_RANGE",
            6 => "LOG_DATA_TOO_LARGE",
            _ => null
        };
        var topicCount = BinaryPrimitives.ReadUInt64LittleEndian(reply.Slice(40, 8));
        var dataLen = BinaryPrimitives.ReadUInt64LittleEndian(reply.Slice(176, 8));
        if (codeName == null || topicCount > 4 || dataLen > 256 || reply.Slice(8, 12).ContainsAnyExcept((byte)0))
            return null;

        var fields = new List<OracleQueryField> { new("Result", $"{codeName} ({code})", "text") };
        if (code != 0) return fields;

        fields.Add(new OracleQueryField("Address", ToPrefixedHex(reply.Slice(20, 20)), "hex"));
        for (var i = 0; i < (int)topicCount; i++)
            fields.Add(new OracleQueryField($"Topic {i}", ToPrefixedHex(reply.Slice(48 + i * 32, 32)), "hex"));
        fields.Add(new OracleQueryField("Data Length", dataLen.ToString(), "uint64"));
        fields.Add(new OracleQueryField("Data", ToPrefixedHex(reply.Slice(184, (int)dataLen)), "hex"));

        return fields;
    }

    // QubicLogRead reply: code (8B) + contractIndex (8B) + logType (8B) + dataLen (8B) + data (256B)
    private static List<OracleQueryField>? ParseQubicLogReadReply(ReadOnlySpan<byte> reply)
    {
        var code = BinaryPrimitives.ReadUInt64LittleEndian(reply);
        var codeName = code switch
        {
            0 => "SUCCESS",
            1 => "BAD_QUERY",
            2 => "TX_NOT_FOUND",
            3 => "TX_NOT_EXECUTED",
            4 => "TICK_MISMATCH",
            5 => "LOG_NOT_FOUND",
            6 => "LOG_DATA_TOO_LARGE",
            _ => null
        };
        var dataLen = BinaryPrimitives.ReadUInt64LittleEndian(reply.Slice(24, 8));
        if (codeName == null || dataLen > 256) return null;

        var fields = new List<OracleQueryField> { new("Result", $"{codeName} ({code})", "text") };
        if (code != 0) return fields;

        var contractIndex = BinaryPrimitives.ReadUInt64LittleEndian(reply.Slice(8, 8));
        fields.Add(new OracleQueryField("Contract Index", contractIndex.ToString(), "uint64"));

        var logType = BinaryPrimitives.ReadUInt64LittleEndian(reply.Slice(16, 8));
        var logTypeName = logType <= byte.MaxValue ? LogTypes.GetName((byte)logType) : null;
        fields.Add(new OracleQueryField("Log Type", WithName(logTypeName, logType), "text"));

        var logData = reply.Slice(32, (int)dataLen);
        fields.Add(new OracleQueryField("Data Length", dataLen.ToString(), "uint64"));
        fields.Add(new OracleQueryField("Data", ToPrefixedHex(logData), "hex"));

        // Contract logs (types 4..7) start with contractIndex u32 + contract log type u32
        if (logType is >= 4 and <= 7 && logData.Length >= 8)
            fields.Add(new OracleQueryField("Contract Log Type", BinaryPrimitives.ReadUInt32LittleEndian(logData.Slice(4, 4)).ToString(), "uint32"));

        return fields;
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
    // Type 11: DOGE / custom-mining shares (880 bytes = 848 packed + 32 dataLock)
    // Same wire layout as the legacy CustomMiningShareCounter (type 8) — just
    // reassigned to a new input_type by the DOGE mining rollout.
    // Source: mining/mining.h DOGE_MINING_SHARE_COUNTER_INPUT_TYPE
    // =========================================================================
    private static ParsedInputData ParseDogeMiningShare(byte[] data)
    {
        const int packedSize = 848;
        var scores = Extract10BitValues(data.AsSpan(0, packedSize), LogTypes.NumberOfComputors);
        var dataLock = ToHexString(data, packedSize, 32);
        return new DogeMiningShareInputData(
            Scores: scores,
            DataLock: dataLock,
            TotalScore: scores.Sum(s => (long)s),
            NonZeroCount: scores.Count(s => s > 0));
    }

    // =========================================================================
    // Type 12: Ant-colony bpp9000 mining solution (48 bytes)
    //   0..3   parentTick (uint) — absolute tick of the parent solution reference
    //   4..7   parentSolutionIndexInTick (uint)
    //   8..11  anchorTick (uint) — RNG anchor
    //   12..15 claimedScore (uint) — refunded only if node re-scores same
    //   16..47 nonce (m256i)
    // Source: mining/mining.h ANT_COLONY_MINING_SOLUTION_INPUT_TYPE
    // =========================================================================
    private static ParsedInputData ParseAntColonyMiningSolution(byte[] data)
    {
        return new AntColonyMiningSolutionInputData(
            ParentTick: BinaryPrimitives.ReadUInt32LittleEndian(data.AsSpan(0, 4)),
            ParentSolutionIndexInTick: BinaryPrimitives.ReadUInt32LittleEndian(data.AsSpan(4, 4)),
            AnchorTick: BinaryPrimitives.ReadUInt32LittleEndian(data.AsSpan(8, 4)),
            ClaimedScore: BinaryPrimitives.ReadUInt32LittleEndian(data.AsSpan(12, 4)),
            Nonce: ToHexString(data, 16, 32));
    }

    // =========================================================================
    // Type 13: Off-chain contract auth signatures (4 + n × 112 bytes, n in [1..9])
    // Header:
    //   0..1  itemCount (u16)
    //   2..3  padding (u16)
    // Per item (112 bytes):
    //   0..7   invocationId (i64)
    //   8..9   interfaceIndex (u16)
    //   10..11 epoch (u16)
    //   12..15 padding (u32)
    //   16..47 paramsDigest (m256i)
    //   48..111 signature (64 bytes)
    // Source: oc_core/oc_transactions.h
    // =========================================================================
    private static ParsedInputData ParseOcAuthSignature(byte[] data)
    {
        var itemCount = BinaryPrimitives.ReadUInt16LittleEndian(data.AsSpan(0, 2));
        const int headerSize = 4;
        const int itemSize = 112;
        var items = new List<OcAuthSignatureItem>();
        // Guard: don't over-read a truncated payload. Item count in the header
        // may claim more items than the tx actually carries.
        var maxItems = Math.Min((int)itemCount, (data.Length - headerSize) / itemSize);
        for (int i = 0; i < maxItems; i++)
        {
            var off = headerSize + i * itemSize;
            items.Add(new OcAuthSignatureItem(
                InvocationId: BinaryPrimitives.ReadInt64LittleEndian(data.AsSpan(off, 8)),
                InterfaceIndex: BinaryPrimitives.ReadUInt16LittleEndian(data.AsSpan(off + 8, 2)),
                Epoch: BinaryPrimitives.ReadUInt16LittleEndian(data.AsSpan(off + 10, 2)),
                ParamsDigest: ToHexString(data, off + 16, 32),
                Signature: ToHexString(data, off + 48, 64)));
        }
        return new OcAuthSignatureInputData(ItemCount: itemCount, Items: items);
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
            3 => "EvmLogRead",
            4 => "QubicLogRead",
            _ => null
        };

        List<OracleQueryField>? parsedFields = null;
        try
        {
            parsedFields = oracleInterfaceIndex switch
            {
                0 => ParsePriceQuery(data.AsSpan(8)),
                1 => ParseMockQuery(data.AsSpan(8)),
                3 => ParseEvmLogReadQuery(data.AsSpan(8)),
                4 => ParseQubicLogReadQuery(data.AsSpan(8)),
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

    // EvmLogRead query: chainId (8B uint64) + txHash (32B) + logIndex (8B uint64)
    private static List<OracleQueryField> ParseEvmLogReadQuery(ReadOnlySpan<byte> queryData)
    {
        if (queryData.Length != 48) return [];

        var chainId = BinaryPrimitives.ReadUInt64LittleEndian(queryData.Slice(0, 8));
        var chainName = chainId switch
        {
            1 => "Ethereum",
            10 => "Optimism",
            56 => "BSC",
            137 => "Polygon",
            250 => "Fantom",
            8453 => "Base",
            43114 => "Avalanche",
            42161 => "Arbitrum",
            11155111 => "Sepolia",
            _ => null
        };
        var logIndex = BinaryPrimitives.ReadUInt64LittleEndian(queryData.Slice(40, 8));

        return
        [
            new OracleQueryField("Chain", WithName(chainName, chainId), "text"),
            new OracleQueryField("Tx Hash", ToPrefixedHex(queryData.Slice(8, 32)), "hex"),
            new OracleQueryField("Log Index", logIndex.ToString(), "uint64")
        ];
    }

    // QubicLogRead query: tick (8B uint64) + txHash (32B digest) + logId (8B uint64)
    private static List<OracleQueryField> ParseQubicLogReadQuery(ReadOnlySpan<byte> queryData)
    {
        if (queryData.Length != 48) return [];

        var tick = BinaryPrimitives.ReadUInt64LittleEndian(queryData.Slice(0, 8));
        var txId = Crypt.GetHumanReadableBytes(queryData.Slice(8, 32).ToArray());
        var logId = BinaryPrimitives.ReadUInt64LittleEndian(queryData.Slice(40, 8));

        return
        [
            new OracleQueryField("Tick", tick.ToString(), "tick"),
            new OracleQueryField("Tx Hash", txId, "txHash"),
            new OracleQueryField("Log Id", logId.ToString(), "uint64")
        ];
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
    /// Matches the C++ extract10Bit() big-endian bit packing: every 4 values occupy 5 bytes,
    /// with bits packed from MSB to LSB within each byte.
    /// </summary>
    private static ushort[] Extract10BitValues(ReadOnlySpan<byte> data, int count)
    {
        var values = new ushort[count];
        for (var i = 0; i < count; i++)
        {
            // C++ layout: byte0 = data[idx + (idx >> 2)], byte1 = data[idx + (idx >> 2) + 1]
            var byteIndex = i + (i >> 2);
            uint byte0 = data[byteIndex];
            uint byte1 = data[byteIndex + 1];
            var lastBit0 = 8 - (i & 3) * 2;
            var firstBit1 = 10 - lastBit0;
            values[i] = (ushort)(((byte0 & ((1u << lastBit0) - 1)) << firstBit1)
                               | (byte1 >> (8 - firstBit1)));
        }
        return values;
    }

    private static string WithName(string? name, ulong value) =>
        name != null ? $"{name} ({value})" : value.ToString();

    private static string ToPrefixedHex(ReadOnlySpan<byte> data) =>
        "0x" + Convert.ToHexString(data).ToLowerInvariant();

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
[JsonDerivedType(typeof(DogeMiningShareInputData), "DOGE_MINING_SHARE")]
[JsonDerivedType(typeof(AntColonyMiningSolutionInputData), "ANT_COLONY_MINING_SOLUTION")]
[JsonDerivedType(typeof(OcAuthSignatureInputData), "OC_AUTH_SIGNATURE")]
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
    string Nonce,
    // Post-Bpp9000 (epoch 224+) mining solutions carry the algorithm code in
    // nonce[0]. LParam/KParam are only meaningful when AlgoTypeName == "Bpp9000".
    byte AlgoType,
    string AlgoTypeName,
    byte LParam,
    byte KParam,
    // Present when the tx carries the full 72-byte payload (score at offset 64).
    // Older classic mining solutions may not include it — null-safe.
    uint? Score
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
    int ReplyDataSize,
    string? OracleInterfaceName,
    List<OracleQueryField>? ParsedReplyFields
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

/// <summary>Type 11 — DOGE / custom-mining shares (same layout as legacy type 8).</summary>
public record DogeMiningShareInputData(
    ushort[] Scores,
    string DataLock,
    long TotalScore,
    int NonZeroCount
) : ParsedInputData
{
    public override string TypeName => "DOGE_MINING_SHARE";
}

/// <summary>Type 12 — Ant-colony bpp9000 mining solution.</summary>
public record AntColonyMiningSolutionInputData(
    uint ParentTick,
    uint ParentSolutionIndexInTick,
    uint AnchorTick,
    uint ClaimedScore,
    string Nonce
) : ParsedInputData
{
    public override string TypeName => "ANT_COLONY_MINING_SOLUTION";
}

public record OcAuthSignatureItem(
    long InvocationId,
    ushort InterfaceIndex,
    ushort Epoch,
    string ParamsDigest,
    string Signature
);

/// <summary>Type 13 — off-chain contract authorization signatures.</summary>
public record OcAuthSignatureInputData(
    ushort ItemCount,
    List<OcAuthSignatureItem> Items
) : ParsedInputData
{
    public override string TypeName => "OC_AUTH_SIGNATURE";
}
