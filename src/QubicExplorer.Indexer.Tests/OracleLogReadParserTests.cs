using System.Buffers.Binary;
using QubicExplorer.Shared.Constants;

namespace QubicExplorer.Indexer.Tests;

public class OracleLogReadParserTests
{
    private static readonly byte[] Hash32 = Enumerable.Range(1, 32).Select(i => (byte)i).ToArray();
    private const string Hash32Hex = "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20";

    private static void PutU64(byte[] buf, int offset, ulong value) =>
        BinaryPrimitives.WriteUInt64LittleEndian(buf.AsSpan(offset), value);

    private static byte[] Query(uint interfaceIndex, ulong first, ulong last)
    {
        var data = new byte[8 + 48];
        BinaryPrimitives.WriteUInt32LittleEndian(data, interfaceIndex);
        BinaryPrimitives.WriteUInt32LittleEndian(data.AsSpan(4), 60000);
        PutU64(data, 8, first);
        Hash32.CopyTo(data, 16);
        PutU64(data, 48, last);
        return data;
    }

    private static byte[] Reveal(int replySize)
    {
        var data = new byte[8 + replySize];
        PutU64(data, 0, 77);
        return data;
    }

    private static T Parse<T>(ushort inputType, byte[] data) where T : ParsedInputData =>
        Assert.IsType<T>(TransactionInputParser.Parse(inputType, Convert.ToHexString(data)));

    private static (string, string, string)[] Tuples(List<OracleQueryField>? fields) =>
        fields!.Select(f => (f.Name, f.Value, f.Type)).ToArray();

    [Fact]
    public void EvmLogReadQuery_IsDecoded()
    {
        var parsed = Parse<OracleUserQueryInputData>(10, Query(3, 1, 5));

        Assert.Equal("EvmLogRead", parsed.OracleInterfaceName);
        Assert.Equal(
            [("Chain", "Ethereum (1)", "text"), ("Tx Hash", "0x" + Hash32Hex, "hex"), ("Log Index", "5", "uint64")],
            Tuples(parsed.ParsedQueryFields));
    }

    [Fact]
    public void EvmLogReadQuery_UnknownChain_ShowsNumber()
    {
        var parsed = Parse<OracleUserQueryInputData>(10, Query(3, 999, 0));

        Assert.Equal(("Chain", "999", "text"), Tuples(parsed.ParsedQueryFields)[0]);
    }

    [Fact]
    public void QubicLogReadQuery_IsDecoded()
    {
        var parsed = Parse<OracleUserQueryInputData>(10, Query(4, 25000000, 9));

        Assert.Equal("QubicLogRead", parsed.OracleInterfaceName);
        Assert.Equal(
            [
                ("Tick", "25000000", "tick"),
                ("Tx Hash", "fyxidfllaopbgahrkwvlhjhchdmajkxjosdhoqyesaldkxgzzeveqgyajvdf", "txHash"),
                ("Log Id", "9", "uint64")
            ],
            Tuples(parsed.ParsedQueryFields));
    }

    // Real devnet query: tick 75330606, logId 200017
    [Fact]
    public void QubicLogReadQuery_RealVector_TxIdMatches()
    {
        var query = Convert.FromHexString(
            "2e747d040000000008c08496cdea811e880446c03b79e78ece3be955674c405a92ad6c25382a162b510d030000000000");
        var data = new byte[8 + 48];
        BinaryPrimitives.WriteUInt32LittleEndian(data, 4);
        query.CopyTo(data, 8);

        var parsed = Parse<OracleUserQueryInputData>(10, data);

        Assert.Equal(
            [("Tick", "75330606", "tick"),
             ("Tx Hash", "crvtehdiuhyaxaactvgimiihoxdeoaawbwejhzvdqcwkmjifhpbfxngbhhoa", "txHash"),
             ("Log Id", "200017", "uint64")],
            Tuples(parsed.ParsedQueryFields));
    }

    [Fact]
    public void EvmLogReadReply_Success_IsDecoded()
    {
        var data = Reveal(440);
        for (var i = 0; i < 20; i++) data[8 + 20 + i] = (byte)(0xA0 + i);
        PutU64(data, 8 + 40, 3);
        Hash32.CopyTo(data, 8 + 48);
        for (var t = 1; t < 3; t++) Array.Fill(data, (byte)(0x11 * (t + 1)), 8 + 48 + t * 32, 32);
        PutU64(data, 8 + 176, 64);
        Array.Fill(data, (byte)0xEE, 8 + 184, 64);

        var parsed = Parse<OracleReplyRevealInputData>(7, data);

        Assert.Equal(77ul, parsed.QueryId);
        Assert.Equal(440, parsed.ReplyDataSize);
        Assert.Equal("EvmLogRead", parsed.OracleInterfaceName);
        Assert.Equal(
            [
                ("Result", "SUCCESS (0)", "text"),
                ("Address", "0xa0a1a2a3a4a5a6a7a8a9aaabacadaeafb0b1b2b3", "hex"),
                ("Topic 0", "0x" + Hash32Hex, "hex"),
                ("Topic 1", "0x" + new string('2', 64), "hex"),
                ("Topic 2", "0x" + new string('3', 64), "hex"),
                ("Data Length", "64", "uint64"),
                ("Data", "0x" + string.Concat(Enumerable.Repeat("ee", 64)), "hex")
            ],
            Tuples(parsed.ParsedReplyFields));
    }

    [Fact]
    public void EvmLogReadReply_Failure_HasOnlyResult()
    {
        var data = Reveal(440);
        PutU64(data, 8, 4);

        var parsed = Parse<OracleReplyRevealInputData>(7, data);

        Assert.Equal("EvmLogRead", parsed.OracleInterfaceName);
        Assert.Equal([("Result", "TX_NOT_FINALIZED (4)", "text")], Tuples(parsed.ParsedReplyFields));
    }

    [Fact]
    public void QubicLogReadReply_ContractLog_IsDecoded()
    {
        var data = Reveal(288);
        PutU64(data, 8 + 8, 17);
        PutU64(data, 8 + 16, 6);
        PutU64(data, 8 + 24, 12);
        BinaryPrimitives.WriteUInt32LittleEndian(data.AsSpan(8 + 32), 17);
        BinaryPrimitives.WriteUInt32LittleEndian(data.AsSpan(8 + 36), 42);
        Array.Fill(data, (byte)0xAB, 8 + 40, 4);

        var parsed = Parse<OracleReplyRevealInputData>(7, data);

        Assert.Equal("QubicLogRead", parsed.OracleInterfaceName);
        Assert.Equal(
            [
                ("Result", "SUCCESS (0)", "text"),
                ("Contract Index", "17", "uint64"),
                ("Log Type", "CONTRACT_INFORMATION_MESSAGE (6)", "text"),
                ("Data Length", "12", "uint64"),
                ("Data", "0x110000002a000000abababab", "hex"),
                ("Contract Log Type", "42", "uint32")
            ],
            Tuples(parsed.ParsedReplyFields));
    }

    [Fact]
    public void QubicLogReadReply_Failure_HasOnlyResult()
    {
        var data = Reveal(288);
        PutU64(data, 8, 5);

        var parsed = Parse<OracleReplyRevealInputData>(7, data);

        Assert.Equal("QubicLogRead", parsed.OracleInterfaceName);
        Assert.Equal([("Result", "LOG_NOT_FOUND (5)", "text")], Tuples(parsed.ParsedReplyFields));
    }

    [Theory]
    [InlineData(0, 12)] // not a contract log
    [InlineData(6, 4)]  // too short for the contract log prefix
    public void QubicLogReadReply_WithoutContractPrefix_HasNoContractLogType(ulong logType, ulong dataLen)
    {
        var data = Reveal(288);
        PutU64(data, 8 + 16, logType);
        PutU64(data, 8 + 24, dataLen);

        var fields = Tuples(Parse<OracleReplyRevealInputData>(7, data).ParsedReplyFields);

        Assert.Equal("Data", fields[^1].Item1);
    }

    // Size matches but bytes do not fit the interface: stay raw hex
    [Theory]
    [InlineData(440, 0, 9)]    // unknown result code
    [InlineData(440, 40, 5)]   // topicCount > 4
    [InlineData(440, 176, 257)] // dataLen > 256
    [InlineData(440, 8, 1)]    // address padding not zero
    [InlineData(288, 0, 9)]
    [InlineData(288, 24, ulong.MaxValue)]
    public void RevealNotFittingInterface_IsNotDecoded(int replySize, int offset, ulong value)
    {
        var data = Reveal(replySize);
        PutU64(data, 8 + offset, value);

        var parsed = Parse<OracleReplyRevealInputData>(7, data);

        Assert.Null(parsed.OracleInterfaceName);
        Assert.Null(parsed.ParsedReplyFields);
        Assert.NotNull(parsed.ReplyDataHex);
    }

    [Fact]
    public void RevealOfOtherSize_IsNotInferred()
    {
        var parsed = Parse<OracleReplyRevealInputData>(7, Reveal(16));

        Assert.Equal(16, parsed.ReplyDataSize);
        Assert.Null(parsed.OracleInterfaceName);
        Assert.Null(parsed.ParsedReplyFields);
    }
}
