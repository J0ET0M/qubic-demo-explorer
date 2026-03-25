using System.Text.Json;
using Qubic.Bob.Models;

namespace QubicExplorer.Indexer.Tests;

public class BobResponseDeserializationTests
{
    // ── BobTransactionResponse.Amount ────────────────────────────────────

    [Fact]
    public void TransactionResponse_AmountAsNumber_Deserializes()
    {
        var json = """{"txHash":"abc","sourceId":"SRC","destId":"DST","amount":1000,"tick":100,"inputType":0,"inputSize":0}""";
        var resp = JsonSerializer.Deserialize<BobTransactionResponse>(json);

        Assert.NotNull(resp);
        Assert.Equal(1000L, resp.AmountValue);
    }

    [Fact]
    public void TransactionResponse_AmountAsString_Deserializes()
    {
        var json = """{"txHash":"abc","sourceId":"SRC","destId":"DST","amount":"5000","tick":100,"inputType":0,"inputSize":0}""";
        var resp = JsonSerializer.Deserialize<BobTransactionResponse>(json);

        Assert.NotNull(resp);
        Assert.Equal(5000L, resp.AmountValue);
    }

    [Fact]
    public void TransactionResponse_LargeAmount_Deserializes()
    {
        var json = """{"txHash":"abc","sourceId":"SRC","destId":"DST","amount":1000000000000,"tick":100,"inputType":0,"inputSize":0}""";
        var resp = JsonSerializer.Deserialize<BobTransactionResponse>(json);

        Assert.NotNull(resp);
        Assert.Equal(1_000_000_000_000L, resp.AmountValue);
    }

    [Fact]
    public void TransactionResponse_InputData_Deserializes()
    {
        var json = """{"txHash":"abc","sourceId":"SRC","destId":"DST","amount":0,"tick":100,"inputType":1,"inputSize":880,"inputData":"deadbeef"}""";
        var resp = JsonSerializer.Deserialize<BobTransactionResponse>(json);

        Assert.NotNull(resp);
        Assert.Equal("deadbeef", resp.InputData);
        Assert.Equal(1, resp.InputType);
    }

    [Fact]
    public void TransactionResponse_NoInputData_DeserializesAsNull()
    {
        var json = """{"txHash":"abc","sourceId":"SRC","destId":"DST","amount":0,"tick":100,"inputType":0,"inputSize":0}""";
        var resp = JsonSerializer.Deserialize<BobTransactionResponse>(json);

        Assert.NotNull(resp);
        Assert.Null(resp.InputData);
    }

    // ── TransactionReceiptResponse.Status ────────────────────────────────

    [Fact]
    public void Receipt_StatusAsBool_True()
    {
        var json = """{"txHash":"abc","status":true,"tick":100}""";
        var resp = JsonSerializer.Deserialize<TransactionReceiptResponse>(json);

        Assert.NotNull(resp);
        Assert.True(resp.Status);
    }

    [Fact]
    public void Receipt_StatusAsBool_False()
    {
        var json = """{"txHash":"abc","status":false,"tick":100}""";
        var resp = JsonSerializer.Deserialize<TransactionReceiptResponse>(json);

        Assert.NotNull(resp);
        Assert.False(resp.Status);
    }

    [Fact]
    public void Receipt_StatusAsString_True()
    {
        var json = """{"txHash":"abc","status":"true","tick":100}""";
        var resp = JsonSerializer.Deserialize<TransactionReceiptResponse>(json);

        Assert.NotNull(resp);
        Assert.True(resp.Status);
    }

    [Fact]
    public void Receipt_StatusAsString_False()
    {
        var json = """{"txHash":"abc","status":"false","tick":100}""";
        var resp = JsonSerializer.Deserialize<TransactionReceiptResponse>(json);

        Assert.NotNull(resp);
        Assert.False(resp.Status);
    }

    // ── BobTickResponse ─────────────────────────────────────────────────

    [Fact]
    public void TickResponse_Deserializes()
    {
        var json = """{"tickNumber":46310070,"epoch":150,"timestamp":1711234567,"tickLeader":"LEADER","signature":"SIG"}""";
        var resp = JsonSerializer.Deserialize<BobTickResponse>(json);

        Assert.NotNull(resp);
        Assert.Equal(46310070u, resp.TickNumber);
        Assert.Equal(150, resp.Epoch);
        Assert.Equal(1711234567L, resp.Timestamp);
    }

    // ── TickLogRange ────────────────────────────────────────────────────

    [Fact]
    public void TickLogRange_WithData_Deserializes()
    {
        var json = """{"tick":46310070,"fromLogId":1000,"length":5}""";
        var resp = JsonSerializer.Deserialize<TickLogRange>(json);

        Assert.NotNull(resp);
        Assert.Equal(46310070u, resp.Tick);
        Assert.Equal(1000L, resp.FromLogId);
        Assert.Equal(5, resp.Length);
    }

    [Fact]
    public void TickLogRange_NoData_Deserializes()
    {
        var json = """{"tick":46310070,"fromLogId":null,"length":null}""";
        var resp = JsonSerializer.Deserialize<TickLogRange>(json);

        Assert.NotNull(resp);
        Assert.Null(resp.FromLogId);
        Assert.Null(resp.Length);
    }
}
