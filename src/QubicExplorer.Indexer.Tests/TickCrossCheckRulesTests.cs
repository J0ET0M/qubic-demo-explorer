using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using QubicExplorer.Indexer.Configuration;
using QubicExplorer.Indexer.Services;
using QubicExplorer.Shared.Configuration;
using static QubicExplorer.Indexer.Services.TickCrossCheckService;

namespace QubicExplorer.Indexer.Tests;

public class TickCrossCheckRulesTests
{
    private readonly TickCrossCheckService _service;

    public TickCrossCheckRulesTests()
    {
        _service = new TickCrossCheckService(
            Substitute.For<ILogger<TickCrossCheckService>>(),
            Options.Create(new BobOptions()),
            Options.Create(new IndexerOptions()),
            Options.Create(new ClickHouseOptions()),
            Substitute.For<ClickHouseWriterService>(
                Substitute.For<ILogger<ClickHouseWriterService>>(),
                Options.Create(new ClickHouseOptions()),
                Options.Create(new IndexerOptions())
            )
        );
    }

    // ── Rule 1: empty + has logs + no tx → refetch ──────────────────────

    [Fact]
    public void Rule1_Empty_HasLogs_NoTx_ShouldRefetch()
    {
        var state = new TickState(Exists: true, IsEmpty: true, TxCount: 0, LogCount: 5, ExecutedTxCount: 0);
        Assert.True(_service.EvaluateRules(1000, state));
    }

    // ── Rule 2: empty + no logs + no tx → OK ────────────────────────────

    [Fact]
    public void Rule2_Empty_NoLogs_NoTx_ShouldBeOk()
    {
        var state = new TickState(Exists: true, IsEmpty: true, TxCount: 0, LogCount: 0, ExecutedTxCount: 0);
        Assert.False(_service.EvaluateRules(1000, state));
    }

    // ── Rule 3: empty + no logs + tx with executed → refetch ────────────

    [Fact]
    public void Rule3_Empty_NoLogs_HasTx_ShouldRefetch()
    {
        var state = new TickState(Exists: true, IsEmpty: true, TxCount: 3, LogCount: 0, ExecutedTxCount: 2);
        Assert.True(_service.EvaluateRules(1000, state));
    }

    [Fact]
    public void Rule3_Empty_NoLogs_HasTx_NoneExecuted_ShouldRefetch()
    {
        var state = new TickState(Exists: true, IsEmpty: true, TxCount: 1, LogCount: 0, ExecutedTxCount: 0);
        Assert.True(_service.EvaluateRules(1000, state));
    }

    // ── Rule 4: non-empty + no logs → refetch ───────────────────────────

    [Fact]
    public void Rule4_NotEmpty_NoLogs_NoTx_ShouldRefetch()
    {
        var state = new TickState(Exists: true, IsEmpty: false, TxCount: 0, LogCount: 0, ExecutedTxCount: 0);
        Assert.True(_service.EvaluateRules(1000, state));
    }

    [Fact]
    public void Rule4_NotEmpty_NoLogs_HasTx_ShouldRefetch()
    {
        var state = new TickState(Exists: true, IsEmpty: false, TxCount: 5, LogCount: 0, ExecutedTxCount: 3);
        Assert.True(_service.EvaluateRules(1000, state));
    }

    // ── Rule 5: non-empty + has logs + no tx → refetch ──────────────────

    [Fact]
    public void Rule5_NotEmpty_HasLogs_NoTx_ShouldRefetch()
    {
        var state = new TickState(Exists: true, IsEmpty: false, TxCount: 0, LogCount: 10, ExecutedTxCount: 0);
        Assert.True(_service.EvaluateRules(1000, state));
    }

    // ── Rule 6: non-empty + has logs + has tx → OK ──────────────────────

    [Fact]
    public void Rule6_NotEmpty_HasLogs_HasTx_ShouldBeOk()
    {
        var state = new TickState(Exists: true, IsEmpty: false, TxCount: 5, LogCount: 10, ExecutedTxCount: 3);
        Assert.False(_service.EvaluateRules(1000, state));
    }

    // ── Edge cases ──────────────────────────────────────────────────────

    [Fact]
    public void TickNotInDatabase_ShouldRefetch()
    {
        var state = new TickState(Exists: false, IsEmpty: true, TxCount: 0, LogCount: 0, ExecutedTxCount: 0);
        Assert.True(_service.EvaluateRules(1000, state));
    }

    [Fact]
    public void Empty_HasLogs_HasTx_ShouldRefetch()
    {
        // Shouldn't be marked empty if it has both
        var state = new TickState(Exists: true, IsEmpty: true, TxCount: 2, LogCount: 3, ExecutedTxCount: 1);
        Assert.True(_service.EvaluateRules(1000, state));
    }
}
