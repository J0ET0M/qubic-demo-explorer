using Qubic.Bob;
using Qubic.Bob.Models;

namespace QubicExplorer.Indexer.Tests;

/// <summary>
/// Live integration tests against Bob node.
/// These tests require network access to https://bob.qubic.li.
/// </summary>
public class LiveBobFetchTests
{
    private const string BobUrl = "https://bob02.qubic.li";
    private const uint TestTick = 46310070;

    private async Task<BobWebSocketClient> ConnectAsync()
    {
        var options = new BobWebSocketOptions
        {
            Nodes = [BobUrl],
            ReconnectDelay = TimeSpan.FromSeconds(2),
            MaxReconnectDelay = TimeSpan.FromSeconds(10)
        };
        var client = new BobWebSocketClient(options);
        await client.ConnectAsync();
        return client;
    }

    [Fact]
    public async Task GetTickByNumber_ReturnsTick()
    {
        using var bob = await ConnectAsync();

        var tick = await bob.GetTickByNumberAsync(TestTick);

        Assert.NotNull(tick);
        Assert.Equal(TestTick, tick.TickNumber);
        Assert.True(tick.Epoch > 0, "Epoch should be > 0");
        Assert.True(tick.Timestamp > 0, "Timestamp should be > 0");
    }

    [Fact]
    public async Task GetTickLogRanges_ReturnsRange()
    {
        using var bob = await ConnectAsync();

        var ranges = await bob.GetTickLogRangesAsync([TestTick]);

        Assert.NotNull(ranges);
        Assert.NotEmpty(ranges);

        var range = ranges.First(r => r.Tick == TestTick);
        Assert.NotNull(range.FromLogId);
        Assert.True(range.Length > 0, $"Expected logs for tick {TestTick}, got length={range.Length}");
    }

    [Fact]
    public async Task GetLogsByIdRange_ReturnsLogs()
    {
        using var bob = await ConnectAsync();

        // First get the log range
        var ranges = await bob.GetTickLogRangesAsync([TestTick]);
        var range = ranges.First(r => r.Tick == TestTick);
        Assert.NotNull(range.FromLogId);
        Assert.True(range.Length > 0);

        // Then fetch the logs
        var tick = await bob.GetTickByNumberAsync(TestTick);
        var epoch = (uint)tick.Epoch;
        var endLogId = range.FromLogId!.Value + range.Length!.Value - 1;

        var logs = await bob.GetLogsByIdRangeAsync(epoch, range.FromLogId.Value, endLogId);

        Assert.NotNull(logs);
        Assert.NotEmpty(logs);

        // Verify logs have expected structure
        foreach (var log in logs.Where(l => l.Ok))
        {
            Assert.Equal(TestTick, log.Tick);
            Assert.True(log.LogId >= range.FromLogId.Value);
        }
    }

    [Fact]
    public async Task GetTransactionByHash_FromLogs_ReturnsTransaction()
    {
        using var bob = await ConnectAsync();

        // Get log range → logs → txHash
        var ranges = await bob.GetTickLogRangesAsync([TestTick]);
        var range = ranges.First(r => r.Tick == TestTick);
        var tick = await bob.GetTickByNumberAsync(TestTick);
        var endLogId = range.FromLogId!.Value + range.Length!.Value - 1;

        var logs = await bob.GetLogsByIdRangeAsync((uint)tick.Epoch, range.FromLogId.Value, endLogId);
        var txHash = logs.Where(l => l.Ok && !string.IsNullOrEmpty(l.TxHash)).Select(l => l.TxHash!).FirstOrDefault();

        Assert.NotNull(txHash);

        // Fetch transaction
        var tx = await bob.GetTransactionByHashAsync(txHash);

        Assert.NotNull(tx);
        Assert.Equal(txHash, tx.TransactionHash);
        Assert.False(string.IsNullOrEmpty(tx.SourceAddress), "SourceAddress should not be empty");
        Assert.False(string.IsNullOrEmpty(tx.DestAddress), "DestAddress should not be empty");
        Assert.Equal(TestTick, tx.Tick);
    }

    [Fact]
    public async Task GetTransactionReceipt_ReturnsStatus()
    {
        using var bob = await ConnectAsync();

        // Get a txHash from logs
        var ranges = await bob.GetTickLogRangesAsync([TestTick]);
        var range = ranges.First(r => r.Tick == TestTick);
        var tick = await bob.GetTickByNumberAsync(TestTick);
        var endLogId = range.FromLogId!.Value + range.Length!.Value - 1;

        var logs = await bob.GetLogsByIdRangeAsync((uint)tick.Epoch, range.FromLogId.Value, endLogId);
        var txHash = logs.Where(l => l.Ok && !string.IsNullOrEmpty(l.TxHash)).Select(l => l.TxHash!).FirstOrDefault();

        Assert.NotNull(txHash);

        // Fetch receipt
        var receipt = await bob.GetTransactionReceiptAsync(txHash);

        Assert.NotNull(receipt);
        Assert.Equal(txHash, receipt.TransactionHash);
        Assert.Equal(TestTick, receipt.Tick);
        // Status should deserialize without error (was crashing before the fix)
        _ = receipt.Status; // just access it — no crash = success
    }

    [Fact]
    public async Task FullRefetchFlow_AssemblesCompleteTickData()
    {
        using var bob = await ConnectAsync();

        // 1. Tick metadata
        var tickResp = await bob.GetTickByNumberAsync(TestTick);
        Assert.NotNull(tickResp);
        var epoch = (uint)tickResp.Epoch;

        // 2. Log ranges
        var ranges = await bob.GetTickLogRangesAsync([TestTick]);
        var range = ranges.First(r => r.Tick == TestTick);
        Assert.NotNull(range.FromLogId);
        Assert.True(range.Length > 0);

        // 3. Fetch logs
        var endLogId = range.FromLogId!.Value + range.Length!.Value - 1;
        var logs = await bob.GetLogsByIdRangeAsync(epoch, range.FromLogId.Value, endLogId);
        var okLogs = logs.Where(l => l.Ok).ToList();
        Assert.NotEmpty(okLogs);

        // 4. Extract txHashes
        var txHashes = okLogs
            .Where(l => !string.IsNullOrEmpty(l.TxHash))
            .Select(l => l.TxHash!)
            .Distinct()
            .ToList();

        // 5+6. Fetch transactions + receipts
        foreach (var txHash in txHashes)
        {
            var tx = await bob.GetTransactionByHashAsync(txHash);
            Assert.NotNull(tx);
            Assert.Equal(txHash, tx.TransactionHash);

            var receipt = await bob.GetTransactionReceiptAsync(txHash);
            Assert.NotNull(receipt);
            _ = receipt.Status; // no crash

            // Verify logIdFrom/logIdLength can be derived
            var txLogs = okLogs.Where(l => l.TxHash == txHash).OrderBy(l => l.LogId).ToList();
            Assert.NotEmpty(txLogs);
        }

        // Summary
        Assert.True(txHashes.Count > 0, $"Tick {TestTick} should have transactions");
        Assert.True(okLogs.Count > 0, $"Tick {TestTick} should have logs");
    }
}
