using ClickHouse.Client.ADO;
using Microsoft.Extensions.Options;
using Qubic.Bob;
using Qubic.Core;
using QubicExplorer.Analytics.Configuration;
using QubicExplorer.Shared.Configuration;
using QubicExplorer.Shared.Services;

namespace QubicExplorer.Analytics.Services;

/// <summary>
/// Periodically snapshots the live QU balance of every known smart contract.
/// Balance = the reserve that funds the contract's execution-fee deductions.
/// Persisted to <c>contract_reserve_history</c> with a 31-day TTL.
///
/// Cadence: <see cref="AnalyticsOptions.ContractReserveSnapshotIntervalMinutes"/>
/// (default 10 minutes). Toggle via <see cref="AnalyticsOptions.EnableContractReserveSnapshots"/>.
/// </summary>
public class ContractReserveSnapshotService : BackgroundService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly IOptions<AnalyticsOptions> _options;
    private readonly IOptions<ClickHouseOptions> _chOptions;
    private readonly ILogger<ContractReserveSnapshotService> _logger;

    public ContractReserveSnapshotService(
        IServiceProvider serviceProvider,
        IOptions<AnalyticsOptions> options,
        IOptions<ClickHouseOptions> chOptions,
        ILogger<ContractReserveSnapshotService> logger)
    {
        _serviceProvider = serviceProvider;
        _options = options;
        _chOptions = chOptions;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_options.Value.EnableContractReserveSnapshots)
        {
            _logger.LogInformation("ContractReserveSnapshotService disabled via config");
            return;
        }

        var interval = TimeSpan.FromMinutes(
            Math.Max(1, _options.Value.ContractReserveSnapshotIntervalMinutes));
        _logger.LogInformation(
            "ContractReserveSnapshotService starting (interval: {Interval}min)",
            interval.TotalMinutes);

        // Initial delay so the rest of the host can warm up.
        await Task.Delay(TimeSpan.FromSeconds(30), stoppingToken);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await SnapshotAsync(stoppingToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _logger.LogError(ex, "Error in ContractReserveSnapshotService cycle");
            }

            try { await Task.Delay(interval, stoppingToken); }
            catch (OperationCanceledException) { break; }
        }
    }

    private async Task SnapshotAsync(CancellationToken ct)
    {
        using var scope = _serviceProvider.CreateScope();
        var labelService = scope.ServiceProvider.GetRequiredService<AddressLabelService>();
        var bobClient = scope.ServiceProvider.GetRequiredService<BobWebSocketClient>();

        await labelService.EnsureFreshDataAsync();

        var contracts = labelService.GetAllAddresses("smartcontract");
        if (contracts.Count == 0)
        {
            _logger.LogDebug("No smart contract addresses found, skipping snapshot");
            return;
        }

        // Query the contract execution-fee reserve via QUtil.QueryFeeReserve (function id 8).
        // Input  = uint32 contractIndex (4 LE bytes)
        // Output = sint64 reserveAmount (8 LE bytes)
        // Reserve is stored in contract 0's contractFeeReserves[] array — this is NOT
        // the same as the contract's QU balance (which would be GetBalanceAsync).
        var rows = new List<(int idx, string addr, long reserve)>();
        foreach (var c in contracts)
        {
            if (ct.IsCancellationRequested) return;
            if (c.ContractIndex == null) continue;

            try
            {
                var input = new byte[4];
                BitConverter.TryWriteBytes(input, (uint)c.ContractIndex.Value);
                var inputHex = Convert.ToHexString(input);

                var resultHex = await bobClient.QuerySmartContractAsync(
                    QubicContracts.Qutil, 8 /* QueryFeeReserve */, inputHex, ct);

                var resultBytes = Convert.FromHexString(resultHex);
                if (resultBytes.Length < 8)
                {
                    _logger.LogDebug("QueryFeeReserve returned {Bytes} bytes for {Addr}, expected 8",
                        resultBytes.Length, c.Address);
                    continue;
                }
                var reserve = BitConverter.ToInt64(resultBytes, 0);
                rows.Add((c.ContractIndex.Value, c.Address, reserve));
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Could not query fee reserve for {Addr} (contract {Idx})",
                    c.Address, c.ContractIndex);
            }
        }

        if (rows.Count == 0)
        {
            _logger.LogDebug("No reserve values fetched this cycle");
            return;
        }

        await PersistAsync(rows, ct);
        _logger.LogInformation("Persisted {Count} contract fee-reserve snapshots", rows.Count);
    }

    private async Task PersistAsync(List<(int idx, string addr, long reserve)> rows, CancellationToken ct)
    {
        await using var connection = new ClickHouseConnection(_chOptions.Value.ConnectionString);
        await connection.OpenAsync(ct);

        // Reserve is sint64 in core; cap negative reserves at 0 (shouldn't happen
        // but defensive — the column is UInt128).
        var values = string.Join(",", rows.Select(r =>
            $"(now64(3),{r.idx},'{r.addr}',{Math.Max(0L, r.reserve)})"));

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = $@"
            INSERT INTO contract_reserve_history (timestamp, contract_index, address, balance)
            VALUES {values}";
        await cmd.ExecuteNonQueryAsync(ct);
    }
}
