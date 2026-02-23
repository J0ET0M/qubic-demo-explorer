using System.Data;
using System.Numerics;
using System.Text;
using ClickHouse.Client.ADO;
using Microsoft.Extensions.Options;
using Qubic.Core;
using QubicExplorer.Shared.Configuration;
using QubicExplorer.Shared.Constants;
using QubicExplorer.Shared.DTOs;
using QubicExplorer.Shared.Services;

namespace QubicExplorer.Api.Services;

public class ClickHouseQueryService : IDisposable
{
    private readonly ClickHouseConnection _connection;
    private readonly ILogger<ClickHouseQueryService> _logger;
    private readonly AddressLabelService _labelService;
    private bool _disposed;

    public ClickHouseQueryService(
        IOptions<ClickHouseOptions> options,
        ILogger<ClickHouseQueryService> logger,
        AddressLabelService labelService)
    {
        _logger = logger;
        _labelService = labelService;
        _connection = new ClickHouseConnection(options.Value.ConnectionString);
        _connection.Open();
    }

    /// <summary>
    /// Safely convert ClickHouse values that may be BigInteger (from UInt128/Int128 type promotion)
    /// </summary>
    private static ulong ToUInt64(object value) =>
        value is BigInteger bi ? (ulong)bi : Convert.ToUInt64(value);

    private static decimal ToDecimal(object value) =>
        value is BigInteger bi ? (decimal)bi : Convert.ToDecimal(value);

    public async Task<PaginatedResponse<TickDto>> GetTicksAsync(int page, int limit, CancellationToken ct = default)
    {
        var offset = (page - 1) * limit;

        await using var countCmd = _connection.CreateCommand();
        countCmd.CommandText = "SELECT count() FROM ticks";
        var totalCount = Convert.ToInt64(await countCmd.ExecuteScalarAsync(ct));

        // Query with computed tx/log counts from actual data if stored counts are 0
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT
                t.tick_number,
                t.epoch,
                t.timestamp,
                if(t.tx_count > 0, t.tx_count, coalesce(tx_counts.cnt, 0)) as tx_count,
                if(t.log_count > 0, t.log_count, coalesce(log_counts.cnt, 0)) as log_count
            FROM ticks t
            LEFT JOIN (
                SELECT tick_number, toUInt32(count()) as cnt
                FROM transactions
                GROUP BY tick_number
            ) tx_counts ON t.tick_number = tx_counts.tick_number
            LEFT JOIN (
                SELECT tick_number, toUInt32(count()) as cnt
                FROM logs
                GROUP BY tick_number
            ) log_counts ON t.tick_number = log_counts.tick_number
            ORDER BY t.tick_number DESC
            LIMIT {limit} OFFSET {offset}";

        var items = new List<TickDto>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            items.Add(new TickDto(
                reader.GetFieldValue<ulong>(0),
                reader.GetFieldValue<uint>(1),
                reader.GetDateTime(2),
                Convert.ToUInt32(reader.GetValue(3)),
                Convert.ToUInt32(reader.GetValue(4))
            ));
        }

        return new PaginatedResponse<TickDto>(
            items, page, limit, totalCount, (int)Math.Ceiling((double)totalCount / limit));
    }

    public async Task<TickDetailDto?> GetTickByNumberAsync(ulong tickNumber, CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT
                t.tick_number,
                t.epoch,
                t.timestamp,
                if(t.tx_count > 0, t.tx_count, (SELECT toUInt32(count()) FROM transactions WHERE tick_number = {tickNumber})) as tx_count,
                if(t.log_count > 0, t.log_count, (SELECT toUInt32(count()) FROM logs WHERE tick_number = {tickNumber})) as log_count
            FROM ticks t
            WHERE t.tick_number = {tickNumber}";

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct))
            return null;

        var tick = new TickDto(
            reader.GetFieldValue<ulong>(0),
            reader.GetFieldValue<uint>(1),
            reader.GetDateTime(2),
            Convert.ToUInt32(reader.GetValue(3)),
            Convert.ToUInt32(reader.GetValue(4))
        );

        var transactions = await GetTransactionsByTickAsync(tickNumber, ct);

        return new TickDetailDto(
            tick.TickNumber, tick.Epoch, tick.Timestamp,
            tick.TxCount, tick.LogCount, transactions);
    }

    public async Task<List<TransactionDto>> GetTransactionsByTickAsync(ulong tickNumber, CancellationToken ct = default)
    {
        var result = await GetTransactionsByTickPagedAsync(tickNumber, 1, 1000, null, null, null, null, ct);
        return result.Items;
    }

    public async Task<PaginatedResponse<TransactionDto>> GetTransactionsByTickPagedAsync(
        ulong tickNumber, int page, int limit, string? address = null, string? direction = null,
        ulong? minAmount = null, bool? executed = null, CancellationToken ct = default)
    {
        var offset = (page - 1) * limit;
        var conditions = new List<string> { $"tick_number = {tickNumber}" };

        // Build filter conditions
        if (!string.IsNullOrEmpty(address))
        {
            var escapedAddr = address.Replace("'", "''");
            if (direction == "from")
                conditions.Add($"from_address = '{escapedAddr}'");
            else if (direction == "to")
                conditions.Add($"to_address = '{escapedAddr}'");
            else
                conditions.Add($"(from_address = '{escapedAddr}' OR to_address = '{escapedAddr}')");
        }
        if (minAmount.HasValue)
            conditions.Add($"amount >= {minAmount.Value}");
        if (executed.HasValue)
            conditions.Add($"executed = {(executed.Value ? 1 : 0)}");

        var whereClause = string.Join(" AND ", conditions);

        // Get total count
        await using var countCmd = _connection.CreateCommand();
        countCmd.CommandText = $"SELECT count() FROM transactions WHERE {whereClause}";
        var totalCount = Convert.ToInt32(await countCmd.ExecuteScalarAsync(ct));

        // Get paginated items
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT hash, tick_number, epoch, from_address, to_address, amount, input_type, executed, timestamp
            FROM transactions
            WHERE {whereClause}
            ORDER BY hash
            LIMIT {limit} OFFSET {offset}";

        var items = new List<TransactionDto>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var rowInputType = reader.GetFieldValue<ushort>(6);
            var rowToAddr = reader.GetString(4);
            var isCoreTx = string.Equals(rowToAddr, AddressLabelService.BurnAddress, StringComparison.OrdinalIgnoreCase);
            items.Add(new TransactionDto(
                reader.GetString(0),
                reader.GetFieldValue<ulong>(1),
                reader.GetFieldValue<uint>(2),
                reader.GetString(3),
                rowToAddr,
                reader.GetFieldValue<ulong>(5),
                rowInputType,
                isCoreTx && CoreTransactionInputTypes.IsKnownType(rowInputType) ? CoreTransactionInputTypes.GetDisplayName(rowInputType) : null,
                reader.GetFieldValue<byte>(7) == 1,
                reader.GetDateTime(8)
            ));
        }

        var totalPages = (int)Math.Ceiling((double)totalCount / limit);
        return new PaginatedResponse<TransactionDto>(items, page, limit, totalCount, totalPages);
    }

    public async Task<List<TransferDto>> GetLogsByTickAsync(ulong tickNumber, CancellationToken ct = default)
    {
        var result = await GetLogsByTickPagedAsync(tickNumber, 1, 1000, null, null, null, null, ct);
        return result.Items;
    }

    public async Task<PaginatedResponse<TransferDto>> GetLogsByTickPagedAsync(
        ulong tickNumber, int page, int limit, string? address = null, byte? logType = null,
        string? direction = null, ulong? minAmount = null, CancellationToken ct = default)
    {
        var offset = (page - 1) * limit;
        var conditions = new List<string> { $"tick_number = {tickNumber}" };

        // Build filter conditions
        if (!string.IsNullOrEmpty(address))
        {
            var escapedAddr = address.Replace("'", "''");
            if (direction == "in")
                conditions.Add($"dest_address = '{escapedAddr}'");
            else if (direction == "out")
                conditions.Add($"source_address = '{escapedAddr}'");
            else
                conditions.Add($"(source_address = '{escapedAddr}' OR dest_address = '{escapedAddr}')");
        }
        if (logType.HasValue)
            conditions.Add($"log_type = {logType.Value}");
        if (minAmount.HasValue)
            conditions.Add($"amount >= {minAmount.Value}");

        var whereClause = string.Join(" AND ", conditions);

        // Get total count
        await using var countCmd = _connection.CreateCommand();
        countCmd.CommandText = $"SELECT count() FROM logs WHERE {whereClause}";
        var totalCount = Convert.ToInt32(await countCmd.ExecuteScalarAsync(ct));

        // Get paginated items
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT tick_number, epoch, log_id, log_type, tx_hash, source_address,
                   dest_address, amount, asset_name, timestamp
            FROM logs
            WHERE {whereClause}
            ORDER BY log_id
            LIMIT {limit} OFFSET {offset}";

        var items = new List<TransferDto>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var logTypeValue = reader.GetFieldValue<byte>(3);
            items.Add(new TransferDto(
                reader.GetFieldValue<ulong>(0),
                reader.GetFieldValue<uint>(1),
                reader.GetFieldValue<uint>(2),
                logTypeValue,
                LogTypes.GetName(logTypeValue),
                reader.IsDBNull(4) ? null : reader.GetString(4),
                reader.IsDBNull(5) ? "" : reader.GetString(5),
                reader.IsDBNull(6) ? "" : reader.GetString(6),
                reader.GetFieldValue<ulong>(7),
                reader.IsDBNull(8) ? null : reader.GetString(8),
                reader.GetDateTime(9)
            ));
        }

        var totalPages = (int)Math.Ceiling((double)totalCount / limit);
        return new PaginatedResponse<TransferDto>(items, page, limit, totalCount, totalPages);
    }

    public async Task<PaginatedResponse<TransactionDto>> GetTransactionsAsync(
        int page, int limit, string? address = null, string? direction = null,
        ulong? minAmount = null, bool? executed = null, int? inputType = null,
        string? toAddress = null, CancellationToken ct = default)
    {
        var offset = (page - 1) * limit;
        var conditions = new List<string>();

        if (!string.IsNullOrEmpty(address))
        {
            if (direction == "from")
                conditions.Add($"from_address = '{address}'");
            else if (direction == "to")
                conditions.Add($"to_address = '{address}'");
            else
                conditions.Add($"(from_address = '{address}' OR to_address = '{address}')");
        }

        if (minAmount.HasValue)
            conditions.Add($"amount >= {minAmount.Value}");

        if (executed.HasValue)
            conditions.Add($"executed = {(executed.Value ? 1 : 0)}");

        if (inputType.HasValue)
            conditions.Add($"input_type = {inputType.Value}");

        if (!string.IsNullOrEmpty(toAddress))
            conditions.Add($"to_address = '{toAddress.Replace("'", "''")}'");

        var whereClause = conditions.Count > 0
            ? "WHERE " + string.Join(" AND ", conditions)
            : "";

        await using var countCmd = _connection.CreateCommand();
        countCmd.CommandText = $"SELECT count() FROM transactions {whereClause}";
        var totalCount = Convert.ToInt64(await countCmd.ExecuteScalarAsync(ct));

        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT hash, tick_number, epoch, from_address, to_address, amount, input_type, executed, timestamp
            FROM transactions
            {whereClause}
            ORDER BY tick_number DESC, hash
            LIMIT {limit} OFFSET {offset}";

        var items = new List<TransactionDto>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var rowInputType = reader.GetFieldValue<ushort>(6);
            var rowToAddr = reader.GetString(4);
            var isCoreTx = string.Equals(rowToAddr, AddressLabelService.BurnAddress, StringComparison.OrdinalIgnoreCase);
            items.Add(new TransactionDto(
                reader.GetString(0),
                reader.GetFieldValue<ulong>(1),
                reader.GetFieldValue<uint>(2),
                reader.GetString(3),
                rowToAddr,
                reader.GetFieldValue<ulong>(5),
                rowInputType,
                isCoreTx && CoreTransactionInputTypes.IsKnownType(rowInputType) ? CoreTransactionInputTypes.GetDisplayName(rowInputType) : null,
                reader.GetFieldValue<byte>(7) == 1,
                reader.GetDateTime(8)
            ));
        }

        return new PaginatedResponse<TransactionDto>(
            items, page, limit, totalCount, (int)Math.Ceiling((double)totalCount / limit));
    }

    public async Task<TransactionDetailDto?> GetTransactionByHashAsync(string hash, CancellationToken ct = default)
    {
        // Check if this is a special transaction (smart contract lifecycle event)
        if (SpecialTransactionTypes.IsSpecialTransaction(hash))
        {
            // Special transactions are handled separately
            return null;
        }

        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT hash, tick_number, epoch, from_address, to_address, amount,
                   input_type, input_data, executed, timestamp, log_id_from, log_id_length
            FROM transactions
            WHERE hash = '{hash}'";

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct))
            return null;

        var txHash = reader.GetString(0);
        var tickNumber = reader.GetFieldValue<ulong>(1);
        var epoch = reader.GetFieldValue<uint>(2);
        var fromAddr = reader.GetString(3);
        var toAddr = reader.GetString(4);
        var amount = reader.GetFieldValue<ulong>(5);
        var inputType = reader.GetFieldValue<ushort>(6);
        var inputData = reader.IsDBNull(7) ? null : reader.GetString(7);
        var executed = reader.GetFieldValue<byte>(8) == 1;
        var timestamp = reader.GetDateTime(9);
        var logIdFrom = reader.GetFieldValue<int>(10);
        var logIdLength = reader.GetFieldValue<ushort>(11);

        // Get associated logs
        var logs = new List<LogDto>();
        if (logIdFrom >= 0 && logIdLength > 0)
        {
            await using var logCmd = _connection.CreateCommand();
            logCmd.CommandText = $@"
                SELECT tick_number, log_id, log_type, tx_hash, source_address,
                       dest_address, amount, asset_name, timestamp
                FROM logs
                WHERE tick_number = {tickNumber}
                  AND log_id >= {logIdFrom}
                  AND log_id < {logIdFrom + logIdLength}
                ORDER BY log_id";

            await using var logReader = await logCmd.ExecuteReaderAsync(ct);
            while (await logReader.ReadAsync(ct))
            {
                var logType = logReader.GetFieldValue<byte>(2);
                logs.Add(new LogDto(
                    logReader.GetFieldValue<ulong>(0),
                    logReader.GetFieldValue<uint>(1),
                    logType,
                    LogTypes.GetName(logType),
                    logReader.IsDBNull(3) ? null : logReader.GetString(3),
                    logReader.IsDBNull(4) ? null : logReader.GetString(4),
                    logReader.IsDBNull(5) ? null : logReader.GetString(5),
                    logReader.GetFieldValue<ulong>(6),
                    logReader.IsDBNull(7) ? null : logReader.GetString(7),
                    logReader.GetDateTime(8)
                ));
            }
        }

        var isCoreTransaction = string.Equals(toAddr, AddressLabelService.BurnAddress, StringComparison.OrdinalIgnoreCase);
        var inputTypeName = isCoreTransaction && CoreTransactionInputTypes.IsKnownType(inputType)
            ? CoreTransactionInputTypes.GetDisplayName(inputType) : null;
        var parsedInput = isCoreTransaction ? TransactionInputParser.Parse(inputType, inputData) : null;

        return new TransactionDetailDto(
            txHash, tickNumber, epoch, fromAddr, toAddr, amount,
            inputType, inputTypeName, inputData, parsedInput, executed, timestamp, logs);
    }

    public async Task<SpecialTransactionDto?> GetSpecialTransactionAsync(string txHash, CancellationToken ct = default)
    {
        var parsed = SpecialTransactionTypes.ParseSpecialTransaction(txHash);
        if (parsed == null)
            return null;

        var (type, tickNumber) = parsed.Value;

        // Query logs that have this special tx hash
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT tick_number, log_id, log_type, tx_hash, source_address,
                   dest_address, amount, asset_name, timestamp
            FROM logs
            WHERE tx_hash = '{txHash}'
            ORDER BY log_id";

        var logs = new List<LogDto>();
        DateTime? timestamp = null;

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var logType = reader.GetFieldValue<byte>(2);
            var logTimestamp = reader.GetDateTime(8);
            timestamp ??= logTimestamp;

            logs.Add(new LogDto(
                reader.GetFieldValue<ulong>(0),
                reader.GetFieldValue<uint>(1),
                logType,
                LogTypes.GetName(logType),
                reader.IsDBNull(3) ? null : reader.GetString(3),
                reader.IsDBNull(4) ? null : reader.GetString(4),
                reader.IsDBNull(5) ? null : reader.GetString(5),
                reader.GetFieldValue<ulong>(6),
                reader.IsDBNull(7) ? null : reader.GetString(7),
                logTimestamp
            ));
        }

        // If no logs found, try to get tick timestamp for display
        if (!timestamp.HasValue)
        {
            await using var tickCmd = _connection.CreateCommand();
            tickCmd.CommandText = $"SELECT timestamp FROM ticks WHERE tick_number = {tickNumber} LIMIT 1";
            var tickResult = await tickCmd.ExecuteScalarAsync(ct);
            timestamp = tickResult is DateTime ts ? ts : DateTime.UtcNow;
        }

        return new SpecialTransactionDto(
            txHash,
            type,
            SpecialTransactionTypes.GetTypeName(type),
            tickNumber,
            timestamp ?? DateTime.UtcNow,
            logs
        );
    }

    public async Task<PaginatedResponse<TransferDto>> GetTransfersAsync(
        int page, int limit, string? address = null, byte? logType = null,
        string? direction = null, ulong? minAmount = null, List<byte>? logTypes = null,
        uint? epoch = null, CancellationToken ct = default)
    {
        var offset = (page - 1) * limit;
        var conditions = new List<string>();

        // Epoch filter (enables partition pruning)
        if (epoch.HasValue)
            conditions.Add($"epoch = {epoch.Value}");

        if (!string.IsNullOrEmpty(address))
        {
            if (direction == "in")
                conditions.Add($"dest_address = '{address}'");
            else if (direction == "out")
                conditions.Add($"source_address = '{address}'");
            else
                conditions.Add($"(source_address = '{address}' OR dest_address = '{address}')");
        }

        // Single log type filter (for backwards compatibility)
        if (logType.HasValue)
            conditions.Add($"log_type = {logType.Value}");

        // Multiple log types filter (e.g., logTypes=0,1,2)
        if (logTypes != null && logTypes.Count > 0)
            conditions.Add($"log_type IN ({string.Join(",", logTypes)})");

        if (minAmount.HasValue)
            conditions.Add($"amount >= {minAmount.Value}");

        var whereClause = conditions.Count > 0
            ? "WHERE " + string.Join(" AND ", conditions)
            : "";

        await using var countCmd = _connection.CreateCommand();
        countCmd.CommandText = $"SELECT count() FROM logs {whereClause}";
        var totalCount = Convert.ToInt64(await countCmd.ExecuteScalarAsync(ct));

        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT tick_number, epoch, log_id, log_type, tx_hash, source_address,
                   dest_address, amount, asset_name, timestamp
            FROM logs
            {whereClause}
            ORDER BY tick_number DESC, log_id DESC
            LIMIT {limit} OFFSET {offset}";

        var items = new List<TransferDto>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var rowLogType = reader.GetFieldValue<byte>(3);
            items.Add(new TransferDto(
                reader.GetFieldValue<ulong>(0),
                reader.GetFieldValue<uint>(1),
                reader.GetFieldValue<uint>(2),
                rowLogType,
                LogTypes.GetName(rowLogType),
                reader.IsDBNull(4) ? null : reader.GetString(4),
                reader.IsDBNull(5) ? "" : reader.GetString(5),
                reader.IsDBNull(6) ? "" : reader.GetString(6),
                reader.GetFieldValue<ulong>(7),
                reader.IsDBNull(8) ? null : reader.GetString(8),
                reader.GetDateTime(9)
            ));
        }

        return new PaginatedResponse<TransferDto>(
            items, page, limit, totalCount, (int)Math.Ceiling((double)totalCount / limit));
    }

    public async Task<AddressDto> GetAddressSummaryAsync(string address, CancellationToken ct = default)
    {
        // Get transaction counts
        await using var txCmd = _connection.CreateCommand();
        txCmd.CommandText = $@"
            SELECT count(), sum(amount)
            FROM transactions
            WHERE from_address = '{address}' OR to_address = '{address}'";

        uint txCount = 0;
        await using (var reader = await txCmd.ExecuteReaderAsync(ct))
        {
            if (await reader.ReadAsync(ct))
                txCount = Convert.ToUInt32(reader.GetFieldValue<ulong>(0));
        }

        // Get incoming amount
        await using var inCmd = _connection.CreateCommand();
        inCmd.CommandText = $@"
            SELECT COALESCE(sum(amount), 0)
            FROM logs
            WHERE dest_address = '{address}' AND log_type = 0";
        var incomingAmount = Convert.ToUInt64(await inCmd.ExecuteScalarAsync(ct));

        // Get outgoing amount
        await using var outCmd = _connection.CreateCommand();
        outCmd.CommandText = $@"
            SELECT COALESCE(sum(amount), 0)
            FROM logs
            WHERE source_address = '{address}' AND log_type = 0";
        var outgoingAmount = Convert.ToUInt64(await outCmd.ExecuteScalarAsync(ct));

        // Get transfer count
        await using var transferCmd = _connection.CreateCommand();
        transferCmd.CommandText = $@"
            SELECT count()
            FROM logs
            WHERE source_address = '{address}' OR dest_address = '{address}'";
        var transferCount = Convert.ToUInt32(await transferCmd.ExecuteScalarAsync(ct));

        return new AddressDto(
            address,
            incomingAmount > outgoingAmount ? incomingAmount - outgoingAmount : 0,
            incomingAmount,
            outgoingAmount,
            txCount,
            transferCount
        );
    }

    public async Task<NetworkStatsDto> GetNetworkStatsAsync(CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = @"
            SELECT
                (SELECT max(tick_number) FROM ticks) as latest_tick,
                (SELECT max(epoch) FROM ticks) as current_epoch,
                (SELECT count() FROM transactions) as total_txs,
                (SELECT count() FROM logs) as total_logs,
                (SELECT COALESCE(sum(amount), 0) FROM transactions) as total_volume";

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        if (await reader.ReadAsync(ct))
        {
            return new NetworkStatsDto(
                reader.IsDBNull(0) ? 0 : reader.GetFieldValue<ulong>(0),
                reader.IsDBNull(1) ? 0 : reader.GetFieldValue<uint>(1),
                reader.IsDBNull(2) ? 0 : reader.GetFieldValue<ulong>(2),
                reader.IsDBNull(3) ? 0 : reader.GetFieldValue<ulong>(3),
                reader.IsDBNull(4) ? 0 : reader.GetFieldValue<ulong>(4),
                DateTime.UtcNow
            );
        }

        return new NetworkStatsDto(0, 0, 0, 0, 0, DateTime.UtcNow);
    }

    public async Task<List<ChartDataPointDto>> GetTxVolumeChartAsync(
        string period, CancellationToken ct = default)
    {
        var days = period switch
        {
            "week" => 7,
            "month" => 30,
            _ => 1
        };

        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT date, tx_count, total_volume
            FROM daily_tx_volume
            WHERE date >= today() - {days}
            ORDER BY date";

        var items = new List<ChartDataPointDto>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            items.Add(new ChartDataPointDto(
                reader.GetDateTime(0),
                reader.GetFieldValue<ulong>(1),
                reader.GetFieldValue<ulong>(2)
            ));
        }

        return items;
    }

    public async Task<SearchResponse> SearchAsync(string query, CancellationToken ct = default)
    {
        var results = new List<SearchResultDto>();

        // Check if it's a special transaction (smart contract lifecycle event)
        if (SpecialTransactionTypes.IsSpecialTransaction(query))
        {
            var parsed = SpecialTransactionTypes.ParseSpecialTransaction(query);
            if (parsed != null)
            {
                results.Add(new SearchResultDto(
                    SearchResultType.Transaction,
                    query,
                    SpecialTransactionTypes.GetTypeName(parsed.Value.Type)));
            }
            return new SearchResponse(query, results);
        }

        // Check if it's a tick number
        if (ulong.TryParse(query, out var tickNumber))
        {
            await using var tickCmd = _connection.CreateCommand();
            tickCmd.CommandText = $"SELECT tick_number FROM ticks WHERE tick_number = {tickNumber} LIMIT 1";
            var tickResult = await tickCmd.ExecuteScalarAsync(ct);
            if (tickResult != null)
            {
                results.Add(new SearchResultDto(SearchResultType.Tick, tickNumber.ToString(), $"Tick {tickNumber}"));
            }
        }

        // Check if it's a transaction hash (60 chars lowercase)
        if (query.Length == 60 && query.All(c => char.IsLetterOrDigit(c)))
        {
            await using var txCmd = _connection.CreateCommand();
            txCmd.CommandText = $"SELECT hash FROM transactions WHERE hash = '{query.ToLowerInvariant()}' LIMIT 1";
            var txResult = await txCmd.ExecuteScalarAsync(ct);
            if (txResult != null)
            {
                results.Add(new SearchResultDto(SearchResultType.Transaction, query.ToLowerInvariant(), "Transaction"));
            }
        }

        // Check if it's an address (60 chars uppercase)
        if (query.Length == 60 && query.All(c => char.IsLetterOrDigit(c)))
        {
            await using var addrCmd = _connection.CreateCommand();
            addrCmd.CommandText = $@"
                SELECT 1 FROM transactions
                WHERE from_address = '{query.ToUpperInvariant()}'
                   OR to_address = '{query.ToUpperInvariant()}'
                LIMIT 1";
            var addrResult = await addrCmd.ExecuteScalarAsync(ct);
            if (addrResult != null)
            {
                results.Add(new SearchResultDto(SearchResultType.Address, query.ToUpperInvariant(), "Address"));
            }
        }

        // Search by label/name if query is not a tick number or exact address/hash match
        // and we haven't found results yet (or always search for partial matches)
        if (results.Count == 0 || (query.Length < 60 && !ulong.TryParse(query, out _)))
        {
            await _labelService.EnsureFreshDataAsync();
            var labelMatches = _labelService.SearchByLabel(query, 10);

            // Track existing addresses to avoid duplicates
            var existingAddresses = results
                .Where(r => r.Type == SearchResultType.Address)
                .Select(r => r.Value.ToUpperInvariant())
                .ToHashSet();

            foreach (var match in labelMatches)
            {
                // Skip if we already have this address in results
                if (existingAddresses.Contains(match.Address.ToUpperInvariant()))
                    continue;

                // Format display name based on type
                var displayName = match.Type switch
                {
                    "smartcontract" => $"[{match.Label}]",
                    "exchange" => $"#{match.Label}",
                    "tokenissuer" => $"${match.Label}",
                    _ => match.Label
                };

                results.Add(new SearchResultDto(SearchResultType.Address, match.Address, displayName));
                existingAddresses.Add(match.Address.ToUpperInvariant());
            }
        }

        return new SearchResponse(query, results);
    }

    // =====================================================
    // EPOCH STATISTICS
    // =====================================================

    public async Task<List<EpochSummaryDto>> GetEpochsAsync(int limit = 50, CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();
        // Use epoch_meta for authoritative tick boundaries (initial_tick, end_tick)
        // For incomplete epochs (is_complete=0 or end_tick=0), fall back to last tick from ticks table
        // For completed epochs with stored stats, use them directly.
        // For the current (incomplete) epoch, fall back to materialized views.
        cmd.CommandText = $@"
            SELECT
                em.epoch,
                if(em.is_complete = 1 AND em.tick_count > 0, em.tick_count, COALESCE(ts.tick_count, 0)) as tick_count,
                if(em.is_complete = 1 AND em.tx_count > 0, em.tx_count, COALESCE(tx.tx_count, 0)) as tx_count,
                if(em.is_complete = 1 AND em.total_volume > 0, em.total_volume, COALESCE(tx.total_volume, 0)) as total_volume,
                if(em.is_complete = 1 AND em.active_addresses > 0, em.active_addresses,
                   COALESCE(tx.unique_senders, 0) + COALESCE(tx.unique_receivers, 0)) as active_addresses,
                COALESCE(ts.start_time, em.updated_at) as start_time,
                COALESCE(ts.end_time, em.updated_at) as end_time,
                em.initial_tick as first_tick,
                if(em.is_complete = 1 AND em.end_tick > 0, em.end_tick, COALESCE(ts.last_tick, em.initial_tick)) as last_tick
            FROM epoch_meta AS em FINAL
            LEFT JOIN (
                SELECT
                    epoch,
                    countMerge(tick_count_state) as tick_count,
                    minMerge(start_time_state) as start_time,
                    maxMerge(end_time_state) as end_time,
                    maxMerge(last_tick_state) as last_tick
                FROM epoch_tick_stats
                GROUP BY epoch
            ) ts ON em.epoch = ts.epoch
            LEFT JOIN (
                SELECT
                    epoch,
                    sum(tx_count) as tx_count,
                    sum(total_volume) as total_volume,
                    uniqMerge(unique_senders_state) as unique_senders,
                    uniqMerge(unique_receivers_state) as unique_receivers
                FROM epoch_tx_stats FINAL
                GROUP BY epoch
            ) tx ON em.epoch = tx.epoch
            ORDER BY em.epoch DESC
            LIMIT {limit}";

        var items = new List<EpochSummaryDto>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            items.Add(new EpochSummaryDto(
                reader.GetFieldValue<uint>(0),  // epoch
                ToUInt64(reader.GetValue(1)),   // tick_count
                ToUInt64(reader.GetValue(2)),   // tx_count
                ToDecimal(reader.GetValue(3)),  // total_volume
                ToUInt64(reader.GetValue(4)),   // active_addresses
                reader.GetDateTime(5),          // start_time
                reader.GetDateTime(6),          // end_time
                ToUInt64(reader.GetValue(7)),   // initial_tick
                ToUInt64(reader.GetValue(8))    // end_tick
            ));
        }

        return items;
    }

    public async Task<EpochStatsDto?> GetEpochStatsAsync(uint epoch, CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();
        // For completed epochs with stored stats, use them directly.
        // For the current (incomplete) epoch, fall back to materialized views.
        cmd.CommandText = $@"
            SELECT
                em.epoch,
                if(em.is_complete = 1 AND em.tick_count > 0, em.tick_count, COALESCE(ts.tick_count, 0)) as tick_count,
                em.initial_tick as first_tick,
                if(em.is_complete = 1 AND em.end_tick > 0, em.end_tick, COALESCE(ts.last_tick, em.initial_tick)) as last_tick,
                COALESCE(ts.start_time, em.updated_at) as start_time,
                COALESCE(ts.end_time, em.updated_at) as end_time,
                if(em.is_complete = 1 AND em.tx_count > 0, em.tx_count, COALESCE(tx.tx_count, 0)) as tx_count,
                if(em.is_complete = 1 AND em.total_volume > 0, em.total_volume, COALESCE(tx.total_volume, 0)) as total_volume,
                COALESCE(tx.unique_senders, 0) as unique_senders,
                COALESCE(tx.unique_receivers, 0) as unique_receivers,
                if(em.is_complete = 1 AND em.active_addresses > 0, em.active_addresses,
                   COALESCE(tx.unique_senders, 0) + COALESCE(tx.unique_receivers, 0)) as active_addresses,
                if(em.is_complete = 1 AND em.transfer_count > 0, em.transfer_count, COALESCE(tr.transfer_count, 0)) as transfer_count,
                if(em.is_complete = 1 AND em.qu_transferred > 0, em.qu_transferred, COALESCE(tr.qu_transferred, 0)) as qu_transferred,
                COALESCE(asset.asset_transfer_count, 0) as asset_transfer_count
            FROM epoch_meta AS em FINAL
            LEFT JOIN (
                SELECT
                    epoch,
                    countMerge(tick_count_state) as tick_count,
                    minMerge(start_time_state) as start_time,
                    maxMerge(end_time_state) as end_time,
                    maxMerge(last_tick_state) as last_tick
                FROM epoch_tick_stats
                WHERE epoch = {epoch}
                GROUP BY epoch
            ) ts ON em.epoch = ts.epoch
            LEFT JOIN (
                SELECT
                    epoch,
                    sum(tx_count) as tx_count,
                    sum(total_volume) as total_volume,
                    uniqMerge(unique_senders_state) as unique_senders,
                    uniqMerge(unique_receivers_state) as unique_receivers
                FROM epoch_tx_stats FINAL
                WHERE epoch = {epoch}
                GROUP BY epoch
            ) tx ON em.epoch = tx.epoch
            LEFT JOIN (
                SELECT
                    epoch,
                    sum(transfer_count) as transfer_count,
                    sum(qu_transferred) as qu_transferred
                FROM epoch_transfer_stats FINAL
                WHERE epoch = {epoch}
                GROUP BY epoch
            ) tr ON em.epoch = tr.epoch
            LEFT JOIN (
                SELECT
                    epoch,
                    sum(count) as asset_transfer_count
                FROM epoch_transfer_by_type FINAL
                WHERE epoch = {epoch} AND log_type != 0
                GROUP BY epoch
            ) asset ON em.epoch = asset.epoch
            WHERE em.epoch = {epoch}";

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct))
            return null;

        return new EpochStatsDto(
            reader.GetFieldValue<uint>(0),
            ToUInt64(reader.GetValue(1)),
            ToUInt64(reader.GetValue(2)),
            ToUInt64(reader.GetValue(3)),
            reader.GetDateTime(4),
            reader.GetDateTime(5),
            ToUInt64(reader.GetValue(6)),
            ToDecimal(reader.GetValue(7)),
            ToUInt64(reader.GetValue(8)),
            ToUInt64(reader.GetValue(9)),
            ToUInt64(reader.GetValue(10)),
            ToUInt64(reader.GetValue(11)),
            ToDecimal(reader.GetValue(12)),
            ToUInt64(reader.GetValue(13))
        );
    }

    public async Task<List<EpochTransferByTypeDto>> GetEpochTransfersByTypeAsync(uint epoch, CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT
                epoch,
                log_type,
                sum(count) as count,
                sum(total_amount) as total_amount
            FROM epoch_transfer_by_type FINAL
            WHERE epoch = {epoch}
            GROUP BY epoch, log_type
            ORDER BY count DESC";

        var items = new List<EpochTransferByTypeDto>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var logType = reader.GetFieldValue<byte>(1);
            items.Add(new EpochTransferByTypeDto(
                reader.GetFieldValue<uint>(0),
                logType,
                LogTypes.GetName(logType),
                ToUInt64(reader.GetValue(2)),
                ToDecimal(reader.GetValue(3))
            ));
        }

        return items;
    }

    // =====================================================
    // SC REWARD DISTRIBUTIONS
    // =====================================================

    public async Task<EpochRewardSummaryDto> GetEpochRewardsAsync(uint epoch, CancellationToken ct = default)
    {
        var distributions = await GetRewardDistributionsAsync(epoch, null, ct);

        var totalDistributed = distributions.Aggregate(0UL, (sum, d) => sum + d.TotalAmount);

        return new EpochRewardSummaryDto(
            epoch,
            distributions,
            totalDistributed
        );
    }

    public async Task<ContractRewardHistoryDto> GetContractRewardsAsync(
        string address, int page = 1, int limit = 20, CancellationToken ct = default)
    {
        var offset = (page - 1) * limit;
        var (distributions, totalCount, totalAllTimeDistributed) = await GetRewardDistributionsPaginatedAsync(address, limit, offset, ct);

        var totalPages = (int)Math.Ceiling((double)totalCount / limit);

        return new ContractRewardHistoryDto(
            address,
            null, // Contract name will be resolved by frontend via label service
            distributions,
            totalAllTimeDistributed,
            page,
            limit,
            totalCount,
            totalPages,
            page < totalPages,
            page > 1
        );
    }

    private async Task<List<RewardDistributionDto>> GetRewardDistributionsAsync(
        uint? epoch, string? contractAddress, CancellationToken ct)
    {
        // Build WHERE clause for filtering START markers
        var startConditions = new List<string> { "log_type = 255" };
        if (epoch.HasValue)
            startConditions.Add($"epoch = {epoch.Value}");

        var startWhereClause = string.Join(" AND ", startConditions);

        // Build WHERE clause for filtering by contract address (applied to transfers)
        var contractFilter = contractAddress != null
            ? $"AND t.source_address = '{contractAddress}'"
            : "";

        // Query to find reward START markers and calculate totals
        // The START marker (6217575821008262227) indicates beginning of distribution
        // The END marker (6217575821008457285) indicates end of distribution
        // All QU_TRANSFER logs between START and END are reward payments
        // The contract address is the source_address of the QU_TRANSFER logs (not from CustomMessage)
        //
        // Strategy: Use CROSS JOIN with WHERE to pair START/END markers (ClickHouse doesn't support inequality in JOIN ON),
        // then find the minimum END for each START, and extract contract address from the first transfer
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            WITH start_markers AS (
                SELECT
                    epoch,
                    tick_number,
                    log_id as start_log_id,
                    timestamp
                FROM logs
                WHERE {startWhereClause}
                  AND JSONExtractUInt(raw_data, 'customMessage') = {LogTypes.CustomMessageOpStartDistributeRewards}
            ),
            end_markers AS (
                SELECT
                    tick_number as end_tick_number,
                    log_id as end_log_id
                FROM logs
                WHERE log_type = 255
                  AND JSONExtractUInt(raw_data, 'customMessage') = {LogTypes.CustomMessageOpEndDistributeRewards}
            ),
            reward_ranges AS (
                SELECT
                    s.epoch,
                    s.tick_number,
                    s.start_log_id,
                    s.timestamp,
                    min(e.end_log_id) as end_log_id
                FROM start_markers s
                CROSS JOIN end_markers e
                WHERE e.end_tick_number = s.tick_number AND e.end_log_id > s.start_log_id
                GROUP BY s.epoch, s.tick_number, s.start_log_id, s.timestamp
            ),
            transfers AS (
                SELECT
                    tick_number as t_tick_number,
                    log_id as t_log_id,
                    source_address,
                    amount
                FROM logs
                WHERE log_type = 0
            )
            SELECT
                dr.epoch,
                any(t.source_address) as contract_address,
                dr.tick_number,
                dr.timestamp,
                sum(t.amount) as total_amount,
                count() as transfer_count
            FROM reward_ranges dr
            INNER JOIN transfers t ON t.t_tick_number = dr.tick_number
            WHERE t.t_log_id > dr.start_log_id AND t.t_log_id < dr.end_log_id
                {contractFilter}
            GROUP BY dr.epoch, dr.tick_number, dr.start_log_id, dr.end_log_id, dr.timestamp
            ORDER BY dr.tick_number DESC";

        var items = new List<RewardDistributionDto>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var totalAmount = ToUInt64(reader.GetValue(4));
            var amountPerShare = (decimal)totalAmount / LogTypes.NumberOfComputors;

            items.Add(new RewardDistributionDto(
                reader.GetFieldValue<uint>(0),
                reader.GetString(1),
                null, // Contract name resolved by frontend
                reader.GetFieldValue<ulong>(2),
                totalAmount,
                amountPerShare,
                Convert.ToUInt32(reader.GetValue(5)),
                reader.GetDateTime(3)
            ));
        }

        return items;
    }

    private async Task<(List<RewardDistributionDto> Distributions, long TotalCount, ulong TotalAllTimeDistributed)>
        GetRewardDistributionsPaginatedAsync(string contractAddress, int limit, int offset, CancellationToken ct)
    {
        // Query to find reward distributions for a specific contract address with pagination
        // Uses same logic as GetRewardDistributionsAsync but with COUNT and LIMIT/OFFSET
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            WITH start_markers AS (
                SELECT
                    epoch,
                    tick_number,
                    log_id as start_log_id,
                    timestamp
                FROM logs
                WHERE log_type = 255
                  AND JSONExtractUInt(raw_data, 'customMessage') = {LogTypes.CustomMessageOpStartDistributeRewards}
            ),
            end_markers AS (
                SELECT
                    tick_number as end_tick_number,
                    log_id as end_log_id
                FROM logs
                WHERE log_type = 255
                  AND JSONExtractUInt(raw_data, 'customMessage') = {LogTypes.CustomMessageOpEndDistributeRewards}
            ),
            reward_ranges AS (
                SELECT
                    s.epoch,
                    s.tick_number,
                    s.start_log_id,
                    s.timestamp,
                    min(e.end_log_id) as end_log_id
                FROM start_markers s
                CROSS JOIN end_markers e
                WHERE e.end_tick_number = s.tick_number AND e.end_log_id > s.start_log_id
                GROUP BY s.epoch, s.tick_number, s.start_log_id, s.timestamp
            ),
            transfers AS (
                SELECT
                    tick_number as t_tick_number,
                    log_id as t_log_id,
                    source_address,
                    amount
                FROM logs
                WHERE log_type = 0 AND source_address = '{contractAddress}'
            ),
            aggregated AS (
                SELECT
                    dr.epoch,
                    any(t.source_address) as contract_address,
                    dr.tick_number,
                    dr.timestamp,
                    sum(t.amount) as total_amount,
                    count() as transfer_count
                FROM reward_ranges dr
                INNER JOIN transfers t ON t.t_tick_number = dr.tick_number
                WHERE t.t_log_id > dr.start_log_id AND t.t_log_id < dr.end_log_id
                GROUP BY dr.epoch, dr.tick_number, dr.start_log_id, dr.end_log_id, dr.timestamp
            )
            SELECT
                epoch,
                contract_address,
                tick_number,
                timestamp,
                total_amount,
                transfer_count,
                count() OVER () as total_count,
                sum(total_amount) OVER () as total_all_time
            FROM aggregated
            ORDER BY tick_number DESC
            LIMIT {limit} OFFSET {offset}";

        var items = new List<RewardDistributionDto>();
        long totalCount = 0;
        ulong totalAllTimeDistributed = 0;

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var totalAmount = ToUInt64(reader.GetValue(4));
            var amountPerShare = (decimal)totalAmount / LogTypes.NumberOfComputors;

            items.Add(new RewardDistributionDto(
                reader.GetFieldValue<uint>(0),
                reader.GetString(1),
                null, // Contract name resolved by frontend
                reader.GetFieldValue<ulong>(2),
                totalAmount,
                amountPerShare,
                Convert.ToUInt32(reader.GetValue(5)),
                reader.GetDateTime(3)
            ));

            // These are the same for all rows due to window functions
            totalCount = Convert.ToInt64(reader.GetValue(6));
            totalAllTimeDistributed = ToUInt64(reader.GetValue(7));
        }

        return (items, totalCount, totalAllTimeDistributed);
    }

    // =====================================================
    // ANALYTICS - TOP ADDRESSES, FLOW, SC USAGE
    // =====================================================

    /// <summary>
    /// Get top addresses by volume (sent + received)
    /// </summary>
    public async Task<List<TopAddressDto>> GetTopAddressesByVolumeAsync(
        int limit = 20, uint? epoch = null, CancellationToken ct = default)
    {
        var epochFilter = epoch.HasValue ? $"WHERE epoch = {epoch.Value}" : "";
        var epochFilterAnd = epoch.HasValue ? $"AND epoch = {epoch.Value}" : "";

        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            WITH sent AS (
                SELECT
                    source_address as address,
                    sum(amount) as sent_volume,
                    count() as sent_count
                FROM logs
                WHERE log_type = 0 AND source_address != '' {epochFilterAnd}
                GROUP BY source_address
            ),
            received AS (
                SELECT
                    dest_address as address,
                    sum(amount) as received_volume,
                    count() as received_count
                FROM logs
                WHERE log_type = 0 AND dest_address != '' {epochFilterAnd}
                GROUP BY dest_address
            )
            SELECT
                coalesce(s.address, r.address) as address,
                coalesce(s.sent_volume, 0) as sent_volume,
                coalesce(r.received_volume, 0) as received_volume,
                coalesce(s.sent_volume, 0) + coalesce(r.received_volume, 0) as total_volume,
                coalesce(s.sent_count, 0) as sent_count,
                coalesce(r.received_count, 0) as received_count,
                coalesce(s.sent_count, 0) + coalesce(r.received_count, 0) as total_count
            FROM sent s
            FULL OUTER JOIN received r ON s.address = r.address
            ORDER BY total_volume DESC
            LIMIT {limit}";

        var items = new List<TopAddressDto>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var address = reader.GetString(0);
            var label = _labelService.GetAddressInfo(address);

            items.Add(new TopAddressDto(
                address,
                label?.Label,
                label?.Type.ToString().ToLowerInvariant(),
                ToUInt64(reader.GetValue(1)),
                ToUInt64(reader.GetValue(2)),
                ToUInt64(reader.GetValue(3)),
                Convert.ToUInt32(reader.GetValue(4)),
                Convert.ToUInt32(reader.GetValue(5)),
                Convert.ToUInt32(reader.GetValue(6))
            ));
        }

        return items;
    }

    /// <summary>
    /// Get transaction flow for a specific address (top counterparties)
    /// </summary>
    public async Task<AddressFlowDto> GetAddressFlowAsync(
        string address, int limit = 10, CancellationToken ct = default)
    {
        // Get top senders TO this address
        await using var inboundCmd = _connection.CreateCommand();
        inboundCmd.CommandText = $@"
            SELECT
                source_address,
                sum(amount) as total_amount,
                count() as tx_count
            FROM logs
            WHERE log_type = 0 AND dest_address = '{address}' AND source_address != ''
            GROUP BY source_address
            ORDER BY total_amount DESC
            LIMIT {limit}";

        var inbound = new List<FlowNodeDto>();
        await using var inReader = await inboundCmd.ExecuteReaderAsync(ct);
        while (await inReader.ReadAsync(ct))
        {
            var sourceAddress = inReader.GetString(0);
            var label = _labelService.GetAddressInfo(sourceAddress);
            inbound.Add(new FlowNodeDto(
                sourceAddress,
                label?.Label,
                label?.Type.ToString().ToLowerInvariant(),
                Convert.ToUInt64(inReader.GetValue(1)),
                Convert.ToUInt32(inReader.GetValue(2))
            ));
        }

        // Get top receivers FROM this address
        await using var outboundCmd = _connection.CreateCommand();
        outboundCmd.CommandText = $@"
            SELECT
                dest_address,
                sum(amount) as total_amount,
                count() as tx_count
            FROM logs
            WHERE log_type = 0 AND source_address = '{address}' AND dest_address != ''
            GROUP BY dest_address
            ORDER BY total_amount DESC
            LIMIT {limit}";

        var outbound = new List<FlowNodeDto>();
        await using var outReader = await outboundCmd.ExecuteReaderAsync(ct);
        while (await outReader.ReadAsync(ct))
        {
            var destAddress = outReader.GetString(0);
            var label = _labelService.GetAddressInfo(destAddress);
            outbound.Add(new FlowNodeDto(
                destAddress,
                label?.Label,
                label?.Type.ToString().ToLowerInvariant(),
                Convert.ToUInt64(outReader.GetValue(1)),
                Convert.ToUInt32(outReader.GetValue(2))
            ));
        }

        var addressLabel = _labelService.GetAddressInfo(address);
        return new AddressFlowDto(
            address,
            addressLabel?.Label,
            addressLabel?.Type.ToString().ToLowerInvariant(),
            inbound,
            outbound
        );
    }

    /// <summary>
    /// Get smart contract usage statistics
    /// </summary>
    public async Task<List<SmartContractUsageDto>> GetSmartContractUsageAsync(
        uint? epoch = null, CancellationToken ct = default)
    {
        var epochFilter = epoch.HasValue ? $"AND epoch = {epoch.Value}" : "";

        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT
                to_address,
                count() as call_count,
                sum(amount) as total_amount,
                uniq(from_address) as unique_callers
            FROM transactions
            WHERE input_type > 0 {epochFilter}
            GROUP BY to_address
            ORDER BY call_count DESC
            LIMIT 50";

        var items = new List<SmartContractUsageDto>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var contractAddress = reader.GetString(0);
            var label = _labelService.GetAddressInfo(contractAddress);

            // Only include known smart contracts
            if (label?.Type == AddressType.SmartContract)
            {
                items.Add(new SmartContractUsageDto(
                    contractAddress,
                    label.Label,
                    label.ContractIndex,
                    ToUInt64(reader.GetValue(1)),
                    ToUInt64(reader.GetValue(2)),
                    Convert.ToUInt32(reader.GetValue(3))
                ));
            }
        }

        return items;
    }

    // =====================================================
    // GLASSNODE-STYLE ANALYTICS
    // =====================================================

    /// <summary>
    /// Get active address trends over time (daily/epoch)
    /// </summary>
    public async Task<List<ActiveAddressTrendDto>> GetActiveAddressTrendsAsync(
        string period = "epoch", int limit = 50, CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();

        if (period == "daily")
        {
            cmd.CommandText = $@"
                SELECT
                    toDate(timestamp) as date,
                    uniq(from_address) as unique_senders,
                    uniq(dest_address) as unique_receivers,
                    uniq(from_address) + uniq(dest_address) as total_active
                FROM logs
                WHERE log_type = 0
                GROUP BY date
                ORDER BY date DESC
                LIMIT {limit}";
        }
        else
        {
            // By epoch
            cmd.CommandText = $@"
                SELECT
                    epoch,
                    uniqMerge(unique_senders_state) as unique_senders,
                    uniqMerge(unique_receivers_state) as unique_receivers,
                    uniqMerge(unique_senders_state) + uniqMerge(unique_receivers_state) as total_active
                FROM epoch_tx_stats FINAL
                GROUP BY epoch
                ORDER BY epoch DESC
                LIMIT {limit}";
        }

        var items = new List<ActiveAddressTrendDto>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            if (period == "daily")
            {
                items.Add(new ActiveAddressTrendDto(
                    null,
                    reader.GetDateTime(0),
                    ToUInt64(reader.GetValue(1)),
                    ToUInt64(reader.GetValue(2)),
                    ToUInt64(reader.GetValue(3))
                ));
            }
            else
            {
                items.Add(new ActiveAddressTrendDto(
                    reader.GetFieldValue<uint>(0),
                    null,
                    ToUInt64(reader.GetValue(1)),
                    ToUInt64(reader.GetValue(2)),
                    ToUInt64(reader.GetValue(3))
                ));
            }
        }

        // Reverse to get chronological order
        items.Reverse();

        return items;
    }

    /// <summary>
    /// Get new vs returning addresses per epoch
    /// </summary>
    public async Task<List<NewVsReturningDto>> GetNewVsReturningAddressesAsync(
        int limit = 50, CancellationToken ct = default)
    {
        // First, get the first epoch each address appeared in
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            WITH first_appearance AS (
                SELECT
                    address,
                    min(epoch) as first_epoch
                FROM (
                    SELECT from_address as address, epoch FROM transactions WHERE from_address != ''
                    UNION ALL
                    SELECT to_address as address, epoch FROM transactions WHERE to_address != ''
                )
                GROUP BY address
            ),
            epoch_addresses AS (
                SELECT DISTINCT
                    epoch,
                    address
                FROM (
                    SELECT from_address as address, epoch FROM transactions WHERE from_address != ''
                    UNION ALL
                    SELECT to_address as address, epoch FROM transactions WHERE to_address != ''
                )
            )
            SELECT
                ea.epoch,
                countIf(fa.first_epoch = ea.epoch) as new_addresses,
                countIf(fa.first_epoch < ea.epoch) as returning_addresses,
                count() as total_addresses
            FROM epoch_addresses ea
            JOIN first_appearance fa ON ea.address = fa.address
            GROUP BY ea.epoch
            ORDER BY ea.epoch DESC
            LIMIT {limit}";

        var items = new List<NewVsReturningDto>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            items.Add(new NewVsReturningDto(
                reader.GetFieldValue<uint>(0),
                ToUInt64(reader.GetValue(1)),
                ToUInt64(reader.GetValue(2)),
                ToUInt64(reader.GetValue(3))
            ));
        }

        items.Reverse();

        return items;
    }

    /// <summary>
    /// Get exchange inflows and outflows
    /// </summary>
    public async Task<ExchangeFlowDto> GetExchangeFlowsAsync(
        int limit = 50, CancellationToken ct = default)
    {
        // Get all exchange addresses from label service
        await _labelService.EnsureFreshDataAsync();
        var exchangeAddresses = _labelService.GetAddressesByType(AddressType.Exchange);

        if (!exchangeAddresses.Any())
        {
            return new ExchangeFlowDto(new List<ExchangeFlowDataPointDto>(), 0, 0);
        }

        var addressList = string.Join("','", exchangeAddresses.Select(e => e.Address));

        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            WITH inflows AS (
                SELECT
                    epoch,
                    sum(amount) as inflow_volume,
                    count() as inflow_count
                FROM logs
                WHERE log_type = 0 AND amount > 0 AND dest_address IN ('{addressList}')
                GROUP BY epoch
            ),
            outflows AS (
                SELECT
                    epoch,
                    sum(amount) as outflow_volume,
                    count() as outflow_count
                FROM logs
                WHERE log_type = 0 AND amount > 0 AND source_address IN ('{addressList}')
                GROUP BY epoch
            )
            SELECT
                coalesce(i.epoch, o.epoch) as epoch,
                coalesce(i.inflow_volume, 0) as inflow_volume,
                coalesce(i.inflow_count, 0) as inflow_count,
                coalesce(o.outflow_volume, 0) as outflow_volume,
                coalesce(o.outflow_count, 0) as outflow_count
            FROM inflows i
            FULL OUTER JOIN outflows o ON i.epoch = o.epoch
            ORDER BY epoch DESC
            LIMIT {limit}";

        var items = new List<ExchangeFlowDataPointDto>();
        ulong totalInflow = 0;
        ulong totalOutflow = 0;

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var inflowVolume = ToUInt64(reader.GetValue(1));
            var outflowVolume = ToUInt64(reader.GetValue(3));
            totalInflow += inflowVolume;
            totalOutflow += outflowVolume;

            items.Add(new ExchangeFlowDataPointDto(
                reader.GetFieldValue<uint>(0),
                inflowVolume,
                Convert.ToUInt32(reader.GetValue(2)),
                outflowVolume,
                Convert.ToUInt32(reader.GetValue(4)),
                (long)inflowVolume - (long)outflowVolume
            ));
        }

        items.Reverse();
        var result = new ExchangeFlowDto(items, totalInflow, totalOutflow);

        return result;
    }

    /// <summary>
    /// Get holder distribution (whale analysis)
    /// Uses spectrum snapshot + transfer delta for accurate balances when available,
    /// falls back to transfer-only calculation otherwise.
    /// </summary>
    public async Task<HolderDistributionDto> GetHolderDistributionAsync(CancellationToken ct = default)
    {
        // Check if we have spectrum snapshots available
        var hasSnapshots = await HasBalanceSnapshotsAsync(ct);

        await using var cmd = _connection.CreateCommand();

        if (hasSnapshots)
        {
            // Use spectrum snapshot + delta from transfers since snapshot
            // Formula: Current Balance = Snapshot Balance + (Incoming since snapshot) - (Outgoing since snapshot)
            _logger.LogDebug("Using spectrum snapshot + delta for holder distribution");
            cmd.CommandText = @"
                WITH
                -- Get the latest snapshot epoch and tick
                latest_snapshot AS (
                    SELECT max(epoch) as epoch, max(tick_number) as tick_number
                    FROM spectrum_imports
                ),
                -- Snapshot balances from the latest import
                snapshot_balances AS (
                    SELECT address, balance as snapshot_balance
                    FROM balance_snapshots
                    WHERE epoch = (SELECT epoch FROM latest_snapshot)
                ),
                -- Transfer deltas since the snapshot tick
                transfer_deltas AS (
                    SELECT
                        address,
                        sum(incoming) - sum(outgoing) as delta
                    FROM (
                        SELECT dest_address as address, toInt64(amount) as incoming, 0 as outgoing
                        FROM logs
                        WHERE log_type = 0 AND dest_address != ''
                          AND tick_number > (SELECT tick_number FROM latest_snapshot)
                        UNION ALL
                        SELECT source_address as address, 0 as incoming, toInt64(amount) as outgoing
                        FROM logs
                        WHERE log_type = 0 AND source_address != ''
                          AND tick_number > (SELECT tick_number FROM latest_snapshot)
                    )
                    GROUP BY address
                ),
                -- Combined current balances
                current_balances AS (
                    SELECT
                        coalesce(s.address, d.address) as address,
                        coalesce(s.snapshot_balance, 0) + coalesce(d.delta, 0) as balance
                    FROM snapshot_balances s
                    FULL OUTER JOIN transfer_deltas d ON s.address = d.address
                    HAVING balance > 0
                )
                SELECT
                    countIf(balance >= 100000000000) as whales,
                    countIf(balance >= 20000000000 AND balance < 100000000000) as large,
                    countIf(balance >= 5000000000 AND balance < 20000000000) as medium,
                    countIf(balance >= 500000000 AND balance < 5000000000) as small,
                    countIf(balance < 500000000) as micro,
                    sumIf(balance, balance >= 100000000000) as whale_balance,
                    sumIf(balance, balance >= 20000000000 AND balance < 100000000000) as large_balance,
                    sumIf(balance, balance >= 5000000000 AND balance < 20000000000) as medium_balance,
                    sumIf(balance, balance >= 500000000 AND balance < 5000000000) as small_balance,
                    sumIf(balance, balance < 500000000) as micro_balance,
                    sum(balance) as total_balance,
                    count() as total_holders
                FROM current_balances";
        }
        else
        {
            // Fallback: Calculate balances from all transfer logs
            _logger.LogDebug("No spectrum snapshots available, using transfer-only calculation");
            cmd.CommandText = @"
                WITH balances AS (
                    SELECT
                        address,
                        sum(incoming) - sum(outgoing) as balance
                    FROM (
                        SELECT dest_address as address, toInt64(amount) as incoming, 0 as outgoing
                        FROM logs WHERE log_type = 0 AND dest_address != ''
                        UNION ALL
                        SELECT source_address as address, 0 as incoming, toInt64(amount) as outgoing
                        FROM logs WHERE log_type = 0 AND source_address != ''
                    )
                    GROUP BY address
                    HAVING balance > 0
                )
                SELECT
                    countIf(balance >= 100000000000) as whales,
                    countIf(balance >= 20000000000 AND balance < 100000000000) as large,
                    countIf(balance >= 5000000000 AND balance < 20000000000) as medium,
                    countIf(balance >= 500000000 AND balance < 5000000000) as small,
                    countIf(balance < 500000000) as micro,
                    sumIf(balance, balance >= 100000000000) as whale_balance,
                    sumIf(balance, balance >= 20000000000 AND balance < 100000000000) as large_balance,
                    sumIf(balance, balance >= 5000000000 AND balance < 20000000000) as medium_balance,
                    sumIf(balance, balance >= 500000000 AND balance < 5000000000) as small_balance,
                    sumIf(balance, balance < 500000000) as micro_balance,
                    sum(balance) as total_balance,
                    count() as total_holders
                FROM balances";
        }

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        if (await reader.ReadAsync(ct))
        {
            var totalBalance = ToDecimal(reader.GetValue(10));

            var result = new HolderDistributionDto(
                new List<HolderBracketDto>
                {
                    new("Whales (≥100B)", ToUInt64(reader.GetValue(0)),
                        ToDecimal(reader.GetValue(5)),
                        totalBalance > 0 ? ToDecimal(reader.GetValue(5)) / totalBalance * 100 : 0),
                    new("Large (20B-100B)", ToUInt64(reader.GetValue(1)),
                        ToDecimal(reader.GetValue(6)),
                        totalBalance > 0 ? ToDecimal(reader.GetValue(6)) / totalBalance * 100 : 0),
                    new("Medium (5B-20B)", ToUInt64(reader.GetValue(2)),
                        ToDecimal(reader.GetValue(7)),
                        totalBalance > 0 ? ToDecimal(reader.GetValue(7)) / totalBalance * 100 : 0),
                    new("Small (500M-5B)", ToUInt64(reader.GetValue(3)),
                        ToDecimal(reader.GetValue(8)),
                        totalBalance > 0 ? ToDecimal(reader.GetValue(8)) / totalBalance * 100 : 0),
                    new("Micro (<500M)", ToUInt64(reader.GetValue(4)),
                        ToDecimal(reader.GetValue(9)),
                        totalBalance > 0 ? ToDecimal(reader.GetValue(9)) / totalBalance * 100 : 0)
                },
                ToUInt64(reader.GetValue(11)),
                totalBalance
            );

            return result;
        }

        return new HolderDistributionDto(new List<HolderBracketDto>(), 0, 0);
    }

    /// <summary>
    /// Check if balance snapshots from spectrum imports are available
    /// </summary>
    private async Task<bool> HasBalanceSnapshotsAsync(CancellationToken ct)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = "SELECT count() FROM spectrum_imports";
        var count = Convert.ToInt64(await cmd.ExecuteScalarAsync(ct));
        return count > 0;
    }

    /// <summary>
    /// Get average transaction size trends
    /// </summary>
    public async Task<List<AvgTxSizeTrendDto>> GetAvgTxSizeTrendsAsync(
        string period = "epoch", int limit = 50, CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();

        if (period == "daily")
        {
            cmd.CommandText = $@"
                SELECT
                    toDate(timestamp) as date,
                    count() as tx_count,
                    sum(amount) as total_volume,
                    avg(amount) as avg_tx_size,
                    median(amount) as median_tx_size
                FROM logs
                WHERE log_type = 0 AND amount > 0
                GROUP BY date
                ORDER BY date DESC
                LIMIT {limit}";
        }
        else
        {
            cmd.CommandText = $@"
                SELECT
                    epoch,
                    count() as tx_count,
                    sum(amount) as total_volume,
                    avg(amount) as avg_tx_size,
                    median(amount) as median_tx_size
                FROM logs
                WHERE log_type = 0 AND amount > 0
                GROUP BY epoch
                ORDER BY epoch DESC
                LIMIT {limit}";
        }

        var items = new List<AvgTxSizeTrendDto>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            if (period == "daily")
            {
                items.Add(new AvgTxSizeTrendDto(
                    null,
                    reader.GetDateTime(0),
                    ToUInt64(reader.GetValue(1)),
                    ToDecimal(reader.GetValue(2)),
                    ToDecimal(reader.GetValue(3)),
                    ToDecimal(reader.GetValue(4))
                ));
            }
            else
            {
                items.Add(new AvgTxSizeTrendDto(
                    reader.GetFieldValue<uint>(0),
                    null,
                    ToUInt64(reader.GetValue(1)),
                    ToDecimal(reader.GetValue(2)),
                    ToDecimal(reader.GetValue(3)),
                    ToDecimal(reader.GetValue(4))
                ));
            }
        }

        items.Reverse();

        return items;
    }

    /// <summary>
    /// Get extended holder distribution with concentration metrics and history
    /// </summary>
    public async Task<HolderDistributionExtendedDto> GetHolderDistributionExtendedAsync(
        int historyLimit = 30, DateTime? from = null, DateTime? to = null, CancellationToken ct = default)
    {
        // Get current distribution with concentration
        var current = await GetHolderDistributionWithConcentrationAsync(ct);

        // Get historical data
        var history = await GetHolderDistributionHistoryAsync(historyLimit, from, to, ct);

        return new HolderDistributionExtendedDto(current, history);
    }

    /// <summary>
    /// Get holder distribution with concentration metrics (top 10/50/100 holders)
    /// </summary>
    public async Task<HolderDistributionDto> GetHolderDistributionWithConcentrationAsync(CancellationToken ct = default)
    {
        // Get base distribution
        var distribution = await GetHolderDistributionAsync(ct);

        // Get concentration metrics
        var concentration = await GetConcentrationMetricsAsync(ct);

        return new HolderDistributionDto(
            distribution.Brackets,
            distribution.TotalHolders,
            distribution.TotalBalance,
            concentration
        );
    }

    /// <summary>
    /// Get concentration metrics showing balance held by top holders
    /// </summary>
    private async Task<ConcentrationMetricsDto> GetConcentrationMetricsAsync(CancellationToken ct)
    {
        var hasSnapshots = await HasBalanceSnapshotsAsync(ct);

        await using var cmd = _connection.CreateCommand();

        if (hasSnapshots)
        {
            cmd.CommandText = @"
                WITH
                latest_snapshot AS (
                    SELECT max(epoch) as epoch, max(tick_number) as tick_number
                    FROM spectrum_imports
                ),
                snapshot_balances AS (
                    SELECT address, balance as snapshot_balance
                    FROM balance_snapshots
                    WHERE epoch = (SELECT epoch FROM latest_snapshot)
                ),
                transfer_deltas AS (
                    SELECT
                        address,
                        sum(incoming) - sum(outgoing) as delta
                    FROM (
                        SELECT dest_address as address, toInt64(amount) as incoming, 0 as outgoing
                        FROM logs
                        WHERE log_type = 0 AND dest_address != ''
                          AND tick_number > (SELECT tick_number FROM latest_snapshot)
                        UNION ALL
                        SELECT source_address as address, 0 as incoming, toInt64(amount) as outgoing
                        FROM logs
                        WHERE log_type = 0 AND source_address != ''
                          AND tick_number > (SELECT tick_number FROM latest_snapshot)
                    )
                    GROUP BY address
                ),
                current_balances AS (
                    SELECT
                        coalesce(s.address, d.address) as address,
                        coalesce(s.snapshot_balance, 0) + coalesce(d.delta, 0) as balance
                    FROM snapshot_balances s
                    FULL OUTER JOIN transfer_deltas d ON s.address = d.address
                    HAVING balance > 0
                ),
                ranked AS (
                    SELECT balance, row_number() OVER (ORDER BY balance DESC) as rank
                    FROM current_balances
                ),
                totals AS (
                    SELECT sum(balance) as total FROM current_balances
                )
                SELECT
                    sumIf(balance, rank <= 10) as top10,
                    sumIf(balance, rank <= 50) as top50,
                    sumIf(balance, rank <= 100) as top100,
                    (SELECT total FROM totals) as total
                FROM ranked";
        }
        else
        {
            cmd.CommandText = @"
                WITH balances AS (
                    SELECT
                        address,
                        sum(incoming) - sum(outgoing) as balance
                    FROM (
                        SELECT dest_address as address, toInt64(amount) as incoming, 0 as outgoing
                        FROM logs WHERE log_type = 0 AND dest_address != ''
                        UNION ALL
                        SELECT source_address as address, 0 as incoming, toInt64(amount) as outgoing
                        FROM logs WHERE log_type = 0 AND source_address != ''
                    )
                    GROUP BY address
                    HAVING balance > 0
                ),
                ranked AS (
                    SELECT balance, row_number() OVER (ORDER BY balance DESC) as rank
                    FROM balances
                ),
                totals AS (
                    SELECT sum(balance) as total FROM balances
                )
                SELECT
                    sumIf(balance, rank <= 10) as top10,
                    sumIf(balance, rank <= 50) as top50,
                    sumIf(balance, rank <= 100) as top100,
                    (SELECT total FROM totals) as total
                FROM ranked";
        }

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        if (await reader.ReadAsync(ct))
        {
            var top10 = ToDecimal(reader.GetValue(0));
            var top50 = ToDecimal(reader.GetValue(1));
            var top100 = ToDecimal(reader.GetValue(2));
            var total = ToDecimal(reader.GetValue(3));

            return new ConcentrationMetricsDto(
                top10,
                total > 0 ? top10 / total * 100 : 0,
                top50,
                total > 0 ? top50 / total * 100 : 0,
                top100,
                total > 0 ? top100 / total * 100 : 0
            );
        }

        return new ConcentrationMetricsDto(0, 0, 0, 0, 0, 0);
    }

    /// <summary>
    /// Get historical holder distribution data
    /// </summary>
    public async Task<List<HolderDistributionHistoryDto>> GetHolderDistributionHistoryAsync(
        int limit = 30, DateTime? from = null, DateTime? to = null, CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();
        var conditions = new List<string>();
        if (from.HasValue)
            conditions.Add($"snapshot_at >= '{from.Value:yyyy-MM-dd HH:mm:ss}'");
        if (to.HasValue)
            conditions.Add($"snapshot_at <= '{to.Value:yyyy-MM-dd HH:mm:ss}'");
        var whereClause = conditions.Count > 0 ? "WHERE " + string.Join(" AND ", conditions) : "";

        cmd.CommandText = $@"
            SELECT
                epoch,
                snapshot_at,
                tick_start,
                tick_end,
                whale_count, large_count, medium_count, small_count, micro_count,
                whale_balance, large_balance, medium_balance, small_balance, micro_balance,
                total_holders, total_balance,
                top10_balance, top50_balance, top100_balance,
                data_source
            FROM holder_distribution_history
            {whereClause}
            ORDER BY snapshot_at DESC
            LIMIT {limit}";

        var items = new List<HolderDistributionHistoryDto>();

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var epoch = reader.GetFieldValue<uint>(0);
            var snapshotAt = reader.GetDateTime(1);
            var tickStart = reader.GetFieldValue<ulong>(2);
            var tickEnd = reader.GetFieldValue<ulong>(3);

            var whaleCount = ToUInt64(reader.GetValue(4));
            var largeCount = ToUInt64(reader.GetValue(5));
            var mediumCount = ToUInt64(reader.GetValue(6));
            var smallCount = ToUInt64(reader.GetValue(7));
            var microCount = ToUInt64(reader.GetValue(8));

            // UInt128 columns return BigInteger, use helper method
            var whaleBalance = ToBigDecimal(reader.GetValue(9));
            var largeBalance = ToBigDecimal(reader.GetValue(10));
            var mediumBalance = ToBigDecimal(reader.GetValue(11));
            var smallBalance = ToBigDecimal(reader.GetValue(12));
            var microBalance = ToBigDecimal(reader.GetValue(13));

            var totalHolders = ToUInt64(reader.GetValue(14));
            var totalBalance = ToBigDecimal(reader.GetValue(15));

            var top10 = ToBigDecimal(reader.GetValue(16));
            var top50 = ToBigDecimal(reader.GetValue(17));
            var top100 = ToBigDecimal(reader.GetValue(18));

            var dataSource = reader.GetString(19);

            var brackets = new List<HolderBracketDto>
            {
                new("Whales (≥100B)", whaleCount, whaleBalance,
                    totalBalance > 0 ? whaleBalance / totalBalance * 100 : 0),
                new("Large (20B-100B)", largeCount, largeBalance,
                    totalBalance > 0 ? largeBalance / totalBalance * 100 : 0),
                new("Medium (5B-20B)", mediumCount, mediumBalance,
                    totalBalance > 0 ? mediumBalance / totalBalance * 100 : 0),
                new("Small (500M-5B)", smallCount, smallBalance,
                    totalBalance > 0 ? smallBalance / totalBalance * 100 : 0),
                new("Micro (<500M)", microCount, microBalance,
                    totalBalance > 0 ? microBalance / totalBalance * 100 : 0)
            };

            var concentration = new ConcentrationMetricsDto(
                top10, totalBalance > 0 ? top10 / totalBalance * 100 : 0,
                top50, totalBalance > 0 ? top50 / totalBalance * 100 : 0,
                top100, totalBalance > 0 ? top100 / totalBalance * 100 : 0
            );

            items.Add(new HolderDistributionHistoryDto(
                epoch, snapshotAt, tickStart, tickEnd, brackets, totalHolders, totalBalance, concentration, dataSource
            ));
        }

        items.Reverse();
        return items;
    }

    // =====================================================
    // NETWORK STATS HISTORY
    // =====================================================

    /// <summary>
    /// Get historical network stats
    /// </summary>
    public async Task<List<NetworkStatsHistoryDto>> GetNetworkStatsHistoryAsync(
        int limit = 30, DateTime? from = null, DateTime? to = null, CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();
        var conditions = new List<string>();
        if (from.HasValue)
            conditions.Add($"snapshot_at >= '{from.Value:yyyy-MM-dd HH:mm:ss}'");
        if (to.HasValue)
            conditions.Add($"snapshot_at <= '{to.Value:yyyy-MM-dd HH:mm:ss}'");
        var whereClause = conditions.Count > 0 ? "WHERE " + string.Join(" AND ", conditions) : "";

        cmd.CommandText = $@"
            SELECT
                epoch, snapshot_at, tick_start, tick_end,
                total_transactions, total_transfers, total_volume,
                unique_senders, unique_receivers, total_active_addresses,
                new_addresses, returning_addresses,
                exchange_inflow_volume, exchange_inflow_count,
                exchange_outflow_volume, exchange_outflow_count, exchange_net_flow,
                sc_call_count, sc_unique_callers,
                avg_tx_size, median_tx_size,
                new_users_100m_plus, new_users_1b_plus, new_users_10b_plus
            FROM network_stats_history
            {whereClause}
            ORDER BY snapshot_at DESC
            LIMIT {limit}";

        var items = new List<NetworkStatsHistoryDto>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            // UInt128 columns (total_volume, exchange volumes) return BigInteger
            items.Add(new NetworkStatsHistoryDto(
                reader.GetFieldValue<uint>(0),    // epoch
                reader.GetDateTime(1),            // snapshot_at
                ToUInt64(reader.GetValue(2)),   // tick_start
                ToUInt64(reader.GetValue(3)),   // tick_end
                ToUInt64(reader.GetValue(4)),   // total_transactions
                ToUInt64(reader.GetValue(5)),   // total_transfers
                ToBigDecimal(reader.GetValue(6)),       // total_volume (UInt128)
                ToUInt64(reader.GetValue(7)),   // unique_senders
                ToUInt64(reader.GetValue(8)),   // unique_receivers
                ToUInt64(reader.GetValue(9)),   // total_active_addresses
                ToUInt64(reader.GetValue(10)),  // new_addresses
                ToUInt64(reader.GetValue(11)),  // returning_addresses
                ToBigDecimal(reader.GetValue(12)),      // exchange_inflow_volume (UInt128)
                ToUInt64(reader.GetValue(13)),  // exchange_inflow_count
                ToBigDecimal(reader.GetValue(14)),      // exchange_outflow_volume (UInt128)
                ToUInt64(reader.GetValue(15)),  // exchange_outflow_count
                ToBigDecimal(reader.GetValue(16)),      // exchange_net_flow (Int128)
                ToUInt64(reader.GetValue(17)),  // sc_call_count
                ToUInt64(reader.GetValue(18)),  // sc_unique_callers
                ToSafeDouble(reader.GetValue(19)),      // avg_tx_size
                ToSafeDouble(reader.GetValue(20)),      // median_tx_size
                ToUInt64(reader.GetValue(21)),  // new_users_100m_plus
                ToUInt64(reader.GetValue(22)),  // new_users_1b_plus
                ToUInt64(reader.GetValue(23))   // new_users_10b_plus
            ));
        }

        items.Reverse();
        return items;
    }

    /// <summary>
    /// Get extended network stats with current and historical data
    /// </summary>
    public async Task<NetworkStatsExtendedDto> GetNetworkStatsExtendedAsync(
        int historyLimit = 30, DateTime? from = null, DateTime? to = null, CancellationToken ct = default)
    {
        var history = await GetNetworkStatsHistoryAsync(historyLimit, from, to, ct);
        var current = history.Count > 0 ? history[^1] : null;
        return new NetworkStatsExtendedDto(current, history);
    }

    /// <summary>
    /// Get the last network stats snapshot time
    /// </summary>
    public async Task<DateTime?> GetLastNetworkStatsSnapshotTimeAsync(CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = "SELECT max(snapshot_at) FROM network_stats_history";
        var result = await cmd.ExecuteScalarAsync(ct);
        if (result == null || result == DBNull.Value)
            return null;
        return Convert.ToDateTime(result);
    }

    // =====================================================
    // BURN STATS HISTORY
    // =====================================================

    /// <summary>
    /// Get burn stats history with optional date range filter
    /// </summary>
    public async Task<List<BurnStatsHistoryDto>> GetBurnStatsHistoryAsync(
        int limit = 30, DateTime? from = null, DateTime? to = null, CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();
        var conditions = new List<string>();
        if (from.HasValue)
            conditions.Add($"snapshot_at >= '{from.Value:yyyy-MM-dd HH:mm:ss}'");
        if (to.HasValue)
            conditions.Add($"snapshot_at <= '{to.Value:yyyy-MM-dd HH:mm:ss}'");
        var whereClause = conditions.Count > 0 ? "WHERE " + string.Join(" AND ", conditions) : "";

        cmd.CommandText = $@"
            SELECT
                epoch, snapshot_at, tick_start, tick_end,
                total_burned, burn_count, burn_amount,
                dust_burn_count, dust_burned,
                transfer_burn_count, transfer_burned,
                unique_burners, largest_burn, cumulative_burned
            FROM burn_stats_history
            {whereClause}
            ORDER BY snapshot_at DESC
            LIMIT {limit}";

        var items = new List<BurnStatsHistoryDto>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            items.Add(new BurnStatsHistoryDto(
                reader.GetFieldValue<uint>(0),          // epoch
                reader.GetDateTime(1),                   // snapshot_at
                ToUInt64(reader.GetValue(2)),    // tick_start
                ToUInt64(reader.GetValue(3)),    // tick_end
                ToUInt64(reader.GetValue(4)),    // total_burned
                ToUInt64(reader.GetValue(5)),    // burn_count
                ToUInt64(reader.GetValue(6)),    // burn_amount
                ToUInt64(reader.GetValue(7)),    // dust_burn_count
                ToUInt64(reader.GetValue(8)),    // dust_burned
                ToUInt64(reader.GetValue(9)),    // transfer_burn_count
                ToUInt64(reader.GetValue(10)),   // transfer_burned
                ToUInt64(reader.GetValue(11)),   // unique_burners
                ToUInt64(reader.GetValue(12)),   // largest_burn
                ToUInt64(reader.GetValue(13))    // cumulative_burned
            ));
        }

        items.Reverse();
        return items;
    }

    /// <summary>
    /// Get extended burn stats with current and historical data
    /// </summary>
    public async Task<BurnStatsExtendedDto> GetBurnStatsExtendedAsync(
        int historyLimit = 30, DateTime? from = null, DateTime? to = null, CancellationToken ct = default)
    {
        var history = await GetBurnStatsHistoryAsync(historyLimit, from, to, ct);
        var current = history.Count > 0 ? history[^1] : null;

        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = "SELECT sum(total_burned) FROM burn_stats_history";
        var allTimeTotal = Convert.ToUInt64(await cmd.ExecuteScalarAsync(ct) ?? 0UL);

        return new BurnStatsExtendedDto(current, history, allTimeTotal);
    }

    /// <summary>
    /// Get the current (latest) epoch from the ticks table (real-time source of truth)
    /// </summary>
    public async Task<uint?> GetCurrentEpochAsync(CancellationToken ct = default)
    {
        // Use ticks table as the real-time source of truth
        // (ticks are indexed continuously, epoch_meta lags behind)
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = "SELECT max(epoch) FROM ticks";
        var result = await cmd.ExecuteScalarAsync(ct);
        if (result != null && result != DBNull.Value)
        {
            var epoch = Convert.ToUInt32(result);
            if (epoch > 0)
                return epoch;
        }

        // Fall back to epoch_meta if ticks table is empty
        await using var metaCmd = _connection.CreateCommand();
        metaCmd.CommandText = "SELECT max(epoch) FROM epoch_meta";
        var metaResult = await metaCmd.ExecuteScalarAsync(ct);
        if (metaResult == null || metaResult == DBNull.Value)
            return null;
        return Convert.ToUInt32(metaResult);
    }

    /// <summary>
    /// Get the tick range (min and max tick_number) for a specific epoch from the ticks table.
    /// </summary>
    public async Task<(ulong MinTick, ulong MaxTick)?> GetTickRangeForEpochAsync(uint epoch, CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT min(tick_number), max(tick_number)
            FROM ticks
            WHERE epoch = {epoch}";

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct) || reader.IsDBNull(0) || reader.IsDBNull(1))
            return null;

        return (ToUInt64(reader.GetValue(0)), ToUInt64(reader.GetValue(1)));
    }

    // =====================================================
    // EPOCH METADATA
    // =====================================================

    /// <summary>
    /// Get epoch metadata by epoch number
    /// </summary>
    public async Task<EpochMetaDto?> GetEpochMetaAsync(uint epoch, CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT epoch, initial_tick, end_tick, end_tick_start_log_id, end_tick_end_log_id,
                   is_complete, updated_at,
                   tick_count, tx_count, total_volume, active_addresses, transfer_count, qu_transferred
            FROM epoch_meta FINAL
            WHERE epoch = {epoch}";

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct))
            return null;

        return new EpochMetaDto(
            reader.GetFieldValue<uint>(0),
            reader.GetFieldValue<ulong>(1),
            reader.GetFieldValue<ulong>(2),
            reader.GetFieldValue<ulong>(3),
            reader.GetFieldValue<ulong>(4),
            reader.GetFieldValue<byte>(5) == 1,
            reader.GetDateTime(6),
            TickCount: reader.GetFieldValue<ulong>(7),
            TxCount: reader.GetFieldValue<ulong>(8),
            TotalVolume: ToBigDecimal(reader.GetValue(9)),
            ActiveAddresses: reader.GetFieldValue<ulong>(10),
            TransferCount: reader.GetFieldValue<ulong>(11),
            QuTransferred: ToBigDecimal(reader.GetValue(12))
        );
    }

    /// <summary>
    /// Get all epoch metadata, ordered by epoch descending
    /// </summary>
    public async Task<List<EpochMetaDto>> GetAllEpochMetaAsync(int limit = 100, CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT epoch, initial_tick, end_tick, end_tick_start_log_id, end_tick_end_log_id,
                   is_complete, updated_at,
                   tick_count, tx_count, total_volume, active_addresses, transfer_count, qu_transferred
            FROM epoch_meta FINAL
            ORDER BY epoch DESC
            LIMIT {limit}";

        var items = new List<EpochMetaDto>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            items.Add(new EpochMetaDto(
                reader.GetFieldValue<uint>(0),
                reader.GetFieldValue<ulong>(1),
                reader.GetFieldValue<ulong>(2),
                reader.GetFieldValue<ulong>(3),
                reader.GetFieldValue<ulong>(4),
                reader.GetFieldValue<byte>(5) == 1,
                reader.GetDateTime(6),
                TickCount: reader.GetFieldValue<ulong>(7),
                TxCount: reader.GetFieldValue<ulong>(8),
                TotalVolume: ToBigDecimal(reader.GetValue(9)),
                ActiveAddresses: reader.GetFieldValue<ulong>(10),
                TransferCount: reader.GetFieldValue<ulong>(11),
                QuTransferred: ToBigDecimal(reader.GetValue(12))
            ));
        }

        return items;
    }

    /// <summary>
    /// Upsert epoch metadata
    /// </summary>
    public async Task UpsertEpochMetaAsync(EpochMetaDto epochMeta, CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            INSERT INTO epoch_meta
            (epoch, initial_tick, end_tick, end_tick_start_log_id, end_tick_end_log_id, is_complete,
             tick_count, tx_count, total_volume, active_addresses, transfer_count, qu_transferred)
            VALUES
            ({epochMeta.Epoch}, {epochMeta.InitialTick}, {epochMeta.EndTick},
             {epochMeta.EndTickStartLogId}, {epochMeta.EndTickEndLogId},
             {(epochMeta.IsComplete ? 1 : 0)},
             {epochMeta.TickCount}, {epochMeta.TxCount}, {epochMeta.TotalVolume},
             {epochMeta.ActiveAddresses}, {epochMeta.TransferCount}, {epochMeta.QuTransferred})";

        await cmd.ExecuteNonQueryAsync(ct);
        _logger.LogInformation("Upserted epoch metadata for epoch {Epoch} (initial_tick={InitialTick}, end_tick={EndTick}, complete={IsComplete})",
            epochMeta.Epoch, epochMeta.InitialTick, epochMeta.EndTick, epochMeta.IsComplete);
    }

    /// <summary>
    /// Get the latest complete epoch from epoch_meta
    /// </summary>
    public async Task<EpochMetaDto?> GetLatestCompleteEpochAsync(CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = @"
            SELECT epoch, initial_tick, end_tick, end_tick_start_log_id, end_tick_end_log_id,
                   is_complete, updated_at,
                   tick_count, tx_count, total_volume, active_addresses, transfer_count, qu_transferred
            FROM epoch_meta FINAL
            WHERE is_complete = 1
            ORDER BY epoch DESC
            LIMIT 1";

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct))
            return null;

        return new EpochMetaDto(
            reader.GetFieldValue<uint>(0),
            reader.GetFieldValue<ulong>(1),
            reader.GetFieldValue<ulong>(2),
            reader.GetFieldValue<ulong>(3),
            reader.GetFieldValue<ulong>(4),
            reader.GetFieldValue<byte>(5) == 1,
            reader.GetDateTime(6),
            TickCount: reader.GetFieldValue<ulong>(7),
            TxCount: reader.GetFieldValue<ulong>(8),
            TotalVolume: ToBigDecimal(reader.GetValue(9)),
            ActiveAddresses: reader.GetFieldValue<ulong>(10),
            TransferCount: reader.GetFieldValue<ulong>(11),
            QuTransferred: ToBigDecimal(reader.GetValue(12))
        );
    }

    /// <summary>
    /// Computes epoch stats from materialized views and stores them in epoch_meta.
    /// Called once when an epoch is marked complete — stats are immutable after that.
    /// </summary>
    public async Task ComputeAndStoreEpochStatsAsync(uint epoch, CancellationToken ct = default)
    {
        // Get existing epoch_meta
        var meta = await GetEpochMetaAsync(epoch, ct);
        if (meta == null)
        {
            _logger.LogWarning("Cannot compute stats for epoch {Epoch}: no epoch_meta found", epoch);
            return;
        }

        // Query all stats from MVs in one go
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT
                COALESCE(ts.tick_count, 0),
                COALESCE(tx.tx_count, 0),
                COALESCE(tx.total_volume, 0),
                COALESCE(tx.unique_senders, 0) + COALESCE(tx.unique_receivers, 0),
                COALESCE(tr.transfer_count, 0),
                COALESCE(tr.qu_transferred, 0)
            FROM (SELECT 1 as _join) d
            LEFT JOIN (
                SELECT
                    countMerge(tick_count_state) as tick_count
                FROM epoch_tick_stats
                WHERE epoch = {epoch}
            ) ts ON 1=1
            LEFT JOIN (
                SELECT
                    sum(tx_count) as tx_count,
                    sum(total_volume) as total_volume,
                    uniqMerge(unique_senders_state) as unique_senders,
                    uniqMerge(unique_receivers_state) as unique_receivers
                FROM epoch_tx_stats FINAL
                WHERE epoch = {epoch}
            ) tx ON 1=1
            LEFT JOIN (
                SELECT
                    sum(transfer_count) as transfer_count,
                    sum(qu_transferred) as qu_transferred
                FROM epoch_transfer_stats FINAL
                WHERE epoch = {epoch}
            ) tr ON 1=1";

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct))
        {
            _logger.LogWarning("No stats data found for epoch {Epoch}", epoch);
            return;
        }

        var updatedMeta = meta with
        {
            TickCount = ToUInt64(reader.GetValue(0)),
            TxCount = ToUInt64(reader.GetValue(1)),
            TotalVolume = ToBigDecimal(reader.GetValue(2)),
            ActiveAddresses = ToUInt64(reader.GetValue(3)),
            TransferCount = ToUInt64(reader.GetValue(4)),
            QuTransferred = ToBigDecimal(reader.GetValue(5)),
            UpdatedAt = DateTime.UtcNow
        };

        await UpsertEpochMetaAsync(updatedMeta, ct);
        _logger.LogInformation(
            "Stored epoch {Epoch} stats: ticks={TickCount}, txs={TxCount}, volume={Volume}, addresses={Addresses}, transfers={Transfers}",
            epoch, updatedMeta.TickCount, updatedMeta.TxCount, updatedMeta.TotalVolume,
            updatedMeta.ActiveAddresses, updatedMeta.TransferCount);
    }

    // =====================================================
    // EPOCH TRANSITION VALIDATION
    // =====================================================

    /// <summary>
    /// Get the maximum log_id for a given epoch
    /// </summary>
    public async Task<ulong> GetMaxLogIdForEpochAsync(uint epoch, CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $"SELECT max(log_id) FROM logs WHERE epoch = {epoch}";
        var result = await cmd.ExecuteScalarAsync(ct);
        if (result == null || result == DBNull.Value)
            return 0;
        return Convert.ToUInt64(result);
    }

    /// <summary>
    /// Count logs in a specific log_id range for an epoch
    /// </summary>
    public async Task<ulong> CountLogsInRangeAsync(uint epoch, ulong startLogId, ulong endLogId, CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $"SELECT count() FROM logs WHERE epoch = {epoch} AND log_id >= {startLogId} AND log_id <= {endLogId}";
        var result = await cmd.ExecuteScalarAsync(ct);
        return result == null || result == DBNull.Value ? 0 : Convert.ToUInt64(result);
    }

    /// <summary>
    /// Check if the END_EPOCH log exists in the specified log range
    /// END_EPOCH log is type 255 (CUSTOM_MESSAGE) with customMessage field matching the OP code
    /// </summary>
    public async Task<bool> HasEndEpochLogAsync(uint epoch, ulong startLogId, ulong endLogId, ulong endEpochOpCode, CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT count() > 0
            FROM logs
            WHERE epoch = {epoch}
              AND log_id >= {startLogId}
              AND log_id <= {endLogId}
              AND log_type = 255
              AND JSONExtractUInt(raw_data, 'customMessage') = {endEpochOpCode}";

        var result = await cmd.ExecuteScalarAsync(ct);
        return result != null && Convert.ToBoolean(result);
    }

    /// <summary>
    /// Insert end epoch logs fetched from Bob
    /// </summary>
    public async Task InsertEndEpochLogsAsync(uint epoch, List<Shared.Models.BobLog> logs, CancellationToken ct = default)
    {
        if (logs.Count == 0)
            return;

        var sb = new StringBuilder();
        sb.AppendLine("INSERT INTO logs (tick_number, epoch, log_id, log_type, tx_hash, source_address, dest_address, amount, asset_name, raw_data, timestamp) VALUES");

        var values = new List<string>();
        foreach (var log in logs)
        {
            var timestamp = DateTime.TryParse(log.Timestamp, out var ts) ? ts : DateTime.UtcNow;
            var rawData = log.GetRawBodyJson() ?? string.Empty;

            values.Add($@"({log.Tick}, {epoch}, {log.LogId}, {log.LogType}, '{EscapeSql(log.TxHash ?? string.Empty)}', '{EscapeSql(log.GetSourceAddress() ?? string.Empty)}', '{EscapeSql(log.GetDestAddress() ?? string.Empty)}', {log.GetAmount()}, '{EscapeSql(log.GetAssetName() ?? string.Empty)}', '{EscapeSql(rawData)}', '{timestamp:yyyy-MM-dd HH:mm:ss.fff}')");
        }

        sb.AppendLine(string.Join(",\n", values));

        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = sb.ToString();
        await cmd.ExecuteNonQueryAsync(ct);

        _logger.LogInformation("Inserted {Count} end epoch logs for epoch {Epoch}", logs.Count, epoch);
    }

    private static string EscapeSql(string value)
    {
        return value.Replace("'", "\\'").Replace("\\", "\\\\");
    }

    // =====================================================
    // COMPUTOR/MINER FLOW TRACKING METHODS
    // =====================================================

    /// <summary>
    /// Checks if computor list for an epoch has been imported
    /// </summary>
    public async Task<bool> IsComputorListImportedAsync(uint epoch, CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $"SELECT count() FROM computor_imports WHERE epoch = {epoch}";
        var count = Convert.ToInt64(await cmd.ExecuteScalarAsync(ct));
        return count > 0;
    }

    /// <summary>
    /// Saves computor list for an epoch
    /// </summary>
    public async Task SaveComputorsAsync(uint epoch, List<string> addresses, CancellationToken ct = default)
    {
        if (addresses.Count == 0) return;

        var sb = new StringBuilder();
        sb.AppendLine("INSERT INTO computors (epoch, address, computor_index) VALUES");

        var values = new List<string>();
        for (int i = 0; i < addresses.Count; i++)
        {
            values.Add($"({epoch}, '{EscapeSql(addresses[i])}', {i})");
        }
        sb.AppendLine(string.Join(",\n", values));

        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = sb.ToString();
        await cmd.ExecuteNonQueryAsync(ct);

        await using var importCmd = _connection.CreateCommand();
        importCmd.CommandText = $@"INSERT INTO computor_imports (epoch, computor_count) VALUES ({epoch}, {addresses.Count})";
        await importCmd.ExecuteNonQueryAsync(ct);

        _logger.LogInformation("Saved {Count} computors for epoch {Epoch}", addresses.Count, epoch);
    }

    /// <summary>
    /// Gets computor list for an epoch
    /// </summary>
    public async Task<ComputorListDto?> GetComputorsAsync(uint epoch, CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT address, computor_index
            FROM computors
            WHERE epoch = {epoch}
            ORDER BY computor_index";

        var computors = new List<ComputorDto>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var address = reader.GetString(0);
            var label = _labelService.GetLabel(address);
            computors.Add(new ComputorDto(
                Epoch: epoch,
                Address: address,
                Index: reader.GetFieldValue<ushort>(1),
                Label: label
            ));
        }

        if (computors.Count == 0) return null;

        // Get import timestamp
        await using var importCmd = _connection.CreateCommand();
        importCmd.CommandText = $"SELECT imported_at FROM computor_imports WHERE epoch = {epoch} LIMIT 1";
        var importedAt = await importCmd.ExecuteScalarAsync(ct);

        return new ComputorListDto(
            Epoch: epoch,
            Computors: computors,
            Count: computors.Count,
            ImportedAt: importedAt != null ? Convert.ToDateTime(importedAt) : null
        );
    }

    /// <summary>
    /// Gets addresses by type from label service
    /// </summary>
    public Task<HashSet<string>> GetAddressesByTypeAsync(string type, CancellationToken ct = default)
    {
        if (!Enum.TryParse<AddressType>(type, ignoreCase: true, out var addressType))
        {
            return Task.FromResult(new HashSet<string>());
        }

        var addresses = _labelService.GetAddressesByType(addressType)
            .Select(a => a.Address)
            .ToHashSet();
        return Task.FromResult(addresses);
    }

    /// <summary>
    /// Gets label for an address
    /// </summary>
    public Task<string?> GetAddressLabelAsync(string address, CancellationToken ct = default)
    {
        var label = _labelService.GetLabel(address);
        return Task.FromResult(label);
    }

    /// <summary>
    /// Gets the input data for a transaction by its hash.
    /// Used for parsing Qutil SendToMany payloads to extract actual destinations.
    /// </summary>
    /// <param name="txHash">The transaction hash</param>
    /// <param name="ct">Cancellation token</param>
    /// <returns>Tuple of (inputType, inputDataHex) or null if not found</returns>
    public async Task<(ushort InputType, string? InputData)?> GetTransactionInputDataAsync(
        string txHash,
        CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT input_type, input_data
            FROM transactions
            WHERE hash = '{EscapeSql(txHash)}'
            LIMIT 1";

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        if (await reader.ReadAsync(ct))
        {
            var inputType = reader.GetFieldValue<ushort>(0);
            var inputData = reader.IsDBNull(1) ? null : reader.GetString(1);
            return (inputType, inputData);
        }

        return null;
    }

    /// <summary>
    /// Gets flow hops for visualization
    /// </summary>
    public async Task<List<FlowHopDto>> GetFlowHopsAsync(
        uint epoch,
        ulong tickStart,
        ulong tickEnd,
        int maxDepth,
        CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT
                epoch, tick_number, timestamp, tx_hash,
                source_address, dest_address, amount,
                origin_address, origin_type, hop_level, dest_type, dest_label
            FROM flow_hops
            WHERE epoch = {epoch}
              AND tick_number BETWEEN {tickStart} AND {tickEnd}
              AND hop_level <= {maxDepth}
            ORDER BY hop_level, tick_number";

        var result = new List<FlowHopDto>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var sourceAddr = reader.GetString(4);
            var destAddr = reader.GetString(5);
            var sourceInfo = _labelService.GetAddressInfo(sourceAddr);
            var destInfo = _labelService.GetAddressInfo(destAddr);
            var destLabel = reader.IsDBNull(11) ? null : reader.GetString(11);
            if (string.IsNullOrEmpty(destLabel))
            {
                destLabel = destInfo?.Label;
            }

            result.Add(new FlowHopDto(
                Epoch: reader.GetFieldValue<uint>(0),
                TickNumber: reader.GetFieldValue<ulong>(1),
                Timestamp: reader.GetDateTime(2),
                TxHash: reader.GetString(3),
                SourceAddress: sourceAddr,
                SourceLabel: sourceInfo?.Label,
                SourceType: sourceInfo?.Type.ToString().ToLowerInvariant(),
                DestAddress: destAddr,
                DestLabel: destLabel,
                DestType: reader.IsDBNull(10) ? null : reader.GetString(10),
                Amount: ToBigDecimal(reader.GetValue(6)),
                OriginAddress: reader.GetString(7),
                OriginType: reader.GetString(8),
                HopLevel: reader.GetFieldValue<byte>(9)
            ));
        }

        return result;
    }

    /// <summary>
    /// Gets all flow hops for a specific emission epoch (across all tick windows).
    /// This is used for visualization to show the complete flow from emission through all tracked hops.
    /// </summary>
    public async Task<List<FlowHopDto>> GetFlowHopsByEmissionEpochAsync(
        uint emissionEpoch,
        int maxDepth,
        CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT
                epoch, tick_number, timestamp, tx_hash,
                source_address, dest_address, amount,
                origin_address, origin_type, hop_level, dest_type, dest_label
            FROM flow_hops FINAL
            WHERE emission_epoch = {emissionEpoch}
              AND hop_level <= {maxDepth}
            ORDER BY hop_level, tick_number";

        var result = new List<FlowHopDto>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var sourceAddr = reader.GetString(4);
            var destAddr = reader.GetString(5);
            var sourceInfo = _labelService.GetAddressInfo(sourceAddr);
            var destInfo = _labelService.GetAddressInfo(destAddr);
            var destLabel = reader.IsDBNull(11) ? null : reader.GetString(11);
            if (string.IsNullOrEmpty(destLabel))
            {
                destLabel = destInfo?.Label;
            }

            result.Add(new FlowHopDto(
                Epoch: reader.GetFieldValue<uint>(0),
                TickNumber: reader.GetFieldValue<ulong>(1),
                Timestamp: reader.GetDateTime(2),
                TxHash: reader.GetString(3),
                SourceAddress: sourceAddr,
                SourceLabel: sourceInfo?.Label,
                SourceType: sourceInfo?.Type.ToString().ToLowerInvariant(),
                DestAddress: destAddr,
                DestLabel: destLabel,
                DestType: reader.IsDBNull(10) ? null : reader.GetString(10),
                Amount: ToBigDecimal(reader.GetValue(6)),
                OriginAddress: reader.GetString(7),
                OriginType: reader.GetString(8),
                HopLevel: reader.GetFieldValue<byte>(9)
            ));
        }

        return result;
    }

    /// <summary>
    /// Gets miner flow stats history.
    /// </summary>
    public async Task<List<MinerFlowStatsDto>> GetMinerFlowStatsHistoryAsync(int limit, DateTime? from = null, DateTime? to = null, CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();
        var conditions = new List<string>();
        if (from.HasValue)
            conditions.Add($"snapshot_at >= '{from.Value:yyyy-MM-dd HH:mm:ss}'");
        if (to.HasValue)
            conditions.Add($"snapshot_at <= '{to.Value:yyyy-MM-dd HH:mm:ss}'");
        var whereClause = conditions.Count > 0 ? "WHERE " + string.Join(" AND ", conditions) : "";

        cmd.CommandText = $@"
            SELECT
                epoch, snapshot_at, tick_start, tick_end, emission_epoch,
                total_emission, computor_count, total_outflow, outflow_tx_count,
                flow_to_exchange_direct, flow_to_exchange_1hop, flow_to_exchange_2hop, flow_to_exchange_3plus,
                flow_to_exchange_total, flow_to_exchange_count, flow_to_other, miner_net_position,
                hop_1_volume, hop_2_volume, hop_3_volume, hop_4_plus_volume
            FROM miner_flow_stats
            {whereClause}
            ORDER BY snapshot_at DESC
            LIMIT {limit}";

        var result = new List<MinerFlowStatsDto>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            result.Add(new MinerFlowStatsDto(
                Epoch: reader.GetFieldValue<uint>(0),
                SnapshotAt: reader.GetDateTime(1),
                TickStart: reader.GetFieldValue<ulong>(2),
                TickEnd: reader.GetFieldValue<ulong>(3),
                EmissionEpoch: reader.GetFieldValue<uint>(4),
                TotalEmission: ToBigDecimal(reader.GetValue(5)),
                ComputorCount: reader.GetFieldValue<ushort>(6),
                TotalOutflow: ToBigDecimal(reader.GetValue(7)),
                OutflowTxCount: reader.GetFieldValue<ulong>(8),
                FlowToExchangeDirect: ToBigDecimal(reader.GetValue(9)),
                FlowToExchange1Hop: ToBigDecimal(reader.GetValue(10)),
                FlowToExchange2Hop: ToBigDecimal(reader.GetValue(11)),
                FlowToExchange3Plus: ToBigDecimal(reader.GetValue(12)),
                FlowToExchangeTotal: ToBigDecimal(reader.GetValue(13)),
                FlowToExchangeCount: reader.GetFieldValue<ulong>(14),
                FlowToOther: ToBigDecimal(reader.GetValue(15)),
                MinerNetPosition: ToBigDecimal(reader.GetValue(16)),
                Hop1Volume: ToBigDecimal(reader.GetValue(17)),
                Hop2Volume: ToBigDecimal(reader.GetValue(18)),
                Hop3Volume: ToBigDecimal(reader.GetValue(19)),
                Hop4PlusVolume: ToBigDecimal(reader.GetValue(20))
            ));
        }

        return result;
    }

    // =====================================================
    // COMPUTOR EMISSIONS
    // =====================================================

    /// <summary>
    /// Checks if emissions have been imported for an epoch
    /// </summary>
    public async Task<bool> IsEmissionImportedAsync(uint epoch, CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $"SELECT count() FROM emission_imports WHERE epoch = {epoch}";
        var result = await cmd.ExecuteScalarAsync(ct);
        return Convert.ToInt64(result ?? 0) > 0;
    }

    /// <summary>
    /// Captures and saves emissions for computors at the end of an epoch.
    /// Scans the end-epoch tick logs for transfers from zero address to computor addresses.
    /// </summary>
    public async Task<(int ComputorCount, decimal TotalEmission)> CaptureEmissionsForEpochAsync(
        uint epoch,
        ulong endTick,
        HashSet<string> computorAddresses,
        Dictionary<string, int> addressToIndex,
        CancellationToken ct = default)
    {
        // Query transfers from zero address to computor addresses in the end tick
        var addressList = string.Join(",", computorAddresses.Select(a => $"'{EscapeSql(a)}'"));

        _logger.LogDebug("Computorslist for Epoch {Epoch} in tick {Tick}: {List}", epoch, endTick, addressList);

        await using var queryCmd = _connection.CreateCommand();
        queryCmd.CommandText = $@"
            SELECT
                dest_address,
                sum(amount) as emission_amount,
                max(timestamp) as emission_timestamp
            FROM logs
            WHERE log_type = 0
              AND tick_number = {endTick}
              AND source_address = '{AddressLabelService.BurnAddress}'
              AND dest_address IN ({addressList})
            GROUP BY dest_address";

        var emissions = new List<(string Address, int Index, decimal Amount, DateTime Timestamp)>();
        decimal totalEmission = 0;

        await using var reader = await queryCmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var address = reader.GetString(0);
            var amount = ToBigDecimal(reader.GetValue(1));
            var timestamp = reader.GetDateTime(2);

            if (addressToIndex.TryGetValue(address, out var index))
            {
                emissions.Add((address, index, amount, timestamp));
                totalEmission += amount;
            }
        }

        if (emissions.Count == 0)
        {
            _logger.LogWarning("No emissions found for epoch {Epoch} at tick {Tick}", epoch, endTick);
            return (0, 0);
        }

        // Save emissions
        var sb = new StringBuilder();
        sb.AppendLine(@"INSERT INTO computor_emissions (
            epoch, computor_index, address, emission_amount, emission_tick, emission_timestamp
        ) VALUES");

        var values = emissions.Select(e => $@"(
            {epoch},
            {e.Index},
            '{EscapeSql(e.Address)}',
            {e.Amount},
            {endTick},
            '{e.Timestamp:yyyy-MM-dd HH:mm:ss}'
        )");

        sb.AppendLine(string.Join(",\n", values));

        await using var insertCmd = _connection.CreateCommand();
        insertCmd.CommandText = sb.ToString();
        await insertCmd.ExecuteNonQueryAsync(ct);

        // Save import record
        await using var importCmd = _connection.CreateCommand();
        importCmd.CommandText = $@"
            INSERT INTO emission_imports (epoch, computor_count, total_emission, emission_tick)
            VALUES ({epoch}, {emissions.Count}, {totalEmission}, {endTick})";
        await importCmd.ExecuteNonQueryAsync(ct);

        _logger.LogInformation(
            "Captured emissions for epoch {Epoch}: {Count} computors, total {Total}",
            epoch, emissions.Count, totalEmission);

        return (emissions.Count, totalEmission);
    }

    /// <summary>
    /// Gets total emissions for the specified emission epochs from emission_imports table.
    /// Used to calculate accurate total emission tracked across snapshots.
    /// </summary>
    public async Task<decimal> GetTotalEmissionsForEpochsAsync(IEnumerable<uint> epochs, CancellationToken ct = default)
    {
        var epochList = epochs.Distinct().ToList();
        if (epochList.Count == 0) return 0;

        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT COALESCE(sum(total_emission), 0)
            FROM emission_imports
            WHERE epoch IN ({string.Join(",", epochList)})";

        var result = await cmd.ExecuteScalarAsync(ct);
        return ToBigDecimal(result ?? 0);
    }

    /// <summary>
    /// Gets emission for a specific computor address in an epoch
    /// </summary>
    public async Task<decimal> GetComputorEmissionAsync(uint epoch, string address, CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT COALESCE(emission_amount, 0)
            FROM computor_emissions
            WHERE epoch = {epoch} AND address = '{EscapeSql(address)}'";

        var result = await cmd.ExecuteScalarAsync(ct);
        return ToBigDecimal(result ?? 0);
    }

    /// <summary>
    /// Gets all emissions for an epoch with computor details
    /// </summary>
    public async Task<List<ComputorEmissionDto>> GetEmissionsForEpochAsync(uint epoch, CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT
                epoch, computor_index, address, emission_amount, emission_tick, emission_timestamp
            FROM computor_emissions
            WHERE epoch = {epoch}
            ORDER BY computor_index";

        var result = new List<ComputorEmissionDto>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var address = reader.GetString(2);
            var label = _labelService.GetLabel(address);

            result.Add(new ComputorEmissionDto(
                Epoch: reader.GetFieldValue<uint>(0),
                ComputorIndex: reader.GetFieldValue<ushort>(1),
                Address: address,
                Label: label,
                EmissionAmount: ToBigDecimal(reader.GetValue(3)),
                EmissionTick: reader.GetFieldValue<ulong>(4),
                EmissionTimestamp: reader.GetDateTime(5)
            ));
        }

        return result;
    }

    /// <summary>
    /// Gets emission summary for an epoch
    /// </summary>
    public async Task<EmissionSummaryDto?> GetEmissionSummaryAsync(uint epoch, CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT computor_count, total_emission, emission_tick, imported_at
            FROM emission_imports
            WHERE epoch = {epoch}";

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        if (await reader.ReadAsync(ct))
        {
            return new EmissionSummaryDto(
                Epoch: epoch,
                ComputorCount: reader.GetFieldValue<ushort>(0),
                TotalEmission: ToBigDecimal(reader.GetValue(1)),
                EmissionTick: reader.GetFieldValue<ulong>(2),
                ImportedAt: reader.GetDateTime(3)
            );
        }

        return null;
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _connection.Dispose();
    }

    /// <summary>
    /// Helper to convert ClickHouse UInt128 (BigInteger) to decimal
    /// </summary>
    private static decimal ToBigDecimal(object value)
    {
        if (value == null || value == DBNull.Value)
            return 0;

        if (value is BigInteger bigInt)
            return (decimal)bigInt;

        return Convert.ToDecimal(value);
    }

    /// <summary>
    /// Helper to safely convert to double, handling NaN/Infinity from empty aggregates
    /// </summary>
    private static double ToSafeDouble(object value)
    {
        if (value == null || value == DBNull.Value)
            return 0;

        var d = Convert.ToDouble(value);
        return double.IsNaN(d) || double.IsInfinity(d) ? 0 : d;
    }
}
