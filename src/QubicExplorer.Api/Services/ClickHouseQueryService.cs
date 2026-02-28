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

        // Build PREWHERE for address (uses bloom filter index) and WHERE for other filters
        var prewhereClause = "";
        var conditions = new List<string>();

        if (!string.IsNullOrEmpty(address))
        {
            if (direction == "from")
                prewhereClause = $"PREWHERE from_address = '{address}'";
            else if (direction == "to")
                prewhereClause = $"PREWHERE to_address = '{address}'";
            else
                prewhereClause = $"PREWHERE from_address = '{address}' OR to_address = '{address}'";
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

        // Run count and data queries in parallel
        var countTask = Task.Run(async () =>
        {
            await using var countCmd = _connection.CreateCommand();
            countCmd.CommandText = $"SELECT count() FROM transactions {prewhereClause} {whereClause}";
            return Convert.ToInt64(await countCmd.ExecuteScalarAsync(ct));
        }, ct);

        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT hash, tick_number, epoch, from_address, to_address, amount, input_type, executed, timestamp
            FROM transactions
            {prewhereClause}
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

        var totalCount = await countTask;
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

        // Build PREWHERE for address (uses bloom/minmax index) and WHERE for other filters
        var prewhereClause = "";
        var conditions = new List<string>();

        // Epoch filter (enables partition pruning)
        if (epoch.HasValue)
            conditions.Add($"epoch = {epoch.Value}");

        if (!string.IsNullOrEmpty(address))
        {
            if (direction == "in")
                prewhereClause = $"PREWHERE dest_address = '{address}'";
            else if (direction == "out")
                prewhereClause = $"PREWHERE source_address = '{address}'";
            else
                prewhereClause = $"PREWHERE source_address = '{address}' OR dest_address = '{address}'";
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

        // Run count and data queries in parallel
        var countTask = Task.Run(async () =>
        {
            await using var countCmd = _connection.CreateCommand();
            countCmd.CommandText = $"SELECT count() FROM logs {prewhereClause} {whereClause}";
            return Convert.ToInt64(await countCmd.ExecuteScalarAsync(ct));
        }, ct);

        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT tick_number, epoch, log_id, log_type, tx_hash, source_address,
                   dest_address, amount, asset_name, timestamp
            FROM logs
            {prewhereClause}
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

        var totalCount = await countTask;
        return new PaginatedResponse<TransferDto>(
            items, page, limit, totalCount, (int)Math.Ceiling((double)totalCount / limit));
    }

    public async Task<AddressDto> GetAddressSummaryAsync(string address, CancellationToken ct = default)
    {
        // Single query: transaction count from transactions table, transfer stats from logs table
        await using var txCmd = _connection.CreateCommand();
        txCmd.CommandText = $@"
            SELECT count()
            FROM transactions
            WHERE from_address = '{address}' OR to_address = '{address}'";
        var txCount = Convert.ToUInt32(await txCmd.ExecuteScalarAsync(ct));

        await using var logCmd = _connection.CreateCommand();
        logCmd.CommandText = $@"
            SELECT
                COALESCE(sumIf(amount, dest_address = '{address}' AND log_type = 0), 0) as incoming,
                COALESCE(sumIf(amount, source_address = '{address}' AND log_type = 0), 0) as outgoing,
                count() as transfer_count
            FROM logs
            PREWHERE source_address = '{address}' OR dest_address = '{address}'";

        ulong incomingAmount = 0, outgoingAmount = 0;
        uint transferCount = 0;
        await using var reader = await logCmd.ExecuteReaderAsync(ct);
        if (await reader.ReadAsync(ct))
        {
            incomingAmount = Convert.ToUInt64(reader.GetValue(0));
            outgoingAmount = Convert.ToUInt64(reader.GetValue(1));
            transferCount = Convert.ToUInt32(reader.GetValue(2));
        }

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

        // Pre-check input format to only run the relevant query
        var isNumber = ulong.TryParse(query, out var tickNumber);
        var is60CharLower = query.Length == 60 && query.All(c => (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9'));
        var is60CharUpper = query.Length == 60 && query.All(c => c >= 'A' && c <= 'Z');

        if (isNumber)
        {
            // Tick lookup
            await using var tickCmd = _connection.CreateCommand();
            tickCmd.CommandText = $"SELECT tick_number FROM ticks WHERE tick_number = {tickNumber} LIMIT 1";
            var tickResult = await tickCmd.ExecuteScalarAsync(ct);
            if (tickResult != null)
                results.Add(new SearchResultDto(SearchResultType.Tick, tickNumber.ToString(), $"Tick {tickNumber}"));
        }
        else if (is60CharLower)
        {
            // Transaction hash lookup (uses bloom filter index on hash)
            await using var txCmd = _connection.CreateCommand();
            txCmd.CommandText = $"SELECT hash FROM transactions PREWHERE hash = '{query}' LIMIT 1";
            var txResult = await txCmd.ExecuteScalarAsync(ct);
            if (txResult != null)
                results.Add(new SearchResultDto(SearchResultType.Transaction, query, "Transaction"));
        }
        else if (is60CharUpper)
        {
            // Address — valid format, return directly (address page handles display)
            results.Add(new SearchResultDto(SearchResultType.Address, query, "Address"));
        }
        else
        {
            // Label/name search (in-memory, no DB query)
            await _labelService.EnsureFreshDataAsync();
            var labelMatches = _labelService.SearchByLabel(query, 10);

            foreach (var match in labelMatches)
            {
                var displayName = match.Type switch
                {
                    "smartcontract" => $"[{match.Label}]",
                    "exchange" => $"#{match.Label}",
                    "tokenissuer" => $"${match.Label}",
                    _ => match.Label
                };
                results.Add(new SearchResultDto(SearchResultType.Address, match.Address, displayName));
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
        // Build WHERE clause for filtering START markers (log_type = 255 is in PREWHERE)
        var startConditions = new List<string>();
        if (epoch.HasValue)
            startConditions.Add($"epoch = {epoch.Value}");

        var startWhereClause = startConditions.Count > 0
            ? string.Join(" AND ", startConditions) + " AND"
            : "";

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
                PREWHERE log_type = 255
                WHERE {startWhereClause} JSONExtractUInt(raw_data, 'customMessage') = {LogTypes.CustomMessageOpStartDistributeRewards}
            ),
            end_markers AS (
                SELECT
                    tick_number as end_tick_number,
                    log_id as end_log_id
                FROM logs
                PREWHERE log_type = 255
                WHERE JSONExtractUInt(raw_data, 'customMessage') = {LogTypes.CustomMessageOpEndDistributeRewards}
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
                PREWHERE log_type = 0
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
                PREWHERE log_type = 255
                WHERE JSONExtractUInt(raw_data, 'customMessage') = {LogTypes.CustomMessageOpStartDistributeRewards}
            ),
            end_markers AS (
                SELECT
                    tick_number as end_tick_number,
                    log_id as end_log_id
                FROM logs
                PREWHERE log_type = 255
                WHERE JSONExtractUInt(raw_data, 'customMessage') = {LogTypes.CustomMessageOpEndDistributeRewards}
            ),
            reward_ranges AS (
                SELECT
                    s.epoch,
                    s.tick_number,
                    s.start_log_id,
                    s.timestamp,
                    min(e.end_log_id) as end_log_id
                FROM start_markers s
                INNER JOIN end_markers e ON e.end_tick_number = s.tick_number
                WHERE e.end_log_id > s.start_log_id
                GROUP BY s.epoch, s.tick_number, s.start_log_id, s.timestamp
            ),
            transfers AS (
                SELECT
                    tick_number as t_tick_number,
                    log_id as t_log_id,
                    source_address,
                    amount
                FROM logs
                PREWHERE source_address = '{contractAddress}' AND log_type = 0
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
                PREWHERE log_type = 0
                WHERE source_address != '' {epochFilterAnd}
                GROUP BY source_address
            ),
            received AS (
                SELECT
                    dest_address as address,
                    sum(amount) as received_volume,
                    count() as received_count
                FROM logs
                PREWHERE log_type = 0
                WHERE dest_address != '' {epochFilterAnd}
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
            PREWHERE log_type = 0
            WHERE dest_address = '{address}' AND source_address != ''
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
            PREWHERE log_type = 0
            WHERE source_address = '{address}' AND dest_address != ''
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
    // RICH LIST & SUPPLY DASHBOARD
    // =====================================================

    /// <summary>
    /// Get rich list (top holders by balance) using spectrum snapshot + transfer deltas
    /// </summary>
    public async Task<RichListDto> GetRichListAsync(int page = 1, int limit = 50, CancellationToken ct = default)
    {
        var hasSnapshots = await HasBalanceSnapshotsAsync(ct);
        var offset = (page - 1) * limit;

        // Get snapshot epoch
        uint snapshotEpoch = 0;
        if (hasSnapshots)
        {
            await using var epochCmd = _connection.CreateCommand();
            epochCmd.CommandText = "SELECT max(epoch) FROM spectrum_imports";
            var epochResult = await epochCmd.ExecuteScalarAsync(ct);
            if (epochResult != null && epochResult != DBNull.Value)
                snapshotEpoch = Convert.ToUInt32(epochResult);
        }

        string balanceCte;
        if (hasSnapshots)
        {
            balanceCte = @"
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
                        PREWHERE log_type = 0
                        WHERE dest_address != ''
                          AND tick_number > (SELECT tick_number FROM latest_snapshot)
                        UNION ALL
                        SELECT source_address as address, 0 as incoming, toInt64(amount) as outgoing
                        FROM logs
                        PREWHERE log_type = 0
                        WHERE source_address != ''
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
                )";
        }
        else
        {
            balanceCte = @"
                WITH current_balances AS (
                    SELECT
                        address,
                        sum(incoming) - sum(outgoing) as balance
                    FROM (
                        SELECT dest_address as address, toInt64(amount) as incoming, 0 as outgoing
                        FROM logs PREWHERE log_type = 0 WHERE dest_address != ''
                        UNION ALL
                        SELECT source_address as address, 0 as incoming, toInt64(amount) as outgoing
                        FROM logs PREWHERE log_type = 0 WHERE source_address != ''
                    )
                    GROUP BY address
                    HAVING balance > 0
                )";
        }

        // Get total count and balance
        await using var countCmd = _connection.CreateCommand();
        countCmd.CommandText = $@"
            {balanceCte}
            SELECT count() as total_count, sum(balance) as total_balance
            FROM current_balances";

        ulong totalCount = 0;
        decimal totalBalance = 0;
        await using (var reader = await countCmd.ExecuteReaderAsync(ct))
        {
            if (await reader.ReadAsync(ct))
            {
                totalCount = ToUInt64(reader.GetValue(0));
                totalBalance = ToDecimal(reader.GetValue(1));
            }
        }

        // Get paginated entries
        await using var listCmd = _connection.CreateCommand();
        listCmd.CommandText = $@"
            {balanceCte}
            SELECT address, balance
            FROM current_balances
            ORDER BY balance DESC
            LIMIT {limit} OFFSET {offset}";

        var entries = new List<RichListEntryDto>();
        var rank = offset + 1;
        await using (var reader = await listCmd.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                var addr = reader.GetString(0);
                var balance = ToDecimal(reader.GetValue(1));
                var info = _labelService.GetAddressInfo(addr);
                var pct = totalBalance > 0 ? balance / totalBalance * 100 : 0;

                entries.Add(new RichListEntryDto(
                    rank++,
                    addr,
                    info?.Label,
                    info?.Type.ToString().ToLowerInvariant(),
                    balance,
                    FormatQubicAmount(balance),
                    Math.Round(pct, 4)
                ));
            }
        }

        var totalPages = totalCount > 0 ? (int)Math.Ceiling((double)totalCount / limit) : 1;

        return new RichListDto(entries, page, limit, totalCount, totalPages, totalBalance, snapshotEpoch);
    }

    /// <summary>
    /// Get supply dashboard data (circulating supply, burns, emissions)
    /// </summary>
    private static readonly decimal IssuanceRatePerEpoch = Qubic.Core.QubicConstants.IssuanceRate;
    private static readonly decimal MaxSupply = Qubic.Core.QubicConstants.MaxSupply;
    private static readonly string ArbAddress = Qubic.Core.QubicConstants.ArbitratorIdentity;

    public async Task<SupplyDashboardDto> GetSupplyDashboardAsync(CancellationToken ct = default)
    {
        // Query 1: Circulating supply from latest spectrum import
        var supplyTask = Task.Run(async () =>
        {
            await using var cmd = _connection.CreateCommand();
            cmd.CommandText = @"
                SELECT epoch, total_balance
                FROM spectrum_imports
                ORDER BY epoch DESC
                LIMIT 1";
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            if (await reader.ReadAsync(ct))
                return (Convert.ToUInt32(reader.GetValue(0)), ToDecimal(reader.GetValue(1)));
            return ((uint)0, 0m);
        }, ct);

        // Query 2: Computor emission totals per epoch
        var emissionTask = Task.Run(async () =>
        {
            await using var cmd = _connection.CreateCommand();
            cmd.CommandText = @"
                SELECT epoch, sum(emission_amount) as total_emission, count() as computor_count
                FROM computor_emissions
                GROUP BY epoch
                ORDER BY epoch DESC
                LIMIT 50";
            var items = new List<(uint Epoch, decimal ComputorEmission, int ComputorCount)>();
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                items.Add((
                    Convert.ToUInt32(reader.GetValue(0)),
                    ToDecimal(reader.GetValue(1)),
                    Convert.ToInt32(reader.GetValue(2))
                ));
            }
            return items;
        }, ct);

        // Query 3: Non-computor emission transfers (ARB + donations) per epoch
        // These are transfers from the zero address at emission ticks to addresses
        // that are NOT computors (i.e. ARB address and donation contract addresses)
        var nonComputorTask = Task.Run(async () =>
        {
            await using var cmd = _connection.CreateCommand();
            cmd.CommandText = $@"
                SELECT l.epoch, l.dest_address, sum(l.amount) as total_amount
                FROM logs l
                WHERE l.tick_number IN (
                    SELECT emission_tick FROM emission_imports ORDER BY epoch DESC LIMIT 50
                )
                AND l.source_address = '{AddressLabelService.BurnAddress}'
                AND l.log_type = 0
                AND l.dest_address NOT IN (
                    SELECT DISTINCT address FROM computor_emissions
                )
                GROUP BY l.epoch, l.dest_address
                ORDER BY l.epoch DESC, total_amount DESC";
            // epoch -> list of (address, amount)
            var result = new Dictionary<uint, List<(string Address, decimal Amount)>>();
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                var epoch = Convert.ToUInt32(reader.GetValue(0));
                var address = reader.GetString(1);
                var amount = ToDecimal(reader.GetValue(2));
                if (!result.ContainsKey(epoch))
                    result[epoch] = new List<(string, decimal)>();
                result[epoch].Add((address, amount));
            }
            return result;
        }, ct);

        // Query 4: Epoch count
        var epochCountTask = Task.Run(async () =>
        {
            await using var cmd = _connection.CreateCommand();
            cmd.CommandText = "SELECT count(DISTINCT epoch) FROM computor_emissions";
            return Convert.ToUInt32(await cmd.ExecuteScalarAsync(ct));
        }, ct);

        // Query 5: Burn history
        var burnHistTask = Task.Run(async () =>
        {
            await using var cmd = _connection.CreateCommand();
            cmd.CommandText = @"
                SELECT epoch, snapshot_at, burn_amount, burn_count
                FROM burn_stats_history FINAL
                ORDER BY snapshot_at DESC
                LIMIT 50";
            var items = new List<BurnDataPointDto>();
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                items.Add(new BurnDataPointDto(
                    Convert.ToUInt32(reader.GetValue(0)),
                    reader.GetDateTime(1),
                    ToUInt64(reader.GetValue(2)),
                    ToUInt64(reader.GetValue(3))
                ));
            }
            return items;
        }, ct);

        await Task.WhenAll(supplyTask, emissionTask, nonComputorTask, epochCountTask, burnHistTask);

        var (snapshotEpoch, circulatingSupply) = supplyTask.Result;
        var emissionRaw = emissionTask.Result;
        var nonComputorMap = nonComputorTask.Result;
        var epochCount = epochCountTask.Result;
        var burnHistory = burnHistTask.Result;

        // Build emission history with full breakdown
        var emissionHistory = new List<EmissionDataPointDto>();
        foreach (var (epoch, computorEmission, computorCount) in emissionRaw)
        {
            var arbRevenue = 0m;
            var donations = new List<EmissionDonationDto>();

            if (nonComputorMap.TryGetValue(epoch, out var nonComputorEntries))
            {
                foreach (var (address, amount) in nonComputorEntries)
                {
                    if (string.Equals(address, ArbAddress, StringComparison.OrdinalIgnoreCase))
                    {
                        arbRevenue = amount;
                    }
                    else
                    {
                        var label = _labelService.GetLabel(address);
                        donations.Add(new EmissionDonationDto(address, label, amount));
                    }
                }
            }

            var donationTotal = donations.Sum(d => d.Amount);

            // Fallback: if we don't have emission tick data, compute remainder
            if (!nonComputorMap.ContainsKey(epoch))
            {
                arbRevenue = IssuanceRatePerEpoch - computorEmission;
            }

            emissionHistory.Add(new EmissionDataPointDto(
                epoch,
                computorEmission,
                arbRevenue,
                donations,
                donationTotal,
                computorCount
            ));
        }

        // Total emitted = number of epochs * 1T per epoch
        var totalEmitted = epochCount * IssuanceRatePerEpoch;
        // Total burned = total emitted - circulating supply
        var totalBurned = totalEmitted - circulatingSupply;
        var latestEpochEmission = emissionHistory.Count > 0 ? emissionHistory[0].ComputorEmission : 0m;
        var supplyCapProgress = circulatingSupply / MaxSupply * 100m;

        return new SupplyDashboardDto(
            circulatingSupply,
            totalEmitted,
            totalBurned,
            latestEpochEmission,
            epochCount,
            snapshotEpoch,
            MaxSupply,
            supplyCapProgress,
            emissionHistory,
            burnHistory
        );
    }

    /// <summary>
    /// Get address activity range (first seen / last seen)
    /// </summary>
    public async Task<AddressActivityRangeDto> GetAddressActivityRangeAsync(
        string address, CancellationToken ct = default)
    {
        // Run first-seen and last-seen queries in parallel
        var firstTask = Task.Run(async () =>
        {
            await using var firstCmd = _connection.CreateCommand();
            firstCmd.CommandText = $@"
                SELECT
                    min(first_tick) as first_tick,
                    min(first_timestamp) as first_timestamp,
                    min(first_epoch) as first_epoch
                FROM address_first_seen FINAL
                WHERE address = '{address}'";

            await using var reader = await firstCmd.ExecuteReaderAsync(ct);
            if (await reader.ReadAsync(ct) && !reader.IsDBNull(0))
                return (ToUInt64(reader.GetValue(0)), (DateTime?)reader.GetDateTime(1), (uint?)Convert.ToUInt32(reader.GetValue(2)));
            return ((ulong?)null, (DateTime?)null, (uint?)null);
        }, ct);

        var lastTask = Task.Run(async () =>
        {
            await using var lastCmd = _connection.CreateCommand();
            lastCmd.CommandText = $@"
                SELECT
                    max(tick_number) as last_tick,
                    max(timestamp) as last_timestamp,
                    max(epoch) as last_epoch
                FROM logs
                PREWHERE source_address = '{address}' OR dest_address = '{address}'";

            await using var reader = await lastCmd.ExecuteReaderAsync(ct);
            if (await reader.ReadAsync(ct) && !reader.IsDBNull(0))
            {
                var tick = ToUInt64(reader.GetValue(0));
                if (tick > 0)
                    return (tick, (DateTime?)reader.GetDateTime(1), (uint?)Convert.ToUInt32(reader.GetValue(2)));
            }
            return ((ulong?)null, (DateTime?)null, (uint?)null);
        }, ct);

        await Task.WhenAll(firstTask, lastTask);
        var (firstTick, firstTimestamp, firstEpoch) = firstTask.Result;
        var (lastTick, lastTimestamp, lastEpoch) = lastTask.Result;

        return new AddressActivityRangeDto(
            firstTick, firstTimestamp, firstEpoch,
            lastTick, lastTimestamp, lastEpoch);
    }

    /// <summary>
    /// Get epoch countdown information (current epoch, average duration, estimated end)
    /// </summary>
    public async Task<EpochCountdownDto?> GetEpochCountdownInfoAsync(CancellationToken ct = default)
    {
        // Get current epoch and latest tick
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = @"
            SELECT epoch, max(tick_number) as max_tick
            FROM ticks
            GROUP BY epoch
            ORDER BY epoch DESC
            LIMIT 1";

        uint currentEpoch;
        ulong currentTick;
        await using (var reader = await cmd.ExecuteReaderAsync(ct))
        {
            if (!await reader.ReadAsync(ct))
                return null;
            currentEpoch = Convert.ToUInt32(reader.GetValue(0));
            currentTick = ToUInt64(reader.GetValue(1));
        }

        // Epochs are fixed: Wednesday 12:00 UTC to Wednesday 12:00 UTC
        var now = DateTime.UtcNow;
        var epochStart = GetCurrentWednesdayNoon(now);
        var epochEnd = epochStart.AddDays(7);
        var durationMs = TimeSpan.FromDays(7).TotalMilliseconds;

        return new EpochCountdownDto(
            currentEpoch,
            epochStart,
            durationMs,
            epochEnd,
            currentTick);
    }

    /// <summary>
    /// Get the most recent Wednesday 12:00 UTC at or before the given time.
    /// </summary>
    private static DateTime GetCurrentWednesdayNoon(DateTime utcNow)
    {
        // Find the most recent Wednesday 12:00 UTC
        var candidate = utcNow.Date.AddHours(12); // today at 12:00 UTC

        // Walk back to Wednesday
        while (candidate.DayOfWeek != DayOfWeek.Wednesday)
            candidate = candidate.AddDays(-1);

        // If that Wednesday noon is in the future, go back one week
        if (candidate > utcNow)
            candidate = candidate.AddDays(-7);

        return candidate;
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
                PREWHERE log_type = 0
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
                PREWHERE log_type = 0
                WHERE amount > 0 AND dest_address IN ('{addressList}')
                GROUP BY epoch
            ),
            outflows AS (
                SELECT
                    epoch,
                    sum(amount) as outflow_volume,
                    count() as outflow_count
                FROM logs
                PREWHERE log_type = 0
                WHERE amount > 0 AND source_address IN ('{addressList}')
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
    /// Get holder distribution with concentration metrics (top 10/50/100 holders) in a single query pass.
    /// </summary>
    public async Task<HolderDistributionDto> GetHolderDistributionWithConcentrationAsync(CancellationToken ct = default)
    {
        var hasSnapshots = await HasBalanceSnapshotsAsync(ct);

        await using var cmd = _connection.CreateCommand();

        // Single query: brackets + concentration metrics from one current_balances CTE
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
                        PREWHERE log_type = 0
                        WHERE dest_address != ''
                          AND tick_number > (SELECT tick_number FROM latest_snapshot)
                        UNION ALL
                        SELECT source_address as address, 0 as incoming, toInt64(amount) as outgoing
                        FROM logs
                        PREWHERE log_type = 0
                        WHERE source_address != ''
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
                )
                SELECT
                    -- Bracket counts
                    countIf(balance >= 100000000000) as whales,
                    countIf(balance >= 20000000000 AND balance < 100000000000) as large,
                    countIf(balance >= 5000000000 AND balance < 20000000000) as medium,
                    countIf(balance >= 500000000 AND balance < 5000000000) as small,
                    countIf(balance < 500000000) as micro,
                    -- Bracket balances
                    sumIf(balance, balance >= 100000000000) as whale_balance,
                    sumIf(balance, balance >= 20000000000 AND balance < 100000000000) as large_balance,
                    sumIf(balance, balance >= 5000000000 AND balance < 20000000000) as medium_balance,
                    sumIf(balance, balance >= 500000000 AND balance < 5000000000) as small_balance,
                    sumIf(balance, balance < 500000000) as micro_balance,
                    -- Totals
                    sum(balance) as total_balance,
                    count() as total_holders,
                    -- Concentration metrics
                    sumIf(balance, rank <= 10) as top10,
                    sumIf(balance, rank <= 50) as top50,
                    sumIf(balance, rank <= 100) as top100
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
                        FROM logs PREWHERE log_type = 0 WHERE dest_address != ''
                        UNION ALL
                        SELECT source_address as address, 0 as incoming, toInt64(amount) as outgoing
                        FROM logs PREWHERE log_type = 0 WHERE source_address != ''
                    )
                    GROUP BY address
                    HAVING balance > 0
                ),
                ranked AS (
                    SELECT balance, row_number() OVER (ORDER BY balance DESC) as rank
                    FROM balances
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
                    count() as total_holders,
                    sumIf(balance, rank <= 10) as top10,
                    sumIf(balance, rank <= 50) as top50,
                    sumIf(balance, rank <= 100) as top100
                FROM ranked";
        }

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        if (await reader.ReadAsync(ct))
        {
            var totalBalance = ToDecimal(reader.GetValue(10));
            var top10 = ToDecimal(reader.GetValue(12));
            var top50 = ToDecimal(reader.GetValue(13));
            var top100 = ToDecimal(reader.GetValue(14));

            var concentration = new ConcentrationMetricsDto(
                top10,
                totalBalance > 0 ? top10 / totalBalance * 100 : 0,
                top50,
                totalBalance > 0 ? top50 / totalBalance * 100 : 0,
                top100,
                totalBalance > 0 ? top100 / totalBalance * 100 : 0
            );

            return new HolderDistributionDto(
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
                totalBalance,
                concentration
            );
        }

        return new HolderDistributionDto(new List<HolderBracketDto>(), 0, 0);
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

    // =====================================================
    // CUSTOM FLOW TRACKING
    // =====================================================

    public async Task CreateCustomFlowJobAsync(string jobId, List<string> addresses, List<ulong> balances,
        ulong startTick, string alias, byte maxHops, CancellationToken ct = default)
    {
        var addrArray = string.Join(",", addresses.Select(a => $"'{EscapeSql(a)}'"));
        var balArray = string.Join(",", balances);

        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            INSERT INTO custom_flow_jobs (
                job_id, alias, start_tick, addresses, balances, max_hops,
                status, last_processed_tick, total_hops_recorded,
                total_terminal_amount, total_pending_amount, error_message
            ) VALUES (
                '{EscapeSql(jobId)}',
                '{EscapeSql(alias)}',
                {startTick},
                [{addrArray}],
                [{balArray}],
                {maxHops},
                'pending',
                0, 0, 0, 0, ''
            )";
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public async Task<CustomFlowJobDto?> GetCustomFlowJobAsync(string jobId, CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT job_id, alias, start_tick, addresses, balances, max_hops, status,
                   last_processed_tick, total_hops_recorded, total_terminal_amount, total_pending_amount,
                   error_message, created_at, updated_at
            FROM custom_flow_jobs FINAL
            WHERE job_id = '{EscapeSql(jobId)}'";

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        if (await reader.ReadAsync(ct))
        {
            return new CustomFlowJobDto(
                JobId: reader.GetString(0),
                Alias: reader.GetString(1),
                StartTick: reader.GetFieldValue<ulong>(2),
                Addresses: ((string[])reader.GetValue(3)).ToList(),
                Balances: ((ulong[])reader.GetValue(4)).ToList(),
                MaxHops: reader.GetFieldValue<byte>(5),
                Status: reader.GetString(6),
                LastProcessedTick: reader.GetFieldValue<ulong>(7),
                TotalHopsRecorded: reader.GetFieldValue<ulong>(8),
                TotalTerminalAmount: ToBigDecimal(reader.GetValue(9)),
                TotalPendingAmount: ToBigDecimal(reader.GetValue(10)),
                ErrorMessage: reader.GetString(11) is { Length: > 0 } err ? err : null,
                CreatedAt: reader.GetFieldValue<DateTime>(12),
                UpdatedAt: reader.GetFieldValue<DateTime>(13)
            );
        }
        return null;
    }

    public async Task<List<CustomFlowHopDto>> GetCustomFlowHopsAsync(
        string jobId, int maxDepth, CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT job_id, tick_number, timestamp, tx_hash,
                   source_address, dest_address, amount,
                   origin_address, hop_level, dest_type, dest_label
            FROM custom_flow_hops FINAL
            WHERE job_id = '{EscapeSql(jobId)}'
              AND hop_level <= {maxDepth}
            ORDER BY hop_level, tick_number";

        var result = new List<CustomFlowHopDto>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var sourceAddr = reader.GetString(4);
            var destAddr = reader.GetString(5);
            result.Add(new CustomFlowHopDto(
                JobId: reader.GetString(0),
                TickNumber: reader.GetFieldValue<ulong>(1),
                Timestamp: reader.GetFieldValue<DateTime>(2),
                TxHash: reader.GetString(3),
                SourceAddress: sourceAddr,
                SourceLabel: _labelService.GetLabel(sourceAddr),
                DestAddress: destAddr,
                DestLabel: reader.GetString(10) is { Length: > 0 } lbl ? lbl : _labelService.GetLabel(destAddr),
                DestType: reader.GetString(9) is { Length: > 0 } dt ? dt : null,
                Amount: Convert.ToDecimal(reader.GetFieldValue<ulong>(6)),
                OriginAddress: reader.GetString(7),
                HopLevel: reader.GetFieldValue<byte>(8)
            ));
        }
        return result;
    }

    public async Task<List<CustomFlowTrackingStateDto>> GetCustomFlowStatesAsync(
        string jobId, CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT job_id, address, origin_address, address_type,
                   received_amount, sent_amount, pending_amount,
                   hop_level, last_tick, is_terminal, is_complete
            FROM custom_flow_state FINAL
            WHERE job_id = '{EscapeSql(jobId)}'
            ORDER BY hop_level ASC, address";

        var result = new List<CustomFlowTrackingStateDto>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            result.Add(new CustomFlowTrackingStateDto(
                JobId: reader.GetString(0),
                Address: reader.GetString(1),
                OriginAddress: reader.GetString(2),
                AddressType: reader.GetString(3),
                ReceivedAmount: ToBigDecimal(reader.GetValue(4)),
                SentAmount: ToBigDecimal(reader.GetValue(5)),
                PendingAmount: ToBigDecimal(reader.GetValue(6)),
                HopLevel: reader.GetFieldValue<byte>(7),
                LastTick: reader.GetFieldValue<ulong>(8),
                IsTerminal: reader.GetFieldValue<byte>(9) == 1,
                IsComplete: reader.GetFieldValue<byte>(10) == 1
            ));
        }
        return result;
    }

    /// <summary>
    /// Get source addresses that sent at least minAmount to exchange addresses in the last N epochs.
    /// depth=1: direct senders to exchanges (deposit addresses).
    /// depth=2: one hop before — the actual user wallets that funded deposit addresses.
    /// Includes clustering to detect addresses that likely belong to the same entity.
    /// </summary>
    public async Task<ExchangeSendersDto> GetExchangeSendersAsync(
        uint epochs = 5, ulong minAmount = 1_000_000_000, int limit = 100, int depth = 1,
        CancellationToken ct = default)
    {
        await _labelService.EnsureFreshDataAsync();
        var exchangeAddresses = _labelService.GetAddressesByType(AddressType.Exchange);

        if (!exchangeAddresses.Any())
            return new ExchangeSendersDto(new List<AddressClusterDto>(), new List<ExchangeSenderDto>(), epochs, minAmount);

        var exchangeAddrSet = exchangeAddresses.Select(e => e.Address).ToHashSet();
        var addressList = string.Join("','", exchangeAddrSet);

        // Build exclude list for clustering: exchanges + known addresses
        var knownAddresses = _labelService.GetAddressesByType(AddressType.Known);
        var excludeFromClustering = new HashSet<string>(exchangeAddrSet);
        foreach (var k in knownAddresses)
            excludeFromClustering.Add(k.Address);
        var excludeList = string.Join("','", excludeFromClustering);

        // Get current epoch
        await using var epochCmd = _connection.CreateCommand();
        epochCmd.CommandText = "SELECT max(epoch) FROM ticks";
        var currentEpoch = Convert.ToUInt32(await epochCmd.ExecuteScalarAsync(ct));

        var minEpoch = currentEpoch >= epochs ? currentEpoch - epochs + 1 : 1;

        List<ExchangeSenderDto> senders;
        HashSet<string> senderAddresses;

        if (depth >= 2)
        {
            // Two-hop: find deposit addresses first, then their funders
            (senders, senderAddresses) = await GetExchangeSendersDepth2Async(
                addressList, exchangeAddrSet, minEpoch, currentEpoch, minAmount, limit, ct);
        }
        else
        {
            // Direct senders to exchanges
            (senders, senderAddresses) = await GetExchangeSendersDepth1Async(
                addressList, minEpoch, currentEpoch, minAmount, limit, ct);
        }

        // Query 2: Clustering — only for unknown addresses (exclude exchange/known)
        var clusterLinks = new List<ClusterLinkDto>();
        var clusterCandidates = senderAddresses.Where(a => !excludeFromClustering.Contains(a)).ToHashSet();

        if (clusterCandidates.Count >= 2)
        {
            var candidateList = string.Join("','", clusterCandidates);

            // Direct transfers between candidate addresses
            await using var directCmd = _connection.CreateCommand();
            directCmd.CommandText = $@"
                SELECT
                    source_address, dest_address, sum(amount) as volume
                FROM logs FINAL
                WHERE log_type = 0
                  AND amount > 0
                  AND source_address IN ('{candidateList}')
                  AND dest_address IN ('{candidateList}')
                  AND epoch >= {minEpoch}
                GROUP BY source_address, dest_address
                ORDER BY volume DESC";

            await using var directReader = await directCmd.ExecuteReaderAsync(ct);
            while (await directReader.ReadAsync(ct))
            {
                clusterLinks.Add(new ClusterLinkDto(
                    Address1: directReader.GetString(0),
                    Address2: directReader.GetString(1),
                    Reason: "direct_transfer",
                    Volume: ToDecimal(directReader.GetValue(2))
                ));
            }

            // Common funding sources: addresses that funded multiple candidates
            // Exclude exchange/known addresses as funders too
            await using var funderCmd = _connection.CreateCommand();
            funderCmd.CommandText = $@"
                SELECT
                    source_address as funder,
                    groupArray(dest_address) as funded_addresses,
                    sum(amount) as total_volume
                FROM logs FINAL
                WHERE log_type = 0
                  AND amount > 0
                  AND dest_address IN ('{candidateList}')
                  AND source_address NOT IN ('{excludeList}')
                  AND source_address NOT IN ('{candidateList}')
                  AND epoch >= {minEpoch}
                GROUP BY source_address
                HAVING length(groupUniqArray(dest_address)) >= 2
                ORDER BY total_volume DESC
                LIMIT 50";

            await using var funderReader = await funderCmd.ExecuteReaderAsync(ct);
            while (await funderReader.ReadAsync(ct))
            {
                var funder = funderReader.GetString(0);
                var fundedAddrs = ((string[])funderReader.GetValue(1)).Distinct().Where(a => clusterCandidates.Contains(a)).ToList();
                var volume = ToDecimal(funderReader.GetValue(2));

                for (var i = 0; i < fundedAddrs.Count; i++)
                {
                    for (var j = i + 1; j < fundedAddrs.Count; j++)
                    {
                        var funderLabel = _labelService.GetLabel(funder);
                        var reason = funderLabel != null
                            ? $"common_funder:{funderLabel}"
                            : $"common_funder:{funder[..8]}...{funder[^8..]}";
                        clusterLinks.Add(new ClusterLinkDto(
                            Address1: fundedAddrs[i],
                            Address2: fundedAddrs[j],
                            Reason: reason,
                            Volume: volume
                        ));
                    }
                }
            }
        }

        // Build volume lookup for cluster totals
        var senderVolumes = senders.ToDictionary(s => s.Address, s => s.TotalVolume);

        // Build clusters using union-find
        var clusters = BuildClusters(clusterCandidates, clusterLinks, senderVolumes);

        // Assign cluster IDs to senders
        var addressToCluster = new Dictionary<string, int>();
        foreach (var cluster in clusters)
            foreach (var addr in cluster.Addresses)
                addressToCluster[addr] = cluster.ClusterId;

        senders = senders.Select(s =>
            addressToCluster.TryGetValue(s.Address, out var cid)
                ? s with { ClusterId = cid }
                : s
        ).ToList();

        return new ExchangeSendersDto(clusters, senders, epochs, minAmount);
    }

    private async Task<(List<ExchangeSenderDto> Senders, HashSet<string> Addresses)> GetExchangeSendersDepth1Async(
        string exchangeList, uint minEpoch, uint maxEpoch, ulong minAmount, int limit,
        CancellationToken ct)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT
                source_address,
                sum(amount) as total_volume,
                count() as tx_count,
                uniq(epoch) as epoch_count
            FROM logs FINAL
            WHERE log_type = 0
              AND amount >= {minAmount}
              AND dest_address IN ('{exchangeList}')
              AND epoch >= {minEpoch}
              AND epoch <= {maxEpoch}
              AND source_address NOT IN ('{exchangeList}')
            GROUP BY source_address
            HAVING total_volume >= {minAmount}
            ORDER BY total_volume DESC
            LIMIT {limit}";

        var senders = new List<ExchangeSenderDto>();
        var addresses = new HashSet<string>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var address = reader.GetString(0);
            addresses.Add(address);
            var label = _labelService.GetLabel(address);
            var info = _labelService.GetAddressInfo(address);
            var type = info?.Type.ToString().ToLowerInvariant();
            var totalVolume = ToDecimal(reader.GetValue(1));

            senders.Add(new ExchangeSenderDto(
                Address: address,
                Label: label,
                Type: type,
                TotalVolume: totalVolume,
                TotalVolumeFormatted: FormatQubicAmount(totalVolume),
                TransactionCount: Convert.ToUInt32(reader.GetValue(2)),
                EpochCount: Convert.ToUInt32(reader.GetValue(3))
            ));
        }
        return (senders, addresses);
    }

    private async Task<(List<ExchangeSenderDto> Senders, HashSet<string> Addresses)> GetExchangeSendersDepth2Async(
        string exchangeList, HashSet<string> exchangeAddrSet, uint minEpoch, uint maxEpoch,
        ulong minAmount, int limit, CancellationToken ct)
    {
        // Step 1: Find deposit addresses (addresses that sent to exchanges)
        await using var depositCmd = _connection.CreateCommand();
        depositCmd.CommandText = $@"
            SELECT DISTINCT source_address
            FROM logs FINAL
            WHERE log_type = 0
              AND amount > 0
              AND dest_address IN ('{exchangeList}')
              AND source_address NOT IN ('{exchangeList}')
              AND epoch >= {minEpoch}
              AND epoch <= {maxEpoch}";

        var depositAddresses = new HashSet<string>();
        await using var depositReader = await depositCmd.ExecuteReaderAsync(ct);
        while (await depositReader.ReadAsync(ct))
            depositAddresses.Add(depositReader.GetString(0));

        if (depositAddresses.Count == 0)
            return (new List<ExchangeSenderDto>(), new HashSet<string>());

        var depositList = string.Join("','", depositAddresses);

        // Step 2: Find addresses that funded those deposit addresses
        // Exclude exchanges and the deposit addresses themselves
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT
                source_address,
                sum(amount) as total_volume,
                count() as tx_count,
                uniq(epoch) as epoch_count,
                groupUniqArray(dest_address) as via_deposits
            FROM logs FINAL
            WHERE log_type = 0
              AND amount > 0
              AND dest_address IN ('{depositList}')
              AND source_address NOT IN ('{exchangeList}')
              AND source_address NOT IN ('{depositList}')
              AND epoch >= {minEpoch}
              AND epoch <= {maxEpoch}
            GROUP BY source_address
            HAVING total_volume >= {minAmount}
            ORDER BY total_volume DESC
            LIMIT {limit}";

        var senders = new List<ExchangeSenderDto>();
        var addresses = new HashSet<string>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var address = reader.GetString(0);
            addresses.Add(address);
            var label = _labelService.GetLabel(address);
            var info = _labelService.GetAddressInfo(address);
            var type = info?.Type.ToString().ToLowerInvariant();
            var totalVolume = ToDecimal(reader.GetValue(1));
            var viaDeposits = ((string[])reader.GetValue(4))
                .Where(d => depositAddresses.Contains(d))
                .ToList();

            senders.Add(new ExchangeSenderDto(
                Address: address,
                Label: label,
                Type: type,
                TotalVolume: totalVolume,
                TotalVolumeFormatted: FormatQubicAmount(totalVolume),
                TransactionCount: Convert.ToUInt32(reader.GetValue(2)),
                EpochCount: Convert.ToUInt32(reader.GetValue(3)),
                ViaDepositAddresses: viaDeposits.Count > 0 ? viaDeposits : null
            ));
        }
        return (senders, addresses);
    }

    private static List<AddressClusterDto> BuildClusters(
        HashSet<string> addresses, List<ClusterLinkDto> links, Dictionary<string, decimal> volumes)
    {
        if (links.Count == 0)
            return new List<AddressClusterDto>();

        // Union-find
        var parent = new Dictionary<string, string>();
        foreach (var addr in addresses)
            parent[addr] = addr;

        string Find(string x)
        {
            while (parent[x] != x)
            {
                parent[x] = parent[parent[x]];
                x = parent[x];
            }
            return x;
        }

        void Union(string a, string b)
        {
            var ra = Find(a);
            var rb = Find(b);
            if (ra != rb) parent[ra] = rb;
        }

        foreach (var link in links)
        {
            if (addresses.Contains(link.Address1) && addresses.Contains(link.Address2))
                Union(link.Address1, link.Address2);
        }

        // Group by root
        var groups = new Dictionary<string, List<string>>();
        foreach (var addr in addresses)
        {
            var root = Find(addr);
            if (!groups.ContainsKey(root))
                groups[root] = new List<string>();
            groups[root].Add(addr);
        }

        // Only return clusters with 2+ members, sorted by total volume desc
        var clusterId = 1;
        var result = new List<AddressClusterDto>();
        foreach (var (_, members) in groups
            .Where(g => g.Value.Count >= 2)
            .OrderByDescending(g => g.Value.Sum(a => volumes.GetValueOrDefault(a, 0))))
        {
            var memberSet = members.ToHashSet();
            var clusterLinks = links
                .Where(l => memberSet.Contains(l.Address1) && memberSet.Contains(l.Address2))
                .ToList();
            var totalVolume = members.Sum(a => volumes.GetValueOrDefault(a, 0));

            result.Add(new AddressClusterDto(clusterId++, members, clusterLinks, totalVolume, FormatQubicAmount(totalVolume)));
        }

        return result;
    }

    // =====================================================
    // TRANSACTION GRAPH
    // =====================================================

    public async Task<TransactionGraphDto> GetAddressGraphAsync(
        string address, int hops = 1, int limit = 20, CancellationToken ct = default)
    {
        var nodes = new Dictionary<string, GraphNodeDto>();
        var links = new List<GraphLinkDto>();
        var addressInfo = _labelService.GetAddressInfo(address);

        // Add center node
        nodes[address] = new GraphNodeDto(
            Address: address,
            Label: addressInfo?.Label,
            Type: addressInfo?.Type.ToString().ToLowerInvariant(),
            TotalVolume: 0,
            Depth: 0
        );

        // Fetch hop-1 counterparties
        var hop1Addresses = await FetchCounterpartiesAsync(address, limit, ct);
        foreach (var (counterparty, amount, txCount) in hop1Addresses)
        {
            var info = _labelService.GetAddressInfo(counterparty);
            nodes.TryAdd(counterparty, new GraphNodeDto(
                Address: counterparty,
                Label: info?.Label,
                Type: info?.Type.ToString().ToLowerInvariant(),
                TotalVolume: amount,
                Depth: 1
            ));
            links.Add(new GraphLinkDto(address, counterparty, amount, txCount));
        }

        // Fetch hop-2 if requested — parallelize all hop-2 queries
        if (hops >= 2)
        {
            var hop1Addrs = hop1Addresses.Select(h => h.address).Take(10).ToList();
            var hop2Tasks = hop1Addrs.Select(async hop1Addr =>
            {
                var hop2 = await FetchCounterpartiesAsync(hop1Addr, 5, ct);
                return (hop1Addr, hop2);
            });
            var hop2Results = await Task.WhenAll(hop2Tasks);

            foreach (var (hop1Addr, hop2) in hop2Results)
            {
                foreach (var (counterparty, amount, txCount) in hop2)
                {
                    if (counterparty == address) continue; // Skip center node
                    var info = _labelService.GetAddressInfo(counterparty);
                    nodes.TryAdd(counterparty, new GraphNodeDto(
                        Address: counterparty,
                        Label: info?.Label,
                        Type: info?.Type.ToString().ToLowerInvariant(),
                        TotalVolume: amount,
                        Depth: 2
                    ));
                    links.Add(new GraphLinkDto(hop1Addr, counterparty, amount, txCount));
                }
            }
        }

        return new TransactionGraphDto(nodes.Values.ToList(), links);
    }

    private async Task<List<(string address, decimal amount, uint txCount)>> FetchCounterpartiesAsync(
        string address, int limit, CancellationToken ct)
    {
        var results = new List<(string, decimal, uint)>();

        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT counterparty, sum(amount) AS total_amount, count() AS tx_count
            FROM (
                SELECT dest_address AS counterparty, amount
                FROM logs
                PREWHERE source_address = {{address:String}} AND log_type = 0
                WHERE amount > 0
                UNION ALL
                SELECT source_address AS counterparty, amount
                FROM logs
                PREWHERE dest_address = {{address:String}} AND log_type = 0
                WHERE amount > 0
            )
            GROUP BY counterparty
            ORDER BY total_amount DESC
            LIMIT {{limit:UInt32}}";
        cmd.Parameters.Add(new ClickHouse.Client.ADO.Parameters.ClickHouseDbParameter
            { ParameterName = "address", Value = address });
        cmd.Parameters.Add(new ClickHouse.Client.ADO.Parameters.ClickHouseDbParameter
            { ParameterName = "limit", Value = (uint)limit });

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            results.Add((
                reader.GetString(0),
                ToDecimal(reader.GetValue(1)),
                Convert.ToUInt32(reader.GetValue(2))
            ));
        }
        return results;
    }

    // =====================================================
    // ASSET EXPLORER
    // =====================================================

    public async Task<List<AssetSummaryDto>> GetAssetsAsync(CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = @"
            SELECT
                asset_name,
                issuer_address,
                number_of_decimal_places,
                sumIf(number_of_shares, record_type = 'ownership') AS total_supply,
                countDistinctIf(holder_address, record_type = 'possession' AND number_of_shares > 0) AS holder_count
            FROM asset_snapshots FINAL
            WHERE epoch = (SELECT max(epoch) FROM universe_imports)
              AND record_type IN ('issuance', 'ownership', 'possession')
            GROUP BY asset_name, issuer_address, number_of_decimal_places
            HAVING asset_name != ''
            ORDER BY total_supply DESC";

        var results = new List<AssetSummaryDto>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var issuerAddress = reader.GetString(1);
            var issuerInfo = _labelService.GetAddressInfo(issuerAddress);

            results.Add(new AssetSummaryDto(
                AssetName: reader.GetString(0),
                IssuerAddress: issuerAddress,
                IssuerLabel: issuerInfo?.Label,
                NumberOfDecimalPlaces: Convert.ToInt32(reader.GetValue(2)),
                TotalSupply: Convert.ToInt64(reader.GetValue(3)),
                HolderCount: Convert.ToInt32(reader.GetValue(4))
            ));
        }
        return results;
    }

    public async Task<AssetDetailDto?> GetAssetDetailAsync(string assetName, string? issuer = null, CancellationToken ct = default)
    {
        // Single query: embed epoch subquery + return epoch as extra column
        var issuerFilter = issuer != null ? "AND issuer_address = {issuer:String}" : "";
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT
                asset_name,
                issuer_address,
                number_of_decimal_places,
                sumIf(number_of_shares, record_type = 'ownership') AS total_supply,
                countDistinctIf(holder_address, record_type = 'possession' AND number_of_shares > 0) AS holder_count,
                (SELECT max(epoch) FROM universe_imports) AS snapshot_epoch
            FROM asset_snapshots FINAL
            WHERE epoch = (SELECT max(epoch) FROM universe_imports)
              AND asset_name = {{name:String}}
              {issuerFilter}
              AND record_type IN ('issuance', 'ownership', 'possession')
            GROUP BY asset_name, issuer_address, number_of_decimal_places
            LIMIT 1";
        cmd.Parameters.Add(new ClickHouse.Client.ADO.Parameters.ClickHouseDbParameter
            { ParameterName = "name", Value = assetName });
        if (issuer != null)
            cmd.Parameters.Add(new ClickHouse.Client.ADO.Parameters.ClickHouseDbParameter
                { ParameterName = "issuer", Value = issuer });

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct))
            return null;

        var issuerAddress = reader.GetString(1);
        var numberOfDecimalPlaces = Convert.ToInt32(reader.GetValue(2));
        var totalSupply = Convert.ToInt64(reader.GetValue(3));
        var holderCount = Convert.ToInt32(reader.GetValue(4));
        var latestEpoch = Convert.ToUInt32(reader.GetValue(5));

        var issuerInfo = _labelService.GetAddressInfo(issuerAddress);

        // Get top holders (by possession)
        var topHolders = await GetAssetHoldersInternalAsync(latestEpoch, assetName, issuerAddress, 1, 20, ct);

        return new AssetDetailDto(
            AssetName: assetName,
            IssuerAddress: issuerAddress,
            IssuerLabel: issuerInfo?.Label,
            NumberOfDecimalPlaces: numberOfDecimalPlaces,
            TotalSupply: totalSupply,
            HolderCount: holderCount,
            SnapshotEpoch: latestEpoch,
            TopHolders: topHolders.Holders
        );
    }

    public async Task<AssetHoldersPageDto> GetAssetHoldersAsync(
        string assetName, string? issuer, int page, int limit, CancellationToken ct = default)
    {
        // Single query to get epoch + issuer if needed
        await using var metaCmd = _connection.CreateCommand();
        metaCmd.CommandText = $@"
            SELECT
                (SELECT max(epoch) FROM universe_imports) AS latest_epoch,
                (SELECT DISTINCT issuer_address FROM asset_snapshots FINAL
                 WHERE epoch = (SELECT max(epoch) FROM universe_imports)
                   AND asset_name = {{name:String}} AND record_type = 'issuance'
                 LIMIT 1) AS resolved_issuer";
        metaCmd.Parameters.Add(new ClickHouse.Client.ADO.Parameters.ClickHouseDbParameter
            { ParameterName = "name", Value = assetName });

        await using var metaReader = await metaCmd.ExecuteReaderAsync(ct);
        if (!await metaReader.ReadAsync(ct) || metaReader.IsDBNull(0))
            return new AssetHoldersPageDto(new List<AssetHolderDetailDto>(), page, limit, 0, 0);

        var latestEpoch = Convert.ToUInt32(metaReader.GetValue(0));
        issuer ??= metaReader.IsDBNull(1) ? "" : metaReader.GetString(1);

        return await GetAssetHoldersInternalAsync(latestEpoch, assetName, issuer, page, limit, ct);
    }

    private async Task<AssetHoldersPageDto> GetAssetHoldersInternalAsync(
        uint epoch, string assetName, string issuerAddress, int page, int limit, CancellationToken ct)
    {
        var offset = (page - 1) * limit;

        // Count total holders
        await using var countCmd = _connection.CreateCommand();
        countCmd.CommandText = $@"
            SELECT count(DISTINCT holder_address)
            FROM asset_snapshots FINAL
            WHERE epoch = {{epoch:UInt32}}
              AND asset_name = {{name:String}}
              AND issuer_address = {{issuer:String}}
              AND record_type = 'possession'
              AND number_of_shares > 0";
        countCmd.Parameters.Add(new ClickHouse.Client.ADO.Parameters.ClickHouseDbParameter
            { ParameterName = "epoch", Value = epoch });
        countCmd.Parameters.Add(new ClickHouse.Client.ADO.Parameters.ClickHouseDbParameter
            { ParameterName = "name", Value = assetName });
        countCmd.Parameters.Add(new ClickHouse.Client.ADO.Parameters.ClickHouseDbParameter
            { ParameterName = "issuer", Value = issuerAddress });
        var totalCount = Convert.ToInt32(await countCmd.ExecuteScalarAsync(ct));

        // Get holders with both ownership and possession shares
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT
                holder_address,
                sumIf(number_of_shares, record_type = 'ownership') AS owned,
                sumIf(number_of_shares, record_type = 'possession') AS possessed
            FROM asset_snapshots FINAL
            WHERE epoch = {{epoch:UInt32}}
              AND asset_name = {{name:String}}
              AND issuer_address = {{issuer:String}}
              AND record_type IN ('ownership', 'possession')
              AND number_of_shares > 0
            GROUP BY holder_address
            HAVING possessed > 0
            ORDER BY possessed DESC
            LIMIT {{limit:UInt32}} OFFSET {{offset:UInt32}}";
        cmd.Parameters.Add(new ClickHouse.Client.ADO.Parameters.ClickHouseDbParameter
            { ParameterName = "epoch", Value = epoch });
        cmd.Parameters.Add(new ClickHouse.Client.ADO.Parameters.ClickHouseDbParameter
            { ParameterName = "name", Value = assetName });
        cmd.Parameters.Add(new ClickHouse.Client.ADO.Parameters.ClickHouseDbParameter
            { ParameterName = "issuer", Value = issuerAddress });
        cmd.Parameters.Add(new ClickHouse.Client.ADO.Parameters.ClickHouseDbParameter
            { ParameterName = "limit", Value = (uint)limit });
        cmd.Parameters.Add(new ClickHouse.Client.ADO.Parameters.ClickHouseDbParameter
            { ParameterName = "offset", Value = (uint)offset });

        var holders = new List<AssetHolderDetailDto>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var address = reader.GetString(0);
            var info = _labelService.GetAddressInfo(address);

            holders.Add(new AssetHolderDetailDto(
                Address: address,
                Label: info?.Label,
                Type: info?.Type.ToString().ToLowerInvariant(),
                OwnedShares: Convert.ToInt64(reader.GetValue(1)),
                PossessedShares: Convert.ToInt64(reader.GetValue(2))
            ));
        }

        var totalPages = totalCount > 0 ? (int)Math.Ceiling((double)totalCount / limit) : 0;
        return new AssetHoldersPageDto(holders, page, limit, totalCount, totalPages);
    }

    // =====================================================
    // WHALE ALERTS
    // =====================================================

    public async Task<List<WhaleAlertDto>> GetWhaleAlertsAsync(
        ulong threshold = 10_000_000_000,
        int limit = 50,
        CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT tick_number, epoch, tx_hash, source_address, dest_address, amount, timestamp
            FROM logs FINAL
            WHERE log_type = 0
              AND amount >= {{threshold:UInt64}}
            ORDER BY tick_number DESC
            LIMIT {{limit:UInt32}}";
        cmd.Parameters.Add(new ClickHouse.Client.ADO.Parameters.ClickHouseDbParameter
            { ParameterName = "threshold", Value = threshold });
        cmd.Parameters.Add(new ClickHouse.Client.ADO.Parameters.ClickHouseDbParameter
            { ParameterName = "limit", Value = (uint)limit });

        var results = new List<WhaleAlertDto>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var sourceAddress = reader.GetString(3);
            var destAddress = reader.GetString(4);
            var amount = ToDecimal(reader.GetValue(5));
            var sourceInfo = _labelService.GetAddressInfo(sourceAddress);
            var destInfo = _labelService.GetAddressInfo(destAddress);

            results.Add(new WhaleAlertDto(
                TickNumber: ToUInt64(reader.GetValue(0)),
                Epoch: Convert.ToUInt32(reader.GetValue(1)),
                TxHash: reader.GetString(2),
                SourceAddress: sourceAddress,
                SourceLabel: sourceInfo?.Label,
                SourceType: sourceInfo?.Type.ToString().ToLowerInvariant(),
                DestAddress: destAddress,
                DestLabel: destInfo?.Label,
                DestType: destInfo?.Type.ToString().ToLowerInvariant(),
                Amount: amount,
                AmountFormatted: FormatQubicAmount(amount),
                Timestamp: reader.GetDateTime(6)
            ));
        }
        return results;
    }

    // =====================================================
    // CSV EXPORT
    // =====================================================

    public async Task StreamAddressTransfersAsCsvAsync(
        string address,
        uint? epoch,
        StreamWriter writer,
        CancellationToken ct = default)
    {
        await writer.WriteLineAsync("Tick,Epoch,LogType,TxHash,Source,Dest,Amount,AssetName,Timestamp");

        await using var cmd = _connection.CreateCommand();
        var epochFilter = epoch.HasValue ? "AND epoch = {epoch:UInt32}" : "";
        cmd.CommandText = $@"
            SELECT tick_number, epoch, log_type, tx_hash, source_address, dest_address, amount, asset_name, timestamp
            FROM logs FINAL
            WHERE (source_address = {{address:String}} OR dest_address = {{address:String}})
              {epochFilter}
            ORDER BY tick_number DESC
            LIMIT 100000";
        cmd.Parameters.Add(new ClickHouse.Client.ADO.Parameters.ClickHouseDbParameter
            { ParameterName = "address", Value = address });
        if (epoch.HasValue)
            cmd.Parameters.Add(new ClickHouse.Client.ADO.Parameters.ClickHouseDbParameter
                { ParameterName = "epoch", Value = epoch.Value });

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var tick = ToUInt64(reader.GetValue(0));
            var ep = Convert.ToUInt32(reader.GetValue(1));
            var logType = Convert.ToByte(reader.GetValue(2));
            var txHash = reader.GetString(3);
            var source = reader.GetString(4);
            var dest = reader.GetString(5);
            var amount = ToDecimal(reader.GetValue(6));
            var assetName = reader.IsDBNull(7) ? "" : reader.GetString(7);
            var timestamp = reader.GetDateTime(8);

            await writer.WriteLineAsync(
                $"{tick},{ep},{logType},\"{txHash}\",\"{source}\",\"{dest}\",{amount},\"{assetName}\",{timestamp:yyyy-MM-ddTHH:mm:ssZ}");
        }
    }

    private static string FormatQubicAmount(decimal amount)
    {
        if (amount >= 1_000_000_000_000m) return $"{amount / 1_000_000_000_000m:0.##}T QU";
        if (amount >= 1_000_000_000m) return $"{amount / 1_000_000_000m:0.##}B QU";
        if (amount >= 1_000_000m) return $"{amount / 1_000_000m:0.##}M QU";
        if (amount >= 1_000m) return $"{amount / 1_000m:0.##}K QU";
        return $"{amount:0} QU";
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

    // =====================================================
    // QEARN STATS
    // =====================================================

    private const string QearnAddress = "JAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAVKHO";

    /// <summary>
    /// Get Qearn stats per epoch.
    /// Completed epochs: read from pre-computed qearn_epoch_stats (no FINAL).
    /// Current epoch: computed live from logs FINAL (cached via API cache TTL).
    /// </summary>
    public async Task<QearnStatsDto> GetQearnStatsAsync(CancellationToken ct = default)
    {
        var epochs = new List<QearnEpochStatsDto>();
        ulong allTimeBurned = 0;
        ulong allTimeInput = 0;
        ulong allTimeOutput = 0;

        // 1. Read persisted completed epochs
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = @"
            SELECT
                epoch, total_burned, burn_count,
                total_input, input_count,
                total_output, output_count,
                unique_lockers, unique_unlockers
            FROM qearn_epoch_stats
            ORDER BY epoch";

        await using (var reader = await cmd.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                var burned = ToUInt64(reader.GetValue(1));
                var input = ToUInt64(reader.GetValue(3));
                var output = ToUInt64(reader.GetValue(5));
                allTimeBurned += burned;
                allTimeInput += input;
                allTimeOutput += output;

                epochs.Add(new QearnEpochStatsDto(
                    Epoch: reader.GetFieldValue<uint>(0),
                    TotalBurned: burned,
                    BurnCount: ToUInt64(reader.GetValue(2)),
                    TotalInput: input,
                    InputCount: ToUInt64(reader.GetValue(4)),
                    TotalOutput: output,
                    OutputCount: ToUInt64(reader.GetValue(6)),
                    UniqueLockers: ToUInt64(reader.GetValue(7)),
                    UniqueUnlockers: ToUInt64(reader.GetValue(8))
                ));
            }
        }

        // 2. Compute current epoch live from logs
        var currentEpoch = await GetCurrentEpochAsync(ct);
        if (currentEpoch != null)
        {
            await using var liveCmd = _connection.CreateCommand();
            liveCmd.CommandText = $@"
                SELECT
                    sumIf(amount, log_type = 8 AND source_address = '{QearnAddress}') AS total_burned,
                    countIf(log_type = 8 AND source_address = '{QearnAddress}') AS burn_count,
                    sumIf(amount, log_type = 0 AND dest_address = '{QearnAddress}') AS total_input,
                    countIf(log_type = 0 AND dest_address = '{QearnAddress}') AS input_count,
                    sumIf(amount, log_type = 0 AND source_address = '{QearnAddress}') AS total_output,
                    countIf(log_type = 0 AND source_address = '{QearnAddress}') AS output_count,
                    uniqIf(source_address, log_type = 0 AND dest_address = '{QearnAddress}') AS unique_lockers,
                    uniqIf(dest_address, log_type = 0 AND source_address = '{QearnAddress}') AS unique_unlockers
                FROM logs FINAL
                WHERE epoch = {currentEpoch.Value}
                  AND (source_address = '{QearnAddress}' OR dest_address = '{QearnAddress}')
                  AND log_type IN (0, 8)";

            await using var liveReader = await liveCmd.ExecuteReaderAsync(ct);
            if (await liveReader.ReadAsync(ct))
            {
                var burned = ToUInt64(liveReader.GetValue(0));
                var input = ToUInt64(liveReader.GetValue(2));
                var output = ToUInt64(liveReader.GetValue(4));

                if (burned > 0 || input > 0 || output > 0)
                {
                    allTimeBurned += burned;
                    allTimeInput += input;
                    allTimeOutput += output;

                    epochs.Add(new QearnEpochStatsDto(
                        Epoch: currentEpoch.Value,
                        TotalBurned: burned,
                        BurnCount: ToUInt64(liveReader.GetValue(1)),
                        TotalInput: input,
                        InputCount: ToUInt64(liveReader.GetValue(3)),
                        TotalOutput: output,
                        OutputCount: ToUInt64(liveReader.GetValue(5)),
                        UniqueLockers: ToUInt64(liveReader.GetValue(6)),
                        UniqueUnlockers: ToUInt64(liveReader.GetValue(7))
                    ));
                }
            }
        }

        return new QearnStatsDto(epochs, allTimeBurned, allTimeInput, allTimeOutput);
    }

    /// <summary>
    /// Backfill Qearn epoch stats for all completed epochs that are missing.
    /// Returns the number of epochs backfilled.
    /// </summary>
    public async Task<(int Backfilled, List<uint> Epochs)> BackfillQearnEpochStatsAsync(CancellationToken ct = default)
    {
        const uint qearnInitialEpoch = 138;

        var currentEpoch = await GetCurrentEpochAsync(ct);
        if (currentEpoch == null || currentEpoch.Value <= qearnInitialEpoch)
            return (0, new List<uint>());

        // Get already-persisted epochs
        await using var existingCmd = _connection.CreateCommand();
        existingCmd.CommandText = "SELECT epoch FROM qearn_epoch_stats";
        var persisted = new HashSet<uint>();
        await using (var existingReader = await existingCmd.ExecuteReaderAsync(ct))
        {
            while (await existingReader.ReadAsync(ct))
                persisted.Add(existingReader.GetFieldValue<uint>(0));
        }

        var backfilled = new List<uint>();

        for (var epoch = qearnInitialEpoch; epoch < currentEpoch.Value && !ct.IsCancellationRequested; epoch++)
        {
            if (persisted.Contains(epoch)) continue;

            await using var queryCmd = _connection.CreateCommand();
            queryCmd.CommandText = $@"
                SELECT
                    sumIf(amount, log_type = 8 AND source_address = '{QearnAddress}') AS total_burned,
                    countIf(log_type = 8 AND source_address = '{QearnAddress}') AS burn_count,
                    sumIf(amount, log_type = 0 AND dest_address = '{QearnAddress}') AS total_input,
                    countIf(log_type = 0 AND dest_address = '{QearnAddress}') AS input_count,
                    sumIf(amount, log_type = 0 AND source_address = '{QearnAddress}') AS total_output,
                    countIf(log_type = 0 AND source_address = '{QearnAddress}') AS output_count,
                    uniqIf(source_address, log_type = 0 AND dest_address = '{QearnAddress}') AS unique_lockers,
                    uniqIf(dest_address, log_type = 0 AND source_address = '{QearnAddress}') AS unique_unlockers
                FROM logs FINAL
                WHERE epoch = {epoch}
                  AND (source_address = '{QearnAddress}' OR dest_address = '{QearnAddress}')
                  AND log_type IN (0, 8)";

            await using var reader = await queryCmd.ExecuteReaderAsync(ct);
            if (!await reader.ReadAsync(ct)) continue;

            var totalBurned = ToUInt64(reader.GetValue(0));
            var totalInput = ToUInt64(reader.GetValue(2));
            var totalOutput = ToUInt64(reader.GetValue(4));

            if (totalBurned == 0 && totalInput == 0 && totalOutput == 0) continue;

            await using var insertCmd = _connection.CreateCommand();
            insertCmd.CommandText = $@"
                INSERT INTO qearn_epoch_stats
                (epoch, total_burned, burn_count, total_input, input_count,
                 total_output, output_count, unique_lockers, unique_unlockers)
                VALUES
                ({epoch}, {totalBurned}, {ToUInt64(reader.GetValue(1))},
                 {totalInput}, {ToUInt64(reader.GetValue(3))},
                 {totalOutput}, {ToUInt64(reader.GetValue(5))},
                 {ToUInt64(reader.GetValue(6))}, {ToUInt64(reader.GetValue(7))})";

            await insertCmd.ExecuteNonQueryAsync(ct);
            backfilled.Add(epoch);
        }

        return (backfilled.Count, backfilled);
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
