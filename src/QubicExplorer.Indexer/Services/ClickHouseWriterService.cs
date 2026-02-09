using System.Data;
using ClickHouse.Client.ADO;
using ClickHouse.Client.Copy;
using Microsoft.Extensions.Options;
using QubicExplorer.Indexer.Configuration;
using QubicExplorer.Indexer.Models;
using QubicExplorer.Shared;
using QubicExplorer.Shared.Models;

namespace QubicExplorer.Indexer.Services;

public class ClickHouseWriterService : IDisposable
{
    private readonly ILogger<ClickHouseWriterService> _logger;
    private readonly ClickHouseOptions _options;
    private readonly IndexerOptions _indexerOptions;
    private ClickHouseConnection? _connection;
    private bool _disposed;

    private readonly List<Tick> _tickBatch = new();
    private readonly List<Transaction> _transactionBatch = new();
    private readonly List<Log> _logBatch = new();
    private readonly SemaphoreSlim _batchLock = new(1, 1);
    private DateTime _lastFlush = DateTime.UtcNow;

    public ClickHouseWriterService(
        ILogger<ClickHouseWriterService> logger,
        IOptions<ClickHouseOptions> options,
        IOptions<IndexerOptions> indexerOptions)
    {
        _logger = logger;
        _options = options.Value;
        _indexerOptions = indexerOptions.Value;
    }

    public async Task InitializeAsync(CancellationToken cancellationToken)
    {
        // First connect without a database to ensure the database and schema exist
        await EnsureSchemaAsync(cancellationToken);

        // Now connect with the database specified
        _connection = new ClickHouseConnection(_options.ConnectionString);
        await _connection.OpenAsync(cancellationToken);
        _logger.LogInformation("Connected to ClickHouse at {Host}:{Port}/{Database}",
            _options.Host, _options.Port, _options.Database);
    }

    private async Task EnsureSchemaAsync(CancellationToken cancellationToken)
    {
        using var serverConnection = new ClickHouseConnection(_options.ServerConnectionString);
        await serverConnection.OpenAsync(cancellationToken);

        // Create database
        await using (var cmd = serverConnection.CreateCommand())
        {
            cmd.CommandText = ClickHouseSchema.CreateDatabase;
            await cmd.ExecuteNonQueryAsync(cancellationToken);
        }

        _logger.LogInformation("Ensured database '{Database}' exists", _options.Database);

        // Create tables, views, and indexes
        var statements = ClickHouseSchema.GetSchemaStatements();
        foreach (var sql in statements)
        {
            await using var cmd = serverConnection.CreateCommand();
            cmd.CommandText = sql;
            await cmd.ExecuteNonQueryAsync(cancellationToken);
        }

        _logger.LogInformation("Schema initialization complete ({Count} statements)", statements.Count);
    }

    public async Task<long?> GetLastIndexedTickAsync(CancellationToken cancellationToken)
    {
        if (_connection == null) throw new InvalidOperationException("Not initialized");

        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = "SELECT value FROM indexer_state FINAL WHERE key = 'last_tick'";

        var result = await cmd.ExecuteScalarAsync(cancellationToken);
        if (result != null && result != DBNull.Value)
        {
            return long.Parse(result.ToString()!);
        }

        return null;
    }

    public async Task WriteTickDataAsync(TickStreamData data, CancellationToken cancellationToken)
    {
        await _batchLock.WaitAsync(cancellationToken);
        try
        {
            var timestamp = DateTime.TryParse(data.Timestamp, out var ts) ? ts : DateTime.UtcNow;

            // Derive epoch from tick data or from first log if tick-level epoch is 0
            var epoch = data.Epoch;
            if (epoch == 0 && data.Logs?.Count > 0)
            {
                epoch = data.Logs[0].Epoch;
            }

            // Add tick to batch
            _tickBatch.Add(new Tick
            {
                TickNumber = data.Tick,
                Epoch = epoch,
                Timestamp = timestamp,
                TxCount = data.TxCountTotal,
                TxCountFiltered = data.TxCountFiltered,
                LogCount = data.LogCountTotal,
                LogCountFiltered = data.LogCountFiltered,
                CreatedAt = DateTime.UtcNow
            });

            // Add transactions to batch
            if (data.Transactions != null)
            {
                foreach (var tx in data.Transactions)
                {
                    _transactionBatch.Add(new Transaction
                    {
                        Hash = tx.Hash,
                        TickNumber = data.Tick,
                        Epoch = epoch,
                        FromAddress = tx.From,
                        ToAddress = tx.To,
                        Amount = tx.Amount,
                        InputType = tx.InputType,
                        InputData = tx.InputData,
                        Executed = tx.Executed,
                        LogIdFrom = tx.LogIdFrom,
                        LogIdLength = tx.LogIdLength,
                        Timestamp = timestamp,
                        CreatedAt = DateTime.UtcNow
                    });
                }
            }

            // Add logs to batch
            if (data.Logs != null)
            {
                // Build lookup for parent transaction InputType
                var txInputTypes = data.Transactions?
                    .ToDictionary(tx => tx.Hash, tx => tx.InputType)
                    ?? new Dictionary<string, ushort>();

                foreach (var log in data.Logs)
                {
                    // Look up InputType from parent transaction
                    var inputType = log.TxHash != null && txInputTypes.TryGetValue(log.TxHash, out var it)
                        ? it
                        : (ushort)0;

                    _logBatch.Add(new Log
                    {
                        TickNumber = data.Tick,
                        Epoch = log.Epoch,
                        LogId = log.LogId,
                        LogType = log.LogType,
                        TxHash = log.TxHash,
                        InputType = inputType,
                        // Use helper methods that handle dynamic body based on log type
                        SourceAddress = log.GetSourceAddress(),
                        DestAddress = log.GetDestAddress(),
                        Amount = log.GetAmount(),
                        AssetName = log.GetAssetName(),
                        RawData = log.GetRawBodyJson(),
                        Timestamp = DateTime.TryParse(log.Timestamp, out var logTs) ? logTs : timestamp,
                        CreatedAt = DateTime.UtcNow
                    });
                }
            }

            // Check if we should flush
            var shouldFlush = _tickBatch.Count >= _indexerOptions.BatchSize ||
                              (DateTime.UtcNow - _lastFlush).TotalMilliseconds >= _indexerOptions.FlushIntervalMs;

            if (shouldFlush)
            {
                await FlushBatchesAsync(cancellationToken);
            }
        }
        finally
        {
            _batchLock.Release();
        }
    }

    public async Task FlushBatchesAsync(CancellationToken cancellationToken)
    {
        if (_connection == null) throw new InvalidOperationException("Not initialized");

        if (_tickBatch.Count == 0 && _transactionBatch.Count == 0 && _logBatch.Count == 0)
            return;

        var tickCount = _tickBatch.Count;
        var txCount = _transactionBatch.Count;
        var logCount = _logBatch.Count;
        var lastTick = _tickBatch.LastOrDefault()?.TickNumber ?? 0;

        try
        {
            // Insert ticks
            if (_tickBatch.Count > 0)
            {
                using var bulkCopy = new ClickHouseBulkCopy(_connection)
                {
                    DestinationTableName = "ticks",
                    BatchSize = _tickBatch.Count
                };

                await bulkCopy.InitAsync();
                await bulkCopy.WriteToServerAsync(CreateTickDataReader(_tickBatch), cancellationToken);
            }

            // Insert transactions
            if (_transactionBatch.Count > 0)
            {
                using var bulkCopy = new ClickHouseBulkCopy(_connection)
                {
                    DestinationTableName = "transactions",
                    BatchSize = _transactionBatch.Count
                };

                await bulkCopy.InitAsync();
                await bulkCopy.WriteToServerAsync(CreateTransactionDataReader(_transactionBatch), cancellationToken);
            }

            // Insert logs
            if (_logBatch.Count > 0)
            {
                using var bulkCopy = new ClickHouseBulkCopy(_connection)
                {
                    DestinationTableName = "logs",
                    BatchSize = _logBatch.Count
                };

                await bulkCopy.InitAsync();
                await bulkCopy.WriteToServerAsync(CreateLogDataReader(_logBatch), cancellationToken);
            }

            // Update last indexed tick
            await UpdateLastTickAsync(lastTick, cancellationToken);

            _logger.LogDebug(
                "Flushed batch: {TickCount} ticks, {TxCount} transactions, {LogCount} logs (last tick: {LastTick})",
                tickCount, txCount, logCount, lastTick);

            // Clear batches
            _tickBatch.Clear();
            _transactionBatch.Clear();
            _logBatch.Clear();
            _lastFlush = DateTime.UtcNow;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to flush batches");
            throw;
        }
    }

    private async Task UpdateLastTickAsync(ulong tick, CancellationToken cancellationToken)
    {
        if (_connection == null) return;

        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            INSERT INTO indexer_state (key, value, updated_at)
            VALUES ('last_tick', '{tick}', now64(3))";
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    private static IEnumerable<object[]> CreateTickDataReader(List<Tick> ticks)
    {
        foreach (var tick in ticks)
        {
            yield return new object[]
            {
                tick.TickNumber,
                tick.Epoch,
                tick.Timestamp,
                tick.TxCount,
                tick.TxCountFiltered,
                tick.LogCount,
                tick.LogCountFiltered,
                tick.CreatedAt
            };
        }
    }

    private static IEnumerable<object[]> CreateTransactionDataReader(List<Transaction> transactions)
    {
        foreach (var tx in transactions)
        {
            yield return new object[]
            {
                tx.Hash,
                tx.TickNumber,
                tx.Epoch,
                tx.FromAddress,
                tx.ToAddress,
                tx.Amount,
                tx.InputType,
                tx.InputData ?? string.Empty,
                tx.Executed ? (byte)1 : (byte)0,
                tx.LogIdFrom,
                tx.LogIdLength,
                tx.Timestamp,
                tx.CreatedAt
            };
        }
    }

    private static IEnumerable<object[]> CreateLogDataReader(List<Log> logs)
    {
        foreach (var log in logs)
        {
            yield return new object[]
            {
                log.TickNumber,
                log.Epoch,
                log.LogId,
                log.LogType,
                log.TxHash ?? string.Empty,
                log.InputType,
                log.SourceAddress ?? string.Empty,
                log.DestAddress ?? string.Empty,
                log.Amount,
                log.AssetName ?? string.Empty,
                log.RawData ?? string.Empty,
                log.Timestamp,
                log.CreatedAt
            };
        }
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        _batchLock.Dispose();
        _connection?.Dispose();
    }
}
