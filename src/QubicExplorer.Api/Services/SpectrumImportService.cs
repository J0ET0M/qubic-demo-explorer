using System.IO.Compression;
using System.Text.Json;
using ClickHouse.Client.ADO;
using ClickHouse.Client.Copy;
using Microsoft.Extensions.Options;
using Qubic.Crypto;
using QubicExplorer.Shared.Configuration;

namespace QubicExplorer.Api.Services;

/// <summary>
/// Service for importing Spectrum files containing address balance snapshots.
/// Spectrum files are downloaded from storage.qubic.li and contain binary data
/// with all entity records (addresses and their balances) at epoch start.
/// </summary>
public class SpectrumImportService : IDisposable
{
    private readonly HttpClient _httpClient;
    private readonly ClickHouseConnection _connection;
    private readonly BobProxyService _bobProxy;
    private readonly ILogger<SpectrumImportService> _logger;
    private readonly string _baseUrl;
    private bool _disposed;

    // Spectrum file constants
    private const int SPECTRUM_DEPTH = 24;
    private const long SPECTRUM_CAPACITY = 1L << SPECTRUM_DEPTH; // 16,777,216 entries
    private const int ENTITY_RECORD_SIZE = 64; // 32 + 8 + 8 + 4 + 4 + 4 + 4 = 64 bytes

    // Qubic crypto for address encoding
    private readonly QubicCrypt _qubicCrypt = new();

    public SpectrumImportService(
        IHttpClientFactory httpClientFactory,
        IOptions<ClickHouseOptions> options,
        BobProxyService bobProxy,
        ILogger<SpectrumImportService> logger)
    {
        _httpClient = httpClientFactory.CreateClient();
        _httpClient.Timeout = TimeSpan.FromMinutes(10); // Large file downloads
        _bobProxy = bobProxy;
        _logger = logger;
        _baseUrl = "https://storage.qubic.li/network";
        _connection = new ClickHouseConnection(options.Value.ConnectionString);
        _connection.Open();
    }

    /// <summary>
    /// Check if a spectrum file has already been imported for the given epoch
    /// </summary>
    public async Task<bool> IsEpochImportedAsync(uint epoch, CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $"SELECT count() FROM spectrum_imports WHERE epoch = {epoch}";
        var count = Convert.ToInt64(await cmd.ExecuteScalarAsync(ct));
        return count > 0;
    }

    /// <summary>
    /// Get the latest imported epoch
    /// </summary>
    public async Task<uint?> GetLatestImportedEpochAsync(CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = "SELECT max(epoch) FROM spectrum_imports";
        var result = await cmd.ExecuteScalarAsync(ct);
        if (result == null || result == DBNull.Value)
            return null;
        return Convert.ToUInt32(result);
    }

    /// <summary>
    /// Import spectrum file for a specific epoch
    /// </summary>
    public async Task<SpectrumImportResult> ImportEpochAsync(uint epoch, CancellationToken ct = default)
    {
        var startTime = DateTime.UtcNow;
        _logger.LogInformation("Starting spectrum import for epoch {Epoch}", epoch);

        try
        {
            // Download the ZIP file
            var zipUrl = $"{_baseUrl}/{epoch}/ep{epoch}-bob.zip";
            _logger.LogInformation("Downloading spectrum file from {Url}", zipUrl);

            using var response = await _httpClient.GetAsync(zipUrl, ct);
            if (!response.IsSuccessStatusCode)
            {
                return new SpectrumImportResult(
                    epoch, false, 0, 0, 0,
                    $"Failed to download: HTTP {(int)response.StatusCode}");
            }

            var zipBytes = await response.Content.ReadAsByteArrayAsync(ct);
            var fileSize = (ulong)zipBytes.Length;
            _logger.LogInformation("Downloaded {Size} bytes for epoch {Epoch}", fileSize, epoch);

            // Extract spectrum file from ZIP
            byte[] spectrumData;
            using (var zipStream = new MemoryStream(zipBytes))
            using (var archive = new ZipArchive(zipStream, ZipArchiveMode.Read))
            {
                // Look for spectrum.{epoch} file (e.g., spectrum.196)
                var spectrumEntry = archive.Entries.FirstOrDefault(e =>
                    e.Name.StartsWith("spectrum.", StringComparison.OrdinalIgnoreCase));

                if (spectrumEntry == null)
                {
                    return new SpectrumImportResult(
                        epoch, false, 0, 0, fileSize,
                        "No spectrum file found in ZIP archive");
                }

                _logger.LogInformation("Extracting {FileName} ({Size} bytes)",
                    spectrumEntry.FullName, spectrumEntry.Length);

                using var entryStream = spectrumEntry.Open();
                using var ms = new MemoryStream();
                await entryStream.CopyToAsync(ms, ct);
                spectrumData = ms.ToArray();
            }

            // Parse spectrum data
            var entities = ParseSpectrumData(spectrumData);
            _logger.LogInformation("Parsed {Count} entities from spectrum file", entities.Count);

            if (entities.Count == 0)
            {
                return new SpectrumImportResult(
                    epoch, false, 0, 0, fileSize,
                    "No valid entities found in spectrum file");
            }

            // Get first tick of epoch for the snapshot
            var firstTick = await GetFirstTickOfEpochAsync(epoch, ct);

            // Insert into ClickHouse
            await InsertBalanceSnapshotsAsync(epoch, firstTick, entities, ct);

            // Calculate total balance
            var totalBalance = entities.Sum(e => e.Balance);

            // Record the import
            await RecordImportAsync(epoch, firstTick, (ulong)entities.Count,
                totalBalance, fileSize, (uint)(DateTime.UtcNow - startTime).TotalMilliseconds, ct);

            var duration = DateTime.UtcNow - startTime;
            _logger.LogInformation(
                "Successfully imported {Count} balances for epoch {Epoch} in {Duration:F2}s",
                entities.Count, epoch, duration.TotalSeconds);

            return new SpectrumImportResult(
                epoch, true, (ulong)entities.Count, totalBalance, fileSize);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to import spectrum for epoch {Epoch}", epoch);
            return new SpectrumImportResult(epoch, false, 0, 0, 0, ex.Message);
        }
    }

    private List<EntityBalance> ParseSpectrumData(byte[] data)
    {
        var entities = new List<EntityBalance>();

        if (data.Length < ENTITY_RECORD_SIZE)
        {
            _logger.LogWarning("Spectrum data too small: {Size} bytes", data.Length);
            return entities;
        }

        // The spectrum file contains SPECTRUM_CAPACITY entries, but most are empty
        var entryCount = data.Length / ENTITY_RECORD_SIZE;
        _logger.LogDebug("Processing {Count} potential entries", entryCount);

        var emptyKey = new byte[32];
        var processedCount = 0;

        for (long i = 0; i < entryCount; i++)
        {
            var offset = (int)(i * ENTITY_RECORD_SIZE);
            if (offset + ENTITY_RECORD_SIZE > data.Length)
                break;

            // Check if public key is empty (all zeros)
            var publicKey = new byte[32];
            Buffer.BlockCopy(data, offset, publicKey, 0, 32);

            if (publicKey.SequenceEqual(emptyKey))
                continue;

            // Parse entity record - amounts are stored as signed long long in spectrum
            var incomingAmount = BitConverter.ToInt64(data, offset + 32);
            var outgoingAmount = BitConverter.ToInt64(data, offset + 40);
            var numberOfIncomingTransfers = BitConverter.ToUInt32(data, offset + 48);
            var numberOfOutgoingTransfers = BitConverter.ToUInt32(data, offset + 52);
            var latestIncomingTransferTick = BitConverter.ToUInt32(data, offset + 56);
            var latestOutgoingTransferTick = BitConverter.ToUInt32(data, offset + 60);

            var balance = incomingAmount - outgoingAmount;

            // Convert public key to Qubic address using Qubic.Crypto library
            var address = _qubicCrypt.GetIdentityFromPublicKey(publicKey);

            // Skip entries with invalid addresses
            if (string.IsNullOrEmpty(address))
                continue;

            // Ensure amounts are non-negative for UInt64 columns
            var safeIncoming = incomingAmount < 0 ? 0UL : (ulong)incomingAmount;
            var safeOutgoing = outgoingAmount < 0 ? 0UL : (ulong)outgoingAmount;

            entities.Add(new EntityBalance(
                address,
                balance,
                safeIncoming,
                safeOutgoing,
                numberOfIncomingTransfers,
                numberOfOutgoingTransfers,
                latestIncomingTransferTick,
                latestOutgoingTransferTick
            ));

            processedCount++;
            if (processedCount % 100000 == 0)
            {
                _logger.LogDebug("Processed {Count} entities so far", processedCount);
            }
        }

        return entities;
    }

    private async Task<ulong> GetFirstTickOfEpochAsync(uint epoch, CancellationToken ct)
    {
        // Get epoch info from Bob RPC to get the initial tick
        var epochInfo = await _bobProxy.GetEpochInfoAsync(epoch, ct);
        if (epochInfo == null)
        {
            throw new InvalidOperationException($"Failed to get epoch info from RPC for epoch {epoch}");
        }

        _logger.LogInformation("Epoch {Epoch} initial tick: {InitialTick}", epoch, epochInfo.InitialTick);
        return epochInfo.InitialTick;
    }

    private async Task InsertBalanceSnapshotsAsync(
        uint epoch,
        ulong tickNumber,
        List<EntityBalance> entities,
        CancellationToken ct)
    {
        _logger.LogInformation("Inserting {Count} balance snapshots for epoch {Epoch}",
            entities.Count, epoch);

        // Delete existing snapshots for this epoch first
        await using var deleteCmd = _connection.CreateCommand();
        deleteCmd.CommandText = $"ALTER TABLE balance_snapshots DELETE WHERE epoch = {epoch}";
        await deleteCmd.ExecuteNonQueryAsync(ct);

        // Insert in batches using parameterized INSERT
        const int batchSize = 10000;
        var inserted = 0;

        for (var i = 0; i < entities.Count; i += batchSize)
        {
            var batch = entities.Skip(i).Take(batchSize).ToList();
            var values = string.Join(",\n", batch.Select(e =>
                $"('{e.Address}', {epoch}, {tickNumber}, {e.Balance}, {e.IncomingAmount}, {e.OutgoingAmount}, " +
                $"{e.IncomingTransferCount}, {e.OutgoingTransferCount}, {e.LatestIncomingTick}, {e.LatestOutgoingTick})"));

            await using var cmd = _connection.CreateCommand();
            cmd.CommandText = $@"
                INSERT INTO balance_snapshots
                (address, epoch, tick_number, balance, incoming_amount, outgoing_amount,
                 incoming_transfer_count, outgoing_transfer_count, latest_incoming_tick, latest_outgoing_tick)
                VALUES {values}";

            await cmd.ExecuteNonQueryAsync(ct);
            inserted += batch.Count;

            if (inserted % 50000 == 0)
            {
                _logger.LogInformation("Inserted {Count}/{Total} balance snapshots", inserted, entities.Count);
            }
        }

        _logger.LogInformation("Inserted {Count} balance snapshots", entities.Count);
    }

    private async Task RecordImportAsync(
        uint epoch,
        ulong tickNumber,
        ulong addressCount,
        long totalBalance,
        ulong fileSize,
        uint durationMs,
        CancellationToken ct)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            INSERT INTO spectrum_imports
            (epoch, tick_number, address_count, total_balance, file_size, import_duration_ms)
            VALUES
            ({epoch}, {tickNumber}, {addressCount}, {(totalBalance < 0 ? 0 : totalBalance)}, {fileSize}, {durationMs})";
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _connection.Dispose();
    }

    private record EntityBalance(
        string Address,
        long Balance,
        ulong IncomingAmount,
        ulong OutgoingAmount,
        uint IncomingTransferCount,
        uint OutgoingTransferCount,
        uint LatestIncomingTick,
        uint LatestOutgoingTick
    );
}

public record SpectrumImportResult(
    uint Epoch,
    bool Success,
    ulong AddressCount,
    long TotalBalance,
    ulong FileSize,
    string? Error = null
);
