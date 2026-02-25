using System.IO.Compression;
using ClickHouse.Client.ADO;
using Microsoft.Extensions.Options;
using Qubic.Crypto;
using QubicExplorer.Shared.Configuration;
using QubicExplorer.Shared.DTOs;

namespace QubicExplorer.Api.Services;

/// <summary>
/// Service for importing Universe files containing asset records.
/// Universe files are in the same ep{epoch}-bob.zip as spectrum files.
/// Each record is 48 bytes: a union of ISSUANCE, OWNERSHIP, or POSSESSION types.
/// </summary>
public class UniverseImportService : IDisposable
{
    private readonly HttpClient _httpClient;
    private readonly ClickHouseConnection _connection;
    private readonly BobProxyService _bobProxy;
    private readonly ILogger<UniverseImportService> _logger;
    private readonly string _baseUrl;
    private bool _disposed;

    // Universe file constants
    private const int ASSET_RECORD_SIZE = 48;
    private const int ASSETS_DEPTH = 24;
    private const long ASSETS_CAPACITY = 1L << ASSETS_DEPTH; // 16,777,216

    // Record types
    private const byte TYPE_EMPTY = 0;
    private const byte TYPE_ISSUANCE = 1;
    private const byte TYPE_OWNERSHIP = 2;
    private const byte TYPE_POSSESSION = 3;

    private readonly QubicCrypt _qubicCrypt = new();

    public UniverseImportService(
        IHttpClientFactory httpClientFactory,
        IOptions<ClickHouseOptions> options,
        BobProxyService bobProxy,
        ILogger<UniverseImportService> logger)
    {
        _httpClient = httpClientFactory.CreateClient();
        _httpClient.Timeout = TimeSpan.FromMinutes(15); // Universe files are large (~768MB)
        _bobProxy = bobProxy;
        _logger = logger;
        _baseUrl = "https://storage.qubic.li/network";
        _connection = new ClickHouseConnection(options.Value.ConnectionString);
        _connection.Open();
    }

    /// <summary>
    /// Check if a universe file has already been imported for the given epoch
    /// </summary>
    public async Task<bool> IsEpochImportedAsync(uint epoch, CancellationToken ct = default)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $"SELECT count() FROM universe_imports WHERE epoch = {epoch}";
        var count = Convert.ToInt64(await cmd.ExecuteScalarAsync(ct));
        return count > 0;
    }

    /// <summary>
    /// Import universe file for a specific epoch
    /// </summary>
    public async Task<UniverseImportResultDto> ImportEpochAsync(uint epoch, CancellationToken ct = default)
    {
        var startTime = DateTime.UtcNow;
        _logger.LogInformation("Starting universe import for epoch {Epoch}", epoch);

        try
        {
            // Download the ZIP file
            var zipUrl = $"{_baseUrl}/{epoch}/ep{epoch}-bob.zip";
            _logger.LogInformation("Downloading universe file from {Url}", zipUrl);

            using var response = await _httpClient.GetAsync(zipUrl, ct);
            if (!response.IsSuccessStatusCode)
            {
                return new UniverseImportResultDto(
                    epoch, false, 0, 0, 0, 0,
                    $"Failed to download: HTTP {(int)response.StatusCode}");
            }

            var zipBytes = await response.Content.ReadAsByteArrayAsync(ct);
            var fileSize = (ulong)zipBytes.Length;
            _logger.LogInformation("Downloaded {Size} bytes for epoch {Epoch}", fileSize, epoch);

            // Extract universe file from ZIP
            byte[] universeData;
            using (var zipStream = new MemoryStream(zipBytes))
            using (var archive = new ZipArchive(zipStream, ZipArchiveMode.Read))
            {
                var universeEntry = archive.Entries.FirstOrDefault(e =>
                    e.Name.StartsWith("universe.", StringComparison.OrdinalIgnoreCase));

                if (universeEntry == null)
                {
                    return new UniverseImportResultDto(
                        epoch, false, 0, 0, 0, fileSize,
                        "No universe file found in ZIP archive");
                }

                _logger.LogInformation("Extracting {FileName} ({Size} bytes)",
                    universeEntry.FullName, universeEntry.Length);

                using var entryStream = universeEntry.Open();
                using var ms = new MemoryStream();
                await entryStream.CopyToAsync(ms, ct);
                universeData = ms.ToArray();
            }

            // Two-pass parsing: first build issuance index, then process ownership/possession
            var (issuances, ownerships, possessions) = ParseUniverseData(universeData);
            _logger.LogInformation(
                "Parsed {Issuances} issuances, {Ownerships} ownerships, {Possessions} possessions",
                issuances.Count, ownerships.Count, possessions.Count);

            if (issuances.Count == 0)
            {
                return new UniverseImportResultDto(
                    epoch, false, 0, 0, 0, fileSize,
                    "No valid issuances found in universe file");
            }

            // Get first tick of epoch
            var firstTick = await GetFirstTickOfEpochAsync(epoch, ct);

            // Insert into ClickHouse
            await InsertAssetSnapshotsAsync(epoch, issuances, ownerships, possessions, ct);

            // Record the import
            await RecordImportAsync(epoch, firstTick,
                (ulong)issuances.Count, (ulong)ownerships.Count, (ulong)possessions.Count,
                fileSize, (uint)(DateTime.UtcNow - startTime).TotalMilliseconds, ct);

            var duration = DateTime.UtcNow - startTime;
            _logger.LogInformation(
                "Successfully imported universe for epoch {Epoch} in {Duration:F2}s",
                epoch, duration.TotalSeconds);

            return new UniverseImportResultDto(
                epoch, true,
                (ulong)issuances.Count, (ulong)ownerships.Count, (ulong)possessions.Count,
                fileSize);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to import universe for epoch {Epoch}", epoch);
            return new UniverseImportResultDto(epoch, false, 0, 0, 0, 0, ex.Message);
        }
    }

    private (List<IssuanceRecord> issuances, List<AssetSnapshotRecord> ownerships, List<AssetSnapshotRecord> possessions)
        ParseUniverseData(byte[] data)
    {
        var issuances = new List<IssuanceRecord>();
        var ownershipRecords = new List<RawOwnershipRecord>();
        var possessionRecords = new List<RawPossessionRecord>();

        var entryCount = data.Length / ASSET_RECORD_SIZE;
        _logger.LogDebug("Processing {Count} potential asset entries", entryCount);

        var emptyKey = new byte[32];

        // Pass 1: Parse all records and build issuance index
        for (long i = 0; i < entryCount; i++)
        {
            var offset = (int)(i * ASSET_RECORD_SIZE);
            if (offset + ASSET_RECORD_SIZE > data.Length)
                break;

            // Check if public key is empty
            var publicKey = new byte[32];
            Buffer.BlockCopy(data, offset, publicKey, 0, 32);
            if (publicKey.SequenceEqual(emptyKey))
                continue;

            var type = data[offset + 32];

            switch (type)
            {
                case TYPE_ISSUANCE:
                {
                    // name: 7 bytes at offset 33
                    var nameBytes = new byte[7];
                    Buffer.BlockCopy(data, offset + 33, nameBytes, 0, 7);
                    var name = System.Text.Encoding.ASCII.GetString(nameBytes).TrimEnd('\0');

                    var numberOfDecimalPlaces = (sbyte)data[offset + 40];

                    var address = _qubicCrypt.GetIdentityFromPublicKey(publicKey);
                    if (string.IsNullOrEmpty(address)) continue;

                    issuances.Add(new IssuanceRecord(
                        Index: (uint)i,
                        IssuerAddress: address,
                        AssetName: name,
                        NumberOfDecimalPlaces: numberOfDecimalPlaces
                    ));
                    break;
                }
                case TYPE_OWNERSHIP:
                {
                    var managingContractIndex = BitConverter.ToUInt16(data, offset + 34);
                    var issuanceIndex = BitConverter.ToUInt32(data, offset + 36);
                    var numberOfShares = BitConverter.ToInt64(data, offset + 40);

                    var address = _qubicCrypt.GetIdentityFromPublicKey(publicKey);
                    if (string.IsNullOrEmpty(address)) continue;

                    ownershipRecords.Add(new RawOwnershipRecord(
                        Index: (uint)i,
                        Address: address,
                        ManagingContractIndex: managingContractIndex,
                        IssuanceIndex: issuanceIndex,
                        NumberOfShares: numberOfShares
                    ));
                    break;
                }
                case TYPE_POSSESSION:
                {
                    var managingContractIndex = BitConverter.ToUInt16(data, offset + 34);
                    var ownershipIndex = BitConverter.ToUInt32(data, offset + 36);
                    var numberOfShares = BitConverter.ToInt64(data, offset + 40);

                    var address = _qubicCrypt.GetIdentityFromPublicKey(publicKey);
                    if (string.IsNullOrEmpty(address)) continue;

                    possessionRecords.Add(new RawPossessionRecord(
                        Address: address,
                        ManagingContractIndex: managingContractIndex,
                        OwnershipIndex: ownershipIndex,
                        NumberOfShares: numberOfShares
                    ));
                    break;
                }
            }

            if ((issuances.Count + ownershipRecords.Count + possessionRecords.Count) % 100000 == 0)
            {
                _logger.LogDebug("Pass 1: processed {Count} records",
                    issuances.Count + ownershipRecords.Count + possessionRecords.Count);
            }
        }

        // Build issuance index: universe array index → issuance record
        var issuanceByIndex = issuances.ToDictionary(i => i.Index);

        // Build ownership index: universe array index → ownership record (for possession→ownership→issuance chain)
        var ownershipByIndex = ownershipRecords.ToDictionary(o => o.Index);

        // Pass 2: Resolve ownership records → issuance name/issuer
        var ownerships = new List<AssetSnapshotRecord>();
        foreach (var o in ownershipRecords)
        {
            if (!issuanceByIndex.TryGetValue(o.IssuanceIndex, out var issuance))
                continue;

            ownerships.Add(new AssetSnapshotRecord(
                AssetName: issuance.AssetName,
                IssuerAddress: issuance.IssuerAddress,
                HolderAddress: o.Address,
                RecordType: "ownership",
                ManagingContractIndex: o.ManagingContractIndex,
                IssuanceIndex: o.IssuanceIndex,
                NumberOfShares: o.NumberOfShares,
                NumberOfDecimalPlaces: issuance.NumberOfDecimalPlaces
            ));
        }

        // Pass 3: Resolve possession records → ownership → issuance
        var possessions = new List<AssetSnapshotRecord>();
        foreach (var p in possessionRecords)
        {
            if (!ownershipByIndex.TryGetValue(p.OwnershipIndex, out var ownership))
                continue;
            if (!issuanceByIndex.TryGetValue(ownership.IssuanceIndex, out var issuance))
                continue;

            possessions.Add(new AssetSnapshotRecord(
                AssetName: issuance.AssetName,
                IssuerAddress: issuance.IssuerAddress,
                HolderAddress: p.Address,
                RecordType: "possession",
                ManagingContractIndex: p.ManagingContractIndex,
                IssuanceIndex: ownership.IssuanceIndex,
                NumberOfShares: p.NumberOfShares,
                NumberOfDecimalPlaces: issuance.NumberOfDecimalPlaces
            ));
        }

        return (issuances, ownerships, possessions);
    }

    private async Task InsertAssetSnapshotsAsync(
        uint epoch,
        List<IssuanceRecord> issuances,
        List<AssetSnapshotRecord> ownerships,
        List<AssetSnapshotRecord> possessions,
        CancellationToken ct)
    {
        _logger.LogInformation("Inserting asset snapshots for epoch {Epoch}", epoch);

        // Delete existing snapshots for this epoch
        await using var deleteCmd = _connection.CreateCommand();
        deleteCmd.CommandText = $"ALTER TABLE asset_snapshots DELETE WHERE epoch = {epoch}";
        await deleteCmd.ExecuteNonQueryAsync(ct);

        // Insert issuances (as records with holder = issuer)
        var issuanceRecords = issuances.Select(i => new AssetSnapshotRecord(
            AssetName: i.AssetName,
            IssuerAddress: i.IssuerAddress,
            HolderAddress: i.IssuerAddress,
            RecordType: "issuance",
            ManagingContractIndex: 0,
            IssuanceIndex: i.Index,
            NumberOfShares: 0,
            NumberOfDecimalPlaces: i.NumberOfDecimalPlaces
        )).ToList();

        await InsertBatchAsync(epoch, issuanceRecords, ct);
        await InsertBatchAsync(epoch, ownerships, ct);
        await InsertBatchAsync(epoch, possessions, ct);

        _logger.LogInformation("Inserted {Total} asset snapshot records",
            issuanceRecords.Count + ownerships.Count + possessions.Count);
    }

    private async Task InsertBatchAsync(
        uint epoch,
        List<AssetSnapshotRecord> records,
        CancellationToken ct)
    {
        const int batchSize = 10000;

        for (var i = 0; i < records.Count; i += batchSize)
        {
            var batch = records.Skip(i).Take(batchSize).ToList();
            var values = string.Join(",\n", batch.Select(r =>
                $"({epoch}, '{EscapeSql(r.AssetName)}', '{r.IssuerAddress}', '{r.HolderAddress}', " +
                $"'{r.RecordType}', {r.ManagingContractIndex}, {r.IssuanceIndex}, " +
                $"{r.NumberOfShares}, {r.NumberOfDecimalPlaces})"));

            await using var cmd = _connection.CreateCommand();
            cmd.CommandText = $@"
                INSERT INTO asset_snapshots
                (epoch, asset_name, issuer_address, holder_address, record_type,
                 managing_contract_index, issuance_index, number_of_shares, number_of_decimal_places)
                VALUES {values}";
            await cmd.ExecuteNonQueryAsync(ct);
        }
    }

    private static string EscapeSql(string value)
    {
        return value.Replace("'", "\\'");
    }

    private async Task<ulong> GetFirstTickOfEpochAsync(uint epoch, CancellationToken ct)
    {
        var epochInfo = await _bobProxy.GetEpochInfoAsync(epoch, ct);
        if (epochInfo == null)
            throw new InvalidOperationException($"Failed to get epoch info from RPC for epoch {epoch}");
        return epochInfo.InitialTick;
    }

    private async Task RecordImportAsync(
        uint epoch, ulong tickNumber,
        ulong issuanceCount, ulong ownershipCount, ulong possessionCount,
        ulong fileSize, uint durationMs,
        CancellationToken ct)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            INSERT INTO universe_imports
            (epoch, tick_number, issuance_count, ownership_count, possession_count, file_size, import_duration_ms)
            VALUES
            ({epoch}, {tickNumber}, {issuanceCount}, {ownershipCount}, {possessionCount}, {fileSize}, {durationMs})";
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _connection.Dispose();
    }

    // Internal record types
    private record IssuanceRecord(uint Index, string IssuerAddress, string AssetName, sbyte NumberOfDecimalPlaces);
    private record RawOwnershipRecord(uint Index, string Address, ushort ManagingContractIndex, uint IssuanceIndex, long NumberOfShares);
    private record RawPossessionRecord(string Address, ushort ManagingContractIndex, uint OwnershipIndex, long NumberOfShares);
    private record AssetSnapshotRecord(string AssetName, string IssuerAddress, string HolderAddress, string RecordType,
        ushort ManagingContractIndex, uint IssuanceIndex, long NumberOfShares, sbyte NumberOfDecimalPlaces);
}
