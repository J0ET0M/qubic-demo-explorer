using System.Text.Json;
using System.Text.Json.Serialization;
using ClickHouse.Client.ADO;
using ClickHouse.Client.Copy;
using Microsoft.Extensions.Options;
using QubicExplorer.Shared.Configuration;

namespace QubicExplorer.Analytics.Services;

/// <summary>
/// Fetches per-computor ownership from the fattydoge revenue tracker and
/// persists it into <c>computor_ownership</c>, keyed by (epoch, identity).
///
/// The upstream API returns the CURRENT epoch's view as a flat array; we
/// snapshot it whenever this service is invoked. Owners can change between
/// epochs (computors trade hands), so per-epoch keying is correct — same
/// computor address may have different owner in epoch N vs N+1.
/// </summary>
public class ComputorOwnershipService : IDisposable
{
    private readonly ClickHouseConnection _connection;
    private readonly IHttpClientFactory _httpClientFactory;
    private readonly ILogger<ComputorOwnershipService> _logger;
    private bool _disposed;

    private const string SourceUrl = "https://fattydoge.top/api/qubic/revenues";

    private static readonly JsonSerializerOptions JsonOpts = new()
    {
        PropertyNameCaseInsensitive = true,
        // upstream sends some numbers in scientific notation (e.g. 3.34298E7) —
        // System.Text.Json handles those fine for doubles, but we only read
        // strings here so this is just guard against trailing-comma cases.
        ReadCommentHandling = JsonCommentHandling.Skip,
        AllowTrailingCommas = true
    };

    public ComputorOwnershipService(
        IOptions<ClickHouseOptions> options,
        IHttpClientFactory httpClientFactory,
        ILogger<ComputorOwnershipService> logger)
    {
        _logger = logger;
        _httpClientFactory = httpClientFactory;
        _connection = new ClickHouseConnection(options.Value.ConnectionString);
        _connection.Open();
    }

    /// <summary>
    /// Pull the current ownership snapshot and persist it for the given epoch.
    /// Returns the number of (identity, owner) rows written.
    /// </summary>
    public async Task<int> RefreshAsync(uint epoch, CancellationToken ct)
    {
        List<FattydogeEntry>? entries;
        try
        {
            var client = _httpClientFactory.CreateClient();
            client.Timeout = TimeSpan.FromSeconds(30);
            entries = await client.GetFromJsonAsync<List<FattydogeEntry>>(SourceUrl, JsonOpts, ct);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "ComputorOwnership: failed to fetch from {Url}", SourceUrl);
            return 0;
        }

        if (entries == null || entries.Count == 0)
        {
            _logger.LogWarning("ComputorOwnership: fattydoge returned no entries");
            return 0;
        }

        var now = DateTime.UtcNow;
        var rows = new List<object[]>(entries.Count);
        foreach (var e in entries)
        {
            if (string.IsNullOrWhiteSpace(e.Identity) || string.IsNullOrWhiteSpace(e.Owner))
                continue;
            if (e.Identity.Length != 60)
                continue;
            rows.Add(new object[] { epoch, e.Identity, e.Owner, now });
        }

        if (rows.Count == 0)
        {
            _logger.LogWarning("ComputorOwnership: all entries filtered out as invalid");
            return 0;
        }

        using var bulk = new ClickHouseBulkCopy(_connection)
        {
            DestinationTableName = "computor_ownership",
            ColumnNames = ["epoch", "identity", "owner", "fetched_at"],
            BatchSize = 5_000
        };
        await bulk.InitAsync();
        await bulk.WriteToServerAsync(rows, ct);

        _logger.LogInformation(
            "ComputorOwnership: persisted {Count} rows for epoch {Epoch} ({OwnerCount} distinct owners)",
            rows.Count, epoch, entries.Select(e => e.Owner).Distinct().Count());
        return rows.Count;
    }

    private sealed class FattydogeEntry
    {
        [JsonPropertyName("identity")]
        public string? Identity { get; set; }

        [JsonPropertyName("owner")]
        public string? Owner { get; set; }
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _connection.Dispose();
    }
}
