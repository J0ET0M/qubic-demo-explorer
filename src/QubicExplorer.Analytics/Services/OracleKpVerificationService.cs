using ClickHouse.Client.ADO;
using ClickHouse.Client.Copy;
using Microsoft.Extensions.Options;
using Qubic.Crypto;
using QubicExplorer.Shared.Configuration;

namespace QubicExplorer.Analytics.Services;

/// <summary>
/// Verifies oracle commit knowledge-proofs against the eventually-revealed reply.
///
/// Background. Each oracle reply commit carries two fields:
///   <list type="bullet">
///     <item><c>reply_digest</c>   = K12(reply_data)            — same for everyone</item>
///     <item><c>knowledge_proof</c> = K12(reply_data || idx_u16) — unique per computor</item>
///   </list>
/// The protocol awards a revenue point to the first 451 committors whose digest
/// matches the eventual quorum digest AND whose KP reproduces from the revealed
/// reply. Commits with the right digest but wrong KP are dropped — they copied
/// the digest from a peer without knowing the actual reply. That's "forgery".
///
/// This service walks every (epoch, query_id) where a reveal exists, re-K12s
/// the expected digest and per-computor KP, and writes:
///   <list type="bullet">
///     <item><c>oracle_kp_verifications</c> — one row per query (rollup)</item>
///     <item><c>oracle_kp_forgeries</c>     — one row per detected forgery</item>
///   </list>
/// Idempotent: skips queries already present in oracle_kp_verifications.
/// </summary>
public class OracleKpVerificationService : IDisposable
{
    private readonly ClickHouseConnection _connection;
    private readonly ILogger<OracleKpVerificationService> _logger;
    private bool _disposed;

    private const int RevealEventType = 1;
    private const int CommitEventType = 0;
    private const int MaxQueriesPerPass = 500;

    public OracleKpVerificationService(
        IOptions<ClickHouseOptions> options,
        ILogger<OracleKpVerificationService> logger)
    {
        _logger = logger;
        _connection = new ClickHouseConnection(options.Value.ConnectionString);
        _connection.Open();
    }

    /// <summary>
    /// Verify up to <see cref="MaxQueriesPerPass"/> not-yet-verified queries.
    /// Returns (verifiedCount, forgeriesFound, hasMore).
    /// </summary>
    public async Task<(int Verified, int Forgeries, bool HasMore)> ProcessAsync(uint upToEpoch, CancellationToken ct)
    {
        // Find queries that have a reveal but haven't been KP-verified yet.
        // The reveal event also gives us the reveal tx hash so we can fetch
        // its input_data directly.
        var pairs = new List<(uint Epoch, ulong QueryId, string TxHash)>();
        await using (var cmd = _connection.CreateCommand())
        {
            cmd.CommandText = $@"
                SELECT epoch, query_id, any(tx_hash) AS tx_hash
                FROM oracle_query_events
                WHERE event_type = {RevealEventType}
                  AND epoch <= {{upToEpoch:UInt32}}
                  AND (epoch, query_id) NOT IN (
                      SELECT epoch, query_id FROM oracle_kp_verifications FINAL
                  )
                GROUP BY epoch, query_id
                ORDER BY epoch ASC, query_id ASC
                LIMIT {MaxQueriesPerPass}";
            AddParam(cmd, "upToEpoch", upToEpoch);

            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                pairs.Add((
                    reader.GetFieldValue<uint>(0),
                    reader.GetFieldValue<ulong>(1),
                    reader.GetString(2)));
            }
        }

        if (pairs.Count == 0) return (0, 0, false);

        var verifications = new List<object[]>(pairs.Count);
        var forgeries = new List<object[]>();
        var now = DateTime.UtcNow;

        foreach (var (epoch, queryId, txHash) in pairs)
        {
            if (ct.IsCancellationRequested) break;

            var revealBytes = await GetRevealInputAsync(txHash, ct);
            if (revealBytes == null || revealBytes.Length < 8)
            {
                // Reveal tx not in transactions table or malformed — skip silently
                // but record the verification row so we don't re-attempt every pass.
                verifications.Add(VerificationRow(epoch, queryId, 0, "", 0, 0, 0, 0, now));
                continue;
            }

            // input_data = queryId(8) + reply_data
            var replyData = new ReadOnlyMemory<byte>(revealBytes, 8, revealBytes.Length - 8);
            var expectedDigest = K12.Hash(replyData.Span, 32);
            var expectedDigestHex = Convert.ToHexString(expectedDigest);

            // Buffer for KP computation: [reply_data][idx_u16_le]
            var kpBuf = new byte[replyData.Length + 2];
            replyData.Span.CopyTo(kpBuf);

            var stats = await VerifyCommitsAsync(
                epoch, queryId, kpBuf, replyData.Length,
                expectedDigestHex, forgeries, ct);

            verifications.Add(VerificationRow(
                epoch, queryId,
                (ushort)replyData.Length, expectedDigestHex,
                stats.Total, stats.MatchingDigest,
                stats.MatchingDigestKpOk, stats.MatchingDigestKpForged,
                now));
        }

        if (verifications.Count > 0)
            await BulkInsertAsync("oracle_kp_verifications",
                ["epoch", "query_id", "reply_data_size", "expected_digest",
                 "total_commits", "matching_digest_count",
                 "matching_digest_kp_ok", "matching_digest_kp_forged",
                 "verified_at"],
                verifications, ct);

        if (forgeries.Count > 0)
            await BulkInsertAsync("oracle_kp_forgeries",
                ["epoch", "query_id", "computor_index", "committed_digest",
                 "committed_kp", "expected_kp", "tick_number", "detected_at"],
                forgeries, ct);

        if (forgeries.Count > 0)
        {
            _logger.LogWarning(
                "Oracle KP: verified {V} queries, found {F} forgery attempt(s)",
                verifications.Count, forgeries.Count);
        }
        else if (verifications.Count > 0)
        {
            _logger.LogInformation(
                "Oracle KP: verified {V} queries, no forgeries",
                verifications.Count);
        }

        return (verifications.Count, forgeries.Count, verifications.Count >= MaxQueriesPerPass);
    }

    private record struct CommitStats(
        uint Total,
        uint MatchingDigest,
        uint MatchingDigestKpOk,
        uint MatchingDigestKpForged);

    private async Task<CommitStats> VerifyCommitsAsync(
        uint epoch, ulong queryId,
        byte[] kpBuf, int replyDataLength,
        string expectedDigestHex,
        List<object[]> forgeries,
        CancellationToken ct)
    {
        uint total = 0, matching = 0, ok = 0, forged = 0;

        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT computor_index, reply_digest, knowledge_proof, tick_number
            FROM oracle_query_events FINAL
            WHERE epoch = {{epoch:UInt32}}
              AND query_id = {{qid:UInt64}}
              AND event_type = {CommitEventType}";
        AddParam(cmd, "epoch", epoch);
        AddParam(cmd, "qid", queryId);

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            total++;
            var idx = reader.GetFieldValue<ushort>(0);
            var commitDigestHex = reader.GetString(1);
            var commitKpHex = reader.GetString(2);
            var tick = reader.GetFieldValue<ulong>(3);

            // Only commits matching the revealed-reply digest are revenue-bearing
            // candidates. Mismatches are honest-minority or random and not what
            // we're hunting for.
            if (!commitDigestHex.Equals(expectedDigestHex, StringComparison.OrdinalIgnoreCase))
                continue;
            matching++;

            // Append computor index little-endian to the buffer's tail
            kpBuf[replyDataLength]     = (byte)(idx & 0xff);
            kpBuf[replyDataLength + 1] = (byte)((idx >> 8) & 0xff);
            var expectedKp = K12.Hash(kpBuf, 32);
            var expectedKpHex = Convert.ToHexString(expectedKp);

            if (commitKpHex.Equals(expectedKpHex, StringComparison.OrdinalIgnoreCase))
            {
                ok++;
            }
            else
            {
                forged++;
                forgeries.Add(new object[]
                {
                    epoch, queryId, idx,
                    commitDigestHex, commitKpHex, expectedKpHex,
                    tick, DateTime.UtcNow
                });
            }
        }

        return new CommitStats(total, matching, ok, forged);
    }

    private async Task<byte[]?> GetRevealInputAsync(string txHash, CancellationToken ct)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = "SELECT input_data FROM transactions FINAL WHERE hash = {hash:String} LIMIT 1";
        AddParam(cmd, "hash", txHash);
        var result = await cmd.ExecuteScalarAsync(ct);
        if (result is not string hex || hex.Length == 0) return null;

        var trimmed = hex.StartsWith("0x", StringComparison.OrdinalIgnoreCase) ? hex[2..] : hex;
        try { return Convert.FromHexString(trimmed); }
        catch (FormatException) { return null; }
    }

    private async Task BulkInsertAsync(
        string table,
        string[] columns,
        IReadOnlyList<object[]> rows,
        CancellationToken ct)
    {
        using var bulk = new ClickHouseBulkCopy(_connection)
        {
            DestinationTableName = table,
            ColumnNames = columns,
            BatchSize = 50_000
        };
        await bulk.InitAsync();
        await bulk.WriteToServerAsync(rows, ct);
    }

    private static object[] VerificationRow(
        uint epoch, ulong queryId,
        ushort replyDataSize, string expectedDigestHex,
        uint totalCommits, uint matchingDigest,
        uint matchingDigestKpOk, uint matchingDigestKpForged,
        DateTime verifiedAt)
    {
        return new object[]
        {
            epoch, queryId, replyDataSize, expectedDigestHex,
            totalCommits, matchingDigest,
            matchingDigestKpOk, matchingDigestKpForged,
            verifiedAt
        };
    }

    private static void AddParam(System.Data.Common.DbCommand cmd, string name, object value)
    {
        var p = cmd.CreateParameter();
        p.ParameterName = name;
        p.Value = value;
        cmd.Parameters.Add(p);
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _connection.Dispose();
    }
}
