using ClickHouse.Client.ADO;
using Microsoft.Extensions.Options;
using QubicExplorer.Shared.Configuration;

namespace QubicExplorer.Analytics.Services;

/// <summary>
/// One-time schema migration for the proposals tables. Version-guarded via
/// <c>indexer_state[proposals_schema_version]</c> so it fires only when the
/// installed schema is older than the code expects — subsequent restarts are a
/// no-op.
///
/// Why not just ALTER: ClickHouse's ORDER BY key cannot be changed by ALTER on
/// a MergeTree family — the whole table must be recreated. And CREATE TABLE
/// IF NOT EXISTS in <see cref="Shared.ClickHouseSchema"/> silently no-ops when
/// a table already exists, so a schema fix in code doesn't take effect. This
/// migration bridges that gap.
///
/// Data is safe to drop: everything comes from the immutable <c>transactions</c>
/// table via <see cref="ProposalIndexingService"/>, which backfills from GQMPROP
/// construction epoch (123) on empty-watermark start.
///
/// CURRENT VERSION = 2:
///   v0/unset: first-ever install
///   v1:       initial proposals schema — keyed only on (epoch, contract, proposal_tick).
///             Two proposers submitting at the same tick collided; SetProposal edits
///             appeared as duplicate rows.
///   v2:       adds proposer_identity into the ORDER BY key + is_cancelled column
///             (payload epoch=0 signals slot clear).
/// </summary>
public static class ProposalsSchemaMigration
{
    private const int CurrentVersion = 2;
    private const string VersionKey = "proposals_schema_version";
    private const string WatermarkKey = "proposals_last_tick";

    /// <summary>
    /// Bring the proposals schema up to <see cref="CurrentVersion"/>. Safe to
    /// call on every startup — no-ops when already at the target version.
    /// </summary>
    public static async Task EnsureAsync(ClickHouseOptions options, ILogger logger, CancellationToken ct)
    {
        await using var connection = new ClickHouseConnection(options.ConnectionString);
        await connection.OpenAsync(ct);

        // The version key lives in indexer_state, which itself may not exist yet
        // on a first-run install. In that case, `indexer_state` will be created
        // by the schema pass right after us and this call becomes a no-op (no
        // rows to read → treated as v0).
        var installedVersion = await GetInstalledVersionAsync(connection, ct);
        if (installedVersion >= CurrentVersion)
        {
            logger.LogDebug("Proposals schema already at v{Version}, no migration needed", installedVersion);
            return;
        }

        logger.LogInformation(
            "Migrating proposals schema v{From} → v{To} (drop + re-create + reset watermark)",
            installedVersion, CurrentVersion);

        // Drop tables. ClickHouse will re-create them from the schema list right
        // after this migration runs.
        foreach (var table in new[] { "proposals", "proposal_votes", "proposal_results" })
        {
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = $"DROP TABLE IF EXISTS {table}";
            await cmd.ExecuteNonQueryAsync(ct);
        }

        // Reset the ingestion watermark so the indexer re-scans from the
        // GQMPROP construction epoch (epoch 123) — otherwise we'd have empty
        // tables with the watermark stuck at whatever tick we processed last.
        await using (var delCmd = connection.CreateCommand())
        {
            delCmd.CommandText = $"ALTER TABLE indexer_state DELETE WHERE key = '{WatermarkKey}'";
            try { await delCmd.ExecuteNonQueryAsync(ct); }
            catch { /* indexer_state may not exist yet on first-ever install; ignore */ }
        }

        // Record the new version so we don't run again.
        await SetInstalledVersionAsync(connection, CurrentVersion, ct);

        logger.LogInformation("Proposals schema migrated to v{Version}", CurrentVersion);
    }

    private static async Task<int> GetInstalledVersionAsync(ClickHouseConnection connection, CancellationToken ct)
    {
        try
        {
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = $"SELECT value FROM indexer_state FINAL WHERE key = '{VersionKey}'";
            var result = await cmd.ExecuteScalarAsync(ct);
            if (result == null || result == DBNull.Value) return 0;
            return int.TryParse(result.ToString(), out var v) ? v : 0;
        }
        catch
        {
            // indexer_state doesn't exist yet — treat as v0 (fresh install).
            return 0;
        }
    }

    private static async Task SetInstalledVersionAsync(ClickHouseConnection connection, int version, CancellationToken ct)
    {
        // indexer_state might not exist on first-ever install. Create a minimal
        // version of it — the shared schema will fill in the rest on the next
        // pass (CREATE TABLE IF NOT EXISTS is a no-op if we got the columns right).
        await using var ensureCmd = connection.CreateCommand();
        ensureCmd.CommandText = @"
            CREATE TABLE IF NOT EXISTS indexer_state (
                key String,
                value String,
                updated_at DateTime64(3) DEFAULT now64(3)
            ) ENGINE = ReplacingMergeTree(updated_at) ORDER BY key";
        await ensureCmd.ExecuteNonQueryAsync(ct);

        var now = DateTime.UtcNow;
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = $@"
            INSERT INTO indexer_state (key, value, updated_at)
            VALUES ('{VersionKey}', '{version}', '{now:yyyy-MM-dd HH:mm:ss.fff}')";
        await cmd.ExecuteNonQueryAsync(ct);
    }
}
