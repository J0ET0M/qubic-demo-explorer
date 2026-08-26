using ClickHouse.Client.ADO;
using ClickHouse.Client.Copy;
using Microsoft.Extensions.Options;
using QubicExplorer.Shared.Configuration;
using QubicExplorer.Shared.Constants;

namespace QubicExplorer.Analytics.Services;

/// <summary>
/// Indexes SetProposal (input_type=1) and Vote (input_type=2) transactions
/// destined to the GQMPROP (contract 6) and CCF (contract 8) contract identities
/// into <c>proposals</c> and <c>proposal_votes</c>.
///
/// The Qubic per-contract "input_type" is the procedure index (1..N) for
/// procedures registered by that contract — see contract.h REGISTER_USER_PROCEDURE
/// blocks. GQMPROP + CCF both register SetProposal=1 and Vote=2, so filtering by
/// (to_address IN &lt;contract identities&gt;, input_type IN (1,2)) is safe.
///
/// Watermark: indexer_state key <c>proposals_last_tick</c>.
/// </summary>
public class ProposalIndexingService : IDisposable
{
    private readonly ClickHouseConnection _connection;
    private readonly ILogger<ProposalIndexingService> _logger;
    private bool _disposed;

    private const string StateKey = "proposals_last_tick";
    private const int BulkBatchSize = 10_000;
    private const int MaxRowsPerPass = 200_000;

    public ProposalIndexingService(
        IOptions<ClickHouseOptions> options,
        ILogger<ProposalIndexingService> logger)
    {
        _logger = logger;
        _connection = new ClickHouseConnection(options.Value.ConnectionString);
        _connection.Open();
    }

    /// <summary>Returns (rows persisted, hasMore).</summary>
    public async Task<(int Rows, bool HasMore)> ProcessAsync(uint currentEpoch, CancellationToken ct)
    {
        var lastProcessed = await GetStateAsync(ct);
        if (lastProcessed == null)
        {
            // First run: backfill from the earliest epoch that has proposals. GQMPROP
            // was constructed at epoch 123, CCF at epoch 127 — so 123 is a safe lower
            // bound. If we don't have that far back in epoch_meta, fall back to the
            // earliest epoch we DO have data for (`epoch_meta` initial_tick).
            lastProcessed = await GetEpochInitialTickAsync(123u, ct)
                         ?? await GetEarliestEpochTickAsync(ct);
            if (lastProcessed == null)
            {
                _logger.LogWarning("Proposals: cannot determine backfill start tick, skipping");
                return (0, false);
            }
            _logger.LogInformation("Proposals: backfill start tick = {Tick}", lastProcessed.Value);
            lastProcessed = lastProcessed.Value > 0 ? lastProcessed.Value - 1 : 0;
        }

        var gqmpropIdentity = ProposalDecoder.GqmpropIdentity;
        var ccfIdentity = ProposalDecoder.CcfIdentity;

        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT tick_number, epoch, hash, from_address, to_address, input_type, input_data, timestamp
            FROM transactions
            WHERE to_address IN ({{gqmprop:String}}, {{ccf:String}})
              AND input_type IN ({ProposalDecoder.SetProposalProcedureIndex}, {ProposalDecoder.VoteProcedureIndex})
              AND tick_number > {{lastTick:UInt64}}
              AND epoch <= {{currentEpoch:UInt32}}
            ORDER BY tick_number ASC, hash ASC
            LIMIT {MaxRowsPerPass}";
        AddParam(cmd, "lastTick", lastProcessed.Value);
        AddParam(cmd, "currentEpoch", currentEpoch);
        AddParam(cmd, "gqmprop", gqmpropIdentity);
        AddParam(cmd, "ccf", ccfIdentity);

        var pendingProposals = new List<object[]>(BulkBatchSize);
        var pendingVotes = new List<object[]>(BulkBatchSize);
        var now = DateTime.UtcNow;

        Dictionary<string, int>? computorLookup = null;
        uint computorLookupEpoch = 0;

        ulong currentTickInScan = 0;
        ulong lastFlushedTick = lastProcessed.Value;
        int rowsRead = 0;
        int proposalsAdded = 0;
        int votesAdded = 0;
        int skippedParse = 0;

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            if (ct.IsCancellationRequested) break;
            rowsRead++;

            var tick = reader.GetFieldValue<ulong>(0);
            var epoch = reader.GetFieldValue<uint>(1);
            var hash = reader.GetString(2);
            var fromAddress = reader.GetString(3);
            var toAddress = reader.GetString(4);
            var inputType = reader.GetFieldValue<ushort>(5);
            var inputDataHex = reader.GetString(6);
            var timestamp = reader.GetDateTime(7);

            if (tick != currentTickInScan)
            {
                if (pendingProposals.Count + pendingVotes.Count >= BulkBatchSize && currentTickInScan > 0)
                {
                    await FlushAsync(pendingProposals, pendingVotes, ct);
                    proposalsAdded += pendingProposals.Count;
                    votesAdded += pendingVotes.Count;
                    pendingProposals.Clear();
                    pendingVotes.Clear();
                    await UpdateStateAsync(currentTickInScan, ct);
                    lastFlushedTick = currentTickInScan;
                }
                currentTickInScan = tick;
            }

            var contractIndex = toAddress == gqmpropIdentity
                ? ProposalDecoder.GqmpropContractIndex
                : ProposalDecoder.CcfContractIndex;

            var hex = inputDataHex.StartsWith("0x", StringComparison.OrdinalIgnoreCase)
                ? inputDataHex[2..] : inputDataHex;

            byte[] data;
            try { data = Convert.FromHexString(hex); }
            catch (FormatException) { skippedParse++; continue; }

            if (inputType == ProposalDecoder.SetProposalProcedureIndex)
            {
                var decoded = ProposalDecoder.DecodeProposal(contractIndex, data);
                if (decoded == null) { skippedParse++; continue; }

                // Payload epoch=0 means "clear my slot" per qpi_proposals.h — the
                // contract wipes that proposer's slot. Track it as a real row
                // (last event of a proposal's lifecycle) with is_cancelled=1.
                var isCancelled = (byte)(decoded.EpochField == 0 ? 1 : 0);

                // proposal_index (contract slot) isn't in the SetProposal payload
                // — the contract assigns it. Store 0xffff as "unknown"; votes
                // that reference the slot index later let us reconstruct it.
                pendingProposals.Add(new object[]
                {
                    epoch, (ushort)contractIndex, tick, (ushort)0xffff,
                    decoded.ProposalType, decoded.ProposalClass, decoded.OptionCount,
                    fromAddress, decoded.Url, timestamp, hash,
                    isCancelled,
                    decoded.TransferDestination, decoded.TransferAmounts,
                    decoded.TransferInEpochTargetEpoch,
                    decoded.VariableId, decoded.VariableValues,
                    decoded.VariableScalarMin, decoded.VariableScalarMax, decoded.VariableScalarProposed,
                    (byte)(decoded.IsSubscription ? 1 : 0), decoded.SubscriptionWeeksPerPeriod,
                    decoded.SubscriptionAmountPerPeriod, decoded.SubscriptionNumberOfPeriods,
                    decoded.SubscriptionStartEpoch,
                    hex, now
                });
            }
            else // Vote
            {
                var vote = ProposalDecoder.DecodeVote(data);
                if (vote == null) { skippedParse++; continue; }

                if (computorLookup == null || epoch != computorLookupEpoch)
                {
                    computorLookup = await GetComputorIndexLookupAsync(epoch, ct);
                    computorLookupEpoch = epoch;
                }
                ushort? computorIndex = computorLookup.TryGetValue(fromAddress, out var ci)
                    ? (ushort)ci
                    : (ushort?)null;

                pendingVotes.Add(new object[]
                {
                    epoch, (ushort)contractIndex, (ulong)vote.ProposalTick,
                    vote.ProposalIndex, vote.ProposalType,
                    fromAddress, computorIndex ?? (object)DBNull.Value,
                    tick, timestamp, vote.VoteValue,
                    (byte)(vote.IsWithdraw ? 1 : 0), vote.Option, hash, now
                });
            }
        }

        if (pendingProposals.Count + pendingVotes.Count > 0)
        {
            await FlushAsync(pendingProposals, pendingVotes, ct);
            proposalsAdded += pendingProposals.Count;
            votesAdded += pendingVotes.Count;
            pendingProposals.Clear();
            pendingVotes.Clear();
        }

        if (currentTickInScan > lastFlushedTick)
            await UpdateStateAsync(currentTickInScan, ct);

        var hasMore = rowsRead >= MaxRowsPerPass;

        if (proposalsAdded > 0 || votesAdded > 0)
        {
            _logger.LogInformation(
                "Proposals: persisted {Proposals} SetProposals + {Votes} Votes from {Rows} txs (skippedParse={SP}, lastTick={Last}, hasMore={HasMore})",
                proposalsAdded, votesAdded, rowsRead, skippedParse, currentTickInScan, hasMore);
        }

        return (proposalsAdded + votesAdded, hasMore);
    }

    private async Task FlushAsync(List<object[]> proposals, List<object[]> votes, CancellationToken ct)
    {
        if (proposals.Count > 0)
        {
            using var bulk = new ClickHouseBulkCopy(_connection)
            {
                DestinationTableName = "proposals",
                ColumnNames = [
                    "epoch", "contract_index", "proposal_tick", "proposal_index",
                    "proposal_type", "proposal_class", "option_count",
                    "proposer_identity", "url", "proposal_time", "tx_hash",
                    "is_cancelled",
                    "transfer_destination", "transfer_amounts",
                    "transfer_in_epoch_target_epoch",
                    "variable_id", "variable_values",
                    "variable_scalar_min", "variable_scalar_max", "variable_scalar_proposed",
                    "is_subscription", "subscription_weeks_per_period",
                    "subscription_amount_per_period", "subscription_number_of_periods",
                    "subscription_start_epoch",
                    "raw_input_hex", "created_at"
                ],
                BatchSize = BulkBatchSize
            };
            await bulk.InitAsync();
            await bulk.WriteToServerAsync(proposals, ct);
        }

        if (votes.Count > 0)
        {
            using var bulk = new ClickHouseBulkCopy(_connection)
            {
                DestinationTableName = "proposal_votes",
                ColumnNames = [
                    "epoch", "contract_index", "proposal_tick",
                    "proposal_index", "proposal_type",
                    "voter_identity", "computor_index",
                    "tick_number", "timestamp", "vote_value",
                    "is_withdraw", "option", "tx_hash", "created_at"
                ],
                BatchSize = BulkBatchSize
            };
            await bulk.InitAsync();
            await bulk.WriteToServerAsync(votes, ct);
        }
    }

    private async Task<Dictionary<string, int>> GetComputorIndexLookupAsync(uint epoch, CancellationToken ct)
    {
        var result = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT address, computor_index FROM computors FINAL
            WHERE epoch = {{epoch:UInt32}}";
        AddParam(cmd, "epoch", epoch);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
            result[reader.GetString(0)] = reader.GetFieldValue<ushort>(1);
        return result;
    }

    private async Task<ulong?> GetEpochInitialTickAsync(uint epoch, CancellationToken ct)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $"SELECT initial_tick FROM epoch_meta FINAL WHERE epoch = {{epoch:UInt32}}";
        AddParam(cmd, "epoch", epoch);
        var result = await cmd.ExecuteScalarAsync(ct);
        if (result == null || result == DBNull.Value) return null;
        return Convert.ToUInt64(result);
    }

    /// <summary>Initial tick of the earliest epoch we have in epoch_meta. Fallback for backfill.</summary>
    private async Task<ulong?> GetEarliestEpochTickAsync(CancellationToken ct)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = "SELECT min(initial_tick) FROM epoch_meta FINAL WHERE initial_tick > 0";
        var result = await cmd.ExecuteScalarAsync(ct);
        if (result == null || result == DBNull.Value) return null;
        return Convert.ToUInt64(result);
    }

    private async Task<ulong?> GetStateAsync(CancellationToken ct)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $"SELECT value FROM indexer_state FINAL WHERE key = '{StateKey}'";
        var result = await cmd.ExecuteScalarAsync(ct);
        if (result == null || result == DBNull.Value) return null;
        return ulong.TryParse(result.ToString(), out var val) ? val : null;
    }

    private async Task UpdateStateAsync(ulong tick, CancellationToken ct)
    {
        var now = DateTime.UtcNow;
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            INSERT INTO indexer_state (key, value, updated_at)
            VALUES ('{StateKey}', '{tick}', '{now:yyyy-MM-dd HH:mm:ss.fff}')";
        await cmd.ExecuteNonQueryAsync(ct);
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
