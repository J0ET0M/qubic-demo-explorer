using ClickHouse.Client.ADO;
using ClickHouse.Client.Copy;
using Microsoft.Extensions.Options;
using QubicExplorer.Shared.Configuration;
using QubicExplorer.Shared.Constants;

namespace QubicExplorer.Analytics.Services;

/// <summary>
/// Finalises proposal outcomes for closed epochs. Runs per-epoch, at most once:
/// picks up any (epoch &lt; currentEpoch) that has proposals but no
/// <c>proposal_results</c> row yet, tallies from <c>proposal_votes</c>, and writes
/// the results row.
///
/// Tally rules (from qpi_proposals + contract sources):
///   - one vote per voter = the LATEST tick_number wins
///   - withdraw votes (is_withdraw=1) don't count toward total_casted
///   - GQMPROP is_committed: total_casted&gt;=451 AND winning_votes&gt;225 AND
///     winning_option&gt;0 AND class IN (Transfer, TransferInEpoch)
///   - CCF   is_committed: total_casted&gt;=451 AND yes&gt;=no AND yes&gt;225
///
/// For CCF-committed non-subscription proposals we also cross-check the actual
/// transfer: search the transactions table around the finalization tick for a tx
/// from the CCF contract to `transfer_destination` with `amount = transfer_amounts[0]`.
/// Marks <c>transfer_verified=1</c> when found. This intentionally does NOT run
/// for GQMPROP (revenue-donation is a fee-share, not a discrete transfer) or CCF
/// subscriptions (paid over multiple epochs — future work).
/// </summary>
public class ProposalResultsService : IDisposable
{
    private readonly ClickHouseConnection _connection;
    private readonly ILogger<ProposalResultsService> _logger;
    private bool _disposed;

    public ProposalResultsService(
        IOptions<ClickHouseOptions> options,
        ILogger<ProposalResultsService> logger)
    {
        _logger = logger;
        _connection = new ClickHouseConnection(options.Value.ConnectionString);
        _connection.Open();
    }

    /// <summary>
    /// Process the oldest closed epoch that still has un-tallied proposals.
    /// Returns true if it processed one, false if there was nothing to do.
    /// </summary>
    public async Task<bool> ProcessNextEpochAsync(uint currentEpoch, CancellationToken ct)
    {
        var candidate = await FindNextUntalliedEpochAsync(currentEpoch, ct);
        if (candidate == null) return false;

        _logger.LogInformation("Proposals: tallying results for epoch {Epoch}", candidate.Value);

        var proposals = await LoadProposalsForEpochAsync(candidate.Value, ct);
        if (proposals.Count == 0) return true; // nothing to tally but epoch counts as processed

        var rows = new List<object[]>(proposals.Count);
        var now = DateTime.UtcNow;

        foreach (var p in proposals)
        {
            var tally = await TallyProposalAsync(p, ct);
            var (winningOption, winningCount) = FindWinner(tally.OptionCounts);
            var thresholdMet = tally.TotalCasted >= ProposalDecoder.Quorum;

            bool isCommitted;
            if (p.ContractIndex == ProposalDecoder.CcfContractIndex)
            {
                // CCF is always YesNo: option[0]=no, option[1]=yes
                var yes = tally.OptionCounts.ElementAtOrDefault(1);
                var no = tally.OptionCounts.ElementAtOrDefault(0);
                isCommitted = thresholdMet && yes >= no && yes > ProposalDecoder.MajorityHalf;
            }
            else
            {
                // GQMPROP: only Transfer/TransferInEpoch commit, needs winning option > "no change" (idx 0)
                var isTransferClass = p.ProposalClass == ProposalDecoder.ClassTransfer
                                    || p.ProposalClass == ProposalDecoder.ClassTransferInEpoch;
                isCommitted = thresholdMet && isTransferClass && winningOption > 0
                              && winningCount > ProposalDecoder.MajorityHalf;
            }

            byte transferVerified = 0;
            string verificationTx = "";
            if (isCommitted && p.ContractIndex == ProposalDecoder.CcfContractIndex
                && !p.IsSubscription && !string.IsNullOrEmpty(p.TransferDestination)
                && p.TransferAmounts.Length > 0)
            {
                var match = await FindCcfTransferAsync(candidate.Value, p.TransferDestination, p.TransferAmounts[0], ct);
                if (match != null)
                {
                    transferVerified = 1;
                    verificationTx = match;
                }
            }

            rows.Add(new object[]
            {
                candidate.Value,
                (ushort)p.ContractIndex,
                p.ProposalTick,
                tally.ProposalIndex,                        // taken from any vote payload
                (uint)ProposalDecoder.NumberOfComputors,
                (uint)tally.TotalCasted,
                tally.OptionCounts.Select(v => (uint)v).ToArray(),
                (uint)tally.OptionCounts.ElementAtOrDefault(1),      // yes
                (uint)tally.OptionCounts.ElementAtOrDefault(0),      // no
                (byte)(winningOption ?? 0xff),
                (byte)(thresholdMet ? 1 : 0),
                (byte)(isCommitted ? 1 : 0),
                transferVerified,
                verificationTx,
                now
            });
        }

        using var bulk = new ClickHouseBulkCopy(_connection)
        {
            DestinationTableName = "proposal_results",
            ColumnNames = [
                "epoch", "contract_index", "proposal_tick", "proposal_index",
                "total_authorized", "total_casted", "option_counts",
                "yes_count", "no_count", "winning_option",
                "threshold_met", "is_committed",
                "transfer_verified", "transfer_verification_tx", "snapshotted_at"
            ],
            BatchSize = rows.Count
        };
        await bulk.InitAsync();
        await bulk.WriteToServerAsync(rows, ct);

        _logger.LogInformation("Proposals: wrote {Count} result rows for epoch {Epoch}", rows.Count, candidate.Value);
        return true;
    }

    private async Task<uint?> FindNextUntalliedEpochAsync(uint currentEpoch, CancellationToken ct)
    {
        // Any closed epoch with proposals AND no proposal_results yet.
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            SELECT p.epoch
            FROM (SELECT DISTINCT epoch FROM proposals WHERE epoch < {{currentEpoch:UInt32}}) p
            LEFT JOIN (SELECT DISTINCT epoch FROM proposal_results) r ON p.epoch = r.epoch
            WHERE r.epoch IS NULL
            ORDER BY p.epoch ASC
            LIMIT 1";
        AddParam(cmd, "currentEpoch", currentEpoch);
        var result = await cmd.ExecuteScalarAsync(ct);
        if (result == null || result == DBNull.Value) return null;
        return Convert.ToUInt32(result);
    }

    private record ProposalRow(
        uint Epoch, int ContractIndex, ulong ProposalTick, ushort ProposalClass,
        string TransferDestination, long[] TransferAmounts, bool IsSubscription);

    private async Task<List<ProposalRow>> LoadProposalsForEpochAsync(uint epoch, CancellationToken ct)
    {
        var list = new List<ProposalRow>();
        // One row per proposer (latest SetProposal wins via argMax on proposal_tick).
        // Cancelled proposals — the proposer's final act was a slot-clear — are
        // excluded: there is no active proposal to tally.
        await using var cmd = _connection.CreateCommand();
        // See ClickHouseQueryService for full explanation: every argMax alias
        // must be DISTINCT from the raw column name (ClickHouse re-resolves
        // aliases into their own definitions → nested aggregate error). Prefix
        // every aggregate output with `l_` in the inner query, rename in outer.
        cmd.CommandText = $@"
            SELECT epoch, contract_index,
                   l_proposal_tick AS proposal_tick,
                   l_proposal_class AS proposal_class,
                   l_transfer_destination AS transfer_destination,
                   l_transfer_amounts AS transfer_amounts,
                   l_is_subscription AS is_subscription
            FROM (
                SELECT epoch, contract_index, proposer_identity,
                       max(proposal_tick) AS l_proposal_tick,
                       argMax(proposal_class, proposal_tick) AS l_proposal_class,
                       argMax(transfer_destination, proposal_tick) AS l_transfer_destination,
                       argMax(transfer_amounts, proposal_tick) AS l_transfer_amounts,
                       argMax(is_subscription, proposal_tick) AS l_is_subscription,
                       argMax(is_cancelled, proposal_tick) AS l_is_cancelled
                FROM proposals FINAL
                WHERE epoch = {{epoch:UInt32}}
                GROUP BY epoch, contract_index, proposer_identity
                HAVING l_is_cancelled = 0
            )";
        AddParam(cmd, "epoch", epoch);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var amounts = reader.GetValue(5) as long[]
                       ?? ((IEnumerable<object>)reader.GetValue(5)).Select(Convert.ToInt64).ToArray();
            list.Add(new ProposalRow(
                reader.GetFieldValue<uint>(0),
                reader.GetFieldValue<ushort>(1),
                reader.GetFieldValue<ulong>(2),
                reader.GetFieldValue<ushort>(3),
                reader.GetString(4),
                amounts,
                reader.GetFieldValue<byte>(6) != 0
            ));
        }
        return list;
    }

    private record TallyResult(ushort ProposalIndex, int TotalCasted, List<int> OptionCounts);

    private async Task<TallyResult> TallyProposalAsync(ProposalRow p, CancellationToken ct)
    {
        // Latest vote per voter (max tick_number wins), then aggregate.
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = $@"
            WITH latest AS (
                SELECT voter_identity,
                       argMax(option, tick_number) AS option,
                       argMax(is_withdraw, tick_number) AS is_withdraw,
                       argMax(proposal_index, tick_number) AS proposal_index
                FROM proposal_votes
                WHERE epoch = {{epoch:UInt32}}
                  AND contract_index = {{contract:UInt16}}
                  AND proposal_tick = {{tick:UInt64}}
                GROUP BY voter_identity
            )
            SELECT option, is_withdraw, proposal_index FROM latest";
        AddParam(cmd, "epoch", p.Epoch);
        AddParam(cmd, "contract", (ushort)p.ContractIndex);
        AddParam(cmd, "tick", p.ProposalTick);

        var counts = new Dictionary<int, int>();
        int totalCasted = 0;
        ushort proposalIndex = 0xffff;

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var isWithdraw = reader.GetFieldValue<byte>(1) != 0;
            var idx = reader.GetFieldValue<ushort>(2);
            if (idx != 0xffff) proposalIndex = idx;
            if (isWithdraw) continue;
            var opt = reader.GetFieldValue<byte>(0);
            counts[opt] = counts.GetValueOrDefault(opt) + 1;
            totalCasted++;
        }

        var maxOpt = counts.Keys.Count > 0 ? counts.Keys.Max() : -1;
        var arr = new List<int>(maxOpt + 1);
        for (int i = 0; i <= maxOpt; i++) arr.Add(counts.GetValueOrDefault(i));
        return new TallyResult(proposalIndex, totalCasted, arr);
    }

    private static (byte? WinningOption, int WinningCount) FindWinner(List<int> counts)
    {
        if (counts.Count == 0) return (null, 0);
        int best = -1, bestOpt = -1;
        for (int i = 0; i < counts.Count; i++)
        {
            if (counts[i] > best) { best = counts[i]; bestOpt = i; }
        }
        return best > 0 ? ((byte)bestOpt, best) : ((byte?)null, 0);
    }

    /// <summary>
    /// CCF cross-check: after a Yes-passing proposal, the contract calls
    /// qpi.transfer(destination, amount) inside END_EPOCH of the voting epoch.
    /// That surfaces as a transaction: from = CCF identity, to = destination,
    /// amount_qus = amount. We scan the LAST tick of the epoch (+/- a small
    /// window for slack) and return the first matching hash.
    /// </summary>
    private async Task<string?> FindCcfTransferAsync(uint epoch, string destination, long amount, CancellationToken ct)
    {
        var ccfIdentity = ProposalDecoder.CcfIdentity;
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = @"
            SELECT hash FROM transactions
            WHERE epoch = {epoch:UInt32}
              AND from_address = {ccf:String}
              AND to_address = {dest:String}
              AND amount = {amount:UInt64}
            LIMIT 1";
        AddParam(cmd, "epoch", epoch);
        AddParam(cmd, "ccf", ccfIdentity);
        AddParam(cmd, "dest", destination);
        AddParam(cmd, "amount", (ulong)amount);
        var result = await cmd.ExecuteScalarAsync(ct);
        return result == null || result == DBNull.Value ? null : result.ToString();
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
