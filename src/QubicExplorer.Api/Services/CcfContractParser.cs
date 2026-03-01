using System.Buffers.Binary;
using System.Text;
using Qubic.Core.Contracts.Ccf;
using Qubic.Crypto;

namespace QubicExplorer.Api.Services;

/// <summary>
/// Manual binary parsers for CCF contract types that are incomplete in Ccf.g.cs.
/// Struct layouts derived from ComputorControlledFund.h in qubic-core.
/// </summary>
public static class CcfContractParser
{
    /// <summary>Number of computors in the Qubic network.</summary>
    public const int NumberOfComputors = 676;

    /// <summary>Quorum threshold: 2/3 + 1 of computors.</summary>
    public const int Quorum = NumberOfComputors * 2 / 3 + 1; // 452

    /// <summary>
    /// Parse the ProposalDataT from GetProposal output.
    /// Layout (304 bytes):
    ///   0-255:   url (256 bytes, zero-terminated)
    ///   256-257: epoch (uint16)
    ///   258-259: type (uint16)
    ///   260-263: tick (uint32)
    ///   264-303: data union (40 bytes) — Transfer: destination(32) + amount(8)
    /// </summary>
    public static ParsedProposal ParseProposalData(ReadOnlySpan<byte> data)
    {
        var url = ReadNullTerminatedString(data[..256]);
        var epoch = BinaryPrimitives.ReadUInt16LittleEndian(data[256..]);
        var type = BinaryPrimitives.ReadUInt16LittleEndian(data[258..]);
        var tick = BinaryPrimitives.ReadUInt32LittleEndian(data[260..]);

        // Parse union based on proposal type
        string? transferDestination = null;
        long? transferAmount = null;

        // Transfer types: 0x100 class = Transfer
        if ((type & 0xF00) == 0x100)
        {
            var destPubKey = data.Slice(264, 32).ToArray();
            if (!destPubKey.All(b => b == 0))
            {
                var crypt = new QubicCrypt();
                transferDestination = crypt.GetIdentityFromPublicKey(destPubKey);
            }
            transferAmount = BinaryPrimitives.ReadInt64LittleEndian(data[296..]);
        }

        return new ParsedProposal
        {
            Url = url,
            Epoch = epoch,
            ProposalType = type,
            Tick = tick,
            TransferDestination = transferDestination,
            TransferAmount = transferAmount
        };
    }

    /// <summary>
    /// Parse ProposalSummarizedVotingDataV1 from GetVotingResults output.
    /// Layout (48 bytes):
    ///   0-1:   proposalIndex (uint16)
    ///   2-3:   optionCount (uint16)
    ///   4-7:   proposalTick (uint32)
    ///   8-11:  totalVotesAuthorized (uint32)
    ///   12-15: totalVotesCast (uint32)
    ///   16-47: union — optionVoteCount: uint32[8] (when optionCount > 0)
    ///                   or scalarVotingResult: int64 (when optionCount == 0)
    /// </summary>
    public static ParsedVotingResults ParseVotingResults(ReadOnlySpan<byte> data)
    {
        var proposalIndex = BinaryPrimitives.ReadUInt16LittleEndian(data);
        var optionCount = BinaryPrimitives.ReadUInt16LittleEndian(data[2..]);
        var proposalTick = BinaryPrimitives.ReadUInt32LittleEndian(data[4..]);
        var totalVotesAuthorized = BinaryPrimitives.ReadUInt32LittleEndian(data[8..]);
        var totalVotesCast = BinaryPrimitives.ReadUInt32LittleEndian(data[12..]);

        var optionVoteCounts = new uint[8];
        for (var i = 0; i < 8; i++)
            optionVoteCounts[i] = BinaryPrimitives.ReadUInt32LittleEndian(data[(16 + i * 4)..]);

        return new ParsedVotingResults
        {
            ProposalIndex = proposalIndex,
            OptionCount = optionCount,
            ProposalTick = proposalTick,
            TotalVotesAuthorized = totalVotesAuthorized,
            TotalVotesCast = totalVotesCast,
            OptionVoteCounts = optionVoteCounts
        };
    }

    /// <summary>
    /// Parse SubscriptionData from GetProposal output.
    /// Layout (312 bytes):
    ///   0-31:    destination (32 bytes, public key)
    ///   32-287:  url (256 bytes)
    ///   288:     weeksPerPeriod (uint8)
    ///   289:     padding
    ///   290-291: padding
    ///   292-295: numberOfPeriods (uint32)
    ///   296-303: amountPerPeriod (uint64)
    ///   304-307: startEpoch (uint32)
    ///   308-311: currentPeriod (int32)
    /// </summary>
    public static ParsedSubscription ParseSubscriptionData(ReadOnlySpan<byte> data)
    {
        var destPubKey = data[..32].ToArray();
        var url = ReadNullTerminatedString(data.Slice(32, 256));
        var weeksPerPeriod = data[288];
        var numberOfPeriods = BinaryPrimitives.ReadUInt32LittleEndian(data[292..]);
        var amountPerPeriod = BinaryPrimitives.ReadUInt64LittleEndian(data[296..]);
        var startEpoch = BinaryPrimitives.ReadUInt32LittleEndian(data[304..]);
        var currentPeriod = BinaryPrimitives.ReadInt32LittleEndian(data[308..]);

        string? destination = null;
        if (!destPubKey.All(b => b == 0))
        {
            var crypt = new QubicCrypt();
            destination = crypt.GetIdentityFromPublicKey(destPubKey);
        }

        return new ParsedSubscription
        {
            Destination = destination,
            Url = url,
            WeeksPerPeriod = weeksPerPeriod,
            NumberOfPeriods = (int)numberOfPeriods,
            AmountPerPeriod = (long)amountPerPeriod,
            StartEpoch = startEpoch,
            CurrentPeriod = currentPeriod
        };
    }

    /// <summary>
    /// Parse the full GetProposal output (raw bytes after hex decode).
    /// Layout:
    ///   0:       okay (bool)
    ///   1:       hasSubscriptionProposal (bool)
    ///   2:       hasActiveSubscription (bool)
    ///   3:       padding (1)
    ///   4-7:     padding (4)
    ///   8-39:    proposerPublicKey (32)
    ///   40-343:  proposal (ProposalDataT, 304 bytes)
    ///   344-655: subscription (SubscriptionData, 312 bytes)
    ///   656+:    subscriptionProposal (SubscriptionProposalData)
    /// </summary>
    public static ParsedGetProposalOutput ParseGetProposalOutput(ReadOnlySpan<byte> data)
    {
        var okay = data[0] != 0;
        var hasSubscriptionProposal = data[1] != 0;
        var hasActiveSubscription = data[2] != 0;

        byte[] proposerPubKey = data.Slice(8, 32).ToArray();
        string? proposerAddress = null;
        if (!proposerPubKey.All(b => b == 0))
        {
            var crypt = new QubicCrypt();
            proposerAddress = crypt.GetIdentityFromPublicKey(proposerPubKey);
        }

        ParsedProposal? proposal = null;
        if (okay && data.Length >= 344)
            proposal = ParseProposalData(data.Slice(40, 304));

        ParsedSubscription? subscription = null;
        if (hasActiveSubscription && data.Length >= 656)
            subscription = ParseSubscriptionData(data.Slice(344, 312));

        return new ParsedGetProposalOutput
        {
            Okay = okay,
            HasSubscriptionProposal = hasSubscriptionProposal,
            HasActiveSubscription = hasActiveSubscription,
            ProposerAddress = proposerAddress,
            Proposal = proposal,
            Subscription = subscription
        };
    }

    /// <summary>
    /// Parse the full GetVotingResults output.
    /// Layout: 1 byte (okay) + padding + ProposalSummarizedVotingDataV1 (48 bytes).
    /// </summary>
    public static ParsedVotingResults? ParseGetVotingResultsOutput(ReadOnlySpan<byte> data)
    {
        if (data.Length < 1 || data[0] == 0)
            return null;

        // okay is at offset 0, then padding to align to the struct
        // The voting data starts at offset 8 (aligned)
        if (data.Length < 56) // 8 + 48
            return null;

        return ParseVotingResults(data[8..]);
    }

    /// <summary>
    /// Parse transfer entries from GetLatestTransfers, filtering out empty slots.
    /// </summary>
    public static List<ParsedTransferEntry> ParseLatestTransfers(ReadOnlySpan<byte> data)
    {
        var output = GetLatestTransfersOutput.FromBytes(data);
        var crypt = new QubicCrypt();
        var result = new List<ParsedTransferEntry>();

        foreach (var entry in output.Entries)
        {
            if (entry.Tick == 0 && entry.Amount == 0)
                continue;
            if (entry.Destination.All(b => b == 0))
                continue;

            result.Add(new ParsedTransferEntry
            {
                Destination = crypt.GetIdentityFromPublicKey(entry.Destination),
                Url = ReadNullTerminatedString(entry.Url),
                Amount = entry.Amount,
                Tick = entry.Tick,
                Success = entry.Success
            });
        }

        return result;
    }

    /// <summary>
    /// Parse regular payment entries from GetRegularPayments, filtering out empty slots.
    /// </summary>
    public static List<ParsedRegularPaymentEntry> ParseRegularPayments(ReadOnlySpan<byte> data)
    {
        var output = GetRegularPaymentsOutput.FromBytes(data);
        var crypt = new QubicCrypt();
        var result = new List<ParsedRegularPaymentEntry>();

        foreach (var entry in output.Entries)
        {
            if (entry.Tick == 0 && entry.Amount == 0)
                continue;
            if (entry.Destination.All(b => b == 0))
                continue;

            result.Add(new ParsedRegularPaymentEntry
            {
                Destination = crypt.GetIdentityFromPublicKey(entry.Destination),
                Url = ReadNullTerminatedString(entry.Url),
                Amount = entry.Amount,
                Tick = entry.Tick,
                PeriodIndex = entry.PeriodIndex,
                Success = entry.Success
            });
        }

        return result;
    }

    private static string ReadNullTerminatedString(ReadOnlySpan<byte> data)
    {
        var nullIdx = data.IndexOf((byte)0);
        var len = nullIdx >= 0 ? nullIdx : data.Length;
        return Encoding.ASCII.GetString(data[..len]).Trim();
    }

    private static string ReadNullTerminatedString(byte[] data)
    {
        var nullIdx = Array.IndexOf(data, (byte)0);
        var len = nullIdx >= 0 ? nullIdx : data.Length;
        return Encoding.ASCII.GetString(data, 0, len).Trim();
    }
}

public class ParsedProposal
{
    public string Url { get; init; } = "";
    public ushort Epoch { get; init; }
    public ushort ProposalType { get; init; }
    public uint Tick { get; init; }
    public string? TransferDestination { get; init; }
    public long? TransferAmount { get; init; }
}

public class ParsedVotingResults
{
    public ushort ProposalIndex { get; init; }
    public ushort OptionCount { get; init; }
    public uint ProposalTick { get; init; }
    public uint TotalVotesAuthorized { get; init; }
    public uint TotalVotesCast { get; init; }
    public uint[] OptionVoteCounts { get; init; } = [];
}

public class ParsedSubscription
{
    public string? Destination { get; init; }
    public string Url { get; init; } = "";
    public byte WeeksPerPeriod { get; init; }
    public int NumberOfPeriods { get; init; }
    public long AmountPerPeriod { get; init; }
    public uint StartEpoch { get; init; }
    public int CurrentPeriod { get; init; }
}

public class ParsedGetProposalOutput
{
    public bool Okay { get; init; }
    public bool HasSubscriptionProposal { get; init; }
    public bool HasActiveSubscription { get; init; }
    public string? ProposerAddress { get; init; }
    public ParsedProposal? Proposal { get; init; }
    public ParsedSubscription? Subscription { get; init; }
}

public class ParsedTransferEntry
{
    public string Destination { get; init; } = "";
    public string Url { get; init; } = "";
    public long Amount { get; init; }
    public uint Tick { get; init; }
    public bool Success { get; init; }
}

public class ParsedRegularPaymentEntry
{
    public string Destination { get; init; } = "";
    public string Url { get; init; } = "";
    public long Amount { get; init; }
    public uint Tick { get; init; }
    public int PeriodIndex { get; init; }
    public bool Success { get; init; }
}
