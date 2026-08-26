using System.Buffers.Binary;
using System.Text;
using Qubic.Crypto;

namespace QubicExplorer.Shared.Constants;

/// <summary>
/// Decodes the on-chain wire formats for GQMPROP + CCF proposal txs.
///
/// Contract framework: qubic/src/qpi/qpi_proposals.h + contracts/{GeneralQuorumProposal,ComputorControlledFund}.h.
///
/// Two input flavours per contract:
///   Procedure 1 (SetProposal):
///     - GQMPROP: raw ProposalDataV1 = 328 bytes
///     - CCF: SetProposal_input = ProposalDataYesNo(304) + { isSubscription(1), weeksPerPeriod(1),
///       pad(2), startEpoch(4), amountPerPeriod(8), numberOfPeriods(4) } = 324 bytes total
///   Procedure 2 (Vote):
///     - Both: ProposalSingleVoteDataV1 = 16 bytes
///       { proposalIndex(2), proposalType(2), proposalTick(4), voteValue(8) }
///
/// ProposalType packing (qpi_proposals.h): upper byte = class, lower byte = optionCount.
///   Classes: 0x0000 GeneralOptions, 0x0100 Transfer, 0x0200 Variable,
///            0x0300 MultiVariables, 0x0400 TransferInEpoch
/// </summary>
public static class ProposalDecoder
{
    public const int NumberOfComputors = 676;
    public const int Quorum = NumberOfComputors * 2 / 3 + 1; // 451
    public const int MajorityHalf = Quorum / 2; // 225

    public const long NoVoteValue = unchecked((long)0x8000_0000_0000_0000UL);

    public const int GqmpropContractIndex = 6;
    public const int CcfContractIndex = 8;

    public const int SetProposalProcedureIndex = 1;
    public const int VoteProcedureIndex = 2;

    // Sizes derived from the qpi_proposals.h static_assert lines.
    public const int ProposalDataV1Size = 328;
    public const int ProposalDataYesNoSize = 304;
    public const int CcfSetProposalInputSize = 324; // ProposalDataYesNo(304) + 20 subscription bytes
    public const int VoteWireSize = 16;

    // Class codes
    public const ushort ClassGeneralOptions = 0x0000;
    public const ushort ClassTransfer = 0x0100;
    public const ushort ClassVariable = 0x0200;
    public const ushort ClassMultiVariables = 0x0300;
    public const ushort ClassTransferInEpoch = 0x0400;

    /// <summary>
    /// Cached QubicCrypt for identity conversion. Thread-safe: identity conversion
    /// is a pure function of the pubkey bytes; the underlying crypt object is
    /// stateless for this operation.
    /// </summary>
    private static readonly QubicCrypt Crypt = new();

    /// <summary>
    /// Deterministic contract identity: the 60-char identity of the pubkey
    /// [contractIndex(u64), 0, 0, 0]. Matches how Qubic core addresses its
    /// contracts on-chain.
    /// </summary>
    public static string GetContractIdentity(int contractIndex)
    {
        var pubkey = new byte[32];
        BinaryPrimitives.WriteUInt64LittleEndian(pubkey, (ulong)contractIndex);
        return Crypt.GetIdentityFromPublicKey(pubkey);
    }

    public static string GqmpropIdentity => _gqmpropIdentity ??= GetContractIdentity(GqmpropContractIndex);
    public static string CcfIdentity => _ccfIdentity ??= GetContractIdentity(CcfContractIndex);
    private static string? _gqmpropIdentity;
    private static string? _ccfIdentity;

    /// <summary>
    /// Decodes a SetProposal transaction's input_data. Returns null when the
    /// data is too short for the expected layout (partial/corrupted tx).
    /// </summary>
    public static DecodedProposal? DecodeProposal(int contractIndex, ReadOnlySpan<byte> data)
    {
        if (contractIndex == GqmpropContractIndex)
        {
            if (data.Length < ProposalDataV1Size) return null;
            return DecodeProposalDataV1(data[..ProposalDataV1Size]);
        }
        if (contractIndex == CcfContractIndex)
        {
            if (data.Length < ProposalDataYesNoSize) return null;
            var proposal = DecodeProposalDataYesNo(data[..ProposalDataYesNoSize]);
            // CCF SetProposal_input has trailing subscription fields when the tx
            // is at least the extended size. Older/simpler forms may just carry
            // the ProposalDataYesNo body — treat subscription as zeroed.
            if (data.Length >= CcfSetProposalInputSize)
            {
                var sub = data.Slice(ProposalDataYesNoSize, 20);
                proposal.IsSubscription = sub[0] != 0;
                proposal.SubscriptionWeeksPerPeriod = sub[1];
                // sub[2..3] padding
                proposal.SubscriptionStartEpoch = BinaryPrimitives.ReadUInt32LittleEndian(sub[4..]);
                proposal.SubscriptionAmountPerPeriod = BinaryPrimitives.ReadUInt64LittleEndian(sub[8..]);
                proposal.SubscriptionNumberOfPeriods = BinaryPrimitives.ReadUInt32LittleEndian(sub[16..]);
            }
            return proposal;
        }
        return null;
    }

    /// <summary>
    /// GQMPROP ProposalDataV1 layout (328 bytes):
    ///   0..255:  uint8[256] url
    ///   256..257: uint16 epoch
    ///   258..259: uint16 type
    ///   260..263: uint32 tick    (output-only, set by chain)
    ///   264..327: union data (64 bytes)
    ///     Transfer      { id destination(32); sint64 amounts[4](32) }
    ///     TransferInEpoch { id destination(32); sint64 amount(8); uint16 targetEpoch(2); pad(22) }
    ///     Variable      { uint64 variable(8); sint64 values[4](32); pad(24) }
    ///     VariableScalar{ uint64 variable(8); sint64 min(8); sint64 max(8); sint64 proposed(8); pad(32) }
    /// </summary>
    private static DecodedProposal DecodeProposalDataV1(ReadOnlySpan<byte> data)
    {
        var proposal = ReadHeader(data);

        var union = data.Slice(264, 64);
        var cls = proposal.ProposalClass;
        var options = proposal.OptionCount;

        if (cls == ClassTransfer)
        {
            proposal.TransferDestination = ReadIdentity(union.Slice(0, 32));
            var count = Math.Max(1, options - 1); // options include the "no change" option
            var amounts = new long[Math.Min(4, count)];
            for (int i = 0; i < amounts.Length; i++)
                amounts[i] = BinaryPrimitives.ReadInt64LittleEndian(union[(32 + i * 8)..]);
            proposal.TransferAmounts = amounts;
        }
        else if (cls == ClassTransferInEpoch)
        {
            proposal.TransferDestination = ReadIdentity(union.Slice(0, 32));
            proposal.TransferAmounts = [BinaryPrimitives.ReadInt64LittleEndian(union[32..])];
            proposal.TransferInEpochTargetEpoch = BinaryPrimitives.ReadUInt16LittleEndian(union[40..]);
        }
        else if (cls == ClassVariable || cls == ClassMultiVariables)
        {
            proposal.VariableId = BinaryPrimitives.ReadUInt64LittleEndian(union);
            if (options == 0)
            {
                // scalar-mean: min, max, proposed
                proposal.VariableScalarMin = BinaryPrimitives.ReadInt64LittleEndian(union[8..]);
                proposal.VariableScalarMax = BinaryPrimitives.ReadInt64LittleEndian(union[16..]);
                proposal.VariableScalarProposed = BinaryPrimitives.ReadInt64LittleEndian(union[24..]);
            }
            else
            {
                var count = Math.Max(1, options - 1);
                var vals = new long[Math.Min(4, count)];
                for (int i = 0; i < vals.Length; i++)
                    vals[i] = BinaryPrimitives.ReadInt64LittleEndian(union[(8 + i * 8)..]);
                proposal.VariableValues = vals;
            }
        }
        // ClassGeneralOptions: no payload — options are label-only ("Yes/No/Abstain").

        return proposal;
    }

    /// <summary>
    /// CCF ProposalDataYesNo layout (304 bytes):
    ///   0..255:  uint8[256] url
    ///   256..257: uint16 epoch
    ///   258..259: uint16 type   (always Transfer(0x0102) YesNo in CCF)
    ///   260..263: uint32 tick
    ///   264..303: union (40 bytes) — Transfer: { id destination(32); sint64 amount(8) }
    /// </summary>
    private static DecodedProposal DecodeProposalDataYesNo(ReadOnlySpan<byte> data)
    {
        var proposal = ReadHeader(data);
        if (proposal.ProposalClass == ClassTransfer)
        {
            proposal.TransferDestination = ReadIdentity(data.Slice(264, 32));
            proposal.TransferAmounts = [BinaryPrimitives.ReadInt64LittleEndian(data[296..])];
        }
        return proposal;
    }

    private static DecodedProposal ReadHeader(ReadOnlySpan<byte> data)
    {
        var url = ReadNullTerminatedString(data[..256]);
        var epoch = BinaryPrimitives.ReadUInt16LittleEndian(data[256..]);
        var type = BinaryPrimitives.ReadUInt16LittleEndian(data[258..]);
        var tick = BinaryPrimitives.ReadUInt32LittleEndian(data[260..]);

        return new DecodedProposal
        {
            Url = url,
            EpochField = epoch,
            ProposalType = type,
            ProposalClass = (ushort)(type & 0xff00),
            OptionCount = (byte)(type & 0x00ff),
            ProposalTickField = tick,
        };
    }

    /// <summary>
    /// Vote wire format (16 bytes): proposalIndex(2), proposalType(2), proposalTick(4), voteValue(8).
    /// Returns null on undersized data.
    /// </summary>
    public static DecodedVote? DecodeVote(ReadOnlySpan<byte> data)
    {
        if (data.Length < VoteWireSize) return null;
        var proposalIndex = BinaryPrimitives.ReadUInt16LittleEndian(data);
        var proposalType = BinaryPrimitives.ReadUInt16LittleEndian(data[2..]);
        var proposalTick = BinaryPrimitives.ReadUInt32LittleEndian(data[4..]);
        var voteValue = BinaryPrimitives.ReadInt64LittleEndian(data[8..]);
        var isWithdraw = voteValue == NoVoteValue;
        var isScalar = (proposalType & 0x00ff) == 0;
        byte option = 0;
        if (!isWithdraw && !isScalar && voteValue >= 0 && voteValue < 256)
            option = (byte)voteValue;
        return new DecodedVote
        {
            ProposalIndex = proposalIndex,
            ProposalType = proposalType,
            ProposalTick = proposalTick,
            VoteValue = voteValue,
            IsWithdraw = isWithdraw,
            Option = option,
        };
    }

    private static string ReadIdentity(ReadOnlySpan<byte> pubkey32)
    {
        var bytes = pubkey32.ToArray();
        if (bytes.All(b => b == 0))
            return "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAFXIB"; // burn / zero
        return Crypt.GetIdentityFromPublicKey(bytes);
    }

    private static string ReadNullTerminatedString(ReadOnlySpan<byte> span)
    {
        var len = span.IndexOf((byte)0);
        if (len < 0) len = span.Length;
        return Encoding.UTF8.GetString(span[..len]);
    }
}

public sealed class DecodedProposal
{
    public string Url { get; set; } = "";
    public ushort EpochField { get; set; }           // epoch encoded inside the proposal payload
    public ushort ProposalType { get; set; }
    public ushort ProposalClass { get; set; }
    public byte OptionCount { get; set; }
    public uint ProposalTickField { get; set; }      // tick encoded in payload (may be 0 pre-commit)

    public string TransferDestination { get; set; } = "";
    public long[] TransferAmounts { get; set; } = [];
    public ushort TransferInEpochTargetEpoch { get; set; }

    public ulong VariableId { get; set; }
    public long[] VariableValues { get; set; } = [];
    public long VariableScalarMin { get; set; }
    public long VariableScalarMax { get; set; }
    public long VariableScalarProposed { get; set; }

    public bool IsSubscription { get; set; }
    public byte SubscriptionWeeksPerPeriod { get; set; }
    public ulong SubscriptionAmountPerPeriod { get; set; }
    public uint SubscriptionNumberOfPeriods { get; set; }
    public uint SubscriptionStartEpoch { get; set; }
}

public sealed class DecodedVote
{
    public ushort ProposalIndex { get; set; }
    public ushort ProposalType { get; set; }
    public uint ProposalTick { get; set; }
    public long VoteValue { get; set; }
    public bool IsWithdraw { get; set; }
    public byte Option { get; set; }
}
