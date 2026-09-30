# Qubic core integration inventory

Everything in the explorer that depends on Qubic C++ core (`/home/claude/qubic`) — grouped by category, with file:line pointers. When core makes a change that could affect this project, walk this list top to bottom.

**Last synced:** core develop @ ~v1.306.0 / epoch 233 (Sept 30 2026). Update this line each time you run the check.

**How to use this:** the [Checklist for a core-sync review](#checklist-for-a-core-sync-review) at the bottom is the actionable playbook. Everything above it is the reference material behind the checklist.

---

## 1. Hand-written binary decoders (C++ struct → C# bytes)

The explorer parses Qubic-core structs directly from raw bytes in a handful of places. Each of these is one refactor away from silent drift if a field is added or reordered on the C++ side.

| Where | What it decodes | Source header |
|---|---|---|
| [TransactionInputParser.cs](../src/QubicExplorer.Shared/Constants/TransactionInputParser.cs) | Every `inputType` payload (types 2, 3, 4, 5, 6, 7, 8-legacy, 9, 10, 11, 12, 13) + Oracle interface sub-parsers (Price / Mock / EvmLogRead / QubicLogRead) + Qubic `DateAndTime` packed uint64 + 10-bit packed decoder | `mining/mining.h`, `files/files.h`, `oracle_core/oracle_transactions.h`, `network_messages/execution_fees.h`, `oc_core/oc_transactions.h` |
| [ProposalDecoder.cs](../src/QubicExplorer.Shared/Constants/ProposalDecoder.cs) | `ProposalDataV1` (328 B), `ProposalDataYesNo` (304 B), `ProposalSingleVoteDataV1` (16 B), `CcfSetProposalInput` (324 B), the 4 class union payloads | `qpi/qpi_proposals.h`, `contracts/GeneralQuorumProposal.h`, `contracts/ComputorControlledFund.h` |
| [CcfContractParser.cs](../src/QubicExplorer.Api/Services/CcfContractParser.cs) | `GetProposal` / `GetVotingResults` / `GetLatestTransfers` / `GetRegularPayments` / `SubscriptionData` output blobs | `contracts/ComputorControlledFund.h` |
| [SpectrumImportService.cs](../src/QubicExplorer.Api/Services/SpectrumImportService.cs) | `spectrum.???` full binary snapshot — 64 B entries (pubkey + incoming/outgoing + counters + last-tick pair) | `spectrum.h` |
| [UniverseImportService.cs](../src/QubicExplorer.Api/Services/UniverseImportService.cs) | `universe.???` asset snapshot — 48 B `AssetRecord` per entry | `assets.h` |
| [OracleEventService.cs](../src/QubicExplorer.Analytics/Services/OracleEventService.cs) | Oracle commit/reveal wire (duplicates parts of `TransactionInputParser`) | `oracle_core/oracle_transactions.h` |
| [ExecutionFeeReportService.cs](../src/QubicExplorer.Analytics/Services/ExecutionFeeReportService.cs) | ExecutionFeeReport variable payload (duplicates part of `TransactionInputParser`) | `network_messages/execution_fees.h` |
| [ContractReserveSnapshotService.cs](../src/QubicExplorer.Analytics/Services/ContractReserveSnapshotService.cs) | Packs contractIndex → QUtil.QueryFeeReserve output | `contracts/QUtil.h` |
| [ComputorFlowService.cs](../src/QubicExplorer.Analytics/Services/ComputorFlowService.cs) | QUtil SendMany input (25 destinations × 32 B + 25 × 8 B amounts = 1000 B) | `contracts/QUtil.h` |
| [ComputorRevenueService.cs](../src/QubicExplorer.Analytics/Services/ComputorRevenueService.cs) | Full V1 revenue formula: packed 10-bit vote/mining decoder, per-source scores, quorum-scaled totals | `revenue.h`, `mining/mining.h` |
| [ClickHouseQueryService.cs (revenue recompute)](../src/QubicExplorer.Api/Services/ClickHouseQueryService.cs) | Query-time V1 revenue recompute (duplicates `ComputorRevenueService` logic — drift risk) | same as above |

**Drift hotspots (duplicate implementations of the same C++ concept):**
- The **packed 10-bit extractor** lives in 4 places: `TransactionInputParser.cs`, `ComputorRevenueService.cs`, `TickVotePersistenceService.cs`, `ClickHouseQueryService.cs`. Consider centralising into `QubicExplorer.Shared`.
- `ExecutionFeeReport` layout is decoded twice (`TransactionInputParser.cs` + `ExecutionFeeReportService.cs`).

---

## 2. Protocol constants

Canonical source: `Qubic.Core.QubicConstants` (in the NuGet). What the explorer **should** use for every one of these, but currently doesn't always:

| Constant | C++ source | NuGet | Where the explorer inlines it as a magic number |
|---|---|---|---|
| `NUMBER_OF_COMPUTORS = 676` | `network_messages/common_def.h` | `QubicConstants.NumberOfComputors` | `ComputorOwnerSummaryService.cs:183,239`; `ClickHouseQueryService.cs:4047,5790,5932,8783,6937`; `StatsController.cs:536,609,642,817,903`; `ClickHouseSchema.cs:913,1237` |
| `QUORUM = 451` (= 676×2/3+1) | `network_messages/common_def.h` | `QubicConstants.Quorum` | `OracleAggregateService.cs:27`; `ComputorRevenueService.cs:347,550`; `ClickHouseQueryService.cs:7138,7344-45,7983-8018,8772` |
| `MajorityHalf = 225` (= Quorum/2) | derived | derived | `ClickHouseQueryService.cs:8778,8780`; `ClickHouseSchema.cs:1226-28` |
| `PackedComputorDataSize = 848` | `network_messages/common_def.h` | `CoreTransactionInputTypes.PackedComputorDataSize` | `TransactionInputParser.cs:75,107,336,355` |
| `SPECTRUM_DEPTH = 24` | `network_messages/common_def.h` | — | `SpectrumImportService.cs:26-28` (defined locally) |
| `ASSETS_DEPTH = 24`, `AssetRecord=48` | `assets.h` | — | `UniverseImportService.cs:25-33` (defined locally) |
| `SIGNATURE_SIZE = 64` | `network_messages/common_def.h` | — | Implicit — payload sizes assume it |
| `IssuanceRate`, `IssuancePerComputor`, `MaxSupply`, `ArbitratorIdentity`, `RevenueScalingThreshold` | `public_settings.h`, `revenue.h` | `QubicConstants.*` | Used correctly via the NuGet |

**Also mirrored locally in [ProposalDecoder.cs:27-29](../src/QubicExplorer.Shared/Constants/ProposalDecoder.cs)** — self-contained "just use `ProposalDecoder.Quorum`" style.

**Also mirrored in [CcfContractParser.cs:15-18](../src/QubicExplorer.Api/Services/CcfContractParser.cs)** — comment says "Quorum = 452" (off by one — cosmetic only, the code doesn't use that constant).

**Multi-dim revenue constants** in [MultiDimRevenueCalculator.cs](../src/QubicExplorer.Shared/Services/MultiDimRevenueCalculator.cs):
- `CONTRACT_DIMS = <contractCount>` — this **must** be bumped when core adds a new user contract.
- `TRANSFER_DIM = 676 + CONTRACT_DIMS`
- `REVENUE_TX_DIM = 676 + CONTRACT_DIMS + 1`
- `DefaultV2FromEpoch = 209` — the epoch mining V2 revenue-factor formula switched over. Not likely to change again, but reference.

---

## 3. Contract indexes & procedures

Canonical source: `contract_core/contract_def.h` (indexes) + each `contracts/<Name>.h` (procedures via `REGISTER_USER_PROCEDURE`).

**Explorer C# side** — hard-coded contract indexes:
- [ProposalDecoder.cs:33-37](../src/QubicExplorer.Shared/Constants/ProposalDecoder.cs) — GQMPROP=6, CCF=8, SetProposal=1, Vote=2
- [ComputorFlowService.cs:26](../src/QubicExplorer.Analytics/Services/ComputorFlowService.cs) — QUtil=4
- [CustomFlowTrackingService.cs:13](../src/QubicExplorer.Analytics/Services/CustomFlowTrackingService.cs) — QUtil=4 (duplicate)
- [ContractReserveSnapshotService.cs:104](../src/QubicExplorer.Analytics/Services/ContractReserveSnapshotService.cs) — QUtil function id 8 = `QueryFeeReserve` inlined
- [OracleEventService.cs:28-29](../src/QubicExplorer.Analytics/Services/OracleEventService.cs) — inputTypes 6=Commit, 7=Reveal

**Explorer frontend side** — full schema table (contract index → procedures with typed fields):
- [contractInputDecoder.ts](../frontend/utils/contractInputDecoder.ts) — `CONTRACT_SCHEMAS` at end of file
- Currently covers indexes 1..20. **Missing 21..30.** See the [full contract table](#current-contract-registry) below.

**Contract identity synthesis** — `ProposalDecoder.GetContractIdentity(index)` builds the on-chain identity from `[uint64 index, 24 zero bytes]`. Matches `isPublicKeyOfContract()` in core.

### Current contract registry

Per core `contract_core/contract_def.h` (v1.306.0, epoch 233):

| Idx | Name | Header | Explorer C# constants | Frontend schema |
|---:|---|---|---|:---:|
| 1 | QX | `Qx.h` | — | ✅ |
| 2 | QUOTTERY | `Quottery.h` | — | ✅ |
| 3 | RANDOM | `Random.h` | — | ✅ |
| 4 | QUTIL | `QUtil.h` | ✅ (multiple) | ✅ |
| 5 | MLM | `MyLastMatch.h` | — | ✅ (no procedures) |
| 6 | GQMPROP | `GeneralQuorumProposal.h` | ✅ | ✅ |
| 7 | SWATCH | `SupplyWatcher.h` | — | ✅ (no procedures) |
| 8 | CCF | `ComputorControlledFund.h` | ✅ | ✅ |
| 9 | QEARN | `Qearn.h` | — | ✅ |
| 10 | QVAULT | `QVAULT.h` | — | ✅ |
| 11 | MSVAULT | `MsVault.h` | — | ✅ |
| 12 | QBAY | `Qbay.h` | — | ✅ |
| 13 | QSWAP | `Qswap.h` | — | ✅ |
| 14 | NOST | `Nostromo.h` | — | ✅ ⚠️ **state schema migrated at epoch 230** |
| 15 | QDRAW | `Qdraw.h` | — | ✅ |
| 16 | RL | `RandomLottery.h` | — | ✅ |
| 17 | QBOND | `QBond.h` | — | ✅ |
| 18 | QIP | `QIP.h` | — | ✅ |
| 19 | QRAFFLE | `QRaffle.h` | — | ✅ |
| 20 | QRWA | `qRWA.h` | — | ✅ |
| 21 | QRP | `QReservePool.h` | — | ✅ |
| 22 | QTF | `QThirtyFour.h` | — | ✅ |
| 23 | QDUEL | `QDuel.h` | — | ✅ |
| 24 | PULSE | `Pulse.h` | — | ✅ |
| 25 | VOTTUNBRIDGE | `VottunBridge.h` | — | ✅ |
| 26 | QUSINO | `Qusino.h` | — | ✅ |
| 27 | ESCROW | `Escrow.h` | — | ✅ |
| 28 | WOLFPACK | `GGWP.h` | — | ✅ |
| 29 | QPAYHUB | `QPayhub.h` | — | ✅ (construction epoch 231) |
| 30 | QTREAT | `QTREAT.h` | — | ✅ (construction epoch 233) |

**Backlog:** frontend schema entries for 21..30 to render decoded input on the tx detail page — see `frontend/utils/contractInputDecoder.ts` for the pattern.

---

## 4. Transaction input types (core protocol txs to burn / computor identities)

Canonical source: every `static constexpr unsigned char transactionType()` in core.

| Type | Struct | Payload | Explorer decoder |
|---:|---|---|---|
| 2 | `MiningSolutionTransaction` | 72 B (seed + nonce + score + reserved) | ✅ `TransactionInputParser.ParseMiningSolution` |
| 3 | `FileHeaderTransaction` | 24 B | ✅ |
| 4 | `FileFragmentTransactionPrefix` | 40 B + payload | ✅ |
| 5 | `FileTrailerTransaction` | 56 B | ✅ |
| 6 | `OracleReplyCommit…` | n × 72 B items | ✅ |
| 7 | `OracleReplyReveal…` | 8 B + reply | ✅ + Oracle-interface sub-parsers (Price, Mock, EvmLogRead, QubicLogRead) |
| 8 | *(legacy CustomMiningShareCounter; unused in current core)* | 880 B | ✅ (kept for historical rows) |
| 9 | `ExecutionFeeReport…` | variable | ✅ |
| 10 | `OracleUserQuery…` | 8 B + query data | ✅ |
| 11 | `DogeMiningShareTransaction` | 880 B (== legacy 8 layout) | ✅ |
| 12 | `AntColonyMiningSolutionTransaction` | 48 B fixed | ✅ |
| 13 | `OcAuthSignatureTransactionPrefix` | 4 + n × 112 B | ✅ |

**When core adds an ID 14+:** follow [docs/updating-transaction-decoders.md](./updating-transaction-decoders.md) — the step-by-step runbook already exists.

**Nonce semantics on type 2 (MiningSolution) — algorithm-specific:**
- `nonce[0]` = `AlgoType` enum: `0 = Neuraxon` (reserved, never executed), `1 = Bpp9000`
- For `Bpp9000` solutions:
  - `nonce[1]` low nibble = changes-per-step (1..10)
  - `nonce[1]` bits 4-5 = mode: `1 = START`, `2 = WIRING`, `3 = LUT`
  - `nonce[2]` = 0 for standalone, else ant-mutation count (0..numberOfMutations)
- Source: `mining/score_common.h`, `mining/score_bpp9000.h`

**Nonce semantics on type 12 (AntColonyMiningSolution):**
- `parentTick=0 && parentSolutionIndexInTick=0xFFFFFFFF` = `ROOT_REF` — depth-1 solution, root derived from the identity's pubkey
- Any other value = shared inherited parent (EP229+ shared-root model)
- Source: `ant_colony/ant_colony.h:75-82`

---

## 5. LogTypes

Canonical source: `logging/logging.h`.

**Three parallel tables in the explorer (drift risk):**
- [LogTypes.cs](../src/QubicExplorer.Shared/Constants/LogTypes.cs) — most complete; source of truth for the analytics services
- [BobMessages.cs](../src/QubicExplorer.Indexer/Models/BobMessages.cs) — indexer-side; subset (was missing 14+15, now needs 16 too)
- [BobLog.cs](../src/QubicExplorer.Shared/Models/BobLog.cs) — API/analytics DTO with per-log-type body decoders

**Current numeric enum** (v1.306.0):
```
0  QU_TRANSFER
1  ASSET_ISSUANCE
2  ASSET_OWNERSHIP_CHANGE
3  ASSET_POSSESSION_CHANGE
4  CONTRACT_ERROR_MESSAGE
5  CONTRACT_WARNING_MESSAGE
6  CONTRACT_INFORMATION_MESSAGE
7  CONTRACT_DEBUG_MESSAGE
8  BURNING
9  DUST_BURNING
10 SPECTRUM_STATS
11 ASSET_OWNERSHIP_MANAGING_CONTRACT_CHANGE
12 ASSET_POSSESSION_MANAGING_CONTRACT_CHANGE
13 CONTRACT_RESERVE_DEDUCTION
14 ORACLE_QUERY_STATUS_CHANGE
15 ORACLE_SUBSCRIBER_MESSAGE
16 OC_INVOCATION_STATUS_CHANGE          <-- new since last sync
255 CUSTOM_MESSAGE
```

**CustomMessage op codes** (magic uint64 in the log payload; header `logging/logging.h`):
```
STA_DDIV  6217575821008262227    (start distribute-dividends)
END_DDIV  ..                     (end distribute-dividends)
STA_EPOC  ..                     (start epoch)
END_EPOC  ..                     (end epoch)
ANT_SOLU  6146374810954124865    <-- new (per-ant-solution outcome)
```

**Payload structs for new log types:**
- `OcInvocationStatusChange` (13 B, type 16): `{ int64 invocationId; uint32 contractIndex; uint32 interfaceIndex; uint8 status; }`
- `AntSolutionLogMessage` (88 B, type 255 with ANT_SOLU op): `{ pubkey source(32); m256 nonce(32); u32 parentTick; u32 parentSolIdxInTick; u32 anchorTick; u32 score; u8 result; }`

---

## 6. Bob JSON-RPC method names

The explorer never speaks the peer-to-peer wire protocol (peer `NetworkMessageType` codes are not touched anywhere). Everything goes through Bob JSON-RPC / websocket.

**Direct method-string literals:**
- `qubic_getTickNumber` — `BobConnectionService.cs:499`
- `qubic_getEndEpochLogs` — `BobProxyService.cs:112`, `Analytics/BobProxyService.cs:108`
- `qubic_getLogsByIdRange` — `BobProxyService.cs:131`
- `qubic_getTickByNumber` — `TickCrossCheckService.cs:440`

**Typed helper calls via `Qubic.Bob` NuGet** (RPC method name owned by the NuGet):
- `.GetBalanceAsync` — balance lookup
- `.GetEpochInfoAsync` — epoch bounds (initial/end tick)
- `.GetComputorsAsync` — 676 computor identities per epoch
- `.QuerySmartContractAsync` — arbitrary contract function call
- `.GetTickLogRangesAsync` — cross-check tick logs
- `.GetTransactionByHashAsync`, `.GetTransactionReceiptAsync` — cross-check tx state
- `.SubscribeAsync` — tick stream
- `.CallAsync<T>` — generic RPC passthrough

**Peer messages we don't touch but should be aware of** (v1.306.0):
- 72..77: `REQUEST/RESPOND_ANT_IDENTITY_TREE`, `REQUEST/RESPOND_ANT_PARENT_ANN`, `REQUEST/RESPOND_ANT_EPOCH_CONTEXT` — the new score-fetching flow for ant-colony solutions. Any future analytics that wants to render ant trees will need Bob-side RPC exposure of these.

---

## 7. Score / mining analytics

Everything that touches solution counting, scoring, or algorithm-specific logic.

**Nonce interpretation** — `TransactionInputParser.cs:126-146` (`ParseMiningSolution`), `:374-382` (`ParseAntColonyMiningSolution`), `:152-157` (`AlgoTypeName`).

**Solution counting** — `input_type=2` transactions to computor identities are counted as "Qubic solutions":
- `ClickHouseQueryService.cs:3799-3800, 3842, 5846-5918, 6037, 6051, 6085-6100`
- `ComputorOwnerSummaryService.cs:67-108, 177-269`
- `EpochStatsDto.cs:19` documents the canonical definition
- `AnalyticsDto.cs:423, 860, 1330-1362` — DTO fields (`MiningScore`, `DogePoints`, `QubicSolutions`, `QubicSolutionsPercent`)
- `ClickHouseSchema.cs:492, 1063` — schema comments

**Per-computor V1 revenue formula** — `ComputorRevenueService.cs`. Anchored by `gTxRevenuePoints[]` LUT copied from `revenue.h` (see [QubicProtocolParams.cs](../src/QubicExplorer.Shared/Services/QubicProtocolParams.cs) for the LUT + `NewTxLimitsFromEpoch=214`).

**V2 revenue factor** — `RevenueV2Calculator.cs` (`DefaultV2FromEpoch=209`, `S=1024`, `GlobalQuorumRank=451`, dogeScore + oracleScore combined). Consumed by `MultiDimRevenueCalculator.cs`.

**Ant-colony score fetching (new in v1.306.0)** — the C++ pipeline was rebuilt around three RPC message pairs (72..77 above) and per-node artifacts:
- `snapshotAntColonyHeader.???`, `snapshotAntColonyRecords.???`, `snapshotAntColonyPool.???`, `antColonySolutions.eoe`, `snapshotAntSolutionFlag`
- `AntSolutionRecord` (112 B) holds score + shift + depth + childAnnHash per solution
- Explorer currently only ingests the on-chain tx (type 12) — it doesn't consume ant tree state. If we later want to render the ant tree UI, that's a separate integration through Bob RPC.

---

## 8. NuGet Qubic.* API surface

Which types/methods the explorer actually depends on. Freeze this list to detect NuGet-side breaking changes.

**`Qubic.Core.QubicConstants`:** `NumberOfComputors`, `Quorum`, `IssuanceRate`, `IssuancePerComputor`, `RevenueScalingThreshold`, `MaxSupply`, `ArbitratorIdentity`.

**`Qubic.Core.QubicContracts`:** `Gqmprop`, `Ccf`, `Qutil`. (Others like `Qx`, `Quottery`, `Random`… exist but aren't currently used by the explorer's server side.)

**`Qubic.Core.CoreTransactionInputTypes`:** `VoteCounter`, `MiningSolution`, `FileHeader`, `FileFragment`, `FileTrailer`, `OracleReplyCommit`, `OracleReplyReveal`, `CustomMiningShareCounter`, `ExecutionFeeReport`, `OracleUserQuery` + helpers `IsKnownType`, `GetDisplayName`, `GetMinInputSize`, `PackedComputorInputSize`, `PackedComputorDataSize`.

**`Qubic.Core.Contracts.Ccf`:** `GetLatestTransfersOutput.FromBytes`, `GetRegularPaymentsOutput.FromBytes`, `GetProposalIndicesInput/Output`, `GetProposalInput`, `GetVotingResultsInput`, `GetProposalFeeOutput`, `CcfContract.Functions.*`.

**`Qubic.Core.Contracts.Gqmprop`:** `GetRevenueDonationOutput.FromBytes`.

**`Qubic.Crypto.QubicCrypt`:** `GetIdentityFromPublicKey`, `GetHumanReadableBytes` (nothing else).

**`Qubic.Bob`:** `BobWebSocketClient` with the 9 methods listed in §6.

**Package versions** (in [src/local-packages/](../src/local-packages/)):
- `Qubic.Core.1.279.0.4.nupkg` — behind v1.306.0 core. Local mirrors in `TransactionInputParser.cs:17-27` cover types 11/12/13 not yet in the NuGet.
- `Qubic.Bob.1.4.1.nupkg`

**Bump-the-NuGet checklist:** whenever `Qubic.Core` gets a new release, check whether the local mirrors in `TransactionInputParser.cs` are now covered by NuGet constants — if yes, delete the local copies and reference the NuGet ones. Same for any new `CoreTransactionInputTypes.*` constant beyond the 10 currently exposed.

---

## 9. Downstream: what depends on all of the above

**ClickHouse schema** — [ClickHouseSchema.cs](../src/QubicExplorer.Shared/ClickHouseSchema.cs). Any column that mirrors a core constant (676, 451, 225, packed sizes) is called out in the comments. The proposals tables are documented at bottom.

**On-wire tick / tx / log models:**
- Bob JSON binding lives at [BobMessages.cs](../src/QubicExplorer.Indexer/Models/BobMessages.cs) (`TickStreamData`, `BobTransaction`) + [BobLog.cs](../src/QubicExplorer.Shared/Models/BobLog.cs).
- Sole binding site from Bob → explorer models is `BobConnectionService.MapToTickStreamData` at [BobConnectionService.cs:352-419](../src/QubicExplorer.Indexer/Services/BobConnectionService.cs).
- Downstream column order for the DB `logs` bulk insert is `ClickHouseWriterService.cs:313`.

---

## Checklist for a core-sync review

Walk this list when core has changed and we need to verify the explorer:

- [ ] **Version constants** — bump the `Last synced` line at the top of this doc. Confirm `VERSION_A/B/C` and epoch number reflect current core.
- [ ] **AlgoType enum** — grep `AlgoTypeName` in `TransactionInputParser.cs`. Are all core algo codes covered?
- [ ] **New tx input types** — grep core for `static constexpr unsigned char transactionType()` and compare vs §4. If new, follow `docs/updating-transaction-decoders.md`.
- [ ] **New contracts** — read `contract_core/contract_def.h`. If new indexes:
    1. Add row to §3 table above
    2. Add schema entry to `frontend/utils/contractInputDecoder.ts`
    3. If they're used for revenue analytics, bump `MultiDimRevenueCalculator.CONTRACT_DIMS`
- [ ] **New log types** — read `logging/logging.h` enum. Update all three tables in §5 if it grew.
- [ ] **New CustomMessage op codes** — same file. Add to `LogTypes.cs` const table.
- [ ] **Proposal framework** — read `qpi/qpi_proposals.h`. Confirm `ProposalDataV1` (328 B), `ProposalDataYesNo` (304 B), `ProposalSingleVoteDataV1` (16 B) static-assert sizes still match `ProposalDecoder.cs` constants.
- [ ] **`QubicConstants`** — if the NuGet was bumped, remove any local mirrors that the NuGet now covers.
- [ ] **`Qubic.Core.Contracts.*`** — new generated types on the NuGet mean we can drop hand-decoders like `CcfContractParser`. Verify.
- [ ] **Bob RPC surface** — if `Qubic.Bob` NuGet was bumped, check §6 method list for renames/additions.
- [ ] **Contract state migrations** — read `contractStateChangeInfos[]` in `contract_def.h`. Any epoch-boundary migration means downstream state caches need invalidation.
- [ ] **NOST at epoch 230 (already handled)** — reference example of a migration.

**Common gotchas** (things this doc calls drift hotspots):
1. Packed 10-bit decoder is duplicated 4× — fix in one place, remember to sync
2. LogTypes exists in 3 tables — update all three
3. `Quorum=451` inlined in 6 files — grep for it
4. `CONTRACT_DIMS` in `MultiDimRevenueCalculator.cs` is a raw number
5. Frontend contract schemas need indexes bumped for new contracts
