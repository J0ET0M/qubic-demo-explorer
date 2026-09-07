# Updating the explorer's transaction decoders

When Qubic core adds a new **transaction input type** (a new `transactionType()` in
one of the C++ headers), the explorer won't decode it until the following changes
land on our side. This is the runbook for that update.

Latest sync: see `git log --grep "sync core tx types"` for the last time this ran.

---

## 0. Prerequisites

- A checkout of Qubic core at a known commit (currently `/home/claude/qubic`).
- The explorer repo (this repo).
- Docker + push access to `docker.io/qubicli/*` (for the deploy step).

---

## 1. Enumerate the current transaction types in core

The authoritative list lives across a handful of headers. Grep for the shared
signature:

```bash
grep -rn "static constexpr unsigned char transactionType()" ~/qubic/src/
```

Also check named constants used in place of literal IDs:

```bash
grep -rEn "_INPUT_TYPE\s*=|const.*INPUT_TYPE" ~/qubic/src/
```

Cross-check the base transaction layout (`network_messages/transactions.h`) hasn't
changed — the `Transaction` prefix is 80 bytes with `inputType` at offset 76.

Every hit should map to one entry in the table below. IDs currently in use as
of this doc (Aug 2026):

| ID | Struct | Header | Explorer support |
|----|--------|--------|-------------------|
| 2  | `MiningSolutionTransaction` | `mining/mining.h` | ✅ |
| 3  | `FileHeaderTransaction` | `files/files.h` | ✅ |
| 4  | `FileFragmentTransactionPrefix` | `files/files.h` | ✅ |
| 5  | `FileTrailerTransaction` | `files/files.h` | ✅ |
| 6  | `OracleReplyCommitTransactionPrefix` | `oracle_core/oracle_transactions.h` | ✅ |
| 7  | `OracleReplyRevealTransactionPrefix` | `oracle_core/oracle_transactions.h` | ✅ |
| 8  | `CustomMiningShareCounter` (legacy) | `mining/mining.h` (old) | ✅ (historical) |
| 9  | `ExecutionFeeReportTransactionPrefix` | `network_messages/execution_fees.h` | ✅ |
| 10 | `OracleUserQueryTransactionPrefix` | `oracle_core/oracle_transactions.h` | ✅ |
| 11 | `DogeMiningShareTransaction` | `mining/mining.h` | ✅ |
| 12 | `AntColonyMiningSolutionTransaction` | `mining/mining.h` | ✅ |
| 13 | `OcAuthSignatureTransactionPrefix` | `oc_core/oc_transactions.h` | ✅ |

**IDs 0 and 1** are unused (legacy `VoteCounter=1` was retired). The NuGet
`Qubic.Core` still exposes `VoteCounter=1` for backwards compatibility.

---

## 2. Confirm what the `Qubic.Core` NuGet already exposes

The NuGet at `/home/claude/qubic-explorer/src/local-packages/Qubic.Core.<version>.nupkg`
provides constants + `GetMinInputSize` / `GetName` / `GetDisplayName` for
already-known types. Inspect its coverage without a live restore:

```bash
unzip -p ~/qubic-explorer/src/local-packages/Qubic.Core.*.nupkg 'lib/net8.0/Qubic.Core.dll' \
  > /tmp/qc.dll
strings /tmp/qc.dll | grep -iE '^[A-Z][a-zA-Z]*(Transaction|Solution|Counter|Query|Reply|Report|Signature|Fragment|Header|Trailer|Share)$'
```

If the NuGet is behind (which is the common case), we add the missing constants
**locally** in `src/QubicExplorer.Shared/Constants/TransactionInputParser.cs`:

```csharp
public const ushort DogeMiningShare = 11;
public const ushort AntColonyMiningSolution = 12;
public const ushort OcAuthSignature = 13;
```

When those types land in a future NuGet release, remove the local `const`s and
switch the parser's `switch` branches to `CoreTransactionInputTypes.<Name>`.

---

## 3. Read the payload byte layout from core

For each new type, open its C++ struct and note **offset, size, meaning** for
every field. The convention is little-endian for all integers. Common types:

- `unsigned char` / `uint8` = 1 byte
- `unsigned short` / `uint16` = 2 bytes LE
- `unsigned int` / `uint32` = 4 bytes LE
- `unsigned long long` / `uint64` = 8 bytes LE
- `long long` / `int64` = 8 bytes LE
- `m256i` = 32 bytes raw (usually rendered as hex)
- Fixed arrays: `T[N]` = N × sizeof(T), contiguous

If the payload wraps a variable-length list (e.g. `OcAuthSignaturePrefix` has
`itemCount` + `padding` + `items[]`), decode the header first, then loop over
items — clamp your loop to `min(itemCount, (data.Length - headerSize) / itemSize)`
so a truncated tx never over-reads.

---

## 4. Add the parser

In `src/QubicExplorer.Shared/Constants/TransactionInputParser.cs`:

1. Add a local `const ushort NewType = <id>;` (if the NuGet doesn't have it).
2. Add the size to the fallback `minSize` switch in `Parse(...)`.
3. Add a `NewType => ParseNewType(data)` branch to the outer switch.
4. Implement `ParseNewType(byte[] data)` using `BinaryPrimitives.ReadXxxLittleEndian`
   and `ToHexString(data, offset, length)`. Return a new record type.
5. Define a new `record NewTypeInputData(...) : ParsedInputData` at the bottom
   of the file with `TypeName => "NEW_TYPE"`.
6. Register the new record in the `[JsonDerivedType(...)]` list on `ParsedInputData`
   with the same `"NEW_TYPE"` discriminator.
7. If the new type is meant to be decoded even when the destination isn't the
   burn address (like mining solutions to computor identities), add its ID to
   `IsCoreProtocolType(ushort)`.

Verify:

```bash
dotnet build src/QubicExplorer.Shared/QubicExplorer.Shared.csproj -c Release
```

---

## 5. Wire the decode gate (only if the tx doesn't go to the burn address)

The API decides whether to run `TransactionInputParser.Parse` at two call sites
in `src/QubicExplorer.Api/Services/ClickHouseQueryService.cs`. Both use:

```csharp
var isCoreTransaction = string.Equals(to, AddressLabelService.BurnAddress, ...);
var isNonContractDest = !isCoreTransaction && !_labelService.IsSmartContract(to);
var parsedInput = (isCoreTransaction ||
                   (isNonContractDest && TransactionInputParser.IsCoreProtocolType(inputType)))
    ? TransactionInputParser.Parse(inputType, inputData)
    : null;
```

Why the `!IsSmartContract` guard: contract procedure indices overlap numerically
with core protocol input types (e.g. Quottery procedure 11 = `TransferQUSD`, same
number as `DogeMiningShare`). Without the guard we'd mis-decode a contract call
as a core tx.

---

## 6. Frontend display

Two files:

- `frontend/composables/useApi.ts` — extend the `interface ParsedInputData`
  with the new optional fields. Everything's `?` so old rows without the new
  fields still deserialize.
- `frontend/components/CoreInputDataViewer.vue` — add a `v-else-if="parsed.typeName === 'NEW_TYPE'"`
  branch. Reuse the existing `detail-row` / `detail-label` / `detail-value` style
  classes; the "Raw Hex" toggle at the bottom already handles fallback.

Verify:

```bash
cd frontend && npx nuxt build
```

---

## 7. Contract procedures (a different flow)

New **contract procedures** (input types 1..N sent to a contract's identity) are
decoded entirely client-side by `frontend/utils/contractInputDecoder.ts`. That
file has one `ContractSchema` entry per contract. To add a new procedure:

1. Find the new `REGISTER_USER_PROCEDURE(_WITH_LOCALS)(Name, index)` in the
   contract's header at `~/qubic/src/contracts/<Name>.h` and the matching
   `<Name>_input` struct definition.
2. Add a `ProcedureSchema` to the contract's `procedures` map in
   `contractInputDecoder.ts` with the numeric index, name, and typed `fields[]`.
3. If it's a brand-new contract, register a new `ContractSchema` and confirm
   the contract index against `~/qubic/src/contract_core/contract_def.h`.
4. Also update the `bundle.min.json` at `static.qubic.org/v1/general/data/`
   with the new contract + procedures — this is what `AddressLabelService`
   loads at runtime to power the `/api/labels/procedure/{addr}/{type}` name
   resolution. Coordinate with whoever owns that bundle.

No backend changes needed for pure contract-procedure additions.

---

## 8. Deploy

```bash
# Full solution build
dotnet build src/QubicExplorer.sln -c Release

# Rebuild + push affected images (both :latest and :prod)
docker build -f src/QubicExplorer.Api/Dockerfile \
  -t qubicli/qubic-explorer-api:latest -t qubicli/qubic-explorer-api:prod src
docker build -f frontend/Dockerfile \
  -t qubicli/qubic-explorer-frontend:latest -t qubicli/qubic-explorer-frontend:prod frontend

for tag in latest prod; do
  docker push qubicli/qubic-explorer-api:$tag
  docker push qubicli/qubic-explorer-frontend:$tag
done
```

Operator side:

```bash
docker compose pull api frontend && docker compose up -d api frontend
```

No DB migration is needed — decoding is on-demand at read time; nothing is
persisted from the parser output.

---

## 9. Verify in a browser

Pick a real transaction of the new type and open its detail page:

```bash
# Find a recent one — e.g. the newest ant-colony mining tx:
docker compose exec clickhouse clickhouse-client --query "
  SELECT hash, tick_number, to_address FROM qubic.transactions
  WHERE input_type = 12 ORDER BY tick_number DESC LIMIT 5"
```

Visit `/tx/<hash>` and confirm the new fields render. If the viewer falls back
to "Raw Hex" only, the JSON polymorphism discriminator didn't match — double-check
the `TypeName` property on the record matches the `[JsonDerivedType]` string
matches the `parsed.typeName === '...'` in the Vue component.

---

## Quick reference: files that always change together

| File | Purpose |
|---|---|
| `src/QubicExplorer.Shared/Constants/TransactionInputParser.cs` | The parser + records + polymorphism registry |
| `src/QubicExplorer.Api/Services/ClickHouseQueryService.cs` | Decode gate (only touch for non-burn destinations) |
| `frontend/composables/useApi.ts` | TypeScript `ParsedInputData` interface |
| `frontend/components/CoreInputDataViewer.vue` | Render branch per type |
| `src/local-packages/Qubic.Core.<version>.nupkg` | Bump when a newer NuGet exposes the new type — then remove the local constants |
