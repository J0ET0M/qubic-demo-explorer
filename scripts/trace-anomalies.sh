#!/usr/bin/env bash
# Trace forward QU flows from contract-anomaly recipients, halting at exchanges.
# Records the deposit address (the sender into the exchange) and the exchange's name.
#
# Causality constraint: at each hop, an address's outgoing transfers are only
# considered if they occurred at or after the tick at which it first received
# the anomalous funds. Pre-existing outflows aren't part of the laundering chain.
#
# Usage:
#   ./trace-anomalies.sh
#   EPOCHS="216,217,218" DEPTH=8 ./trace-anomalies.sh
#   LABELS_API=http://localhost:8080/api/labels ./trace-anomalies.sh
#
# Outputs (in $OUT, default /tmp/anomaly-trace):
#   level-N.tsv         non-exchange edges at hop N (from,to,amount,tick,tx)
#   edges.tsv           all non-exchange edges with leading depth column
#   exchange-hits.tsv   edges that landed at an exchange (terminal)
#                       cols: depth, deposit_addr, exchange_addr, exchange_name,
#                             amount, tick, tx_hash
#   deposits.tsv        aggregated (deposit, exchange) -> total QU
#   exchanges.tsv       loaded exchange labels (addr \t name)
#   tip-addresses.tsv   distinct addresses ever touched

set -euo pipefail

EPOCHS=${EPOCHS:-217}
DEPTH=${DEPTH:-5}
OUT=${OUT:-./anomaly-trace}
MAX_ADDR_PER_LEVEL=${MAX_ADDR_PER_LEVEL:-500}
LABELS_API=${LABELS_API:-https://analytics.qubic.li/api/labels}
# Contract address pattern: first 2 chars are the (uppercase A-Z) base-26 digits
# of the contract index, next 54 chars are A (zero padding of the public key),
# last 4 chars are the checksum. Covers contract indices 0..675 (26²).
# Examples: BAAA…RMID (idx 1, QX), ABAA…YYHH (idx 26), BBAA…XPZM (idx 27).
# Burn address (all-A pubkey + FXIB checksum) matches this pattern too and must
# be excluded separately via the BURN_ADDR != check.
CONTRACT_REGEX="'^[A-Z]{2}A{54}[A-Z]{4}\$'"
BURN_ADDR="'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAFXIB'"

mkdir -p "$OUT"
rm -f "$OUT"/level-*.tsv "$OUT"/summary-*.tsv "$OUT"/.summary-*.tsv \
       "$OUT/edges.tsv" "$OUT/tip-addresses.tsv" \
       "$OUT/exchange-hits.tsv" "$OUT/exchanges.tsv" "$OUT/deposits.tsv" \
       "$OUT/seen.tsv"
# Tracks unique "from_addr \t to_addr \t tx_hash" keys seen at any earlier
# level so the same transfer isn't re-captured downstream when an address
# appears at multiple levels via different paths.
printf 'from_addr\tto_addr\ttx_hash\n' > "$OUT/seen.tsv"
# Pre-create output files with header rows. All awk readers below skip line 1
# with NR>1 / FNR>1 so the headers don't get treated as data.
printf 'from_addr\tto_addr\tamount\ttick_number\ttick_datetime\ttx_hash\n'                                          > "$OUT/level-headers.tsv"
printf 'depth\tdeposit_addr\texchange_addr\texchange_name\tamount\ttick_number\ttick_datetime\ttx_hash\n'          > "$OUT/exchange-hits.tsv"
printf 'depth\tfrom_addr\tto_addr\tamount\ttick_number\ttick_datetime\ttx_hash\n'                                  > "$OUT/edges.tsv"

# ---- Fetch exchanges with their names: TSV "address \t name". ----
echo "=== Fetching exchange labels from $LABELS_API ==="
{ printf 'address\tname\n'; curl -sf "$LABELS_API?type=exchange" \
    | jq -r '.[] | [.address, (.label // "unknown")] | @tsv'; } \
    > "$OUT/exchanges.tsv"
# Merge in local overrides — addresses we've identified as exchanges but aren't
# in the upstream labels yet. Put one "address<TAB>name" per line in this file.
EXTRA_EXCHANGES=${EXTRA_EXCHANGES:-./extra-exchanges.tsv}
if [ -f "$EXTRA_EXCHANGES" ]; then
    cat "$EXTRA_EXCHANGES" >> "$OUT/exchanges.tsv"
    echo "  merged $(wc -l < "$EXTRA_EXCHANGES") local exchanges from $EXTRA_EXCHANGES"
fi
# Preserve the header on line 1 while sorting the rest.
{ head -n1 "$OUT/exchanges.tsv"; tail -n +2 "$OUT/exchanges.tsv" | sort -u; } \
    > "$OUT/exchanges.tsv.tmp" && mv "$OUT/exchanges.tsv.tmp" "$OUT/exchanges.tsv"
ex_count=$(( $(wc -l < "$OUT/exchanges.tsv") - 1 ))
if [ "$ex_count" = "0" ]; then
    echo "WARNING: no exchanges returned by labels API — trace won't terminate at exchanges."
else
    echo "  loaded $ex_count exchange addresses total"
fi

run_ch() { docker compose exec -T clickhouse clickhouse-client --query "$1"; }

# Per-level summary: top N recipients at this level, with transfer count and
# total QU received. Includes both continuing (level-$d.tsv) and exchange-
# landing edges. $1 = depth.
summarize_level() {
    local d="$1"
    local top="${SUMMARY_TOP:-15}"
    local file="$OUT/summary-$d.tsv"

    # Write the full per-recipient summary to summary-N.tsv with header.
    printf 'recipient\ttransfers\ttotal_qu\tfirst_tick\tlast_tick\tfirst_datetime\tlast_datetime\n' > "$file"
    {
        # Continuing edges (level-$d.tsv): recipient $2, amount $3, tick $4, datetime $5
        awk -F'\t' 'NR>1 {print $2"\t"$3"\t"$4"\t"$5}' "$OUT/level-$d.tsv" 2>/dev/null
        # Exchange-terminating edges (exchange-hits.tsv): depth $1, addr $3, name $4,
        # amount $5, tick $6, datetime $7
        awk -F'\t' -v d="$d" 'NR>1 && $1==d {print $3" ["$4"]\t"$5"\t"$6"\t"$7}' \
            "$OUT/exchange-hits.tsv" 2>/dev/null
    } | awk -F'\t' '
        {
            addr=$1; amt=$2; tick=$3; dt=$4
            cnt[addr]++; sum[addr] += amt
            if (!(addr in tmin) || tick+0 < tmin[addr]+0) { tmin[addr]=tick; dtmin[addr]=dt }
            if (!(addr in tmax) || tick+0 > tmax[addr]+0) { tmax[addr]=tick; dtmax[addr]=dt }
        }
        END {
            for (a in cnt) printf "%s\t%d\t%d\t%s\t%s\t%s\t%s\n",
                                  a, cnt[a], sum[a], tmin[a], tmax[a], dtmin[a], dtmax[a]
        }
    ' | sort -t$'\t' -k3 -rn >> "$file"

    # Pretty-print the top N to stdout from the persisted file.
    awk -F'\t' -v top="$top" 'NR>1 && NR<=top+1 {
        if ($4 == $5) {
            printf "    %-65s  %4d xfers, %18d QU  tick %s (%s UTC)\n",
                   $1, $2, $3, $4, $6
        } else {
            printf "    %-65s  %4d xfers, %18d QU  ticks %s..%s (%s -> %s UTC)\n",
                   $1, $2, $3, $4, $5, $6, $7
        }
    }' "$file"
}

# split_edges reads raw edges on stdin (from \t to \t amount \t tick \t tx),
# routes them into:
#   - non-exchange edges -> $1 (continue tracing)
#   - exchange-terminating edges -> exchange-hits.tsv, augmented with the
#     exchange's name. Here from_addr is the *deposit address* — the user's
#     deposit wallet at that exchange.
split_edges() {
    local keep="$1"
    local depth="$2"
    # Raw input cols: from $1, to $2, amount $3, tick $4, datetime $5, tx $6
    # Also dedupes against previously-seen (from, to, tx_hash) keys so a transfer
    # already captured at an earlier level isn't recorded again here.
    awk -F'\t' -v ex="$OUT/exchanges.tsv" -v seenf="$OUT/seen.tsv" -v keep="$keep" \
                -v hit="$OUT/exchange-hits.tsv" -v depth="$depth" '
        BEGIN {
            getline line < ex      # discard header line
            while ((getline line < ex) > 0) {
                split(line, a, "\t"); exch[a[1]] = a[2]
            }
            close(ex)
            getline line < seenf       # discard header line
            while ((getline line < seenf) > 0) {
                seen_set[line] = 1
            }
            close(seenf)
        }
        {
            key = $1 "\t" $2 "\t" $6
            if (key in seen_set) next
            seen_set[key] = 1
            if ($2 in exch) {
                # exchange-hits cols: depth, deposit, exch_addr, exch_name, amount, tick, datetime, tx
                printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
                       depth, $1, $2, exch[$2], $3, $4, $5, $6 >> hit
            } else {
                print >> keep
            }
        }
    '
}

# Append the (from, to, tx_hash) keys of everything captured at $1 to seen.tsv,
# so the next level's split_edges skips duplicates.
record_seen_at_level() {
    local d="$1"
    {
        awk -F'\t' 'NR>1 {print $1"\t"$2"\t"$6}' "$OUT/level-$d.tsv"
        awk -F'\t' -v d="$d" 'NR>1 && $1==d {print $2"\t"$3"\t"$8}' "$OUT/exchange-hits.tsv"
    } >> "$OUT/seen.tsv"
}

# Initialise a level-N.tsv with the standard header row.
init_level_file() {
    cp "$OUT/level-headers.tsv" "$1"
}

# ---- Level 0: anomaly seeds ----
echo "=== Level 0: anomaly outflows from contracts in epochs $EPOCHS ==="
run_ch "
SELECT l.source_address AS from_addr,
       l.dest_address   AS to_addr,
       l.amount,
       l.tick_number,
       toString(toDateTime(l.timestamp, 'UTC')) AS tick_datetime,
       l.tx_hash
FROM qubic.logs l
INNER JOIN (
    SELECT hash, to_address FROM qubic.transactions WHERE epoch IN ($EPOCHS)
) t ON t.hash = l.tx_hash
WHERE l.log_type = 0
  AND l.epoch IN ($EPOCHS)
  AND match(l.source_address, $CONTRACT_REGEX)
  AND NOT startsWith(l.tx_hash, 'SC_')
  AND t.to_address != l.source_address
  AND NOT match(t.to_address, $CONTRACT_REGEX)
  AND l.dest_address != $BURN_ADDR
  AND l.source_address != $BURN_ADDR
FORMAT TSV
" > "$OUT/level-0-raw.tsv"
init_level_file "$OUT/level-0.tsv"
split_edges "$OUT/level-0.tsv" 0 < "$OUT/level-0-raw.tsv"
rm -f "$OUT/level-0-raw.tsv"
record_seen_at_level 0
seeds=$(( $(wc -l < "$OUT/level-0.tsv") - 1 ))
seed_ex=$(awk -F'\t' 'NR>1 && $1==0' "$OUT/exchange-hits.tsv" 2>/dev/null | wc -l)
echo "  $seeds non-exchange seeds, $seed_ex seeds already at an exchange"
awk -F'\t' 'NR>1 {print "0\t"$0}' "$OUT/level-0.tsv" >> "$OUT/edges.tsv"
echo "  Top recipients at level 0:"
summarize_level 0

# ---- Walk forward ----
for depth in $(seq 1 "$DEPTH"); do
    prev=$((depth - 1))
    # For each recipient at the previous level, capture the earliest tick at which
    # it received "dirty" funds. The next-level expansion only traces outflows at
    # that tick or later — anything earlier predates the anomaly and is irrelevant.
    # level-N.tsv cols: from $1, to $2, amount $3, tick $4, datetime $5, tx $6
    addrs_file="$OUT/.level-$prev-addrs.tsv"
    awk -F'\t' 'NR>1 {
        addr=$2; t=$4
        if (!(addr in m) || t+0 < m[addr]+0) m[addr]=t
    } END {
        for (a in m) print a "\t" m[a]
    }' "$OUT/level-$prev.tsv" | sort -t$'\t' -k2 -n | head -n "$MAX_ADDR_PER_LEVEL" > "$addrs_file"

    if [ ! -s "$addrs_file" ]; then
        echo "Level $depth: no addresses to expand — done"
        rm -f "$addrs_file"
        break
    fi
    n_addrs=$(wc -l < "$addrs_file")
    echo "=== Level $depth: expanding $n_addrs addresses (each filtered to tick >= first-receive) ==="

    # Build (source_address='X' AND tick_number >= N) OR-chain.
    or_clause=$(awk -F'\t' '
        NR>1 { printf " OR " }
        { printf "(source_address='\''%s'\'' AND tick_number >= %s)", $1, $2 }
    ' "$addrs_file")
    rm -f "$addrs_file"

    run_ch "
    SELECT source_address,
           dest_address,
           amount,
           tick_number,
           toString(toDateTime(timestamp, 'UTC')) AS tick_datetime,
           tx_hash
    FROM qubic.logs
    WHERE log_type = 0
      AND epoch IN ($EPOCHS)
      AND ($or_clause)
      AND dest_address != $BURN_ADDR
      AND source_address != $BURN_ADDR
      AND NOT startsWith(tx_hash, 'SC_')
    ORDER BY tick_number
    FORMAT TSV
    " > "$OUT/level-$depth-raw.tsv"

    init_level_file "$OUT/level-$depth.tsv"
    split_edges "$OUT/level-$depth.tsv" "$depth" < "$OUT/level-$depth-raw.tsv"
    rm -f "$OUT/level-$depth-raw.tsv"
    record_seen_at_level "$depth"

    cnt=$(( $(wc -l < "$OUT/level-$depth.tsv") - 1 ))
    ex_cnt=$(awk -F'\t' -v d="$depth" 'NR>1 && $1==d' "$OUT/exchange-hits.tsv" 2>/dev/null | wc -l)
    echo "  $cnt continuing transfers, $ex_cnt hit an exchange (terminal)"

    awk -v d="$depth" -F'\t' 'NR>1 {print d"\t"$0}' "$OUT/level-$depth.tsv" >> "$OUT/edges.tsv"
    echo "  Top recipients at level $depth:"
    summarize_level "$depth"
    [ "$cnt" = "0" ] && break
done

# ---- Build (deposit, exchange) aggregate ----
# exchange-hits cols: depth $1, deposit $2, exch_addr $3, exch_name $4,
#                     amount $5, tick $6, datetime $7, tx $8
printf 'deposit_address\texchange_address\texchange_name\ttotal_qu\tnum_transfers\tfirst_tick\tlast_tick\tfirst_datetime\tlast_datetime\n' > "$OUT/deposits.tsv"
if [ "$(wc -l < "$OUT/exchange-hits.tsv")" -gt 1 ]; then
    awk -F'\t' 'NR>1 {
        key = $2 SUBSEP $3 SUBSEP $4
        sum[key] += $5
        cnt[key] += 1
        if (!(key in tmin) || $6+0 < tmin[key]+0) { tmin[key] = $6; dtmin[key] = $7 }
        if (!(key in tmax) || $6+0 > tmax[key]+0) { tmax[key] = $6; dtmax[key] = $7 }
    }
    END {
        for (k in sum) {
            split(k, a, SUBSEP)
            printf "%s\t%s\t%s\t%d\t%d\t%s\t%s\t%s\t%s\n",
                   a[1], a[2], a[3], sum[k], cnt[k], tmin[k], tmax[k], dtmin[k], dtmax[k]
        }
    }' "$OUT/exchange-hits.tsv" | sort -t$'\t' -k4 -rn >> "$OUT/deposits.tsv"
fi

{ printf 'address\n'
  awk -F'\t' 'FNR>1 {print $3}' "$OUT/edges.tsv" "$OUT/exchange-hits.tsv" 2>/dev/null \
      | sort -u
} > "$OUT/tip-addresses.tsv"

echo
echo "=== Summary ==="
echo "Total non-exchange edges traced : $(( $(wc -l < "$OUT/edges.tsv") - 1 ))"
echo "Exchange-terminating edges      : $(( $(wc -l < "$OUT/exchange-hits.tsv") - 1 ))"
echo "Distinct (deposit, exchange)    : $(( $(wc -l < "$OUT/deposits.tsv") - 1 ))"
echo
echo "Top deposit→exchange flows:"
printf "  %-15s %-20s %-60s %s\n" "QU_TOTAL" "EXCHANGE" "DEPOSIT_ADDRESS" "EXCHANGE_ADDRESS"
awk -F'\t' 'NR>1 { printf "  %-15d %-20s %-60s %s\n", $4, $3, $1, $2 }' \
    "$OUT/deposits.tsv" 2>/dev/null | head -20
