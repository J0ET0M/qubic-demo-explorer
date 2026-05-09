#!/usr/bin/env bash
# Backfill all materialized views from the source tables (transactions, logs, ticks).
#
# Use this after recreating MVs (e.g., post-migration). MVs only populate from
# future inserts — they don't replay history when CREATE'd. This script truncates
# each MV's storage and re-runs its SELECT over all existing source data.
#
# IMPORTANT: stop the indexer (and any other writers) before running. While the
# script is processing, new inserts to source tables would fire the MVs and
# double-count rows that the backfill is also producing.
#
# Usage:
#   cd /root/explorer
#   docker compose stop indexer
#   ./docker/clickhouse/backfill_materialized_views.sh
#   docker compose start indexer
#
# Environment:
#   DB=qubic                 # database name
#   ONLY_MV=mv_name          # process only this one MV (debugging)

set -euo pipefail

DB=${DB:-qubic}
ONLY_MV=${ONLY_MV:-}

CH_CONTAINER=$(docker compose ps -q clickhouse)
if [ -z "$CH_CONTAINER" ]; then
  echo "ERROR: clickhouse container not running" >&2
  exit 1
fi

ch() {
  docker exec -i "$CH_CONTAINER" clickhouse-client --multiquery
}

ch_query() {
  docker exec -i "$CH_CONTAINER" clickhouse-client --query "$1"
}

# Confirm the indexer is stopped
INDEXER_STATE=$(docker compose ps --status running --services 2>/dev/null | grep -x indexer || true)
if [ -n "$INDEXER_STATE" ]; then
  echo "ERROR: indexer is still running. Stop it first:"
  echo "  docker compose stop indexer"
  exit 1
fi

echo "=== Backfilling materialized views in DB: $DB ==="

# Each MV's: truncate + INSERT SELECT.
# Order matters only insofar as we want sources of address_first_seen done last.

run_mv() {
  local name=$1
  local truncate_target=$2
  local insert_sql=$3

  if [ -n "$ONLY_MV" ] && [ "$ONLY_MV" != "$name" ]; then
    return 0
  fi

  echo
  echo "--- $name ---"
  echo "TRUNCATE $truncate_target"
  ch_query "TRUNCATE TABLE IF EXISTS $DB.$truncate_target"

  echo "Running backfill INSERT (this can take a few minutes for large tables) ..."
  local start=$(date +%s)
  echo "$insert_sql" | ch
  local elapsed=$(( $(date +%s) - start ))

  local count
  count=$(ch_query "SELECT count() FROM $DB.$truncate_target FORMAT TabSeparated")
  echo "  $name done in ${elapsed}s — $count rows in $truncate_target"
}

# 1. daily_tx_volume (SummingMergeTree)
run_mv "daily_tx_volume" "daily_tx_volume" "
INSERT INTO $DB.daily_tx_volume
SELECT toDate(timestamp) AS date, count() AS tx_count, sum(amount) AS total_volume
FROM $DB.transactions
GROUP BY date;
"

# 2. daily_log_stats (SummingMergeTree)
run_mv "daily_log_stats" "daily_log_stats" "
INSERT INTO $DB.daily_log_stats
SELECT toDate(timestamp) AS date, log_type, count() AS log_count
FROM $DB.logs
GROUP BY date, log_type;
"

# 3. hourly_activity (SummingMergeTree)
run_mv "hourly_activity" "hourly_activity" "
INSERT INTO $DB.hourly_activity
SELECT toStartOfHour(timestamp) AS hour, count() AS tx_count, sum(amount) AS total_volume,
       uniq(from_address) AS unique_senders, uniq(to_address) AS unique_receivers
FROM $DB.transactions
GROUP BY hour;
"

# 4. epoch_tx_stats (SummingMergeTree + uniqState)
run_mv "epoch_tx_stats" "epoch_tx_stats" "
INSERT INTO $DB.epoch_tx_stats
SELECT epoch, count() AS tx_count, sum(amount) AS total_volume,
       uniqState(from_address) AS unique_senders_state,
       uniqState(to_address) AS unique_receivers_state
FROM $DB.transactions
GROUP BY epoch;
"

# 5. epoch_transfer_stats (SummingMergeTree, log_type = 0 only)
run_mv "epoch_transfer_stats" "epoch_transfer_stats" "
INSERT INTO $DB.epoch_transfer_stats
SELECT epoch, count() AS transfer_count, sum(amount) AS qu_transferred
FROM $DB.logs
WHERE log_type = 0
GROUP BY epoch;
"

# 6. epoch_transfer_by_type (SummingMergeTree)
run_mv "epoch_transfer_by_type" "epoch_transfer_by_type" "
INSERT INTO $DB.epoch_transfer_by_type
SELECT epoch, log_type, count() AS count, sum(amount) AS total_amount
FROM $DB.logs
GROUP BY epoch, log_type;
"

# 7. epoch_tick_stats (AggregatingMergeTree) — uses is_empty
run_mv "epoch_tick_stats" "epoch_tick_stats" "
INSERT INTO $DB.epoch_tick_stats
SELECT
    epoch,
    countState() AS tick_count_state,
    sumState(is_empty) AS empty_tick_count_state,
    sumState(tx_count) AS total_tx_in_ticks_state,
    sumState(log_count) AS total_logs_in_ticks_state,
    minState(tick_number) AS first_tick_state,
    maxState(tick_number) AS last_tick_state,
    minState(timestamp) AS start_time_state,
    maxState(timestamp) AS end_time_state
FROM $DB.ticks
GROUP BY epoch;
"

# 8. epoch_sender_stats (AggregatingMergeTree)
run_mv "epoch_sender_stats" "epoch_sender_stats" "
INSERT INTO $DB.epoch_sender_stats
SELECT epoch, uniqState(from_address) AS sender_addresses_state
FROM $DB.transactions
GROUP BY epoch;
"

# 9. epoch_receiver_stats (AggregatingMergeTree)
run_mv "epoch_receiver_stats" "epoch_receiver_stats" "
INSERT INTO $DB.epoch_receiver_stats
SELECT epoch, uniqState(to_address) AS receiver_addresses_state
FROM $DB.transactions
WHERE to_address != ''
GROUP BY epoch;
"

# 10. epoch_tx_size_stats (AggregatingMergeTree, log_type = 0)
run_mv "epoch_tx_size_stats" "epoch_tx_size_stats" "
INSERT INTO $DB.epoch_tx_size_stats
SELECT epoch,
       countState() AS tx_count_state,
       sumState(amount) AS total_volume_state,
       avgState(amount) AS avg_tx_size_state,
       quantileState(0.5)(amount) AS median_state
FROM $DB.logs
WHERE log_type = 0
GROUP BY epoch;
"

# 11. daily_tx_size_stats (AggregatingMergeTree, log_type = 0)
run_mv "daily_tx_size_stats" "daily_tx_size_stats" "
INSERT INTO $DB.daily_tx_size_stats
SELECT toDate(timestamp) AS date,
       countState() AS tx_count_state,
       sumState(amount) AS total_volume_state,
       avgState(amount) AS avg_tx_size_state,
       quantileState(0.5)(amount) AS median_state
FROM $DB.logs
WHERE log_type = 0
GROUP BY date;
"

# 12 + 13. address_first_seen (target of mv_address_first_seen_from + _to)
# Truncate the target and INSERT both sides. ReplacingMergeTree(created_at) on
# ORDER BY address dedupes — since both sides write the same address, we INSERT
# with strictly increasing created_at so the second insert wins last; querying
# with FINAL or after OPTIMIZE collapses to one row per address.
if [ -z "$ONLY_MV" ] || [ "$ONLY_MV" = "address_first_seen" ]; then
  echo
  echo "--- address_first_seen (from mv_address_first_seen_{from,to}) ---"
  echo "TRUNCATE address_first_seen"
  ch_query "TRUNCATE TABLE IF EXISTS $DB.address_first_seen"

  echo "Running backfill from from_address ..."
  start=$(date +%s)
  echo "
  INSERT INTO $DB.address_first_seen
  SELECT
      from_address AS address,
      min(epoch) AS first_epoch,
      min(tick_number) AS first_tick,
      min(timestamp) AS first_timestamp,
      now64(3) AS created_at
  FROM $DB.transactions
  WHERE from_address != ''
  GROUP BY from_address;
  " | ch
  echo "  from-side done in $(( $(date +%s) - start ))s"

  echo "Running backfill from to_address ..."
  start=$(date +%s)
  echo "
  INSERT INTO $DB.address_first_seen
  SELECT
      to_address AS address,
      min(epoch) AS first_epoch,
      min(tick_number) AS first_tick,
      min(timestamp) AS first_timestamp,
      now64(3) AS created_at
  FROM $DB.transactions
  WHERE to_address != ''
  GROUP BY to_address;
  " | ch
  echo "  to-side done in $(( $(date +%s) - start ))s"

  count=$(ch_query "SELECT count() FROM $DB.address_first_seen FORMAT TabSeparated")
  count_final=$(ch_query "SELECT count() FROM $DB.address_first_seen FINAL FORMAT TabSeparated")
  echo "  address_first_seen — raw: $count rows, deduped (FINAL): $count_final unique addresses"
  echo
  echo "Optional: optimize for permanent dedup"
  echo "  docker exec -i \$(docker compose ps -q clickhouse) clickhouse-client --query 'OPTIMIZE TABLE $DB.address_first_seen FINAL'"
fi

echo
echo "=== All MV backfills complete ==="
echo
echo "Spot-check (compare MV row counts to source counts):"
cat <<HELP
docker exec -i \$(docker compose ps -q clickhouse) clickhouse-client --multiquery <<EOF
SELECT 'epoch_tx_stats' AS mv, sum(tx_count) AS sum_count, (SELECT count() FROM $DB.transactions) AS source_count FROM $DB.epoch_tx_stats;
SELECT 'daily_tx_volume', sum(tx_count), (SELECT count() FROM $DB.transactions) FROM $DB.daily_tx_volume;
SELECT 'epoch_transfer_stats', sum(transfer_count), (SELECT countIf(log_type=0) FROM $DB.logs) FROM $DB.epoch_transfer_stats;
EOF
HELP
echo
echo "Now restart the indexer:"
echo "  docker compose start indexer"
