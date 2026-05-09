#!/usr/bin/env bash
# Parallel per-epoch migration for qubic.logs.
#
# Why this instead of 03_logs.sql:
#   - Single-threaded INSERT SELECT on a large table is CPU-bound on LZ4HC compression.
#   - Per-epoch INSERTs let us run multiple in parallel and give us resumability.
#   - The new table partitions by epoch, so per-epoch inserts each create one part —
#     no global resort needed.
#
# Resumability:
#   - Target is ReplacingMergeTree(created_at), so re-inserting an epoch is idempotent.
#   - If a run dies mid-epoch, just rerun this script — already-inserted epochs are
#     detected via row counts and skipped.
#
# Usage:
#   cd /root/explorer
#   ./docker/clickhouse/migrate_add_partitioning/03_logs_parallel.sh
#
# Tunables (env vars):
#   PARALLEL=4               # How many epochs to migrate in parallel
#   THREADS_PER_QUERY=4      # max_insert_threads per query
#   SKIP_RENAME=1            # Don't do the final swap (just copy data)

set -euo pipefail

PARALLEL=${PARALLEL:-4}
THREADS_PER_QUERY=${THREADS_PER_QUERY:-4}
SKIP_RENAME=${SKIP_RENAME:-0}

CH_CONTAINER=$(docker compose ps -q clickhouse)
if [ -z "$CH_CONTAINER" ]; then
  echo "ERROR: clickhouse container not running" >&2
  exit 1
fi

ch() {
  docker exec -i "$CH_CONTAINER" clickhouse-client "$@"
}

ch_query() {
  docker exec -i "$CH_CONTAINER" clickhouse-client --query "$1"
}

echo "=== Step 1: Create qubic.logs_new (if not exists) ==="
ch --multiquery <<'SQL'
CREATE TABLE IF NOT EXISTS qubic.logs_new (
    tick_number UInt64 CODEC(DoubleDelta, LZ4),
    epoch UInt32 CODEC(DoubleDelta, LZ4),
    log_id UInt32 CODEC(DoubleDelta, LZ4),
    log_type UInt8 CODEC(LZ4),
    tx_hash String CODEC(LZ4HC),
    input_type UInt16 CODEC(LZ4),
    source_address String CODEC(LZ4HC),
    dest_address String CODEC(LZ4HC),
    amount UInt64 CODEC(T64, LZ4),
    asset_name String CODEC(LZ4HC),
    raw_data String CODEC(LZ4HC),
    timestamp DateTime64(3) CODEC(Delta, LZ4),
    created_at DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(created_at)
PARTITION BY epoch
ORDER BY (tick_number, log_id);
SQL

echo "=== Step 2: Determine epoch range and per-epoch row counts ==="
EPOCH_RANGE=$(ch_query "SELECT min(epoch), max(epoch) FROM qubic.logs FORMAT TabSeparated")
MIN_EPOCH=$(echo "$EPOCH_RANGE" | awk '{print $1}')
MAX_EPOCH=$(echo "$EPOCH_RANGE" | awk '{print $2}')
echo "Epoch range: $MIN_EPOCH to $MAX_EPOCH"

# Build worklist of epochs that still need migration:
#   - has rows in qubic.logs
#   - row count in qubic.logs_new is less than in qubic.logs
TODO=$(ch_query "
  SELECT a.epoch
  FROM (
      SELECT epoch, count() AS c FROM qubic.logs GROUP BY epoch
  ) AS a
  LEFT JOIN (
      SELECT epoch, count() AS c FROM qubic.logs_new GROUP BY epoch
  ) AS b USING (epoch)
  WHERE a.c > coalesce(b.c, 0)
  ORDER BY a.epoch
  FORMAT TabSeparated
")

if [ -z "$TODO" ]; then
  echo "All epochs already migrated. Nothing to do."
else
  TOTAL=$(echo "$TODO" | wc -l)
  echo "Epochs needing migration: $TOTAL"
  echo "Running with PARALLEL=$PARALLEL THREADS_PER_QUERY=$THREADS_PER_QUERY"
  echo

  migrate_epoch() {
    local e=$1
    local start=$(date +%s)
    docker exec -i "$CH_CONTAINER" clickhouse-client \
      --max_insert_threads=$THREADS_PER_QUERY \
      --max_threads=$THREADS_PER_QUERY \
      --max_insert_block_size=1048576 \
      --min_insert_block_size_rows=1048576 \
      --min_insert_block_size_bytes=268435456 \
      --query "INSERT INTO qubic.logs_new SELECT * FROM qubic.logs WHERE epoch = $e"
    local elapsed=$(( $(date +%s) - start ))
    echo "  epoch $e done in ${elapsed}s"
  }
  export -f migrate_epoch
  export CH_CONTAINER THREADS_PER_QUERY

  echo "$TODO" | xargs -n 1 -P "$PARALLEL" -I {} bash -c 'migrate_epoch "$@"' _ {}
fi

echo
echo "=== Step 3: Verify counts ==="
ch --query "
  SELECT
    (SELECT count() FROM qubic.logs FINAL)     AS old_dedup,
    (SELECT count() FROM qubic.logs_new FINAL) AS new_dedup,
    (SELECT count() FROM qubic.logs)           AS old_raw,
    (SELECT count() FROM qubic.logs_new)       AS new_raw
  FORMAT Vertical"

if [ "$SKIP_RENAME" = "1" ]; then
  echo
  echo "SKIP_RENAME=1: not renaming. Run these manually when ready:"
  echo "  RENAME TABLE qubic.logs TO qubic.logs_old, qubic.logs_new TO qubic.logs;"
  echo "  ALTER TABLE qubic.logs ADD INDEX IF NOT EXISTS idx_source source_address TYPE bloom_filter GRANULARITY 4;"
  echo "  ALTER TABLE qubic.logs ADD INDEX IF NOT EXISTS idx_dest dest_address TYPE bloom_filter GRANULARITY 4;"
  echo "  ALTER TABLE qubic.logs ADD INDEX IF NOT EXISTS idx_type log_type TYPE set(20) GRANULARITY 4;"
  echo "  ALTER TABLE qubic.logs ADD INDEX IF NOT EXISTS idx_tx_hash tx_hash TYPE bloom_filter GRANULARITY 4;"
  echo
  echo "After verifying counts:"
  echo "  DROP TABLE qubic.logs_old;"
  exit 0
fi

echo
echo "=== Step 4: Verify FINAL counts match before swap ==="
MISMATCH=$(ch_query "
  SELECT
      (SELECT count() FROM qubic.logs FINAL)
      <> (SELECT count() FROM qubic.logs_new FINAL)
  FORMAT TabSeparated")

if [ "$MISMATCH" = "1" ]; then
  echo "ERROR: dedup count mismatch between qubic.logs and qubic.logs_new" >&2
  echo "Investigate before running the swap. Re-run this script to fill any gaps," >&2
  echo "or pass SKIP_RENAME=1 and inspect manually." >&2
  exit 1
fi
echo "FINAL counts match."

echo
echo "=== Step 5: Atomic swap + indexes ==="
ch --multiquery <<'SQL'
RENAME TABLE qubic.logs TO qubic.logs_old, qubic.logs_new TO qubic.logs;
ALTER TABLE qubic.logs ADD INDEX IF NOT EXISTS idx_source source_address TYPE bloom_filter GRANULARITY 4;
ALTER TABLE qubic.logs ADD INDEX IF NOT EXISTS idx_dest dest_address TYPE bloom_filter GRANULARITY 4;
ALTER TABLE qubic.logs ADD INDEX IF NOT EXISTS idx_type log_type TYPE set(20) GRANULARITY 4;
ALTER TABLE qubic.logs ADD INDEX IF NOT EXISTS idx_tx_hash tx_hash TYPE bloom_filter GRANULARITY 4;
SQL

echo
echo "=== Done ==="
echo "Once you're confident, free disk with:"
echo "  docker exec -it \$(docker compose ps -q clickhouse) clickhouse-client --query 'DROP TABLE qubic.logs_old'"
