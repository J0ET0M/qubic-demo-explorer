-- =============================================================
-- 04 — Migrate balance_snapshots
-- Steps: CREATE _new -> INSERT SELECT -> RENAME swap -> ADD INDEXes
-- After verifying counts, uncomment the DROP at the bottom to free disk.
-- =============================================================

-- 1. Create the new partitioned table
CREATE TABLE IF NOT EXISTS qubic.balance_snapshots_new (
    address String CODEC(LZ4HC),
    epoch UInt32 CODEC(DoubleDelta, LZ4),
    tick_number UInt64 CODEC(DoubleDelta, LZ4),
    balance Int64 CODEC(T64, LZ4),
    incoming_amount UInt64 CODEC(T64, LZ4),
    outgoing_amount UInt64 CODEC(T64, LZ4),
    incoming_transfer_count UInt32 CODEC(LZ4),
    outgoing_transfer_count UInt32 CODEC(LZ4),
    latest_incoming_tick UInt32 CODEC(LZ4),
    latest_outgoing_tick UInt32 CODEC(LZ4),
    imported_at DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(imported_at)
PARTITION BY epoch
ORDER BY (epoch, address);

-- 2. Copy data (this is the long step)
INSERT INTO qubic.balance_snapshots_new SELECT * FROM qubic.balance_snapshots;

-- 3. Atomic swap: original -> _old, _new -> active
RENAME TABLE qubic.balance_snapshots TO qubic.balance_snapshots_old, qubic.balance_snapshots_new TO qubic.balance_snapshots;

-- 4. Re-add secondary indexes
ALTER TABLE qubic.balance_snapshots ADD INDEX IF NOT EXISTS idx_address address TYPE bloom_filter GRANULARITY 4;

-- 5. Verify counts match before dropping the old table:
--    SELECT count() FROM qubic.balance_snapshots;
--    SELECT count() FROM qubic.balance_snapshots_old;

-- 6. Once verified, uncomment to free disk space:
-- DROP TABLE IF EXISTS qubic.balance_snapshots_old;
