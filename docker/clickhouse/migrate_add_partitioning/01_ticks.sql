-- =============================================================
-- 01 — Migrate ticks
-- Steps: CREATE _new -> INSERT SELECT -> RENAME swap -> ADD INDEXes
-- After verifying counts, uncomment the DROP at the bottom to free disk.
-- =============================================================

-- 1. Create the new partitioned table
CREATE TABLE IF NOT EXISTS qubic.ticks_new (
    tick_number UInt64 CODEC(DoubleDelta, LZ4),
    epoch UInt32 CODEC(DoubleDelta, LZ4),
    timestamp DateTime64(3) CODEC(Delta, LZ4),
    tx_count UInt32 CODEC(LZ4),
    tx_count_filtered UInt32 CODEC(LZ4),
    log_count UInt32 CODEC(LZ4),
    log_count_filtered UInt32 CODEC(LZ4),
    created_at DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(created_at)
PARTITION BY epoch
ORDER BY tick_number;

-- 2. Copy data (this is the long step)
-- Note: old ticks table has an extra `is_empty` column that the current schema dropped,
-- so we list columns explicitly to skip it.
INSERT INTO qubic.ticks_new
    (tick_number, epoch, timestamp, tx_count, tx_count_filtered, log_count, log_count_filtered, created_at)
SELECT
    tick_number, epoch, timestamp, tx_count, tx_count_filtered, log_count, log_count_filtered, created_at
FROM qubic.ticks;

-- 3. Atomic swap: original -> _old, _new -> active
RENAME TABLE qubic.ticks TO qubic.ticks_old, qubic.ticks_new TO qubic.ticks;

-- 5. Verify counts match before dropping the old table:
--    SELECT count() FROM qubic.ticks;
--    SELECT count() FROM qubic.ticks_old;

-- 6. Once verified, uncomment to free disk space:
-- DROP TABLE IF EXISTS qubic.ticks_old;
