-- =============================================================
-- 03 — Migrate logs
-- Steps: CREATE _new -> INSERT SELECT -> RENAME swap -> ADD INDEXes
-- After verifying counts, uncomment the DROP at the bottom to free disk.
-- =============================================================

-- 1. Create the new partitioned table
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

-- 2. Copy data (this is the long step)
INSERT INTO qubic.logs_new SELECT * FROM qubic.logs;

-- 3. Atomic swap: original -> _old, _new -> active
RENAME TABLE qubic.logs TO qubic.logs_old, qubic.logs_new TO qubic.logs;

-- 4. Re-add secondary indexes
ALTER TABLE qubic.logs ADD INDEX IF NOT EXISTS idx_source source_address TYPE bloom_filter GRANULARITY 4;
ALTER TABLE qubic.logs ADD INDEX IF NOT EXISTS idx_dest dest_address TYPE bloom_filter GRANULARITY 4;
ALTER TABLE qubic.logs ADD INDEX IF NOT EXISTS idx_type log_type TYPE set(20) GRANULARITY 4;
ALTER TABLE qubic.logs ADD INDEX IF NOT EXISTS idx_tx_hash tx_hash TYPE bloom_filter GRANULARITY 4;

-- 5. Verify counts match before dropping the old table:
--    SELECT count() FROM qubic.logs;
--    SELECT count() FROM qubic.logs_old;

-- 6. Once verified, uncomment to free disk space:
-- DROP TABLE IF EXISTS qubic.logs_old;
