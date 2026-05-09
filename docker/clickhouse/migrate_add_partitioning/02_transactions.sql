-- =============================================================
-- 02 — Migrate transactions
-- Steps: CREATE _new -> INSERT SELECT -> RENAME swap -> ADD INDEXes
-- After verifying counts, uncomment the DROP at the bottom to free disk.
-- =============================================================

-- 1. Create the new partitioned table
CREATE TABLE IF NOT EXISTS qubic.transactions_new (
    hash String CODEC(LZ4HC),
    tick_number UInt64 CODEC(DoubleDelta, LZ4),
    epoch UInt32 CODEC(DoubleDelta, LZ4),
    from_address String CODEC(LZ4HC),
    to_address String CODEC(LZ4HC),
    amount UInt64 CODEC(T64, LZ4),
    input_type UInt16 CODEC(LZ4),
    input_data String CODEC(LZ4HC),
    executed UInt8 CODEC(LZ4),
    log_id_from Int32 CODEC(LZ4),
    log_id_length UInt16 CODEC(LZ4),
    timestamp DateTime64(3) CODEC(Delta, LZ4),
    created_at DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(created_at)
PARTITION BY epoch
ORDER BY (tick_number, hash);

-- 2. Copy data (this is the long step)
INSERT INTO qubic.transactions_new SELECT * FROM qubic.transactions;

-- 3. Atomic swap: original -> _old, _new -> active
RENAME TABLE qubic.transactions TO qubic.transactions_old, qubic.transactions_new TO qubic.transactions;

-- 4. Re-add secondary indexes
ALTER TABLE qubic.transactions ADD INDEX IF NOT EXISTS idx_from from_address TYPE bloom_filter GRANULARITY 4;
ALTER TABLE qubic.transactions ADD INDEX IF NOT EXISTS idx_to to_address TYPE bloom_filter GRANULARITY 4;
ALTER TABLE qubic.transactions ADD INDEX IF NOT EXISTS idx_hash hash TYPE bloom_filter GRANULARITY 4;

-- 5. Verify counts match before dropping the old table:
--    SELECT count() FROM qubic.transactions;
--    SELECT count() FROM qubic.transactions_old;

-- 6. Once verified, uncomment to free disk space:
-- DROP TABLE IF EXISTS qubic.transactions_old;
