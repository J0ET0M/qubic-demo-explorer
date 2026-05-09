-- =============================================================
-- 05 — Migrate flow_hops
-- Steps: CREATE _new -> INSERT SELECT -> RENAME swap -> ADD INDEXes
-- After verifying counts, uncomment the DROP at the bottom to free disk.
-- =============================================================

-- 1. Create the new partitioned table
CREATE TABLE IF NOT EXISTS qubic.flow_hops_new (
    epoch UInt32 CODEC(DoubleDelta, LZ4),
    emission_epoch UInt32 CODEC(DoubleDelta, LZ4),
    tick_number UInt64 CODEC(DoubleDelta, LZ4),
    timestamp DateTime64(3) CODEC(Delta, LZ4),
    tx_hash String CODEC(LZ4HC),
    source_address String CODEC(LZ4HC),
    dest_address String CODEC(LZ4HC),
    amount UInt64 CODEC(T64, LZ4),
    origin_address String CODEC(LZ4HC),
    origin_type String CODEC(LZ4),
    hop_level UInt8 CODEC(LZ4),
    dest_type String DEFAULT '' CODEC(LZ4),
    dest_label String DEFAULT '' CODEC(LZ4),
    created_at DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(created_at)
PARTITION BY emission_epoch
ORDER BY (emission_epoch, origin_address, hop_level, tick_number, tx_hash, dest_address);

-- 2. Copy data (this is the long step)
INSERT INTO qubic.flow_hops_new SELECT * FROM qubic.flow_hops;

-- 3. Atomic swap: original -> _old, _new -> active
RENAME TABLE qubic.flow_hops TO qubic.flow_hops_old, qubic.flow_hops_new TO qubic.flow_hops;

-- 4. Re-add secondary indexes
ALTER TABLE qubic.flow_hops ADD INDEX IF NOT EXISTS idx_flow_emission_epoch emission_epoch TYPE minmax GRANULARITY 4;
ALTER TABLE qubic.flow_hops ADD INDEX IF NOT EXISTS idx_flow_origin origin_address TYPE bloom_filter GRANULARITY 4;
ALTER TABLE qubic.flow_hops ADD INDEX IF NOT EXISTS idx_flow_dest dest_address TYPE bloom_filter GRANULARITY 4;
ALTER TABLE qubic.flow_hops ADD INDEX IF NOT EXISTS idx_flow_origin_type origin_type TYPE set(10) GRANULARITY 4;
ALTER TABLE qubic.flow_hops ADD INDEX IF NOT EXISTS idx_flow_dest_type dest_type TYPE set(10) GRANULARITY 4;

-- 5. Verify counts match before dropping the old table:
--    SELECT count() FROM qubic.flow_hops;
--    SELECT count() FROM qubic.flow_hops_old;

-- 6. Once verified, uncomment to free disk space:
-- DROP TABLE IF EXISTS qubic.flow_hops_old;
