-- =============================================================
-- 06 — Migrate flow_tracking_state
-- Steps: CREATE _new -> INSERT SELECT -> RENAME swap -> ADD INDEXes
-- After verifying counts, uncomment the DROP at the bottom to free disk.
-- =============================================================

-- 1. Create the new partitioned table
CREATE TABLE IF NOT EXISTS qubic.flow_tracking_state_new (
    emission_epoch UInt32,
    address String CODEC(LZ4HC),
    origin_address String CODEC(LZ4HC),
    address_type String CODEC(LZ4),
    received_amount Decimal(38, 0),
    sent_amount Decimal(38, 0),
    pending_amount Decimal(38, 0),
    hop_level UInt8,
    last_tick UInt64,
    is_terminal UInt8 DEFAULT 0,
    is_complete UInt8 DEFAULT 0,
    created_at DateTime64(3) DEFAULT now64(3),
    updated_at DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (emission_epoch, address, origin_address)
TTL toDateTime(updated_at) + INTERVAL 90 DAY WHERE is_complete = 1;

-- 2. Copy data (this is the long step)
INSERT INTO qubic.flow_tracking_state_new SELECT * FROM qubic.flow_tracking_state;

-- 3. Atomic swap: original -> _old, _new -> active
RENAME TABLE qubic.flow_tracking_state TO qubic.flow_tracking_state_old, qubic.flow_tracking_state_new TO qubic.flow_tracking_state;

-- 4. Re-add secondary indexes
ALTER TABLE qubic.flow_tracking_state ADD INDEX IF NOT EXISTS idx_fts_pending (emission_epoch, is_complete) TYPE set(2) GRANULARITY 4;

-- 5. Verify counts match before dropping the old table:
--    SELECT count() FROM qubic.flow_tracking_state;
--    SELECT count() FROM qubic.flow_tracking_state_old;

-- 6. Once verified, uncomment to free disk space:
-- DROP TABLE IF EXISTS qubic.flow_tracking_state_old;
