-- =============================================================
-- Storage Optimization Migration Script
-- Adds: PARTITION BY epoch, column codecs (LZ4HC/DoubleDelta/T64), TTL
-- =============================================================
-- IMPORTANT: Run during a maintenance window.
-- Stop the indexer before running to avoid writes to old tables.
-- The INSERT SELECTs may take time depending on data volume.
-- After migration, restart the application to recreate MVs.
-- =============================================================

-- Step 1: Drop materialized views (they reference old tables)
DROP VIEW IF EXISTS qubic.daily_tx_volume;
DROP VIEW IF EXISTS qubic.hourly_activity;
DROP VIEW IF EXISTS qubic.epoch_tx_stats;
DROP VIEW IF EXISTS qubic.epoch_sender_stats;
DROP VIEW IF EXISTS qubic.epoch_receiver_stats;
DROP VIEW IF EXISTS qubic.mv_address_first_seen_from;
DROP VIEW IF EXISTS qubic.mv_address_first_seen_to;
DROP VIEW IF EXISTS qubic.daily_log_stats;
DROP VIEW IF EXISTS qubic.epoch_transfer_stats;
DROP VIEW IF EXISTS qubic.epoch_transfer_by_type;
DROP VIEW IF EXISTS qubic.epoch_tick_stats;
DROP VIEW IF EXISTS qubic.epoch_tx_size_stats;
DROP VIEW IF EXISTS qubic.daily_tx_size_stats;

-- Step 2: Create new tables with partitioning + codecs, migrate data

-- Migrate: ticks
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

INSERT INTO qubic.ticks_new SELECT * FROM qubic.ticks;

-- Migrate: transactions (LARGEST TABLE - may take significant time)
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

INSERT INTO qubic.transactions_new SELECT * FROM qubic.transactions;

-- Migrate: logs (LARGEST TABLE - may take significant time)
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

INSERT INTO qubic.logs_new SELECT * FROM qubic.logs;

-- Migrate: balance_snapshots
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

INSERT INTO qubic.balance_snapshots_new SELECT * FROM qubic.balance_snapshots;

-- Migrate: flow_hops
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

INSERT INTO qubic.flow_hops_new SELECT * FROM qubic.flow_hops;

-- Migrate: flow_tracking_state (adds TTL for completed entries)
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

INSERT INTO qubic.flow_tracking_state_new SELECT * FROM qubic.flow_tracking_state;

-- Step 3: Atomic rename (old -> _old, new -> active)
RENAME TABLE qubic.ticks TO qubic.ticks_old, qubic.ticks_new TO qubic.ticks;
RENAME TABLE qubic.transactions TO qubic.transactions_old, qubic.transactions_new TO qubic.transactions;
RENAME TABLE qubic.logs TO qubic.logs_old, qubic.logs_new TO qubic.logs;
RENAME TABLE qubic.balance_snapshots TO qubic.balance_snapshots_old, qubic.balance_snapshots_new TO qubic.balance_snapshots;
RENAME TABLE qubic.flow_hops TO qubic.flow_hops_old, qubic.flow_hops_new TO qubic.flow_hops;
RENAME TABLE qubic.flow_tracking_state TO qubic.flow_tracking_state_old, qubic.flow_tracking_state_new TO qubic.flow_tracking_state;

-- Step 4: Re-add secondary indexes on new tables
ALTER TABLE qubic.transactions ADD INDEX IF NOT EXISTS idx_from from_address TYPE bloom_filter GRANULARITY 4;
ALTER TABLE qubic.transactions ADD INDEX IF NOT EXISTS idx_to to_address TYPE bloom_filter GRANULARITY 4;
ALTER TABLE qubic.transactions ADD INDEX IF NOT EXISTS idx_hash hash TYPE bloom_filter GRANULARITY 4;
ALTER TABLE qubic.logs ADD INDEX IF NOT EXISTS idx_source source_address TYPE bloom_filter GRANULARITY 4;
ALTER TABLE qubic.logs ADD INDEX IF NOT EXISTS idx_dest dest_address TYPE bloom_filter GRANULARITY 4;
ALTER TABLE qubic.logs ADD INDEX IF NOT EXISTS idx_type log_type TYPE set(20) GRANULARITY 4;
ALTER TABLE qubic.logs ADD INDEX IF NOT EXISTS idx_tx_hash tx_hash TYPE bloom_filter GRANULARITY 4;
ALTER TABLE qubic.balance_snapshots ADD INDEX IF NOT EXISTS idx_address address TYPE bloom_filter GRANULARITY 4;
ALTER TABLE qubic.flow_hops ADD INDEX IF NOT EXISTS idx_flow_emission_epoch emission_epoch TYPE minmax GRANULARITY 4;
ALTER TABLE qubic.flow_hops ADD INDEX IF NOT EXISTS idx_flow_origin origin_address TYPE bloom_filter GRANULARITY 4;
ALTER TABLE qubic.flow_hops ADD INDEX IF NOT EXISTS idx_flow_dest dest_address TYPE bloom_filter GRANULARITY 4;
ALTER TABLE qubic.flow_hops ADD INDEX IF NOT EXISTS idx_flow_origin_type origin_type TYPE set(10) GRANULARITY 4;
ALTER TABLE qubic.flow_hops ADD INDEX IF NOT EXISTS idx_flow_dest_type dest_type TYPE set(10) GRANULARITY 4;
ALTER TABLE qubic.flow_tracking_state ADD INDEX IF NOT EXISTS idx_fts_pending (emission_epoch, is_complete) TYPE set(2) GRANULARITY 4;

-- Step 5: Materialize indexes on existing data
ALTER TABLE qubic.transactions MATERIALIZE INDEX idx_from;
ALTER TABLE qubic.transactions MATERIALIZE INDEX idx_to;
ALTER TABLE qubic.transactions MATERIALIZE INDEX idx_hash;
ALTER TABLE qubic.logs MATERIALIZE INDEX idx_source;
ALTER TABLE qubic.logs MATERIALIZE INDEX idx_dest;
ALTER TABLE qubic.logs MATERIALIZE INDEX idx_type;
ALTER TABLE qubic.logs MATERIALIZE INDEX idx_tx_hash;
ALTER TABLE qubic.balance_snapshots MATERIALIZE INDEX idx_address;
ALTER TABLE qubic.flow_hops MATERIALIZE INDEX idx_flow_emission_epoch;
ALTER TABLE qubic.flow_hops MATERIALIZE INDEX idx_flow_origin;
ALTER TABLE qubic.flow_hops MATERIALIZE INDEX idx_flow_dest;
ALTER TABLE qubic.flow_hops MATERIALIZE INDEX idx_flow_origin_type;
ALTER TABLE qubic.flow_hops MATERIALIZE INDEX idx_flow_dest_type;
ALTER TABLE qubic.flow_tracking_state MATERIALIZE INDEX idx_fts_pending;

-- Step 6: Restart the application to recreate materialized views.
-- The app's schema initialization (GetSchemaStatements) uses CREATE ... IF NOT EXISTS
-- and will recreate all 13 MVs pointing at the new tables.

-- Step 7: After verifying everything works correctly, drop the old tables.
-- UNCOMMENT these lines only after thorough verification:
-- DROP TABLE IF EXISTS qubic.ticks_old;
-- DROP TABLE IF EXISTS qubic.transactions_old;
-- DROP TABLE IF EXISTS qubic.logs_old;
-- DROP TABLE IF EXISTS qubic.balance_snapshots_old;
-- DROP TABLE IF EXISTS qubic.flow_hops_old;
-- DROP TABLE IF EXISTS qubic.flow_tracking_state_old;
