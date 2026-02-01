-- Migration script: Convert MergeTree tables to ReplacingMergeTree
-- This prevents duplicate data when the indexer runs twice for the same tick range
--
-- IMPORTANT: This migration requires downtime as it recreates the tables
-- Backup your data before running this script!

USE qubic;

-- Step 1: Show current row counts (for verification)
SELECT 'Current row counts:' as info;
SELECT 'Ticks:' as table, count() as count FROM ticks;
SELECT 'Transactions:' as table, count() as count FROM transactions;
SELECT 'Logs:' as table, count() as count FROM logs;

-- Step 2: Rename existing tables
RENAME TABLE ticks TO ticks_old;
RENAME TABLE transactions TO transactions_old;
RENAME TABLE logs TO logs_old;

-- Step 3: Create new tables with ReplacingMergeTree engine
CREATE TABLE ticks (
    tick_number UInt64,
    epoch UInt32,
    timestamp DateTime64(3),
    tx_count UInt32,
    tx_count_filtered UInt32,
    log_count UInt32,
    log_count_filtered UInt32,
    created_at DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(created_at)
ORDER BY tick_number;

CREATE TABLE transactions (
    hash String,
    tick_number UInt64,
    epoch UInt32,
    from_address String,
    to_address String,
    amount UInt64,
    input_type UInt16,
    input_data String,
    executed UInt8,
    log_id_from Int32,
    log_id_length UInt16,
    timestamp DateTime64(3),
    created_at DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(created_at)
ORDER BY (tick_number, hash);

CREATE TABLE logs (
    tick_number UInt64,
    epoch UInt32,
    log_id UInt32,
    log_type UInt8,
    tx_hash String,
    source_address String,
    dest_address String,
    amount UInt64,
    asset_name String,
    raw_data String,
    timestamp DateTime64(3),
    created_at DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(created_at)
ORDER BY (tick_number, log_id);

-- Step 4: Copy data from old tables (this also deduplicates during insert)
INSERT INTO ticks SELECT * FROM ticks_old;
INSERT INTO transactions SELECT * FROM transactions_old;
INSERT INTO logs SELECT * FROM logs_old;

-- Step 5: Recreate indexes
ALTER TABLE transactions ADD INDEX IF NOT EXISTS idx_from from_address TYPE bloom_filter GRANULARITY 4;
ALTER TABLE transactions ADD INDEX IF NOT EXISTS idx_to to_address TYPE bloom_filter GRANULARITY 4;
ALTER TABLE transactions ADD INDEX IF NOT EXISTS idx_hash hash TYPE bloom_filter GRANULARITY 4;

ALTER TABLE logs ADD INDEX IF NOT EXISTS idx_source source_address TYPE bloom_filter GRANULARITY 4;
ALTER TABLE logs ADD INDEX IF NOT EXISTS idx_dest dest_address TYPE bloom_filter GRANULARITY 4;
ALTER TABLE logs ADD INDEX IF NOT EXISTS idx_type log_type TYPE set(20) GRANULARITY 4;
ALTER TABLE logs ADD INDEX IF NOT EXISTS idx_tx_hash tx_hash TYPE bloom_filter GRANULARITY 4;

-- Step 6: Force deduplication by optimizing
OPTIMIZE TABLE ticks FINAL;
OPTIMIZE TABLE transactions FINAL;
OPTIMIZE TABLE logs FINAL;

-- Step 7: Verify new row counts (should be same or less if duplicates existed)
SELECT 'New row counts after migration:' as info;
SELECT 'Ticks:' as table, count() as count FROM ticks;
SELECT 'Transactions:' as table, count() as count FROM transactions;
SELECT 'Logs:' as table, count() as count FROM logs;

-- Step 8: Drop old tables (ONLY after verifying data is correct!)
-- Uncomment these lines after verification:
-- DROP TABLE ticks_old;
-- DROP TABLE transactions_old;
-- DROP TABLE logs_old;

-- Note: ReplacingMergeTree deduplication happens:
-- 1. During background merges (automatic, may take some time)
-- 2. When running OPTIMIZE TABLE ... FINAL (manual, forces immediate deduplication)
-- 3. At query time with SELECT ... FINAL (slower queries, not recommended for production)
--
-- How ReplacingMergeTree works:
-- - Rows with the same ORDER BY key are considered duplicates
-- - The version column (created_at) determines which row to keep (highest value wins)
-- - Deduplication is NOT immediate - it happens during background merges
-- - For guaranteed deduplication, run OPTIMIZE TABLE ... FINAL after re-indexing
--
-- Recommended workflow after re-indexing the same tick range:
-- 1. Wait for the indexer to finish
-- 2. Run: OPTIMIZE TABLE qubic.ticks FINAL;
-- 3. Run: OPTIMIZE TABLE qubic.transactions FINAL;
-- 4. Run: OPTIMIZE TABLE qubic.logs FINAL;
-- Or use the deduplicate_tables.sql script
