-- Migration script: Update flow_hops ORDER BY key to include dest_address
--
-- Problem: When a single transaction sends to multiple destinations (e.g., via Qutil),
-- all those hops have the same (emission_epoch, origin_address, hop_level, tick_number, tx_hash)
-- key, causing ReplacingMergeTree to deduplicate them down to just one record.
--
-- Solution: Add dest_address to the ORDER BY key to distinguish multiple destinations
-- from the same transaction.
--
-- IMPORTANT: This migration requires recreating the table and re-analyzing flow data.

USE qubic;

-- Step 1: Show current row counts
SELECT 'Current row counts:' as info;
SELECT 'flow_hops:' as table, count() as count FROM flow_hops;

-- Step 2: Rename existing table
RENAME TABLE flow_hops TO flow_hops_old;

-- Step 3: Create new table with updated ORDER BY key (includes dest_address)
CREATE TABLE flow_hops (
    -- Context
    epoch UInt32,                       -- Current epoch when the hop occurred
    emission_epoch UInt32,              -- The emission epoch being tracked (computors from this epoch)
    tick_number UInt64,
    timestamp DateTime64(3),

    -- Transfer details
    tx_hash String,
    source_address String,
    dest_address String,
    amount UInt64,

    -- Flow tracking
    origin_address String,             -- Original source (e.g., computor address)
    origin_type String,                -- 'computor', 'miner', 'pool', 'custom'
    hop_level UInt8,                   -- 1 = direct from origin, 2 = 1 hop away, etc.

    -- Destination classification (if known)
    dest_type String DEFAULT '',       -- 'exchange', 'pool', 'miner', etc.
    dest_label String DEFAULT '',      -- Human-readable label

    created_at DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(created_at)
ORDER BY (emission_epoch, origin_address, hop_level, tick_number, tx_hash, dest_address);

-- Step 4: Add indexes
ALTER TABLE flow_hops ADD INDEX IF NOT EXISTS idx_flow_emission_epoch emission_epoch TYPE minmax GRANULARITY 4;
ALTER TABLE flow_hops ADD INDEX IF NOT EXISTS idx_flow_origin origin_address TYPE bloom_filter GRANULARITY 4;
ALTER TABLE flow_hops ADD INDEX IF NOT EXISTS idx_flow_dest dest_address TYPE bloom_filter GRANULARITY 4;
ALTER TABLE flow_hops ADD INDEX IF NOT EXISTS idx_flow_origin_type origin_type TYPE set(10) GRANULARITY 4;
ALTER TABLE flow_hops ADD INDEX IF NOT EXISTS idx_flow_dest_type dest_type TYPE set(10) GRANULARITY 4;

-- Step 5: Migrate data from old table
-- Note: With the new key, records that were previously deduplicated will remain
-- as single records. Full re-analysis is recommended for accurate data.
INSERT INTO flow_hops
SELECT
    epoch,
    emission_epoch,
    tick_number,
    timestamp,
    tx_hash,
    source_address,
    dest_address,
    amount,
    origin_address,
    origin_type,
    hop_level,
    dest_type,
    dest_label,
    created_at
FROM flow_hops_old;

-- Step 6: Verify new row counts
SELECT 'New row counts after migration:' as info;
SELECT 'flow_hops:' as table, count() as count FROM flow_hops;

-- Step 7: Drop old table (ONLY after verifying data is correct!)
-- Uncomment after verification:
-- DROP TABLE flow_hops_old;

-- IMPORTANT: After this migration, you should:
-- 1. Clear flow_tracking_state: TRUNCATE TABLE flow_tracking_state;
-- 2. Clear flow_hops: TRUNCATE TABLE flow_hops;
-- 3. Re-run the flow analysis to regenerate data with proper multi-destination support
