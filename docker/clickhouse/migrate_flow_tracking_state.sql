-- Migration script: Update flow_tracking_state to support multiple origins per address
--
-- Problem: An intermediary can receive funds from multiple computors (origins),
-- but the current schema only allows one origin per address.
--
-- Solution: Change the ORDER BY key to include origin_address, allowing multiple
-- rows per (emission_epoch, address) - one for each origin computor.
--
-- IMPORTANT: This migration requires downtime as it recreates the table.
-- Existing tracking state will be lost and needs to be re-analyzed.

USE qubic;

-- Step 1: Show current row counts (for verification)
SELECT 'Current row counts:' as info;
SELECT 'flow_tracking_state:' as table, count() as count FROM flow_tracking_state;

-- Step 2: Rename existing table
RENAME TABLE flow_tracking_state TO flow_tracking_state_old;

-- Step 3: Create new table with updated ORDER BY key
-- The key now includes origin_address, allowing an address to be tracked
-- separately for each computor origin
CREATE TABLE flow_tracking_state (
    emission_epoch UInt32,                 -- The emission epoch being tracked (N)
    address String,                        -- Address being tracked
    origin_address String,                 -- Original computor this flow originated from
    address_type String,                   -- 'computor' or 'intermediary'
    received_amount Decimal(38, 0),        -- Total amount received by this address from this origin
    sent_amount Decimal(38, 0),            -- Total amount sent out by this address for this origin
    pending_amount Decimal(38, 0),         -- Amount still to be traced (received - sent)
    hop_level UInt8,                       -- Current hop level (1 = computor, 2+ = intermediary)
    last_tick UInt64,                      -- Last tick this address was processed
    is_terminal UInt8 DEFAULT 0,           -- 1 if this is an exchange (final destination)
    is_complete UInt8 DEFAULT 0,           -- 1 if all funds from this origin have been traced
    created_at DateTime64(3) DEFAULT now64(3),
    updated_at DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (emission_epoch, address, origin_address);

-- Step 4: Add index for looking up pending addresses
ALTER TABLE flow_tracking_state ADD INDEX IF NOT EXISTS idx_fts_pending (emission_epoch, is_complete) TYPE set(2) GRANULARITY 4;

-- Step 5: Migrate data from old table (if needed)
-- Note: Existing data should be compatible since it already had origin_address
INSERT INTO flow_tracking_state
SELECT
    emission_epoch,
    address,
    origin_address,
    address_type,
    received_amount,
    sent_amount,
    pending_amount,
    hop_level,
    last_tick,
    is_terminal,
    is_complete,
    created_at,
    updated_at
FROM flow_tracking_state_old;

-- Step 6: Verify new row counts
SELECT 'New row counts after migration:' as info;
SELECT 'flow_tracking_state:' as table, count() as count FROM flow_tracking_state;

-- Step 7: Drop old table (ONLY after verifying data is correct!)
-- Uncomment after verification:
-- DROP TABLE flow_tracking_state_old;

-- Note: After this migration, the same address can appear multiple times
-- in the tracking state table - once per origin computor. This allows
-- accurate tracking of flow completion per-origin.
--
-- Example:
-- emission_epoch | address       | origin_address | pending_amount
-- 150            | IntermedX     | ComputorA      | 1000
-- 150            | IntermedX     | ComputorB      | 2500
--
-- This shows IntermedX received funds from both ComputorA and ComputorB,
-- with different pending amounts to track for each origin.
