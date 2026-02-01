-- Script to clear flow data for a specific emission epoch to allow re-analysis
-- Usage: Replace {EMISSION_EPOCH} with the epoch number you want to re-analyze
-- e.g., for epoch 150, replace {EMISSION_EPOCH} with 150
--
-- After running this script, trigger the flow analysis again via the API or indexer

USE qubic;

-- Show current row counts before clearing
SELECT 'Row counts before clearing:' as info;
SELECT 'flow_tracking_state for epoch:' as table, count() as count
FROM flow_tracking_state
WHERE emission_epoch = {EMISSION_EPOCH};

SELECT 'flow_hops for epoch:' as table, count() as count
FROM flow_hops
WHERE emission_epoch = {EMISSION_EPOCH};

-- Clear flow_tracking_state for this emission epoch
-- This resets the tracking state so addresses can be re-analyzed
ALTER TABLE flow_tracking_state DELETE WHERE emission_epoch = {EMISSION_EPOCH};

-- Clear flow_hops for this emission epoch
-- This removes all recorded hop data so it can be regenerated
ALTER TABLE flow_hops DELETE WHERE emission_epoch = {EMISSION_EPOCH};

-- Wait for mutations to complete
SELECT 'Waiting for delete mutations to complete...' as info;
-- Note: Lightweight deletes are async. Check mutations with:
-- SELECT * FROM system.mutations WHERE is_done = 0;

-- Verify data was cleared
SELECT 'Row counts after clearing (may take a moment for async delete):' as info;
SELECT 'flow_tracking_state for epoch:' as table, count() as count
FROM flow_tracking_state
WHERE emission_epoch = {EMISSION_EPOCH};

SELECT 'flow_hops for epoch:' as table, count() as count
FROM flow_hops
WHERE emission_epoch = {EMISSION_EPOCH};

-- Alternative: Use TRUNCATE to clear ALL data from both tables (for full reset)
-- WARNING: This deletes ALL epochs, not just one!
-- TRUNCATE TABLE flow_tracking_state;
-- TRUNCATE TABLE flow_hops;
