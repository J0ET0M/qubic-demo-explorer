-- Deduplicate tables script
-- Run this after re-indexing the same tick range to remove duplicates
-- This forces immediate deduplication instead of waiting for background merges

USE qubic;

-- Show current row counts before deduplication
SELECT 'Before deduplication:' as info;
SELECT 'Ticks:' as table, count() as count FROM ticks;
SELECT 'Transactions:' as table, count() as count FROM transactions;
SELECT 'Logs:' as table, count() as count FROM logs;

-- Force deduplication on all tables
-- This may take a while for large tables
OPTIMIZE TABLE ticks FINAL;
OPTIMIZE TABLE transactions FINAL;
OPTIMIZE TABLE logs FINAL;

-- Show row counts after deduplication
SELECT 'After deduplication:' as info;
SELECT 'Ticks:' as table, count() as count FROM ticks;
SELECT 'Transactions:' as table, count() as count FROM transactions;
SELECT 'Logs:' as table, count() as count FROM logs;

-- Check for any remaining duplicates (should be 0)
SELECT 'Duplicate check (should all be 0):' as info;
SELECT 'Duplicate ticks:' as check_type,
       count() - countDistinct(tick_number) as duplicate_count
FROM ticks;

SELECT 'Duplicate transactions:' as check_type,
       count() - countDistinct(tick_number, hash) as duplicate_count
FROM transactions;

SELECT 'Duplicate logs:' as check_type,
       count() - countDistinct(tick_number, log_id) as duplicate_count
FROM logs;
