-- Fix epoch 0 records in ClickHouse
-- This script updates ticks, transactions, and logs that have epoch = 0
-- by deriving the correct epoch from logs that have valid epoch values

-- Step 1: Create a dictionary for fast lookups
CREATE DICTIONARY IF NOT EXISTS qubic.tick_epoch_dict
(
    tick_number UInt64,
    correct_epoch UInt32
)
PRIMARY KEY tick_number
SOURCE(CLICKHOUSE(
    QUERY 'SELECT tick_number, any(epoch) as correct_epoch FROM qubic.logs WHERE epoch > 0 GROUP BY tick_number'
))
LAYOUT(FLAT())
LIFETIME(0);

-- Step 2: Show how many records will be affected (for verification)
SELECT 'Ticks with epoch 0:' as info, count() as count FROM qubic.ticks WHERE epoch = 0;
SELECT 'Transactions with epoch 0:' as info, count() as count FROM qubic.transactions WHERE epoch = 0;
SELECT 'Logs with epoch 0:' as info, count() as count FROM qubic.logs WHERE epoch = 0;

-- Step 3: Update ticks table using dictionary lookup
ALTER TABLE qubic.ticks
UPDATE epoch = dictGet('qubic.tick_epoch_dict', 'correct_epoch', tick_number)
WHERE epoch = 0
  AND dictHas('qubic.tick_epoch_dict', tick_number);

-- Step 4: Update transactions table using dictionary lookup
ALTER TABLE qubic.transactions
UPDATE epoch = dictGet('qubic.tick_epoch_dict', 'correct_epoch', tick_number)
WHERE epoch = 0
  AND dictHas('qubic.tick_epoch_dict', tick_number);

-- Step 5: Update logs table (for any logs that somehow have epoch 0)
ALTER TABLE qubic.logs
UPDATE epoch = dictGet('qubic.tick_epoch_dict', 'correct_epoch', tick_number)
WHERE epoch = 0
  AND dictHas('qubic.tick_epoch_dict', tick_number);

-- Step 6: Wait for mutations to complete (check status)
-- Run this query to monitor mutation progress:
-- SELECT * FROM system.mutations WHERE is_done = 0;

-- Step 7: Clean up dictionary
DROP DICTIONARY IF EXISTS qubic.tick_epoch_dict;

-- Step 8: Verify the fix
SELECT 'Remaining ticks with epoch 0:' as info, count() as count FROM qubic.ticks WHERE epoch = 0;
SELECT 'Remaining transactions with epoch 0:' as info, count() as count FROM qubic.transactions WHERE epoch = 0;
SELECT 'Remaining logs with epoch 0:' as info, count() as count FROM qubic.logs WHERE epoch = 0;

-- Note: Mutations in ClickHouse are asynchronous.
-- The actual data modification happens in the background.
-- You can check mutation progress with:
-- SELECT database, table, mutation_id, command, create_time, is_done
-- FROM system.mutations
-- WHERE is_done = 0;
