-- Qubic Explorer ClickHouse Schema
-- This file is executed on container startup

CREATE DATABASE IF NOT EXISTS qubic;

-- Ticks table
-- Uses ReplacingMergeTree to deduplicate on tick_number (e.g., if indexer re-runs same range)
CREATE TABLE IF NOT EXISTS qubic.ticks (
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

-- Transactions table
-- Uses ReplacingMergeTree to deduplicate on (tick_number, hash)
CREATE TABLE IF NOT EXISTS qubic.transactions (
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

-- Secondary indexes for transactions
ALTER TABLE qubic.transactions ADD INDEX IF NOT EXISTS idx_from from_address TYPE bloom_filter GRANULARITY 4;
ALTER TABLE qubic.transactions ADD INDEX IF NOT EXISTS idx_to to_address TYPE bloom_filter GRANULARITY 4;
ALTER TABLE qubic.transactions ADD INDEX IF NOT EXISTS idx_hash hash TYPE bloom_filter GRANULARITY 4;

-- Logs table (all log types including QU_TRANSFER, ASSET_*, etc.)
-- Note: log_type_name is derived in the API layer from log_type
-- Uses ReplacingMergeTree to deduplicate on (tick_number, log_id)
CREATE TABLE IF NOT EXISTS qubic.logs (
    tick_number UInt64,
    epoch UInt32,
    log_id UInt32,
    log_type UInt8,
    tx_hash String,
    input_type UInt16,
    source_address String,
    dest_address String,
    amount UInt64,
    asset_name String,
    raw_data String,
    timestamp DateTime64(3),
    created_at DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(created_at)
ORDER BY (tick_number, log_id);

-- Secondary indexes for logs
ALTER TABLE qubic.logs ADD INDEX IF NOT EXISTS idx_source source_address TYPE bloom_filter GRANULARITY 4;
ALTER TABLE qubic.logs ADD INDEX IF NOT EXISTS idx_dest dest_address TYPE bloom_filter GRANULARITY 4;
ALTER TABLE qubic.logs ADD INDEX IF NOT EXISTS idx_type log_type TYPE set(20) GRANULARITY 4;
ALTER TABLE qubic.logs ADD INDEX IF NOT EXISTS idx_tx_hash tx_hash TYPE bloom_filter GRANULARITY 4;

-- Assets table for tracking asset issuance
CREATE TABLE IF NOT EXISTS qubic.assets (
    asset_name String,
    issuer_address String,
    tick_number UInt64,
    total_supply UInt64,
    created_at DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(created_at)
ORDER BY asset_name;

-- Indexer state (for resumption)
CREATE TABLE IF NOT EXISTS qubic.indexer_state (
    key String,
    value String,
    updated_at DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY key;

-- Materialized view for daily transaction volume (for charts)
CREATE MATERIALIZED VIEW IF NOT EXISTS qubic.daily_tx_volume
ENGINE = SummingMergeTree()
ORDER BY date
AS SELECT
    toDate(timestamp) as date,
    count() as tx_count,
    sum(amount) as total_volume
FROM qubic.transactions
GROUP BY date;

-- Materialized view for daily log counts by type
CREATE MATERIALIZED VIEW IF NOT EXISTS qubic.daily_log_stats
ENGINE = SummingMergeTree()
ORDER BY (date, log_type)
AS SELECT
    toDate(timestamp) as date,
    log_type,
    count() as log_count
FROM qubic.logs
GROUP BY date, log_type;

-- Materialized view for hourly network activity (for real-time charts)
CREATE MATERIALIZED VIEW IF NOT EXISTS qubic.hourly_activity
ENGINE = SummingMergeTree()
ORDER BY hour
AS SELECT
    toStartOfHour(timestamp) as hour,
    count() as tx_count,
    sum(amount) as total_volume,
    uniq(from_address) as unique_senders,
    uniq(to_address) as unique_receivers
FROM qubic.transactions
GROUP BY hour;

-- =====================================================
-- EPOCH STATISTICS
-- =====================================================

-- Epoch transaction stats (from transactions table)
CREATE MATERIALIZED VIEW IF NOT EXISTS qubic.epoch_tx_stats
ENGINE = SummingMergeTree()
ORDER BY epoch
AS SELECT
    epoch,
    count() as tx_count,
    sum(amount) as total_volume,
    uniqState(from_address) as unique_senders_state,
    uniqState(to_address) as unique_receivers_state
FROM qubic.transactions
GROUP BY epoch;

-- Epoch transfer stats (from logs table - QU transfers only, type 0)
CREATE MATERIALIZED VIEW IF NOT EXISTS qubic.epoch_transfer_stats
ENGINE = SummingMergeTree()
ORDER BY epoch
AS SELECT
    epoch,
    count() as transfer_count,
    sum(amount) as qu_transferred
FROM qubic.logs
WHERE log_type = 0
GROUP BY epoch;

-- Epoch transfer stats by type (all log types)
-- Note: log_type_name is derived in the API layer from log_type
CREATE MATERIALIZED VIEW IF NOT EXISTS qubic.epoch_transfer_by_type
ENGINE = SummingMergeTree()
ORDER BY (epoch, log_type)
AS SELECT
    epoch,
    log_type,
    count() as count,
    sum(amount) as total_amount
FROM qubic.logs
GROUP BY epoch, log_type;

-- Epoch tick stats (from ticks table)
-- Uses AggregatingMergeTree with proper state functions for min/max
CREATE MATERIALIZED VIEW IF NOT EXISTS qubic.epoch_tick_stats
ENGINE = AggregatingMergeTree()
ORDER BY epoch
AS SELECT
    epoch,
    countState() as tick_count_state,
    sumState(tx_count) as total_tx_in_ticks_state,
    sumState(log_count) as total_logs_in_ticks_state,
    minState(tick_number) as first_tick_state,
    maxState(tick_number) as last_tick_state,
    minState(timestamp) as start_time_state,
    maxState(timestamp) as end_time_state
FROM qubic.ticks
GROUP BY epoch;

-- Epoch sender addresses (unique senders per epoch)
CREATE MATERIALIZED VIEW IF NOT EXISTS qubic.epoch_sender_stats
ENGINE = AggregatingMergeTree()
ORDER BY epoch
AS SELECT
    epoch,
    uniqState(from_address) as sender_addresses_state
FROM qubic.transactions
GROUP BY epoch;

-- Epoch receiver addresses (unique receivers per epoch)
CREATE MATERIALIZED VIEW IF NOT EXISTS qubic.epoch_receiver_stats
ENGINE = AggregatingMergeTree()
ORDER BY epoch
AS SELECT
    epoch,
    uniqState(to_address) as receiver_addresses_state
FROM qubic.transactions
WHERE to_address != ''
GROUP BY epoch;

-- Combined epoch stats table (for faster querying - populated by query, not MV)
CREATE TABLE IF NOT EXISTS qubic.epoch_stats (
    epoch UInt32,
    tick_count UInt64,
    first_tick UInt64,
    last_tick UInt64,
    start_time DateTime64(3),
    end_time DateTime64(3),
    tx_count UInt64,
    total_volume UInt128,
    unique_senders UInt64,
    unique_receivers UInt64,
    active_addresses UInt64,
    transfer_count UInt64,
    qu_transferred UInt128,
    asset_transfer_count UInt64,
    updated_at DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY epoch;

-- =====================================================
-- ANALYTICS MATERIALIZED VIEWS
-- =====================================================

-- First address appearance tracking (for new vs returning analysis)
-- Stores when each address first appeared on the network
CREATE TABLE IF NOT EXISTS qubic.address_first_seen (
    address String,
    first_epoch UInt32,
    first_tick UInt64,
    first_timestamp DateTime64(3),
    created_at DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(created_at)
ORDER BY address;

-- Materialized view to populate address_first_seen from senders
CREATE MATERIALIZED VIEW IF NOT EXISTS qubic.mv_address_first_seen_from
TO qubic.address_first_seen
AS SELECT
    from_address as address,
    min(epoch) as first_epoch,
    min(tick_number) as first_tick,
    min(timestamp) as first_timestamp,
    now64(3) as created_at
FROM qubic.transactions
WHERE from_address != ''
GROUP BY from_address;

-- Materialized view to populate address_first_seen from receivers
CREATE MATERIALIZED VIEW IF NOT EXISTS qubic.mv_address_first_seen_to
TO qubic.address_first_seen
AS SELECT
    to_address as address,
    min(epoch) as first_epoch,
    min(tick_number) as first_tick,
    min(timestamp) as first_timestamp,
    now64(3) as created_at
FROM qubic.transactions
WHERE to_address != ''
GROUP BY to_address;

-- Average transaction size per epoch (pre-aggregated)
CREATE MATERIALIZED VIEW IF NOT EXISTS qubic.epoch_tx_size_stats
ENGINE = AggregatingMergeTree()
ORDER BY epoch
AS SELECT
    epoch,
    countState() as tx_count_state,
    sumState(amount) as total_volume_state,
    avgState(amount) as avg_tx_size_state,
    quantileState(0.5)(amount) as median_state
FROM qubic.logs
WHERE log_type = 0
GROUP BY epoch;

-- Daily average transaction size (pre-aggregated)
CREATE MATERIALIZED VIEW IF NOT EXISTS qubic.daily_tx_size_stats
ENGINE = AggregatingMergeTree()
ORDER BY date
AS SELECT
    toDate(timestamp) as date,
    countState() as tx_count_state,
    sumState(amount) as total_volume_state,
    avgState(amount) as avg_tx_size_state,
    quantileState(0.5)(amount) as median_state
FROM qubic.logs
WHERE log_type = 0
GROUP BY date;

-- =====================================================
-- BALANCE SNAPSHOTS (from Spectrum files)
-- =====================================================

-- Balance snapshots from Spectrum files
-- Each row represents an address balance at the start of an epoch
CREATE TABLE IF NOT EXISTS qubic.balance_snapshots (
    address String,
    epoch UInt32,
    tick_number UInt64,              -- First tick of the epoch (snapshot point)
    balance Int64,                   -- incomingAmount - outgoingAmount (can be negative temporarily)
    incoming_amount UInt64,
    outgoing_amount UInt64,
    incoming_transfer_count UInt32,
    outgoing_transfer_count UInt32,
    latest_incoming_tick UInt32,
    latest_outgoing_tick UInt32,
    imported_at DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(imported_at)
ORDER BY (epoch, address);

-- Index for looking up specific addresses
ALTER TABLE qubic.balance_snapshots ADD INDEX IF NOT EXISTS idx_address address TYPE bloom_filter GRANULARITY 4;

-- Track which spectrum files have been imported
CREATE TABLE IF NOT EXISTS qubic.spectrum_imports (
    epoch UInt32,
    tick_number UInt64,
    address_count UInt64,
    total_balance UInt128,
    file_size UInt64,
    import_duration_ms UInt32,
    imported_at DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(imported_at)
ORDER BY epoch;

-- =====================================================
-- HOLDER DISTRIBUTION HISTORY
-- =====================================================

-- Historical snapshots of holder distribution for trend analysis
CREATE TABLE IF NOT EXISTS qubic.holder_distribution_history (
    epoch UInt32,
    snapshot_at DateTime64(3) DEFAULT now64(3),

    -- Tick range for this window (0 = entire epoch)
    tick_start UInt64 DEFAULT 0,
    tick_end UInt64 DEFAULT 0,

    -- Holder counts by bracket
    whale_count UInt64,           -- ≥100B
    large_count UInt64,           -- 20B-100B
    medium_count UInt64,          -- 5B-20B
    small_count UInt64,           -- 500M-5B
    micro_count UInt64,           -- <500M

    -- Balances by bracket
    whale_balance UInt128,
    large_balance UInt128,
    medium_balance UInt128,
    small_balance UInt128,
    micro_balance UInt128,

    -- Totals
    total_holders UInt64,
    total_balance UInt128,

    -- Concentration metrics
    top10_balance UInt128,        -- Balance held by top 10 holders
    top50_balance UInt128,        -- Balance held by top 50 holders
    top100_balance UInt128,       -- Balance held by top 100 holders

    -- Computed from spectrum import or transfers
    data_source String DEFAULT 'transfers'  -- 'spectrum' or 'transfers'
) ENGINE = ReplacingMergeTree(snapshot_at)
ORDER BY (epoch, snapshot_at);

-- =====================================================
-- EPOCH METADATA
-- =====================================================

-- Metadata about each epoch (boundaries, tick ranges, etc.)
-- This is the source of truth for epoch information
CREATE TABLE IF NOT EXISTS qubic.epoch_meta (
    epoch UInt32,
    initial_tick UInt64,              -- First tick of the epoch
    end_tick UInt64,                  -- Last tick of the epoch (0 if ongoing)
    end_tick_start_log_id UInt64,     -- Starting log ID for end tick
    end_tick_end_log_id UInt64,       -- Ending log ID for end tick
    is_complete UInt8 DEFAULT 0,      -- 1 if epoch is complete (has end_tick)
    updated_at DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY epoch;

-- =====================================================
-- NETWORK STATS HISTORY
-- =====================================================

-- Historical snapshots of network statistics for trend analysis
-- Each snapshot represents metrics for a specific time window (e.g., 4 hours)
CREATE TABLE IF NOT EXISTS qubic.network_stats_history (
    epoch UInt32,
    snapshot_at DateTime64(3) DEFAULT now64(3),

    -- Tick range for this window (0 = entire epoch)
    tick_start UInt64 DEFAULT 0,
    tick_end UInt64 DEFAULT 0,

    -- Core metrics (for the window)
    total_transactions UInt64,
    total_transfers UInt64,
    total_volume UInt128,

    -- Active addresses
    unique_senders UInt64,
    unique_receivers UInt64,
    total_active_addresses UInt64,
    new_addresses UInt64,
    returning_addresses UInt64,

    -- Exchange flows
    exchange_inflow_volume UInt128,
    exchange_inflow_count UInt64,
    exchange_outflow_volume UInt128,
    exchange_outflow_count UInt64,
    exchange_net_flow Int128,

    -- Smart contract activity
    sc_call_count UInt64,
    sc_unique_callers UInt64,

    -- Transaction size metrics
    avg_tx_size Float64,
    median_tx_size Float64,

    -- New users with significant balances
    new_users_100m_plus UInt64 DEFAULT 0,   -- New addresses with ≥100M balance
    new_users_1b_plus UInt64 DEFAULT 0,     -- New addresses with ≥1B balance
    new_users_10b_plus UInt64 DEFAULT 0     -- New addresses with ≥10B balance

) ENGINE = ReplacingMergeTree(snapshot_at)
ORDER BY (epoch, snapshot_at);

-- =====================================================
-- COMPUTOR/MINER FLOW TRACKING
-- =====================================================

-- Store computor lists per epoch (fetched from qubic_getComputors RPC)
-- 676 computors per epoch
CREATE TABLE IF NOT EXISTS qubic.computors (
    epoch UInt32,
    address String,
    computor_index UInt16,             -- Position 0-675 in computor list
    created_at DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(created_at)
ORDER BY (epoch, computor_index);

-- Index for address lookups
ALTER TABLE qubic.computors ADD INDEX IF NOT EXISTS idx_computor_address address TYPE bloom_filter GRANULARITY 4;

-- Track which computor lists have been fetched
CREATE TABLE IF NOT EXISTS qubic.computor_imports (
    epoch UInt32,
    computor_count UInt16,             -- Should be 676
    imported_at DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(imported_at)
ORDER BY epoch;

-- Flow tracking: stores individual transfer hops for flow analysis
-- This allows tracking money flow up to N hops
-- Each row represents: at tick T, amount A flowed from source S to dest D at hop level H
CREATE TABLE IF NOT EXISTS qubic.flow_hops (
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

-- Indexes for flow queries
ALTER TABLE qubic.flow_hops ADD INDEX IF NOT EXISTS idx_flow_emission_epoch emission_epoch TYPE minmax GRANULARITY 4;
ALTER TABLE qubic.flow_hops ADD INDEX IF NOT EXISTS idx_flow_origin origin_address TYPE bloom_filter GRANULARITY 4;
ALTER TABLE qubic.flow_hops ADD INDEX IF NOT EXISTS idx_flow_dest dest_address TYPE bloom_filter GRANULARITY 4;
ALTER TABLE qubic.flow_hops ADD INDEX IF NOT EXISTS idx_flow_origin_type origin_type TYPE set(10) GRANULARITY 4;
ALTER TABLE qubic.flow_hops ADD INDEX IF NOT EXISTS idx_flow_dest_type dest_type TYPE set(10) GRANULARITY 4;

-- Aggregated flow statistics per snapshot window
-- Summarizes flow tracking results for visualization
CREATE TABLE IF NOT EXISTS qubic.miner_flow_stats (
    epoch UInt32,
    snapshot_at DateTime64(3),

    -- Tick range for this snapshot window
    tick_start UInt64,
    tick_end UInt64,

    -- Emission tracking (rewards received by computors)
    -- Note: computors from epoch N receive emission at start of epoch N+1
    emission_epoch UInt32,             -- Epoch whose computors we're tracking
    total_emission Decimal(38, 0),     -- Total received by computors in window
    computor_count UInt16,             -- Number of computors tracked

    -- Outflow from computors
    total_outflow Decimal(38, 0),      -- Total sent by computors
    outflow_tx_count UInt64,           -- Number of outflow transactions

    -- Flow to exchanges (direct + via hops)
    flow_to_exchange_direct Decimal(38, 0),    -- Hop 1: direct computor → exchange
    flow_to_exchange_1hop Decimal(38, 0),      -- Hop 2: computor → X → exchange
    flow_to_exchange_2hop Decimal(38, 0),      -- Hop 3: computor → X → Y → exchange
    flow_to_exchange_3plus Decimal(38, 0),     -- Hop 4+: deeper flows
    flow_to_exchange_total Decimal(38, 0),     -- Sum of all
    flow_to_exchange_count UInt64,             -- Number of transfers to exchanges

    -- Flow to other destinations
    flow_to_other Decimal(38, 0),              -- Amount not reaching exchanges

    -- Net position change
    miner_net_position Decimal(38, 0),         -- Emission - Outflow (+ = accumulating)

    -- Breakdown by hop depth (for visualization)
    hop_1_volume Decimal(38, 0),       -- Direct transfers from computors
    hop_2_volume Decimal(38, 0),       -- 1 hop away
    hop_3_volume Decimal(38, 0),       -- 2 hops away
    hop_4_plus_volume Decimal(38, 0),  -- 3+ hops away

    data_source String DEFAULT 'tick_window'
) ENGINE = ReplacingMergeTree(snapshot_at)
ORDER BY (epoch, emission_epoch, snapshot_at);

-- Flow tracking configuration: addresses to track (extensible for future use)
-- Allows adding custom addresses (pools, large miners, etc.) for flow analysis
CREATE TABLE IF NOT EXISTS qubic.flow_tracking_addresses (
    address String,
    address_type String,               -- 'computor', 'miner', 'pool', 'custom'
    epoch UInt32 DEFAULT 0,            -- 0 = all epochs, >0 = specific epoch
    label String DEFAULT '',           -- Human-readable name
    enabled UInt8 DEFAULT 1,           -- Whether to track this address
    created_at DateTime64(3) DEFAULT now64(3),
    updated_at DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (address_type, address);

-- Index for looking up by type
ALTER TABLE qubic.flow_tracking_addresses ADD INDEX IF NOT EXISTS idx_fta_type address_type TYPE set(10) GRANULARITY 4;

-- =====================================================
-- COMPUTOR EMISSIONS
-- =====================================================

-- Store emission amounts received by each computor at the end of each epoch
-- Populated by scanning end-epoch logs for transfers from zero address to computors
CREATE TABLE IF NOT EXISTS qubic.computor_emissions (
    epoch UInt32,                          -- The epoch that ended (N)
    computor_index UInt16,                 -- Position 0-675 in computor list
    address String,                        -- Computor address
    emission_amount Decimal(38, 0),        -- Amount received from zero address
    emission_tick UInt64,                  -- The tick where emission was received
    emission_timestamp DateTime64(3),      -- Timestamp of emission
    created_at DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(created_at)
ORDER BY (epoch, computor_index);

-- Index for address lookups
ALTER TABLE qubic.computor_emissions ADD INDEX IF NOT EXISTS idx_emission_address address TYPE bloom_filter GRANULARITY 4;

-- Track which emission records have been captured
CREATE TABLE IF NOT EXISTS qubic.emission_imports (
    epoch UInt32,
    computor_count UInt16,                 -- Number of computors with emission
    total_emission Decimal(38, 0),         -- Total emission for the epoch
    emission_tick UInt64,                  -- Last tick of the epoch
    imported_at DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(imported_at)
ORDER BY epoch;

-- =====================================================
-- FLOW TRACKING STATE
-- =====================================================

-- Track addresses that still have funds to trace across tick windows.
-- When emission flows from computor → intermediary, we continue tracking the intermediary
-- in subsequent windows until funds reach an exchange (terminal).
-- The key includes origin_address to track funds from each computor separately,
-- allowing an intermediary to be tracked with multiple origins.
CREATE TABLE IF NOT EXISTS qubic.flow_tracking_state (
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

-- Index for looking up pending addresses
ALTER TABLE qubic.flow_tracking_state ADD INDEX IF NOT EXISTS idx_fts_pending (emission_epoch, is_complete) TYPE set(2) GRANULARITY 4;
