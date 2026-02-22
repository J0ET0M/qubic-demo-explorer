namespace QubicExplorer.Shared;

/// <summary>
/// Contains the ClickHouse schema DDL statements for the qubic database.
/// Each statement is a separate string to be executed individually.
/// </summary>
public static class ClickHouseSchema
{
    public const string DatabaseName = "qubic";

    public static string CreateDatabase => $"CREATE DATABASE IF NOT EXISTS {DatabaseName}";

    /// <summary>
    /// Returns all DDL statements needed to initialize the schema.
    /// Must be executed in order after creating the database.
    /// </summary>
    public static IReadOnlyList<string> GetSchemaStatements() =>
    [
        // Ticks table
        $"""
        CREATE TABLE IF NOT EXISTS {DatabaseName}.ticks (
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
        ORDER BY tick_number
        """,

        // Transactions table
        $"""
        CREATE TABLE IF NOT EXISTS {DatabaseName}.transactions (
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
        ORDER BY (tick_number, hash)
        """,

        // Transaction indexes
        $"ALTER TABLE {DatabaseName}.transactions ADD INDEX IF NOT EXISTS idx_from from_address TYPE bloom_filter GRANULARITY 4",
        $"ALTER TABLE {DatabaseName}.transactions ADD INDEX IF NOT EXISTS idx_to to_address TYPE bloom_filter GRANULARITY 4",
        $"ALTER TABLE {DatabaseName}.transactions ADD INDEX IF NOT EXISTS idx_hash hash TYPE bloom_filter GRANULARITY 4",

        // Logs table
        $"""
        CREATE TABLE IF NOT EXISTS {DatabaseName}.logs (
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
        ORDER BY (tick_number, log_id)
        """,

        // Migration: add input_type column to existing logs table
        $"ALTER TABLE {DatabaseName}.logs ADD COLUMN IF NOT EXISTS input_type UInt16 DEFAULT 0 AFTER tx_hash",

        // Log indexes
        $"ALTER TABLE {DatabaseName}.logs ADD INDEX IF NOT EXISTS idx_source source_address TYPE bloom_filter GRANULARITY 4",
        $"ALTER TABLE {DatabaseName}.logs ADD INDEX IF NOT EXISTS idx_dest dest_address TYPE bloom_filter GRANULARITY 4",
        $"ALTER TABLE {DatabaseName}.logs ADD INDEX IF NOT EXISTS idx_type log_type TYPE set(20) GRANULARITY 4",
        $"ALTER TABLE {DatabaseName}.logs ADD INDEX IF NOT EXISTS idx_tx_hash tx_hash TYPE bloom_filter GRANULARITY 4",

        // Assets table
        $"""
        CREATE TABLE IF NOT EXISTS {DatabaseName}.assets (
            asset_name String,
            issuer_address String,
            tick_number UInt64,
            total_supply UInt64,
            created_at DateTime64(3) DEFAULT now64(3)
        ) ENGINE = ReplacingMergeTree(created_at)
        ORDER BY asset_name
        """,

        // Indexer state
        $"""
        CREATE TABLE IF NOT EXISTS {DatabaseName}.indexer_state (
            key String,
            value String,
            updated_at DateTime64(3) DEFAULT now64(3)
        ) ENGINE = ReplacingMergeTree(updated_at)
        ORDER BY key
        """,

        // Daily transaction volume
        $"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {DatabaseName}.daily_tx_volume
        ENGINE = SummingMergeTree()
        ORDER BY date
        AS SELECT
            toDate(timestamp) as date,
            count() as tx_count,
            sum(amount) as total_volume
        FROM {DatabaseName}.transactions
        GROUP BY date
        """,

        // Daily log stats
        $"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {DatabaseName}.daily_log_stats
        ENGINE = SummingMergeTree()
        ORDER BY (date, log_type)
        AS SELECT
            toDate(timestamp) as date,
            log_type,
            count() as log_count
        FROM {DatabaseName}.logs
        GROUP BY date, log_type
        """,

        // Hourly activity
        $"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {DatabaseName}.hourly_activity
        ENGINE = SummingMergeTree()
        ORDER BY hour
        AS SELECT
            toStartOfHour(timestamp) as hour,
            count() as tx_count,
            sum(amount) as total_volume,
            uniq(from_address) as unique_senders,
            uniq(to_address) as unique_receivers
        FROM {DatabaseName}.transactions
        GROUP BY hour
        """,

        // Epoch transaction stats
        $"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {DatabaseName}.epoch_tx_stats
        ENGINE = SummingMergeTree()
        ORDER BY epoch
        AS SELECT
            epoch,
            count() as tx_count,
            sum(amount) as total_volume,
            uniqState(from_address) as unique_senders_state,
            uniqState(to_address) as unique_receivers_state
        FROM {DatabaseName}.transactions
        GROUP BY epoch
        """,

        // Epoch transfer stats
        $"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {DatabaseName}.epoch_transfer_stats
        ENGINE = SummingMergeTree()
        ORDER BY epoch
        AS SELECT
            epoch,
            count() as transfer_count,
            sum(amount) as qu_transferred
        FROM {DatabaseName}.logs
        WHERE log_type = 0
        GROUP BY epoch
        """,

        // Epoch transfer by type
        $"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {DatabaseName}.epoch_transfer_by_type
        ENGINE = SummingMergeTree()
        ORDER BY (epoch, log_type)
        AS SELECT
            epoch,
            log_type,
            count() as count,
            sum(amount) as total_amount
        FROM {DatabaseName}.logs
        GROUP BY epoch, log_type
        """,

        // Epoch tick stats
        $"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {DatabaseName}.epoch_tick_stats
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
        FROM {DatabaseName}.ticks
        GROUP BY epoch
        """,

        // Epoch sender stats
        $"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {DatabaseName}.epoch_sender_stats
        ENGINE = AggregatingMergeTree()
        ORDER BY epoch
        AS SELECT
            epoch,
            uniqState(from_address) as sender_addresses_state
        FROM {DatabaseName}.transactions
        GROUP BY epoch
        """,

        // Epoch receiver stats
        $"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {DatabaseName}.epoch_receiver_stats
        ENGINE = AggregatingMergeTree()
        ORDER BY epoch
        AS SELECT
            epoch,
            uniqState(to_address) as receiver_addresses_state
        FROM {DatabaseName}.transactions
        WHERE to_address != ''
        GROUP BY epoch
        """,

        // Epoch stats table
        $"""
        CREATE TABLE IF NOT EXISTS {DatabaseName}.epoch_stats (
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
        ORDER BY epoch
        """,

        // Address first seen
        $"""
        CREATE TABLE IF NOT EXISTS {DatabaseName}.address_first_seen (
            address String,
            first_epoch UInt32,
            first_tick UInt64,
            first_timestamp DateTime64(3),
            created_at DateTime64(3) DEFAULT now64(3)
        ) ENGINE = ReplacingMergeTree(created_at)
        ORDER BY address
        """,

        // MV: address first seen from senders
        $"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {DatabaseName}.mv_address_first_seen_from
        TO {DatabaseName}.address_first_seen
        AS SELECT
            from_address as address,
            min(epoch) as first_epoch,
            min(tick_number) as first_tick,
            min(timestamp) as first_timestamp,
            now64(3) as created_at
        FROM {DatabaseName}.transactions
        WHERE from_address != ''
        GROUP BY from_address
        """,

        // MV: address first seen from receivers
        $"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {DatabaseName}.mv_address_first_seen_to
        TO {DatabaseName}.address_first_seen
        AS SELECT
            to_address as address,
            min(epoch) as first_epoch,
            min(tick_number) as first_tick,
            min(timestamp) as first_timestamp,
            now64(3) as created_at
        FROM {DatabaseName}.transactions
        WHERE to_address != ''
        GROUP BY to_address
        """,

        // Epoch tx size stats
        $"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {DatabaseName}.epoch_tx_size_stats
        ENGINE = AggregatingMergeTree()
        ORDER BY epoch
        AS SELECT
            epoch,
            countState() as tx_count_state,
            sumState(amount) as total_volume_state,
            avgState(amount) as avg_tx_size_state,
            quantileState(0.5)(amount) as median_state
        FROM {DatabaseName}.logs
        WHERE log_type = 0
        GROUP BY epoch
        """,

        // Daily tx size stats
        $"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {DatabaseName}.daily_tx_size_stats
        ENGINE = AggregatingMergeTree()
        ORDER BY date
        AS SELECT
            toDate(timestamp) as date,
            countState() as tx_count_state,
            sumState(amount) as total_volume_state,
            avgState(amount) as avg_tx_size_state,
            quantileState(0.5)(amount) as median_state
        FROM {DatabaseName}.logs
        WHERE log_type = 0
        GROUP BY date
        """,

        // Balance snapshots
        $"""
        CREATE TABLE IF NOT EXISTS {DatabaseName}.balance_snapshots (
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
        ORDER BY (epoch, address)
        """,

        $"ALTER TABLE {DatabaseName}.balance_snapshots ADD INDEX IF NOT EXISTS idx_address address TYPE bloom_filter GRANULARITY 4",

        // Spectrum imports
        $"""
        CREATE TABLE IF NOT EXISTS {DatabaseName}.spectrum_imports (
            epoch UInt32,
            tick_number UInt64,
            address_count UInt64,
            total_balance UInt128,
            file_size UInt64,
            import_duration_ms UInt32,
            imported_at DateTime64(3) DEFAULT now64(3)
        ) ENGINE = ReplacingMergeTree(imported_at)
        ORDER BY epoch
        """,

        // Holder distribution history
        $"""
        CREATE TABLE IF NOT EXISTS {DatabaseName}.holder_distribution_history (
            epoch UInt32,
            snapshot_at DateTime64(3) DEFAULT now64(3),
            tick_start UInt64 DEFAULT 0,
            tick_end UInt64 DEFAULT 0,
            whale_count UInt64,
            large_count UInt64,
            medium_count UInt64,
            small_count UInt64,
            micro_count UInt64,
            whale_balance UInt128,
            large_balance UInt128,
            medium_balance UInt128,
            small_balance UInt128,
            micro_balance UInt128,
            total_holders UInt64,
            total_balance UInt128,
            top10_balance UInt128,
            top50_balance UInt128,
            top100_balance UInt128,
            data_source String DEFAULT 'transfers'
        ) ENGINE = ReplacingMergeTree(snapshot_at)
        ORDER BY (epoch, snapshot_at)
        """,

        // Epoch metadata
        $"""
        CREATE TABLE IF NOT EXISTS {DatabaseName}.epoch_meta (
            epoch UInt32,
            initial_tick UInt64,
            end_tick UInt64,
            end_tick_start_log_id UInt64,
            end_tick_end_log_id UInt64,
            is_complete UInt8 DEFAULT 0,
            updated_at DateTime64(3) DEFAULT now64(3)
        ) ENGINE = ReplacingMergeTree(updated_at)
        ORDER BY epoch
        """,

        // Network stats history
        $"""
        CREATE TABLE IF NOT EXISTS {DatabaseName}.network_stats_history (
            epoch UInt32,
            snapshot_at DateTime64(3) DEFAULT now64(3),
            tick_start UInt64 DEFAULT 0,
            tick_end UInt64 DEFAULT 0,
            total_transactions UInt64,
            total_transfers UInt64,
            total_volume UInt128,
            unique_senders UInt64,
            unique_receivers UInt64,
            total_active_addresses UInt64,
            new_addresses UInt64,
            returning_addresses UInt64,
            exchange_inflow_volume UInt128,
            exchange_inflow_count UInt64,
            exchange_outflow_volume UInt128,
            exchange_outflow_count UInt64,
            exchange_net_flow Int128,
            sc_call_count UInt64,
            sc_unique_callers UInt64,
            avg_tx_size Float64,
            median_tx_size Float64,
            new_users_100m_plus UInt64 DEFAULT 0,
            new_users_1b_plus UInt64 DEFAULT 0,
            new_users_10b_plus UInt64 DEFAULT 0
        ) ENGINE = ReplacingMergeTree(snapshot_at)
        ORDER BY (epoch, snapshot_at)
        """,

        // Burn stats history (4-hour snapshots)
        $"""
        CREATE TABLE IF NOT EXISTS {DatabaseName}.burn_stats_history (
            epoch UInt32,
            snapshot_at DateTime64(3) DEFAULT now64(3),
            tick_start UInt64 DEFAULT 0,
            tick_end UInt64 DEFAULT 0,
            total_burned UInt64,
            burn_count UInt64,
            burn_amount UInt64,
            dust_burn_count UInt64,
            dust_burned UInt64,
            transfer_burn_count UInt64,
            transfer_burned UInt64,
            unique_burners UInt64,
            largest_burn UInt64,
            cumulative_burned UInt64
        ) ENGINE = ReplacingMergeTree(snapshot_at)
        ORDER BY (epoch, snapshot_at)
        """,

        // Computors
        $"""
        CREATE TABLE IF NOT EXISTS {DatabaseName}.computors (
            epoch UInt32,
            address String,
            computor_index UInt16,
            created_at DateTime64(3) DEFAULT now64(3)
        ) ENGINE = ReplacingMergeTree(created_at)
        ORDER BY (epoch, computor_index)
        """,

        $"ALTER TABLE {DatabaseName}.computors ADD INDEX IF NOT EXISTS idx_computor_address address TYPE bloom_filter GRANULARITY 4",

        // Computor imports
        $"""
        CREATE TABLE IF NOT EXISTS {DatabaseName}.computor_imports (
            epoch UInt32,
            computor_count UInt16,
            imported_at DateTime64(3) DEFAULT now64(3)
        ) ENGINE = ReplacingMergeTree(imported_at)
        ORDER BY epoch
        """,

        // Flow hops
        $"""
        CREATE TABLE IF NOT EXISTS {DatabaseName}.flow_hops (
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
        ORDER BY (emission_epoch, origin_address, hop_level, tick_number, tx_hash, dest_address)
        """,

        $"ALTER TABLE {DatabaseName}.flow_hops ADD INDEX IF NOT EXISTS idx_flow_emission_epoch emission_epoch TYPE minmax GRANULARITY 4",
        $"ALTER TABLE {DatabaseName}.flow_hops ADD INDEX IF NOT EXISTS idx_flow_origin origin_address TYPE bloom_filter GRANULARITY 4",
        $"ALTER TABLE {DatabaseName}.flow_hops ADD INDEX IF NOT EXISTS idx_flow_dest dest_address TYPE bloom_filter GRANULARITY 4",
        $"ALTER TABLE {DatabaseName}.flow_hops ADD INDEX IF NOT EXISTS idx_flow_origin_type origin_type TYPE set(10) GRANULARITY 4",
        $"ALTER TABLE {DatabaseName}.flow_hops ADD INDEX IF NOT EXISTS idx_flow_dest_type dest_type TYPE set(10) GRANULARITY 4",

        // Miner flow stats
        $"""
        CREATE TABLE IF NOT EXISTS {DatabaseName}.miner_flow_stats (
            epoch UInt32,
            snapshot_at DateTime64(3),
            tick_start UInt64,
            tick_end UInt64,
            emission_epoch UInt32,
            total_emission Decimal(38, 0),
            computor_count UInt16,
            total_outflow Decimal(38, 0),
            outflow_tx_count UInt64,
            flow_to_exchange_direct Decimal(38, 0),
            flow_to_exchange_1hop Decimal(38, 0),
            flow_to_exchange_2hop Decimal(38, 0),
            flow_to_exchange_3plus Decimal(38, 0),
            flow_to_exchange_total Decimal(38, 0),
            flow_to_exchange_count UInt64,
            flow_to_other Decimal(38, 0),
            miner_net_position Decimal(38, 0),
            hop_1_volume Decimal(38, 0),
            hop_2_volume Decimal(38, 0),
            hop_3_volume Decimal(38, 0),
            hop_4_plus_volume Decimal(38, 0),
            data_source String DEFAULT 'tick_window'
        ) ENGINE = ReplacingMergeTree(snapshot_at)
        ORDER BY (epoch, emission_epoch, snapshot_at)
        """,

        // Flow tracking addresses
        $"""
        CREATE TABLE IF NOT EXISTS {DatabaseName}.flow_tracking_addresses (
            address String,
            address_type String,
            epoch UInt32 DEFAULT 0,
            label String DEFAULT '',
            enabled UInt8 DEFAULT 1,
            created_at DateTime64(3) DEFAULT now64(3),
            updated_at DateTime64(3) DEFAULT now64(3)
        ) ENGINE = ReplacingMergeTree(updated_at)
        ORDER BY (address_type, address)
        """,

        $"ALTER TABLE {DatabaseName}.flow_tracking_addresses ADD INDEX IF NOT EXISTS idx_fta_type address_type TYPE set(10) GRANULARITY 4",

        // Computor emissions
        $"""
        CREATE TABLE IF NOT EXISTS {DatabaseName}.computor_emissions (
            epoch UInt32,
            computor_index UInt16,
            address String,
            emission_amount Decimal(38, 0),
            emission_tick UInt64,
            emission_timestamp DateTime64(3),
            created_at DateTime64(3) DEFAULT now64(3)
        ) ENGINE = ReplacingMergeTree(created_at)
        ORDER BY (epoch, computor_index)
        """,

        $"ALTER TABLE {DatabaseName}.computor_emissions ADD INDEX IF NOT EXISTS idx_emission_address address TYPE bloom_filter GRANULARITY 4",

        // Emission imports
        $"""
        CREATE TABLE IF NOT EXISTS {DatabaseName}.emission_imports (
            epoch UInt32,
            computor_count UInt16,
            total_emission Decimal(38, 0),
            emission_tick UInt64,
            imported_at DateTime64(3) DEFAULT now64(3)
        ) ENGINE = ReplacingMergeTree(imported_at)
        ORDER BY epoch
        """,

        // Flow tracking state (TTL: completed entries expire after 90 days)
        $"""
        CREATE TABLE IF NOT EXISTS {DatabaseName}.flow_tracking_state (
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
        TTL toDateTime(updated_at) + INTERVAL 90 DAY WHERE is_complete = 1
        """,

        $"ALTER TABLE {DatabaseName}.flow_tracking_state ADD INDEX IF NOT EXISTS idx_fts_pending (emission_epoch, is_complete) TYPE set(2) GRANULARITY 4",
    ];

    /// <summary>
    /// Tables that need migration to add PARTITION BY epoch and codecs.
    /// Each entry: (table_name, create_new_table_ddl).
    /// The migration process for each table:
    ///   1. CREATE TABLE {table}_new ... (with partitioning + codecs)
    ///   2. INSERT INTO {table}_new SELECT * FROM {table}
    ///   3. RENAME TABLE {table} TO {table}_old, {table}_new TO {table}
    ///   4. DROP TABLE {table}_old (after verification)
    /// </summary>
    public static IReadOnlyList<(string TableName, string CreateNewTableDdl)> GetPartitionMigrations() =>
    [
        ("ticks", $"""
            CREATE TABLE IF NOT EXISTS {DatabaseName}.ticks_new (
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
            ORDER BY tick_number
            """),

        ("transactions", $"""
            CREATE TABLE IF NOT EXISTS {DatabaseName}.transactions_new (
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
            ORDER BY (tick_number, hash)
            """),

        ("logs", $"""
            CREATE TABLE IF NOT EXISTS {DatabaseName}.logs_new (
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
            ORDER BY (tick_number, log_id)
            """),

        ("balance_snapshots", $"""
            CREATE TABLE IF NOT EXISTS {DatabaseName}.balance_snapshots_new (
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
            ORDER BY (epoch, address)
            """),

        ("flow_hops", $"""
            CREATE TABLE IF NOT EXISTS {DatabaseName}.flow_hops_new (
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
            ORDER BY (emission_epoch, origin_address, hop_level, tick_number, tx_hash, dest_address)
            """),

        ("flow_tracking_state", $"""
            CREATE TABLE IF NOT EXISTS {DatabaseName}.flow_tracking_state_new (
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
            TTL toDateTime(updated_at) + INTERVAL 90 DAY WHERE is_complete = 1
            """),
    ];

    /// <summary>
    /// Generates the full migration SQL script for an existing database.
    /// This migrates tables to use PARTITION BY, codecs, and TTL.
    /// IMPORTANT: Run during a maintenance window - the INSERT SELECTs can take time on large tables.
    /// The materialized views that feed into transactions/logs must be recreated to target the new tables.
    /// </summary>
    public static string GenerateMigrationScript()
    {
        var sb = new System.Text.StringBuilder();
        sb.AppendLine("-- =============================================================");
        sb.AppendLine("-- Storage Optimization Migration Script");
        sb.AppendLine("-- Adds: PARTITION BY epoch, column codecs, TTL");
        sb.AppendLine("-- =============================================================");
        sb.AppendLine("-- IMPORTANT: Run during a maintenance window.");
        sb.AppendLine("-- Stop the indexer before running to avoid writes to old tables.");
        sb.AppendLine("-- The INSERT SELECTs may take time depending on data volume.");
        sb.AppendLine("-- =============================================================");
        sb.AppendLine();

        // Step 1: Drop materialized views that reference the tables being migrated
        sb.AppendLine("-- Step 1: Drop materialized views (they reference old tables)");
        var mvsToRecreate = new[]
        {
            "daily_tx_volume", "hourly_activity", "epoch_tx_stats",
            "epoch_sender_stats", "epoch_receiver_stats",
            "mv_address_first_seen_from", "mv_address_first_seen_to",
            "daily_log_stats", "epoch_transfer_stats", "epoch_transfer_by_type",
            "epoch_tick_stats", "epoch_tx_size_stats", "daily_tx_size_stats"
        };
        foreach (var mv in mvsToRecreate)
        {
            sb.AppendLine($"DROP VIEW IF EXISTS {DatabaseName}.{mv};");
        }
        sb.AppendLine();

        // Step 2: Create new tables and migrate data
        sb.AppendLine("-- Step 2: Create new tables with partitioning + codecs, migrate data");
        foreach (var (tableName, createDdl) in GetPartitionMigrations())
        {
            sb.AppendLine($"-- Migrate: {tableName}");
            sb.AppendLine($"{createDdl.TrimEnd()};");
            sb.AppendLine($"INSERT INTO {DatabaseName}.{tableName}_new SELECT * FROM {DatabaseName}.{tableName};");
            sb.AppendLine();
        }

        // Step 3: Atomic rename
        sb.AppendLine("-- Step 3: Atomic rename (old -> _old, new -> active)");
        foreach (var (tableName, _) in GetPartitionMigrations())
        {
            sb.AppendLine($"RENAME TABLE {DatabaseName}.{tableName} TO {DatabaseName}.{tableName}_old, {DatabaseName}.{tableName}_new TO {DatabaseName}.{tableName};");
        }
        sb.AppendLine();

        // Step 4: Re-add indexes on renamed tables
        sb.AppendLine("-- Step 4: Re-add secondary indexes");
        sb.AppendLine($"ALTER TABLE {DatabaseName}.transactions ADD INDEX IF NOT EXISTS idx_from from_address TYPE bloom_filter GRANULARITY 4;");
        sb.AppendLine($"ALTER TABLE {DatabaseName}.transactions ADD INDEX IF NOT EXISTS idx_to to_address TYPE bloom_filter GRANULARITY 4;");
        sb.AppendLine($"ALTER TABLE {DatabaseName}.transactions ADD INDEX IF NOT EXISTS idx_hash hash TYPE bloom_filter GRANULARITY 4;");
        sb.AppendLine($"ALTER TABLE {DatabaseName}.logs ADD INDEX IF NOT EXISTS idx_source source_address TYPE bloom_filter GRANULARITY 4;");
        sb.AppendLine($"ALTER TABLE {DatabaseName}.logs ADD INDEX IF NOT EXISTS idx_dest dest_address TYPE bloom_filter GRANULARITY 4;");
        sb.AppendLine($"ALTER TABLE {DatabaseName}.logs ADD INDEX IF NOT EXISTS idx_type log_type TYPE set(20) GRANULARITY 4;");
        sb.AppendLine($"ALTER TABLE {DatabaseName}.logs ADD INDEX IF NOT EXISTS idx_tx_hash tx_hash TYPE bloom_filter GRANULARITY 4;");
        sb.AppendLine($"ALTER TABLE {DatabaseName}.balance_snapshots ADD INDEX IF NOT EXISTS idx_address address TYPE bloom_filter GRANULARITY 4;");
        sb.AppendLine($"ALTER TABLE {DatabaseName}.flow_hops ADD INDEX IF NOT EXISTS idx_flow_emission_epoch emission_epoch TYPE minmax GRANULARITY 4;");
        sb.AppendLine($"ALTER TABLE {DatabaseName}.flow_hops ADD INDEX IF NOT EXISTS idx_flow_origin origin_address TYPE bloom_filter GRANULARITY 4;");
        sb.AppendLine($"ALTER TABLE {DatabaseName}.flow_hops ADD INDEX IF NOT EXISTS idx_flow_dest dest_address TYPE bloom_filter GRANULARITY 4;");
        sb.AppendLine($"ALTER TABLE {DatabaseName}.flow_hops ADD INDEX IF NOT EXISTS idx_flow_origin_type origin_type TYPE set(10) GRANULARITY 4;");
        sb.AppendLine($"ALTER TABLE {DatabaseName}.flow_hops ADD INDEX IF NOT EXISTS idx_flow_dest_type dest_type TYPE set(10) GRANULARITY 4;");
        sb.AppendLine($"ALTER TABLE {DatabaseName}.flow_tracking_state ADD INDEX IF NOT EXISTS idx_fts_pending (emission_epoch, is_complete) TYPE set(2) GRANULARITY 4;");
        sb.AppendLine();

        // Step 5: Recreate materialized views (from GetSchemaStatements, they use CREATE ... IF NOT EXISTS)
        sb.AppendLine("-- Step 5: Recreate materialized views");
        sb.AppendLine("-- Run the application's schema initialization (GetSchemaStatements) to recreate MVs.");
        sb.AppendLine("-- Or restart the application - it will recreate them on startup.");
        sb.AppendLine();

        // Step 6: Verify and drop old tables
        sb.AppendLine("-- Step 6: After verifying everything works, drop old tables");
        foreach (var (tableName, _) in GetPartitionMigrations())
        {
            sb.AppendLine($"-- DROP TABLE IF EXISTS {DatabaseName}.{tableName}_old;");
        }
        sb.AppendLine();

        // Also add TTL to existing flow_tracking_state (if already migrated via rename)
        sb.AppendLine("-- TTL is applied via the new table definition above.");
        sb.AppendLine("-- For existing tables not migrated, apply TTL with:");
        sb.AppendLine($"-- ALTER TABLE {DatabaseName}.flow_tracking_state MODIFY TTL toDateTime(updated_at) + INTERVAL 90 DAY WHERE is_complete = 1;");

        return sb.ToString();
    }
}
