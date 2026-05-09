-- =============================================================
-- 00 — Drop materialized views
-- Run this FIRST. MVs will be recreated by the indexer on next startup.
-- =============================================================
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
