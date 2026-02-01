# Docker Configuration

This directory contains Docker-related configuration files for the Qubic Demo Explorer stack.

## Structure

```
docker/
├── clickhouse/
│   └── init.sql      # ClickHouse database schema (fallback)
└── nginx/
    └── nginx.conf    # Reverse proxy configuration
```

## ClickHouse

### init.sql

The ClickHouse schema definition file. This serves as a reference and fallback — the application itself creates the database and schema on startup via `ClickHouseSchema.cs` in the Shared project.

**Tables:**
- `ticks` — Blockchain ticks (blocks)
- `transactions` — All transactions
- `logs` — All logs (transfers, asset operations, etc.)
- `assets` — Tracked assets
- `indexer_state` — Indexer resume state
- `balance_snapshots` — Address balance snapshots from Spectrum files
- `epoch_stats` — Combined epoch statistics
- `epoch_meta` — Epoch boundary metadata
- `address_first_seen` — First appearance tracking per address
- `holder_distribution_history` — Historical holder distribution snapshots
- `network_stats_history` — Historical network statistics
- `computors` — Computor lists per epoch
- `computor_emissions` — Emission amounts per computor
- `flow_hops` — Individual transfer hops for flow analysis
- `miner_flow_stats` — Aggregated miner flow statistics
- `flow_tracking_addresses` — Addresses tracked for flow analysis
- `flow_tracking_state` — Intermediate flow tracking state

**Indexes:**
- Bloom filter indexes on addresses for fast lookups
- Set index on log types
- MinMax indexes on epoch fields

**Materialized Views:**
- `daily_tx_volume` — Pre-aggregated daily transaction stats
- `daily_log_stats` — Pre-aggregated daily log counts by type
- `hourly_activity` — Hourly network activity metrics
- `epoch_tx_stats` / `epoch_transfer_stats` / `epoch_tick_stats` — Epoch-level aggregations
- `epoch_sender_stats` / `epoch_receiver_stats` — Unique address tracking per epoch
- `epoch_tx_size_stats` / `daily_tx_size_stats` — Transaction size analysis
- `mv_address_first_seen_from` / `mv_address_first_seen_to` — Auto-populate address first seen

### Engine Types

- **ReplacingMergeTree**: Used for main tables, provides upsert/deduplication behavior
- **SummingMergeTree**: Used for materialized views with simple aggregations (count, sum)
- **AggregatingMergeTree**: Used for materialized views with state-based aggregations (uniq, quantile)

## Nginx

### nginx.conf

Reverse proxy configuration that routes:

| Path | Target | Description |
|------|--------|-------------|
| `/api/*` | `api:8080` | REST API endpoints |
| `/hubs/*` | `api:8080` | SignalR hub (WebSocket support) |
| `/*` | `frontend:3000` | Frontend application |
| `/health` | _(inline)_ | Health check (returns 200 OK) |

Features:
- WebSocket upgrade support for SignalR
- Gzip compression
- Proper header forwarding (X-Real-IP, X-Forwarded-For, X-Forwarded-Proto)
- Long read timeout (86400s) for WebSocket connections

## Docker Compose

These files are used by `docker-compose.yml`:

```yaml
services:
  clickhouse:
    volumes:
      - ./docker/clickhouse/init.sql:/docker-entrypoint-initdb.d/init.sql:ro

  nginx:
    volumes:
      - ./docker/nginx/nginx.conf:/etc/nginx/nginx.conf:ro
```

### Port Mapping

| Service | Internal Port | External Port | Description |
|---------|--------------|---------------|-------------|
| ClickHouse | 8123 | 8123 | HTTP interface |
| ClickHouse | 9000 | 9000 | Native protocol |
| API | 8080 | 5000 | REST API + SignalR |
| Frontend | 3000 | 3000 | Nuxt SSR |
| Nginx | 80 | 80 | Reverse proxy (production entry point) |
