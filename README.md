# Qubic Demo Explorer

> **Demo Project** — This is a demonstration project showcasing the [Qubic.Net](https://www.nuget.org/packages?q=Qubic) framework and [Qubic Core Bob](https://github.com/qubic/bob) JSON-RPC interface. It is not meant for production use. Use it as a reference for building your own Qubic applications with the Qubic.Net NuGet packages.

A blockchain explorer for Qubic that indexes data from a Bob node via the [Qubic.Bob](https://www.nuget.org/packages/Qubic.Bob) library, stores it in ClickHouse for efficient analytics, and provides a web interface for exploring ticks, transactions, transfers, and addresses.

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   Bob Node      │────>│   Indexer        │────>│   ClickHouse    │<────│   API Server    │
│   (Qubic.Bob)   │     │   (.NET 8)      │     │                 │     │   (.NET 8)      │
└─────────────────┘     └─────────────────┘     └─────────────────┘     └─────────────────┘
                                                                               │
                                                                               v
                                                                        ┌─────────────────┐
                                                                        │   Frontend      │
                                                                        │   (Nuxt 3)      │
                                                                        └─────────────────┘
```

## Features

- **Tick Browser**: View all ticks with transaction and log counts
- **Transaction Explorer**: Search and view transaction details with associated logs
- **Transfer Tracking**: Browse all transfers (QU and assets) with filtering
- **Address View**: See address balance, incoming/outgoing transactions and transfers
- **Global Search**: Search by tick number, transaction hash, or address
- **Real-time Updates**: Live updates via SignalR when new ticks arrive
- **Analytics**: Network statistics, epoch stats, holder distribution, miner flow tracking
- **Configurable Indexing**: Start from any tick or resume from last indexed
- **Self-Initializing**: Database schema is created automatically on first startup

## Project Structure

```
qubic-explorer/
├── src/
│   ├── QubicExplorer.Indexer/    # Indexer service (subscribes to Bob node via Qubic.Bob)
│   ├── QubicExplorer.Api/        # REST API + SignalR hub
│   └── QubicExplorer.Shared/     # Shared models, DTOs, and schema definitions
├── frontend/                      # Nuxt 3 web application
├── docker/
│   ├── clickhouse/               # ClickHouse schema (fallback)
│   └── nginx/                    # Reverse proxy config
├── docker-compose.yml            # Full stack deployment
├── docker-compose.override.yml   # Development overrides
└── README.md
```

## Quick Start

### Using Docker Compose

```bash
# Start all services
docker compose up -d

# View logs
docker compose logs -f

# Stop all services
docker compose down

# Full reset (removes database volume)
docker compose down -v
```

The explorer will be available at:
- **Frontend**: http://localhost (via nginx reverse proxy)
- **API**: http://localhost:5000 (direct)
- **API Docs (Swagger)**: http://localhost:5000/swagger (development only)
- **ClickHouse**: http://localhost:8123 (development only)

### Development Setup

#### Prerequisites
- .NET 8 SDK
- Node.js 20+
- Docker (for ClickHouse)

#### 1. Start ClickHouse

```bash
docker compose up -d clickhouse
```

#### 2. Run the Indexer

```bash
cd src/QubicExplorer.Indexer
dotnet run
```

#### 3. Run the API

```bash
cd src/QubicExplorer.Api
dotnet run
```

#### 4. Run the Frontend

```bash
cd frontend
npm install
npm run dev
```

## Configuration

### Indexer

Edit `src/QubicExplorer.Indexer/appsettings.json` or use environment variables:

| Setting | Environment Variable | Default | Description |
|---------|---------------------|---------|-------------|
| Bob:Nodes:0 | `Bob__Nodes__0` | `http://localhost:21841` | Bob node URL |
| Bob:ReconnectDelayMs | `Bob__ReconnectDelayMs` | `5000` | Reconnect delay (ms) |
| Bob:MaxReconnectDelayMs | `Bob__MaxReconnectDelayMs` | `60000` | Max reconnect delay (ms) |
| ClickHouse:Host | `ClickHouse__Host` | `localhost` | ClickHouse host |
| ClickHouse:Port | `ClickHouse__Port` | `8123` | ClickHouse HTTP port |
| ClickHouse:Database | `ClickHouse__Database` | `qubic` | ClickHouse database name |
| Indexer:StartTick | `Indexer__StartTick` | `0` | Starting tick number |
| Indexer:StartFromLatest | `Indexer__StartFromLatest` | `false` | Start from latest tick |
| Indexer:ResumeFromLastTick | `Indexer__ResumeFromLastTick` | `true` | Resume from last indexed |
| Indexer:BatchSize | `Indexer__BatchSize` | `1000` | Batch size for inserts |
| Indexer:FlushIntervalMs | `Indexer__FlushIntervalMs` | `1000` | Flush interval (ms) |

### API

Edit `src/QubicExplorer.Api/appsettings.json` or use environment variables:

| Setting | Environment Variable | Default | Description |
|---------|---------------------|---------|-------------|
| Bob:Nodes:0 | `Bob__Nodes__0` | `http://localhost:21841` | Bob node URL |
| ClickHouse:Host | `ClickHouse__Host` | `localhost` | ClickHouse host |
| ClickHouse:Port | `ClickHouse__Port` | `8123` | ClickHouse HTTP port |
| ClickHouse:Database | `ClickHouse__Database` | `qubic` | ClickHouse database name |
| AddressLabels:BundleUrl | `AddressLabels__BundleUrl` | `https://static.qubic.org/...` | Address label bundle URL |

### Frontend

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `NUXT_API_URL` | `http://api:8080` | Server-side API URL (SSR, internal) |
| `NUXT_PUBLIC_API_URL` | _(empty)_ | Client-side API URL (empty = relative, via nginx) |

## API Endpoints

### Ticks
- `GET /api/ticks` — List ticks (paginated)
- `GET /api/ticks/{tickNumber}` — Get tick details with transactions

### Transactions
- `GET /api/transactions` — List transactions (paginated, filterable by address)
- `GET /api/transactions/{hash}` — Get transaction details with logs

### Transfers
- `GET /api/transfers` — List transfers (paginated, filterable by address/type/direction)

### Address
- `GET /api/address/{address}` — Get address summary
- `GET /api/address/{address}/transactions` — Get address transactions
- `GET /api/address/{address}/transfers` — Get address transfers

### Search
- `GET /api/search?q={query}` — Search by tick, hash, or address

### Stats
- `GET /api/stats` — Get network statistics
- `GET /api/stats/chart/tx-volume?period={day|week|month}` — Get volume chart data

### Real-time (SignalR)
- Hub URL: `/hubs/live`
- Methods: `SubscribeToTicks()`, `SubscribeToAddress(address)`
- Events: `OnNewTick`, `OnNewTransaction`, `OnAddressUpdate`

## Qubic.Net Packages Used

This project uses the following [Qubic.Net](https://www.nuget.org/packages?q=Qubic) NuGet packages:

| Package | Description |
|---------|-------------|
| [Qubic.Bob](https://www.nuget.org/packages/Qubic.Bob) | Bob JSON-RPC client with WebSocket subscriptions and multi-node failover |
| [Qubic.Crypto](https://www.nuget.org/packages/Qubic.Crypto) | K12 hashing, FourQ, SchnorrQ signatures, identity verification |

## Database Schema

The ClickHouse schema is automatically created on application startup. It includes:
- **ReplacingMergeTree** tables for ticks, transactions, logs, and deduplication
- **Bloom filter indexes** for fast address lookups
- **Materialized views** for pre-aggregated statistics (daily volumes, epoch stats, etc.)
- **Analytics tables** for holder distribution, miner flow tracking, and network stats history

See [docker/clickhouse/init.sql](docker/clickhouse/init.sql) for the full schema reference.

## Technology Stack

- **Backend**: .NET 8, ASP.NET Core, SignalR
- **Bob Client**: [Qubic.Bob](https://www.nuget.org/packages/Qubic.Bob) (WebSocket + HTTP RPC)
- **Database**: ClickHouse
- **Frontend**: Vue 3, Nuxt 3, TypeScript, Tailwind CSS
- **Deployment**: Docker, Docker Compose, Nginx

## Why This Stack?

### .NET 8 + Qubic.Bob

The [Qubic.Bob](https://www.nuget.org/packages/Qubic.Bob) NuGet package provides a typed WebSocket client for Bob's JSON-RPC interface — managed subscriptions (tick stream, new ticks), RPC query methods, multi-node failover, and automatic reconnection. The explorer uses `BobWebSocketClient` for all communication with the Qubic network, keeping the application code focused on indexing and serving data rather than connection management.

ASP.NET Core is a natural fit for the API layer: built-in dependency injection, controller patterns, and SignalR for real-time WebSocket push to the frontend — all with high throughput and low allocation overhead.

### ClickHouse

A blockchain explorer is fundamentally an analytics workload: append-heavy writes (new ticks, transactions, logs arriving in order), heavy aggregation reads (volume charts, epoch statistics, address summaries), and sparse point lookups (find a transaction by hash). ClickHouse is purpose-built for this pattern:

- **Columnar storage** compresses blockchain data extremely well (addresses, amounts, and timestamps stored in separate columns) and makes aggregation queries over millions of rows fast.
- **MergeTree engine family** handles the append-only write pattern natively. `ReplacingMergeTree` gives upsert semantics for deduplication when the indexer re-processes a tick range. `SummingMergeTree` and `AggregatingMergeTree` power materialized views that pre-aggregate statistics at insert time — no batch jobs needed.
- **Bloom filter indexes** on address columns give fast point lookups without the overhead of a B-tree index on high-cardinality string columns.

Compared to PostgreSQL, ClickHouse handles the "scan millions of rows for charts and stats" queries orders of magnitude faster while still serving point lookups. The trade-off (no transactions, eventual consistency on merges) is acceptable for a read-heavy explorer.

### Nuxt 3 + Vue 3

Nuxt provides server-side rendering out of the box, which matters for an explorer: when a user shares a link to a transaction or address, the page should render with data on first load (SEO, social previews, perceived performance). The Composition API keeps component logic clean as complexity grows (70+ API methods, real-time subscriptions, contract input decoding). Tailwind CSS avoids the overhead of a component library while keeping the UI consistent.

### Docker Compose + Nginx

The full stack (ClickHouse, Indexer, API, Frontend, Nginx) runs with a single `docker compose up`. Nginx acts as the single entry point — routing `/api/*` to the backend and everything else to the frontend, handling WebSocket upgrades for SignalR, and providing gzip compression. This keeps the deployment topology simple and the frontend free from CORS complexity.

