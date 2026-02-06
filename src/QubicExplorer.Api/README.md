# QubicExplorer.Api

ASP.NET Core Web API that provides REST endpoints for querying blockchain data, a SignalR hub for real-time updates, and proxied Bob RPC queries via the [Qubic.Bob](https://www.nuget.org/packages/Qubic.Bob) library.

## Features

- **REST API**: Endpoints for ticks, transactions, transfers, addresses, epochs, and analytics
- **Pagination**: All list endpoints support pagination
- **Search**: Global search by tick number, transaction hash, or address
- **Statistics**: Network stats, epoch stats, holder distribution, miner flow analysis
- **Real-time Updates**: SignalR hub for live tick and transaction notifications
- **Bob RPC Proxy**: Proxied and cached queries to Bob nodes via `BobWebSocketClient`
- **Address Labels**: Fetches and caches known address labels from Qubic
- **Swagger**: API documentation at `/swagger` (development mode)
- **Self-Initializing**: Creates ClickHouse database and schema on startup

## API Endpoints

### Ticks

| Method | Endpoint                                | Description                                                                       |
|--------|-----------------------------------------|-----------------------------------------------------------------------------------|
| GET    | `/api/ticks`                            | List ticks (paginated)                                                            |
| GET    | `/api/ticks/{tickNumber}`               | Get tick details with transactions                                                |
| GET    | `/api/ticks/{tickNumber}/transactions`  | Get transactions for a tick (filterable by address, direction, minAmount, executed)|
| GET    | `/api/ticks/{tickNumber}/logs`          | Get logs for a tick (filterable by address, type, direction, minAmount)            |

### Transactions

| Method | Endpoint                     | Description                                                                        |
|--------|------------------------------|------------------------------------------------------------------------------------|
| GET    | `/api/transactions`          | List transactions (paginated, filterable by address, direction, minAmount, executed)|
| GET    | `/api/transactions/{hash}`   | Get transaction details with logs                                                  |

### Transfers

| Method | Endpoint           | Description                                                                                                                                          |
|--------|--------------------|------------------------------------------------------------------------------------------------------------------------------------------------------|
| GET    | `/api/transfers`   | List transfers — paginated, filterable by address, type (0=QU_TRANSFER, 1=ASSET_ISSUANCE, 8=BURNING, etc.), types (comma-separated), direction, minAmount |

### Address

| Method | Endpoint                               | Description                                                            |
|--------|----------------------------------------|------------------------------------------------------------------------|
| GET    | `/api/address/{address}`               | Get address summary (includes live balance from Bob node)              |
| GET    | `/api/address/{address}/transactions`  | Get address transactions (filterable by direction, minAmount, executed) |
| GET    | `/api/address/{address}/transfers`     | Get address transfers (filterable by type, direction, minAmount)       |
| GET    | `/api/address/{address}/rewards`       | Get SC reward distribution history for a contract address (paginated)  |
| GET    | `/api/address/{address}/flow`          | Get address flow data (top senders/receivers)                          |

### Epochs

| Method | Endpoint                              | Description                                       |
|--------|---------------------------------------|---------------------------------------------------|
| GET    | `/api/epoch`                          | List epochs with stats                            |
| GET    | `/api/epoch/{epoch}`                  | Get epoch statistics                              |
| GET    | `/api/epoch/{epoch}/transfers-by-type`| Get transfer breakdown by type for an epoch       |
| GET    | `/api/epoch/{epoch}/rewards`          | Get SC reward distributions for an epoch          |
| GET    | `/api/epoch/{epoch}/meta`             | Get epoch metadata (tick boundaries)              |
| GET    | `/api/epoch/meta`                     | Get all epoch metadata                            |
| GET    | `/api/epoch/meta/current`             | Get current epoch metadata                        |
| POST   | `/api/epoch/{epoch}/meta`             | Upsert epoch metadata (admin)                     |
| POST   | `/api/epoch/{epoch}/fetch-end-logs`   | Fetch and insert end-epoch logs from Bob (admin)  |

### Search

| Method | Endpoint                | Description                      | Example                     |
|--------|-------------------------|----------------------------------|-----------------------------|
| GET    | `/api/search?q={query}` | Search by tick, hash, or address | `/api/search?q=42087259`    |

### Stats

| Method | Endpoint                                                         | Description                                          | Example                                                |
|--------|------------------------------------------------------------------|------------------------------------------------------|--------------------------------------------------------|
| GET    | `/api/stats`                                                     | Get network statistics                               |                                                        |
| GET    | `/api/stats/chart/tx-volume?period={period}`                     | Get TX volume chart (period: day, week, month)       | `/api/stats/chart/tx-volume?period=day`                |
| GET    | `/api/stats/top-addresses?limit={limit}&epoch={epoch}`           | Get top addresses by volume                          | `/api/stats/top-addresses?limit=20&epoch=150`          |
| GET    | `/api/stats/smart-contract-usage?epoch={epoch}`                  | Get smart contract usage stats                       | `/api/stats/smart-contract-usage?epoch=150`            |
| GET    | `/api/stats/active-addresses?period={period}&limit={limit}`      | Get active address trends (period: epoch, daily)     | `/api/stats/active-addresses?period=daily&limit=50`    |
| GET    | `/api/stats/new-vs-returning?limit={limit}`                      | Get new vs returning address trends                  | `/api/stats/new-vs-returning?limit=50`                 |
| GET    | `/api/stats/exchange-flows?limit={limit}`                        | Get exchange flow trends                             | `/api/stats/exchange-flows?limit=50`                   |
| GET    | `/api/stats/holder-distribution`                                 | Get holder distribution with concentration           |                                                        |
| GET    | `/api/stats/holder-distribution/extended?historyLimit={limit}`   | Get extended holder distribution with history        | `/api/stats/holder-distribution/extended?historyLimit=30` |
| GET    | `/api/stats/holder-distribution/history?limit={limit}`           | Get holder distribution history                      | `/api/stats/holder-distribution/history?limit=30`      |
| GET    | `/api/stats/avg-tx-size?period={period}&limit={limit}`           | Get average TX size trends (period: epoch, daily)    | `/api/stats/avg-tx-size?period=epoch&limit=50`         |
| GET    | `/api/stats/network-stats/history?limit={limit}`                 | Get network stats history                            | `/api/stats/network-stats/history?limit=30`            |
| GET    | `/api/stats/network-stats/extended?historyLimit={limit}`         | Get extended network stats with history              | `/api/stats/network-stats/extended?historyLimit=30`    |
| POST   | `/api/stats/network-stats/snapshot/{epoch}`                      | Save network stats snapshot (admin)                  | `/api/stats/network-stats/snapshot/150`                |

### Labels

| Method | Endpoint                                            | Description                                       |
|--------|-----------------------------------------------------|---------------------------------------------------|
| GET    | `/api/labels`                                       | Get all known addresses (filterable by type)       |
| GET    | `/api/labels/{address}`                             | Get label for an address                           |
| POST   | `/api/labels/batch`                                 | Batch lookup labels for up to 100 addresses        |
| GET    | `/api/labels/stats`                                 | Get label statistics (counts by type)              |
| POST   | `/api/labels/refresh`                               | Refresh labels from remote bundle                  |
| GET    | `/api/labels/procedure/{contractAddress}/{inputType}`| Get procedure name for a contract input type      |

### Miner Flow

| Method | Endpoint                                                            | Description                                                       | Example                                              |
|--------|---------------------------------------------------------------------|-------------------------------------------------------------------|------------------------------------------------------|
| GET    | `/api/miner-flow/stats?limit={limit}`                               | Get miner flow statistics history                                 | `/api/miner-flow/stats?limit=30`                     |
| GET    | `/api/miner-flow/computors/{epoch}`                                 | Get computor list for an epoch                                    | `/api/miner-flow/computors/150`                      |
| GET    | `/api/miner-flow/visualization/{emissionEpoch}?maxDepth={maxDepth}` | Get Sankey flow visualization data for an emission epoch          | `/api/miner-flow/visualization/150?maxDepth=10`      |
| GET    | `/api/miner-flow/hops/{epoch}`                                      | Get raw flow hops (filterable by tickStart, tickEnd, maxDepth, limit) | `/api/miner-flow/hops/150`                       |
| GET    | `/api/miner-flow/validate/{emissionEpoch}`                          | Validate flow conservation for an emission epoch                  | `/api/miner-flow/validate/150`                       |
| GET    | `/api/miner-flow/emissions/{epoch}`                                 | Get emission summary for an epoch                                 | `/api/miner-flow/emissions/150`                      |
| GET    | `/api/miner-flow/emissions/{epoch}/details`                         | Get detailed emissions for all computors in an epoch              | `/api/miner-flow/emissions/150/details`              |
| GET    | `/api/miner-flow/emissions/{epoch}/address/{address}`               | Get emission for a specific computor address                      | `/api/miner-flow/emissions/150/address/ABCD...`      |
| POST   | `/api/miner-flow/analyze/{currentEpoch}`                            | Trigger flow analysis for a tick window (admin)                   | `/api/miner-flow/analyze/150`                        |
| POST   | `/api/miner-flow/analyze-emission/{emissionEpoch}`                  | Trigger full emission flow analysis (admin)                       | `/api/miner-flow/analyze-emission/150`               |
| POST   | `/api/miner-flow/import-computors/{epoch}`                          | Import computors from RPC (admin)                                 | `/api/miner-flow/import-computors/150`               |
| POST   | `/api/miner-flow/emissions/{epoch}/capture`                         | Capture emissions for an epoch (admin)                            | `/api/miner-flow/emissions/150/capture`              |
| POST   | `/api/miner-flow/recalculate-emissions`                             | Recalculate all miner flow stats emissions (admin)                |                                                      |

### Spectrum

| Method | Endpoint                          | Description                                          |
|--------|-----------------------------------|------------------------------------------------------|
| GET    | `/api/spectrum/status`            | Get spectrum import status                           |
| GET    | `/api/spectrum/{epoch}/status`    | Check if a specific epoch has been imported          |
| POST   | `/api/spectrum/{epoch}/import`    | Import spectrum file for an epoch (admin)            |
| POST   | `/api/spectrum/import-latest`     | Import latest available spectrum (admin)             |

### Health

| Method | Endpoint  | Description        |
|--------|-----------|--------------------|
| GET    | `/health` | Health check endpoint |

## SignalR Hub

**URL**: `/hubs/live`

### Client Methods

```javascript
// Subscribe to new ticks
connection.invoke("SubscribeToTicks");
connection.invoke("UnsubscribeFromTicks");

// Subscribe to address updates
connection.invoke("SubscribeToAddress", "ABCD...");
connection.invoke("UnsubscribeFromAddress", "ABCD...");
```

### Server Events

```javascript
connection.on("OnNewTick", (tickData) => { ... });
connection.on("OnNewTransaction", (txData) => { ... });
connection.on("OnAddressUpdate", (data) => { ... });
```

## Configuration

### appsettings.json

```json
{
  "ClickHouse": {
    "Host": "localhost",
    "Port": 8123,
    "Database": "qubic"
  },
  "Bob": {
    "Nodes": ["http://localhost:21841"]
  },
  "AddressLabels": {
    "BundleUrl": "https://static.qubic.org/v1/general/data/bundle.min.json"
  }
}
```

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `ClickHouse__Host` | `localhost` | ClickHouse host |
| `ClickHouse__Port` | `8123` | ClickHouse HTTP port |
| `ClickHouse__Database` | `qubic` | ClickHouse database name |
| `Bob__Nodes__0` | `http://localhost:21841` | Bob node URL (supports multiple: `__0`, `__1`, etc.) |
| `AddressLabels__BundleUrl` | `https://static.qubic.org/...` | Address label bundle URL |

### Docker Compose

```yaml
environment:
  - ClickHouse__Host=clickhouse
  - ClickHouse__Port=8123
  - ClickHouse__Database=qubic
  - Bob__Nodes__0=https://bob02.qubic.li
```

## Running

```bash
# Development (with hot reload)
dotnet watch run

# Production
dotnet publish -c Release
./bin/Release/net8.0/QubicExplorer.Api
```

## Docker

```bash
docker build -t qubic-api -f QubicExplorer.Api/Dockerfile .
docker run -p 5000:8080 \
  -e ClickHouse__Host=clickhouse \
  -e Bob__Nodes__0=https://bob02.qubic.li \
  qubic-api
```

The API will be available at `http://localhost:5000`.
