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

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/ticks` | List ticks (paginated) |
| GET | `/api/ticks/{tickNumber}` | Get tick details with transactions |
| GET | `/api/ticks/{tickNumber}/transactions` | Get transactions for a tick |

### Transactions

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/transactions` | List transactions (paginated) |
| GET | `/api/transactions?address={addr}` | Filter by address |
| GET | `/api/transactions/{hash}` | Get transaction details with logs |

### Transfers

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/transfers` | List transfers (paginated) |
| GET | `/api/transfers?address={addr}` | Filter by address |
| GET | `/api/transfers?type={logType}` | Filter by log type |
| GET | `/api/transfers?direction=in\|out` | Filter by direction |

### Address

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/address/{address}` | Get address summary |
| GET | `/api/address/{address}/transactions` | Get address transactions |
| GET | `/api/address/{address}/transfers` | Get address transfers |

### Search

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/search?q={query}` | Search by tick, hash, or address |

### Stats

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/stats` | Get network statistics |
| GET | `/api/stats/chart/tx-volume?period=day\|week\|month` | Get volume chart |

### Health

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Health check endpoint |

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
