# QubicExplorer.Indexer

Background worker service that connects to a Bob node via [Qubic.Bob](https://www.nuget.org/packages/Qubic.Bob)'s `BobWebSocketClient`, subscribes to the tick stream, and persists blockchain data to ClickHouse.

## Features

- **WebSocket Connection**: Connects to Bob nodes via `BobWebSocketClient` with multi-node failover
- **Tick Stream Subscription**: Subscribes to `tickStream` for real-time blockchain data
- **Batch Processing**: Efficiently batches inserts for high throughput during catch-up
- **Resume Capability**: Automatically resumes from the last indexed tick on restart
- **Self-Initializing**: Creates ClickHouse database and schema on startup
- **Auto-Reconnect**: Handles connection drops with configurable exponential backoff

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   Bob Node      │────>│ BobConnection   │────>│ ClickHouseWriter│
│  (Qubic.Bob)   │     │    Service      │     │    Service      │
└─────────────────┘     └─────────────────┘     └─────────────────┘
                               │
                               v
                        ┌─────────────────┐
                        │  IndexerWorker  │
                        │ (BackgroundSvc) │
                        └─────────────────┘
```

## Services

### BobConnectionService

Manages the WebSocket connection to the Bob node using `BobWebSocketClient`:
- Establishes and maintains connection with multi-node failover
- Sends subscription requests for tick stream data
- Receives and parses tick stream messages
- Publishes tick data to an internal channel

### ClickHouseWriterService

Handles data persistence to ClickHouse:
- Creates database and schema on initialization
- Batch inserts for ticks, transactions, and logs
- Configurable batch size and flush interval
- Tracks last indexed tick for resume capability

### IndexerWorker

The main background service that orchestrates:
- Determines starting tick (config, resume, or latest)
- Coordinates connection and writer services
- Handles graceful shutdown with final flush

## Configuration

### appsettings.json

```json
{
  "Bob": {
    "Nodes": ["http://localhost:21841"],
    "ReconnectDelayMs": 5000,
    "MaxReconnectDelayMs": 60000
  },
  "ClickHouse": {
    "Host": "localhost",
    "Port": 8123,
    "Database": "qubic"
  },
  "Indexer": {
    "StartTick": 0,
    "StartFromLatest": false,
    "ResumeFromLastTick": true,
    "BatchSize": 1000,
    "FlushIntervalMs": 1000,
    "IncludeInputData": true,
    "SkipEmptyTicks": false,
    "TxFilters": [],
    "LogFilters": []
  }
}
```

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `Bob__Nodes__0` | `http://localhost:21841` | Bob node URL (supports multiple: `__0`, `__1`, etc.) |
| `Bob__ReconnectDelayMs` | `5000` | Initial reconnect delay (ms) |
| `Bob__MaxReconnectDelayMs` | `60000` | Max reconnect delay (ms) |
| `ClickHouse__Host` | `localhost` | ClickHouse host |
| `ClickHouse__Port` | `8123` | ClickHouse HTTP port |
| `ClickHouse__Database` | `qubic` | ClickHouse database name |
| `Indexer__StartTick` | `0` | Starting tick number |
| `Indexer__StartFromLatest` | `false` | Start from latest tick |
| `Indexer__ResumeFromLastTick` | `true` | Resume from last indexed |
| `Indexer__BatchSize` | `1000` | Batch size for inserts |
| `Indexer__FlushIntervalMs` | `1000` | Flush interval (ms) |
| `Indexer__IncludeInputData` | `true` | Include transaction input data |
| `Indexer__SkipEmptyTicks` | `false` | Skip ticks with no transactions |

### Docker Compose

```yaml
environment:
  - Bob__Nodes__0=https://bob02.qubic.li
  - ClickHouse__Host=clickhouse
  - ClickHouse__Port=8123
  - ClickHouse__Database=qubic
  - Indexer__StartTick=0
  - Indexer__StartFromLatest=false
  - Indexer__BatchSize=1000
  - Indexer__FlushIntervalMs=1000
```

## Running

```bash
# Development
dotnet run

# Production
dotnet publish -c Release
./bin/Release/net8.0/QubicExplorer.Indexer
```

## Docker

```bash
docker build -t qubic-indexer -f QubicExplorer.Indexer/Dockerfile .
docker run \
  -e Bob__Nodes__0=https://bob02.qubic.li \
  -e ClickHouse__Host=clickhouse \
  qubic-indexer
```
