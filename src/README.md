# Qubic Demo Explorer — Backend Services

This directory contains the .NET 8 backend services for the Qubic Demo Explorer.

Both services use the [Qubic.Bob](https://www.nuget.org/packages/Qubic.Bob) NuGet package for communication with Bob nodes via WebSocket (subscriptions and RPC queries).

## Projects

### QubicExplorer.Shared

Shared library containing models, DTOs, constants, and the ClickHouse schema definition used by both the Indexer and API.

### QubicExplorer.Indexer

Background worker service that connects to a Bob node via `BobWebSocketClient`, subscribes to the tick stream, and persists data to ClickHouse. Automatically creates the database and schema on startup.

### QubicExplorer.Api

ASP.NET Core Web API that provides REST endpoints for querying blockchain data, a SignalR hub for real-time updates, and proxied Bob RPC queries. Automatically creates the database and schema on startup.

## Building

```bash
# Build all projects
dotnet build

# Build specific project
dotnet build QubicExplorer.Api/QubicExplorer.Api.csproj
```

## Running

```bash
# Run the Indexer
dotnet run --project QubicExplorer.Indexer

# Run the API
dotnet run --project QubicExplorer.Api
```

## Solution Structure

```
src/
├── QubicExplorer.Shared/          # Shared library (models, DTOs, schema)
├── QubicExplorer.Indexer/         # Indexer service
└── QubicExplorer.Api/             # API service
```

## NuGet Dependencies

| Package | Used By | Purpose |
|---------|---------|---------|
| [Qubic.Bob](https://www.nuget.org/packages/Qubic.Bob) | Indexer, API | Bob WebSocket client with multi-node failover |
| [Qubic.Crypto](https://www.nuget.org/packages/Qubic.Crypto) | API | Identity verification and address validation |
| [ClickHouse.Client](https://www.nuget.org/packages/ClickHouse.Client) | Indexer, API | ClickHouse ADO.NET driver |
