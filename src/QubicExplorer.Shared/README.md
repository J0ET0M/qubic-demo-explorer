# QubicExplorer.Shared

Shared library containing models, DTOs, constants, and the ClickHouse schema definition used by both the Indexer and API services.

## Contents

### Models

Domain models representing blockchain entities:

- `Tick` — Represents a Qubic tick (block)
- `Transaction` — Represents a transaction
- `Log` — Represents a log entry (transfer, asset operation, etc.)
- `Asset` — Represents an issued asset

### DTOs

Data Transfer Objects for API responses:

- `TickDto` / `TickDetailDto` — Tick data for API responses
- `TransactionDto` / `TransactionDetailDto` — Transaction data
- `LogDto` / `TransferDto` — Log and transfer data
- `AddressDto` / `AddressBalanceDto` — Address information
- `NetworkStatsDto` / `ChartDataPointDto` — Statistics and chart data
- `PaginatedResponse<T>` — Generic paginated response wrapper
- `SearchResultDto` / `SearchResponse` — Search results

### Constants

- `LogTypes` — Qubic log type definitions (QU_TRANSFER, ASSET_ISSUANCE, etc.)

### ClickHouseSchema

Static class containing all ClickHouse DDL statements for the `qubic` database. Used by both the Indexer and API to create the database and all tables/views/indexes on startup.

```csharp
using QubicExplorer.Shared;

// Create the database
ClickHouseSchema.CreateDatabase;  // "CREATE DATABASE IF NOT EXISTS qubic"

// Get all schema DDL statements (tables, views, indexes)
var statements = ClickHouseSchema.GetSchemaStatements();
```

## Usage

This library is referenced by both `QubicExplorer.Indexer` and `QubicExplorer.Api` projects.

```csharp
using QubicExplorer.Shared;
using QubicExplorer.Shared.Models;
using QubicExplorer.Shared.DTOs;
using QubicExplorer.Shared.Constants;
```
