# Ad Analytics Pipeline

Production-grade Java application for processing large CSV datasets (~1GB) containing advertising performance records. Built with **Clean Architecture (Hexagonal/Ports & Adapters)**, designed for high performance, memory efficiency, and extensibility.

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                  Presentation Layer                  │
│          CLI (CliRunner) │ REST API (Spring Boot)    │
├─────────────────────────────────────────────────────┤
│                  Application Layer                   │
│  AggregateCampaignUseCase │ TopKSelector             │
│  ┌──────────────────────────────────────────────┐   │
│  │ Ports: DataSourcePort │ ResultSinkPort        │   │
│  └──────────────────────────────────────────────┘   │
├─────────────────────────────────────────────────────┤
│                  Infrastructure Layer                │
│  Adapters:                                           │
│  CSV (BufferedReader / Memory-Mapped IO)             │
│  PostgreSQL │ ClickHouse │ Kafka │ Spark (stubs)     │
│  CSV Result Sink │ DataSourceFactory                 │
├─────────────────────────────────────────────────────┤
│                    Domain Layer                      │
│  AdRecord │ CampaignMetrics │ CampaignSnapshot       │
│  Pure business logic (CTR, CPA) — no dependencies   │
└─────────────────────────────────────────────────────┘
```

## Features

- **Streaming Processing**: O(N) time, O(K) memory where K = unique campaigns
- **Zero-Copy CSV Parser**: Avoids `String.split()` — index-based comma scanning
- **Dual-Mode IO**: BufferedReader or Memory-Mapped IO via `FileChannel + MappedByteBuffer`
- **Thread-Safe Aggregation**: `ConcurrentHashMap` + `LongAdder` / `DoubleAdder`
- **Top-K Selection**: `PriorityQueue`-based with O(N log K) complexity
- **Pluggable Data Sources**: CSV, PostgreSQL, ClickHouse, Kafka, Spark
- **Graceful Error Handling**: Malformed rows are skipped with logging
- **CLI + REST API**: Dual interface support

## Prerequisites

- **Java 17+**
- **Maven 3.9+**
- **Docker** (optional)

## Quick Start

### Build

```bash
cd ad-analytics-pipeline
mvn clean package
```

### Run Tests

```bash
mvn test
```

### CLI Mode

```bash
# Process a CSV file
java -jar target/ad-analytics-pipeline.jar --input ad_data.csv --output results/ --source csv

# Disable memory-mapped IO (use BufferedReader)
java -jar target/ad-analytics-pipeline.jar --input ad_data.csv --output results/ --source csv --no-mmap

# Show help
java -jar target/ad-analytics-pipeline.jar --help
```

### REST API Mode

```bash
# Start server (no --input argument → API mode)
java -jar target/ad-analytics-pipeline.jar

# Query endpoints
curl http://localhost:8080/analytics/top-ctr
curl http://localhost:8080/analytics/top-cpa
```

For API mode, configure the input file in `application.properties`:
```properties
datasource.csv.input-path=ad_data.csv
```

### API Response Format

```json
{
  "campaigns": [
    {
      "campaignId": "CMP001",
      "totalImpressions": 20000,
      "totalClicks": 1100,
      "totalSpend": 250.0,
      "totalConversions": 50,
      "ctr": 0.055,
      "cpa": 5.0
    }
  ],
  "totalRowsProcessed": 20000000,
  "uniqueCampaigns": 1000
}
```
```

## Docker

### Build & Run

```bash
# Build image
docker build -t ad-analytics-pipeline .

# Run in API mode
docker run -p 8080:8080 -v $(pwd)/data:/app/data ad-analytics-pipeline

# Run in CLI mode
docker run -v $(pwd)/data:/app/data -v $(pwd)/results:/app/results \
  ad-analytics-pipeline --input /app/data/ad_data.csv --output /app/results/
```

## Configuration

All settings are configurable via `application.properties` or command-line:

| Property | Default | Description |
|---|---|---|
| `datasource.type` | `csv` | Data source: csv, postgres, clickhouse, kafka, spark |
| `datasource.csv.input-path` | `ad_data.csv` | Input CSV file path |
| `datasource.csv.use-memory-mapped` | `false` | Use memory-mapped IO |
| `output.directory` | `results/` | Output directory for CSV results |
| `server.port` | `8080` | REST API port |

### Adding a New Data Source

1. Create a class implementing `DataSourcePort`
2. Add a case in `DataSourceFactory`
3. Configure via `datasource.type`

No changes to domain or application layer required.

## Output Files

### `top10_ctr.csv` — Top 10 by CTR (descending)

```csv
campaign_id,total_impressions,total_clicks,total_spend,total_conversions,CTR,CPA
CMP042,15000,1500,250.00,75,0.100000,3.33
...
```

### `top10_cpa.csv` — Top 10 by CPA (ascending, excludes 0-conversion campaigns)

```csv
campaign_id,total_impressions,total_clicks,total_spend,total_conversions,CTR,CPA
CMP017,12000,600,30.00,60,0.050000,0.50
...
```

## Design Decisions & Trade-offs

| Decision | Rationale |
|---|---|
| `ConcurrentHashMap` + `LongAdder` | Lock-free concurrent accumulation with minimal contention |
| `PriorityQueue` for Top-K | O(N log K) vs O(N log N) for full sort — significant for large K |
| Zero-copy parser | Avoids `String.split()` allocation overhead in hot path |
| Memory-mapped IO option | Higher throughput for large files via OS page cache |
| Stub adapters | Demonstrates extensibility without adding heavyweight dependencies |
| Spring Boot for DI | Production-ready IoC with minimal boilerplate |
| Records for DTOs | Immutable, concise, and auto-generate equals/hashCode/toString |
| Graceful error handling | Skip bad rows with logging rather than fail-fast on large datasets |
