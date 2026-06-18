# Financial Market Lakehouse

A local Data Lakehouse project processing financial market data using the Medallion Architecture.

## Architecture

| Component | Technology |
|---|---|
| Storage | MinIO (S3-compatible) |
| Processing | Apache Spark (PySpark) |
| Orchestration | Apache Airflow |
| Table Format | Delta Lake |
| Architecture | Medallion (Bronze / Silver / Gold) |

## Data Sources

- **NBP API** — Polish National Bank exchange rates
- **CoinGecko API** — Cryptocurrency market data
- **Yahoo Finance** — Stock market data

## Project Structure

```
├── dags/          # Airflow DAG definitions
├── src/
│   ├── bronze/    # Raw ingestion from APIs
│   ├── silver/    # Validation, cleansing, Delta Lake tables
│   ├── gold/      # Business-ready aggregations and metrics
│   └── common/    # Shared utilities (Spark session, MinIO client)
├── tests/         # Unit tests
└── docker/        # Docker Compose stack
```

## Local Setup

1. Copy `.env.example` to `.env` and fill in the values
2. Start the stack:
```bash
docker compose --env-file .env -f docker/docker-compose.yml up -d
```
3. Access services:
   - Airflow UI: http://localhost:8080
   - MinIO Console: http://localhost:9001
   - Spark UI: http://localhost:8081
