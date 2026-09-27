# Sahmk Market Data Engineering Pipeline

A Dockerized ETL pipeline that extracts daily Saudi Stock Exchange (Tadawul) market data from the Sahmk API and loads it into a PostgreSQL star schema, orchestrated with Apache Airflow.

Company metadata and the trading calendar are stored as dimensions, and daily prices are stored as a fact table keyed by date and symbol.

## Stack

| Component | Technology | Role |
| --- | --- | --- |
| Orchestration | Apache Airflow 3.3.2 (TaskFlow API) | Schedules and runs the ETL DAGs |
| Warehouse | PostgreSQL 16 | Stores the star schema |
| Infrastructure | Docker Compose | Runs Airflow and the warehouse as two separate stacks |
| Networking | `sahmk-network` | External Docker network shared by both stacks |
| Database access | Airflow `PostgresHook` | Runs warehouse SQL from Airflow tasks |

## Data Model

### `dim_company`

One row per Tadawul security. Columns cover the trading symbol, Arabic and English names, market and market segment, security type, listing status, and ETF classification.

### `dim_date`

A static calendar from 2020 through 2030 with ISO calendar attributes and a trading-day flag. Fridays and Saturdays are marked as weekend days.

### `fact_daily_prices`

One row per company per trading date, holding open, high, low, and close prices and trading volume.

The primary key is `(date_key, symbol)`, so a company can have only one record per date. Foreign keys link `date_key` to `dim_date` and `symbol` to `dim_company`.

## Pipelines

### `dim_company_dag`

Defined in `dags/dim_company_load.py`. Runs weekly with catchup disabled.

1. `extract_companies` calls the Sahmk API using the `sahmk_api` Airflow Connection and reads the paginated JSON response.
2. `load_companies` upserts the records into `dim_company` with `INSERT ... ON CONFLICT (symbol) DO UPDATE`, so reruns update existing rows instead of duplicating them.

### `fact_daily_prices_dag`

Defined in `dags/fact_daily_prices_load.py`. Runs daily with catchup disabled.

1. `extract_company_symbols` reads the symbol list from `dim_company`.
2. `extract_daily_market_data` requests the current quote for each symbol, authenticating with an `X-API-Key` header from the `sahmk_api` Connection, and shapes each quote into a row with `date_key`, `symbol`, OHLC prices, and volume.
3. `load_market_data` upserts the rows into `fact_daily_prices` on `(date_key, symbol)`, including volume.

API keys and database credentials are kept out of the code. They live in Airflow Connections and local `.env` files.

## Prerequisites

- Docker Desktop with Docker Compose
- At least 4 GB of memory and 2 CPUs allocated to Docker
- A Sahmk API key

## Setup

### 1. Create the environment files

In the project root, create `.env`:

```dotenv
AIRFLOW_UID=50000
FERNET_KEY=<airflow-fernet-key>
```

Generate the Fernet key with:

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

In `postgres/`, create `.env`:

```dotenv
WAREHOUSE_PASSWORD=<warehouse-password>
```

### 2. Create the shared network

```bash
docker network create sahmk-network
```

### 3. Start the warehouse

```bash
cd postgres
docker compose up -d
cd ..
```

The warehouse is reachable at `localhost:5433` from the host and at `warehouse:5432` from containers on `sahmk-network`.

### 4. Start Airflow

```bash
docker compose up -d
docker compose ps
```

Once the services are healthy, open `http://localhost:8080`.

### 5. Add the Airflow Connections

Under **Admin > Connections**, create:

**`sahmk_api`** (HTTP or Generic)

| Field | Value |
| --- | --- |
| Host | Sahmk API base URL |
| Password | Your Sahmk API key |

**`postgres_warehouse`** (Postgres)

| Field | Value |
| --- | --- |
| Host | `warehouse` |
| Port | `5432` |
| Database | `warehouse` |
| User | `yazeed` |
| Password | Value of `WAREHOUSE_PASSWORD` |

### 6. Run the DAGs

DAGs start paused. Run `dim_company_dag` first, since the daily DAG reads its symbols from `dim_company`.

From the UI, unpause the DAG and trigger it. From the CLI:

```bash
docker compose exec airflow-scheduler airflow dags trigger dim_company_dag
docker compose exec airflow-scheduler airflow dags trigger fact_daily_prices_dag
```

## Project Structure

```text
.
├── config/                        # Airflow configuration
├── dags/
│   ├── dim_company_load.py        # Company dimension DAG
│   └── fact_daily_prices_load.py  # Daily prices DAG
├── plugins/                       # Custom Airflow plugins
├── logs/                          # Airflow runtime logs
├── docker-compose.yaml            # Airflow, Redis, Airflow metadata DB
└── postgres/
    └── docker-compose.yml         # PostgreSQL warehouse
```

## Stopping

```bash
docker compose down              # Airflow, from the project root
cd postgres && docker compose down   # Warehouse
```

To also delete the warehouse data volume, run `docker compose down -v` inside `postgres/`. This permanently removes all loaded data.
