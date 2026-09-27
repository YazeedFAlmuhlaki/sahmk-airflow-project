# Sahmk Market Data Engineering Pipeline

An automated, Dockerized ETL pipeline that extracts daily Saudi Stock Exchange (Tadawul) market data from the Sahmk API and loads it into a PostgreSQL data warehouse.

The platform uses Apache Airflow for workflow orchestration and a dimensional star schema for analytical storage. It separates descriptive company and calendar attributes from daily market measures to support reliable reporting and historical analysis.

## Architecture and Technologies

| Area | Technology | Role |
| --- | --- | --- |
| Infrastructure | Docker Compose | Runs the Airflow platform and PostgreSQL warehouse as isolated services |
| Networking | `sahmk-network` | Provides communication between the Airflow and warehouse Compose stacks |
| Orchestration | Apache Airflow 3.3.2 | Schedules, executes, and monitors ETL workflows |
| Execution model | Airflow TaskFlow API | Defines Python tasks and task dependencies with `@dag` and `@task` |
| Warehouse | PostgreSQL 16 | Stores the analytical star schema |
| Programming language | Python | Implements API extraction and database loading logic |
| API client | `requests` | Handles authenticated HTTP requests and paginated JSON responses |
| Database integration | Airflow `PostgresHook` | Executes warehouse SQL operations from Airflow tasks |

The Airflow web interface is available at `http://localhost:8080`. The warehouse is available at `localhost:5433` from the host machine and at port `5432` to containers connected to `sahmk-network`.

## Data Model

The warehouse uses a dimensional star schema with two dimensions surrounding the daily market fact table.

### `dim_company`

Stores structural attributes for 519 Tadawul entities:

- Trading symbol
- Arabic and English names
- Market and market segment
- Security type
- Listing status
- ETF classification

### `dim_date`

Stores a static calendar covering 2020 through 2030. It includes ISO-based calendar attributes and flags that identify Saudi trading days. Fridays and Saturdays are marked as weekend days.

### `fact_daily_prices`

Stores daily market metrics for each company and trading date:

- Open price
- High price
- Low price
- Close price
- Trading volume

The table uses a composite primary key on `(date_key, symbol)`. Foreign keys link `date_key` to `dim_date` and `symbol` to `dim_company`, preventing duplicate company-date records and preserving referential integrity.

## ETL Pipeline

The ETL workflows use Python-based Airflow DAGs and the TaskFlow API. Company metadata is synchronized weekly, while daily market quotes are extracted on a daily schedule.

### Company Dimension Load

`dags/dim_company_load.py` defines `dim_company_dag`, which runs weekly with catchup disabled.

1. `extract_companies` reads the Sahmk API connection from Airflow.
2. The API key is sent through request headers and paginated JSON responses are parsed.
3. `load_companies` writes company records to PostgreSQL through `PostgresHook`.
4. `INSERT ... ON CONFLICT (symbol) DO UPDATE` provides idempotent updates.

### Daily Market Price Load

`dags/fact_daily_prices_load.py` defines `fact_daily_prices_dag`, which runs daily with catchup disabled.

1. `extract_company_symbols` reads company symbols from `dim_company` through `PostgresHook`.
2. `extract_daily_market_data` requests the current quote for each symbol from the Sahmk API.
3. API authentication uses the `sahmk_api` Airflow Connection and an `X-API-Key` request header.
4. Each quote is normalized into a daily price record containing `date_key`, `symbol`, OHLC prices, and volume.
5. `load_market_data` upserts the records into `fact_daily_prices` using `(date_key, symbol)` as the conflict key.

The upsert updates existing company-date records and includes volume updates, preserving company-date uniqueness and referential integrity through the fact table's composite and foreign keys.

API keys, database credentials, and other secrets are not embedded in Python code. They are managed through Airflow Connections and environment files excluded from source control.

## Prerequisites

- Docker Desktop with Docker Compose support
- At least 4 GB of memory allocated to Docker
- At least 2 CPUs available to Docker
- A valid Sahmk API key
- Access to the Airflow web interface or Airflow CLI

## Setup and Execution

### 1. Configure environment variables

Create `.env` in the project root for Airflow configuration:

```dotenv
AIRFLOW_UID=50000
FERNET_KEY=<airflow-fernet-key>
```

Generate a Fernet key with:

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Create `postgres/.env` for the warehouse password:

```dotenv
WAREHOUSE_PASSWORD=<warehouse-password>
```

Do not commit either environment file. They contain local credentials and secrets.

### 2. Create the Docker network

The two Compose files share an external bridge network. Create it once before starting the services:

```bash
docker network create sahmk-network
```

If the network already exists, Docker will report that it is already present and no additional action is required.

### 3. Start the PostgreSQL warehouse

From the project root, start the warehouse stack:

```bash
cd postgres
docker compose up -d
cd ..
```

The warehouse database is exposed on host port `5433`.

### 4. Start Airflow

Start the Airflow stack from the project root:

```bash
docker compose up -d
```

Check service status with:

```bash
docker compose ps
```

Open the Airflow interface at `http://localhost:8080` after the services become healthy.

### 5. Configure Airflow Connections

Create the following connections in Airflow under **Admin > Connections**.

| Connection ID | Connection type | Configuration |
| --- | --- | --- |
| `sahmk_api` | HTTP or Generic | Set the host to the Sahmk API base URL and store the API key in the password field |
| `postgres_warehouse` | Postgres | Connect to the warehouse using the database, username, and password configured in `postgres/docker-compose.yml` |

For connections made from Airflow containers, use the warehouse service hostname and internal port:

| Setting | Value |
| --- | --- |
| Host | `warehouse` |
| Port | `5432` |
| Database | `warehouse` |
| User | `yazeed` |
| Password | Value of `WAREHOUSE_PASSWORD` |

### 6. Enable and trigger a DAG

Airflow creates DAGs paused by default in this configuration. In the web interface:

1. Open the **DAGs** page.
2. Find `dim_company_dag` or `fact_daily_prices_dag`.
3. Enable the selected DAG with the toggle.
4. Select the trigger action to start a manual run.

The DAGs can also be triggered from the Airflow CLI inside the running Airflow container:

```bash
docker compose exec airflow-scheduler airflow dags trigger dim_company_dag
docker compose exec airflow-scheduler airflow dags trigger fact_daily_prices_dag
```

Monitor task logs and run status from the Airflow web interface.

## Project Structure

```text
.
├── config/                    # Airflow configuration
├── dags/                      # Airflow DAG definitions
│   ├── dim_company_load.py    # Company dimension extraction and loading DAG
│   └── fact_daily_prices_load.py # Daily market quote extraction DAG
├── docker-compose.yaml        # Airflow, Redis, and Airflow metadata database
├── plugins/                   # Custom Airflow plugins
├── postgres/
│   ├── docker-compose.yml     # PostgreSQL warehouse service
│   └── .env                   # Warehouse credentials
└── logs/                      # Airflow runtime logs
```

## Stopping the Services

Stop Airflow from the project root:

```bash
docker compose down
```

Stop the warehouse:

```bash
cd postgres
docker compose down
```

To remove the warehouse container and its persistent data volume, run the following from `postgres`:

```bash
docker compose down -v
```

This permanently deletes the local warehouse data.
