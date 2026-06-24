# Data Warehouse ETL Pipeline

This project implements an Airflow-driven ETL pipeline that extracts incremental data from PostgreSQL, writes it to S3 as Parquet, validates row counts against Snowflake raw tables, and then runs dbt transformations in Snowflake.

## What this project does

- Extracts source tables from PostgreSQL into S3 raw storage.
- Uses Airflow to orchestrate the full workflow.
- Waits for PostgreSQL and Snowflake raw tables to synchronize before running dbt.
- Runs dbt staging, snapshot, dimension, fact, marketing, and sales models in Snowflake.
- Stores audit metadata for extraction runs.

## Current architecture

```text
PostgreSQL source tables (customers, products, orders)
    │
    ▼
Airflow DAG: datawarehousing
    │
    ├─ Python task: extract_postgres_to_s3
    ├─ Python task: wait_for_row_count_sync
    ├─ dbt tasks: staging, snapshot, dimensions, facts, marketing, sales, tests
    ▼
S3 raw layer (Parquet files)
    ▼
Snowflake raw/stage/marts layers
```

## Repository layout

```text
.
├── airflow/
│   └── dags/
│       └── dbt_snowflake_dag.py
├── extract/
│   ├── audit.py
│   ├── repository.py
│   ├── service.py
│   └── utils/
├── sql/
│   ├── 00_setup_schemas_and_control.sql
│   ├── postgres_table.sql
│   ├── raw_tables_snowflake.sql
│   └── snowflake_stage_pipes.sql
├── tests/
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── README.md
```

## Core components

- Airflow DAG: [airflow/dags/dbt_snowflake_dag.py](airflow/dags/dbt_snowflake_dag.py)
- Extraction logic: [extract/service.py](extract/service.py)
- Database utilities: [extract/utils](extract/utils)
- SQL setup scripts: [sql](sql)
- dbt project: [airflow/dags/dbt](airflow/dags/dbt)

## Prerequisites

- Docker and Docker Compose
- Python 3.10+ (for local development and tests)
- Access to PostgreSQL, S3-compatible storage, and Snowflake

## Quick start

1. Build and start the stack:

   ```bash
   docker compose up -d --build
   ```

2. Open Airflow at:

   ```text
   http://localhost:8080
   ```

3. Trigger the DAG named `datawarehousing` from the Airflow UI.

## Environment configuration

The services in [docker-compose.yml](docker-compose.yml) set the runtime environment variables used by the pipeline. Before running the stack, make sure the following values are configured correctly:

- PostgreSQL connection settings
- Snowflake account, user, warehouse, role, and schema settings
- S3 bucket and prefix values
- dbt profile settings for the Snowflake target

## dbt workflow

The current DAG runs the following dbt steps:

1. `dbt deps`
2. `dbt run --select stage --target snowflake`
3. `dbt snapshot`
4. `dbt run --select marts.dimension`
5. `dbt run --select marts.fact`
6. `dbt run --select marts.marketing`
7. `dbt run --select marts.sales`
8. `dbt test`

## Testing

Run the existing test suite with:

```bash
pytest -q
```

## Notes

- The extraction layer writes Parquet files to a temporary location before uploading them to S3.
- The pipeline uses an audit table to track extraction state and run history.
- Row-count synchronization between PostgreSQL and Snowflake raw tables is enforced before dbt execution continues.
