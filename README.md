# Enterprise DWH Pipeline

Postgres → S3 (Parquet) → Redshift STG → dbt (Staging → SCD2 Snapshot →
Dimension → Fact) → Sales & Marketing Datamarts → Power BI / Tableau,
orchestrated by Airflow every 4 hours.

## 1. Architecture

```
Postgres (customers, products, orders)
   │  extract_<table>_to_s3 (PythonOperator)
   ▼
S3 Raw Layer (Parquet, partitioned by dt=)
   │  load_<table>_to_stg (PythonOperator -> COPY)
   ▼
Redshift STG Layer (stg.stg_customers / stg_products / stg_orders)
   │  dbt_run_staging
   ▼
dbt Staging Models (stg_*, int_customers_current, int_products_current)
   │  dbt_snapshot_scd2
   ▼
dbt Snapshots — SCD2 (snapshots.snap_customers, snapshots.snap_products)
   │  dbt_run_dimensions
   ▼
Dimension Layer (dwh.dim_customers, dwh.dim_products)
   │  dbt_run_facts
   ▼
Fact Layer (dwh.fact_orders)
   │  dbt_test_core_layers
   ▼
   ├── dbt_run_sales_mart      → sales_mart.sales_daily_summary, sales_mart.sales_customer_ltv
   └── dbt_run_marketing_mart  → marketing_mart.marketing_customer_segments, marketing_mart.marketing_channel_performance
   │  dbt_test_datamarts
   ▼
Power BI / Tableau
```

## 2. Repository layout

```
enterprise_dwh_pipeline/
├── extract/postgres_to_s3.py        # Step 1: incremental Postgres -> S3 Parquet
├── load/s3_to_redshift.py           # Step 2: S3 -> Redshift STG (TRUNCATE + COPY)
├── sql/
│   ├── 00_setup_schemas_and_control.sql   # schemas + watermark/run-log tables
│   ├── 01_stg_tables.sql                  # STG layer DDL
│   └── 02_source_postgres_tables.sql      # sample SOURCE Postgres DDL + triggers
├── iam/redshift_s3_copy_role_policy.json  # IAM policy for Redshift COPY role
├── airflow/dags/enterprise_dwh_pipeline.py  # full orchestration DAG (4-hourly)
├── dbt_project/
│   ├── dbt_project.yml / packages.yml / profiles_example.yml
│   ├── models/staging/        # stg_customers, stg_products, stg_orders,
│   │                           # int_customers_current, int_products_current
│   ├── snapshots/              # snap_customers, snap_products (SCD2)
│   ├── models/marts/dimension/ # dim_customers, dim_products
│   ├── models/marts/fact/      # fact_orders
│   ├── models/marts/sales/     # sales_daily_summary, sales_customer_ltv
│   ├── models/marts/marketing/ # marketing_customer_segments, marketing_channel_performance
│   └── tests/                  # singular validation tests
├── requirements.txt
└── .env.example
```

## 3. Why an `int_*_current` layer before the SCD2 snapshot?

The STG tables are **truncate-and-load every run** — they only ever hold the
current incremental batch (rows changed since the last watermark). `dbt
snapshot` needs to diff a **full current-state** result set against history
to correctly open/close SCD2 records.

`int_customers_current` / `int_products_current` are incremental `merge`
models that continuously upsert each batch into a full "current state"
table. `snap_customers` / `snap_products` snapshot *that* table, so:

- New customers/products → new SCD2 row opens.
- Changed attributes (address, price, etc.) → old row closed
  (`dbt_valid_to` set), new row opens with `dbt_valid_from = updated_at`.
- Untouched records this run → unaffected, no spurious "delete" events.

`dim_customers` / `dim_products` then read the snapshot and expose
`effective_from`, `effective_to`, and `is_current` for easy current/historical
filtering.

## 4. Airflow DAG — task list (`enterprise_dwh_pipeline`, every 4 hours)

| # | Task ID                  | Operator        | Purpose |
|---|---------------------------|-----------------|---------|
| 0 | `start` / `end`           | EmptyOperator   | DAG boundaries |
| 1 | `extract_customers_to_s3` | PythonOperator  | Incremental Postgres → S3 Parquet |
| 1 | `extract_products_to_s3`  | PythonOperator  | Incremental Postgres → S3 Parquet |
| 1 | `extract_orders_to_s3`    | PythonOperator  | Incremental Postgres → S3 Parquet |
| 2 | `load_customers_to_stg`   | PythonOperator  | TRUNCATE + COPY into `stg.stg_customers` |
| 2 | `load_products_to_stg`    | PythonOperator  | TRUNCATE + COPY into `stg.stg_products` |
| 2 | `load_orders_to_stg`      | PythonOperator  | TRUNCATE + COPY into `stg.stg_orders` |
| 3 | `dbt_run_staging`         | BashOperator    | `dbt run --select staging` |
| 4 | `dbt_snapshot_scd2`       | BashOperator    | `dbt snapshot` (SCD2 for customers/products) |
| 5 | `dbt_run_dimensions`      | BashOperator    | `dbt run --select marts.dimension` |
| 6 | `dbt_run_facts`           | BashOperator    | `dbt run --select marts.fact` |
| 7 | `dbt_test_core_layers`    | BashOperator    | `dbt test --select staging marts.dimension marts.fact` |
| 8 | `dbt_run_sales_mart`      | BashOperator    | `dbt run --select marts.sales` |
| 8 | `dbt_run_marketing_mart`  | BashOperator    | `dbt run --select marts.marketing` |
| 9 | `dbt_test_datamarts`      | BashOperator    | `dbt test --select marts.sales marts.marketing` |

Dependency graph:

```
start -> [extract_* -> load_*] -> dbt_run_staging -> dbt_snapshot_scd2
      -> dbt_run_dimensions -> dbt_run_facts -> dbt_test_core_layers
      -> [dbt_run_sales_mart, dbt_run_marketing_mart] -> dbt_test_datamarts -> end
```

## 5. Validation / data quality checks

- **Source freshness** (`models/staging/_staging__sources.yml`): warns/errors
  if `stg_*` tables haven't refreshed in 6h / 12h — catches a stalled DAG.
- **Schema tests** on every layer: `unique`, `not_null`, `accepted_values`,
  `relationships` (fact → dimension FK integrity), and `dbt_utils.accepted_range`
  for prices/amounts/conversion rates.
- **Singular tests** (`dbt_project/tests/`):
  - `assert_one_current_dim_customer.sql` — SCD2 integrity (exactly one
    `is_current = true` row per `customer_id`).
  - `assert_no_negative_order_amounts.sql` — no negative quantities/amounts
    in `fact_orders`.

All of these run via `dbt_test_core_layers` and `dbt_test_datamarts`. A
failure fails the Airflow task (and the whole DAG run), so bad data never
silently reaches Power BI / Tableau.

## 6. Setup checklist

1. **Source DB**: run `sql/02_source_postgres_tables.sql` (creates sample
   `customers`/`products`/`orders` with `updated_at` triggers — required for
   incremental extraction).
2. **Redshift**: run `sql/00_setup_schemas_and_control.sql` then
   `sql/01_stg_tables.sql`.
3. **IAM**: attach `iam/redshift_s3_copy_role_policy.json` to a role, assign
   it to your Redshift cluster, and set `REDSHIFT_COPY_IAM_ROLE`.
4. **Env vars**: copy `.env.example` → configure as Airflow Connections /
   Variables (do not commit secrets).
5. **dbt**: `cd dbt_project && dbt deps && dbt build` for a first manual run;
   then point `DBT_PROJECT_DIR`/`DBT_PROFILES_DIR` in the DAG at your deployed
   project path.
6. **Airflow**: deploy `airflow/dags/enterprise_dwh_pipeline.py`, ensure
   `extract/` and `load/` are importable (PYTHONPATH), and confirm the
   `0 */4 * * *` schedule.

## 7. Suggested enhancements (beyond this starter project)

- Replace the homegrown watermark table with **Airflow's dataset/asset
  scheduling** or a CDC tool (Debezium) for lower-latency capture.
- Add **Great Expectations** or **dbt-expectations** for richer data quality
  rules beyond the built-in dbt tests.
- Add **SLAs / `on_failure_callback`** to the DAG for Slack/PagerDuty alerts.
- Generate and host **`dbt docs`** for data lineage and column-level
  documentation for BI consumers.
- Add a **dead-letter / quarantine** S3 prefix for rows that fail the
  staging `WHERE` filters, so they're not silently dropped.
