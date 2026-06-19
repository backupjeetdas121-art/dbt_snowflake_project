"""
Extract source tables from Postgres and land them as Parquet files in S3.

Pattern: incremental "high-watermark" extraction based on `updated_at`.
  - Watermarks are stored in Redshift (etl_control.extract_watermark)
  - Each run writes ONLY new/changed rows to:
        s3://<bucket>/raw/<table>/dt=<execution_date>/<table>_<run_ts>.parquet
  - The function returns the S3 key (or "" if no new data), which is
    pushed to XCom and consumed by the load step (load/s3_to_redshift.py)

Tables covered: customers, products, orders
"""

import os
import logging
from datetime import datetime, timezone

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
import psycopg2
import redshift_connector

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration — wire these to Airflow Connections/Variables in production
# ---------------------------------------------------------------------------
PG_CONFIG = {
    "host": os.getenv("PG_HOST", "source-postgres.internal"),
    "port": os.getenv("PG_PORT", "5432"),
    "dbname": os.getenv("PG_DATABASE", "app_db"),
    "user": os.getenv("PG_USER", "etl_reader"),
    "password": os.getenv("PG_PASSWORD"),
}

RS_CONFIG = {
    "host": os.getenv("RS_HOST", "my-redshift-cluster.xxxx.redshift.amazonaws.com"),
    "port": int(os.getenv("RS_PORT", "5439")),
    "database": os.getenv("RS_DATABASE", "analytics"),
    "user": os.getenv("RS_USER", "etl_user"),
    "password": os.getenv("RS_PASSWORD"),
}

S3_BUCKET = os.getenv("S3_RAW_BUCKET", "my-company-dwh-raw")
S3_PREFIX = "raw"

# table_name -> incremental column used as the watermark
TABLE_CONFIG = {
    "customers": {"incremental_col": "updated_at"},
    "products": {"incremental_col": "updated_at"},
    "orders": {"incremental_col": "updated_at"},
}


def get_pg_connection():
    return psycopg2.connect(**PG_CONFIG)


def get_rs_connection():
    return redshift_connector.connect(**RS_CONFIG)


def get_last_watermark(table_name: str) -> str:
    """Return the last successfully-extracted watermark for a table."""
    query = """
        SELECT COALESCE(MAX(last_extracted_at), '1900-01-01 00:00:00')
        FROM etl_control.extract_watermark
        WHERE table_name = %s
    """
    conn = get_rs_connection()
    try:
        cur = conn.cursor()
        cur.execute(query, (table_name,))
        return str(cur.fetchone()[0])
    finally:
        conn.close()


def update_watermark(table_name: str, watermark_value: str) -> None:
    """Persist the new high-watermark after a successful extract."""
    conn = get_rs_connection()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM etl_control.extract_watermark WHERE table_name = %s;", (table_name,))
        cur.execute(
            """
            INSERT INTO etl_control.extract_watermark (table_name, last_extracted_at, updated_at)
            VALUES (%s, %s, %s);
            """,
            (table_name, watermark_value, datetime.now(timezone.utc)),
        )
        conn.commit()
    finally:
        conn.close()


def extract_table_to_s3(table_name: str, **context) -> str:
    """
    Pull rows where `updated_at` > last watermark from Postgres and write
    them to S3 as a single Parquet file (Snappy compressed).

    Returns the S3 key written, or "" if there was no new data
    (used by downstream COPY task to skip the load).
    """
    cfg = TABLE_CONFIG[table_name]
    incremental_col = cfg["incremental_col"]

    watermark = get_last_watermark(table_name)
    logger.info("Extracting '%s' where %s > %s", table_name, incremental_col, watermark)

    query = f"""
        SELECT *
        FROM {table_name}
        WHERE {incremental_col} > %(watermark)s
        ORDER BY {incremental_col} ASC
    """

    pg_conn = get_pg_connection()
    try:
        df = pd.read_sql(query, pg_conn, params={"watermark": watermark})
    finally:
        pg_conn.close()

    if df.empty:
        logger.info("No new/updated rows for '%s'. Nothing written to S3.", table_name)
        return ""

    execution_date = context["ds"]        # e.g. 2026-06-15
    run_ts = context["ts_nodash"]          # e.g. 20260615T120000

    file_name = f"{table_name}_{run_ts}.parquet"
    local_path = f"/tmp/{file_name}"
    s3_key = f"{S3_PREFIX}/{table_name}/dt={execution_date}/{file_name}"

    arrow_table = pa.Table.from_pandas(df)
    pq.write_table(arrow_table, local_path, compression="snappy")

    boto3.client("s3").upload_file(local_path, S3_BUCKET, s3_key)
    os.remove(local_path)

    logger.info("Wrote %s row(s) for '%s' to s3://%s/%s", len(df), table_name, S3_BUCKET, s3_key)

    new_watermark = df[incremental_col].max()
    update_watermark(table_name, str(new_watermark))

    return s3_key


# ---------------------------------------------------------------------------
# Standalone run: python postgres_to_s3.py <table_name>
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import sys

    table = sys.argv[1] if len(sys.argv) > 1 else "customers"
    now = datetime.utcnow()
    fake_context = {"ds": now.strftime("%Y-%m-%d"), "ts_nodash": now.strftime("%Y%m%dT%H%M%S")}
    result = extract_table_to_s3(table, **fake_context)
    print(f"S3 key written: {result or '(no new data)'}")
