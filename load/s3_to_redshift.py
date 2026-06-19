"""
Load Parquet files from S3 into Redshift STG tables.

Pattern:
  - STG tables hold ONLY the current run's batch (TRUNCATE + COPY).
  - COPY reads the date-partitioned prefix written by extract/postgres_to_s3.py:
        s3://<bucket>/raw/<table>/dt=<execution_date>/
  - dbt staging models (stg_*) clean this batch, and `int_*_current` models
    MERGE it into a full "current state" table used for SCD2 snapshots.

If the extract step produced no file for a table (no new/changed rows),
the corresponding STG table is simply left empty for this run and every
downstream dbt step is a safe no-op for that table.
"""

import os
import logging

import redshift_connector

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

RS_CONFIG = {
    "host": os.getenv("RS_HOST", "my-redshift-cluster.xxxx.redshift.amazonaws.com"),
    "port": int(os.getenv("RS_PORT", "5439")),
    "database": os.getenv("RS_DATABASE", "analytics"),
    "user": os.getenv("RS_USER", "etl_user"),
    "password": os.getenv("RS_PASSWORD"),
}

S3_BUCKET = os.getenv("S3_RAW_BUCKET", "my-company-dwh-raw")
IAM_ROLE = os.getenv("REDSHIFT_COPY_IAM_ROLE", "arn:aws:iam::123456789012:role/RedshiftCopyRole")

STG_TABLE_MAP = {
    "customers": "stg.stg_customers",
    "products": "stg.stg_products",
    "orders": "stg.stg_orders",
}


def get_rs_connection():
    return redshift_connector.connect(**RS_CONFIG)


def load_table_to_stg(table_name: str, **context) -> None:
    """TRUNCATE the STG table, then COPY the current run's S3 partition into it."""
    s3_key = context["ti"].xcom_pull(task_ids=f"extract_{table_name}_to_s3")

    stg_table = STG_TABLE_MAP[table_name]

    if not s3_key:
        logger.info("No new file for '%s'. Truncating %s and skipping COPY.", table_name, stg_table)
        conn = get_rs_connection()
        try:
            cur = conn.cursor()
            cur.execute(f"TRUNCATE TABLE {stg_table};")
            conn.commit()
        finally:
            conn.close()
        return

    execution_date = context["ds"]
    s3_prefix_path = f"s3://{S3_BUCKET}/raw/{table_name}/dt={execution_date}/"

    conn = get_rs_connection()
    try:
        cur = conn.cursor()

        logger.info("Truncating %s", stg_table)
        cur.execute(f"TRUNCATE TABLE {stg_table};")

        copy_sql = f"""
            COPY {stg_table}
            FROM '{s3_prefix_path}'
            IAM_ROLE '{IAM_ROLE}'
            FORMAT AS PARQUET;
        """
        logger.info("COPY %s FROM %s", stg_table, s3_prefix_path)
        cur.execute(copy_sql)
        conn.commit()

        cur.execute(f"SELECT COUNT(*) FROM {stg_table};")
        row_count = cur.fetchone()[0]
        logger.info("Loaded %s row(s) into %s", row_count, stg_table)

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Standalone run: python s3_to_redshift.py <table_name>
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import sys
    from datetime import datetime

    table = sys.argv[1] if len(sys.argv) > 1 else "customers"

    class _FakeTI:
        def xcom_pull(self, task_ids):
            return f"raw/{table}/dt={datetime.utcnow().strftime('%Y-%m-%d')}/sample.parquet"

    load_table_to_stg(table, ti=_FakeTI(), ds=datetime.utcnow().strftime("%Y-%m-%d"))
