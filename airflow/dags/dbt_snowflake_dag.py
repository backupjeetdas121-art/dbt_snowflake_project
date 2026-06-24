import os
import sys
import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


sys.path.append("/opt/airflow")

from extract.service import extract_all
from extract.postgres_to_s3 import extract_table_to_s3

from extract.utils.config import RAW_SCHEMA, SOURCE_SCHEMA, TABLE_CONFIG
from extract.utils.db_utils import get_pg_connection, get_sf_connection
from extract.utils.logging_config import configure_logging, logger

configure_logging()


def extract_postgres_to_s3_task(**context):
    """Wrapper task to extract PostgreSQL tables to S3."""
    execution_date = context.get("ds")
    run_ts = context.get("ts_nodash")
    return extract_all(execution_date=execution_date, run_ts=run_ts)


def wait_for_row_count_sync(**context):
    """Wait until PostgreSQL and Snowflake raw tables have matching row counts."""
    tables = [table_name.lower() for table_name in TABLE_CONFIG]
    snowflake_schema = RAW_SCHEMA
    max_attempts = 10
    sleep_seconds = 60

    for attempt in range(1, max_attempts + 1):
        pg_counts = {}
        sf_counts = {}

        with get_pg_connection() as pg_conn:
            with pg_conn.cursor() as pg_cursor:
                for table_name in tables:
                    pg_cursor.execute(f"SELECT COUNT(*) FROM {SOURCE_SCHEMA}.{table_name}")
                    pg_counts[table_name] = pg_cursor.fetchone()[0]

        with get_sf_connection(schema=RAW_SCHEMA) as sf_conn:
            with sf_conn.cursor() as sf_cursor:
                for table_name in tables:
                    sf_cursor.execute(f"SELECT COUNT(*) FROM {snowflake_schema}.{table_name.upper()}")
                    sf_counts[table_name] = sf_cursor.fetchone()[0]

        logger.info("Row count check attempt %s: postgres=%s, snowflake=%s", attempt, pg_counts, sf_counts)

        if pg_counts == sf_counts:
            logger.info("Row counts are synchronized for all configured tables.")
            return True

        if attempt < max_attempts:
            logger.info("Row counts are not yet aligned. Waiting %s seconds before retrying.", sleep_seconds)
            time.sleep(sleep_seconds)

    raise AirflowException(
        f"Timed out waiting for row counts to match for tables: {', '.join(tables)}"
    )


DBT_PROJECT_DIR = "/opt/airflow/dags/dbt"
DBT_PROFILES_DIR = "/opt/airflow/dags/dbt"

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email": ["data-eng-alerts@company.com"],
}

with DAG(
    dag_id="postgres_to_s3_and_dbt",
    description="Extract from PostgreSQL to S3, wait 5 minutes, then run dbt",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["postgres", "s3", "dbt", "snowflake"],
) as dag:

    # extract_postgres_to_s3 = PythonOperator(
    #     task_id="extract_postgres_to_s3",
    #     python_callable=extract_table_to_s3,
    # )

    extract_postgres_to_s3 = PythonOperator(
        task_id="extract_postgres_to_s3",
        python_callable=extract_postgres_to_s3_task,
    )

    wait_for_row_count_sync = PythonOperator(
        task_id="wait_for_row_count_sync",
        python_callable=wait_for_row_count_sync,
    )

    check_dbt = BashOperator(
        task_id="check_dbt",
        bash_command="""
        echo "===== DBT VERSION ====="
        which dbt || true
        dbt --version || true

        echo "===== INSTALLED DBT PACKAGES ====="
        pip list | grep dbt || true

        echo "===== PROJECT DIRECTORY ====="
        ls -la /opt/airflow/dags/dbt || true
        """,
    )

    dbt_packages = BashOperator(
        task_id="dbt_packages",
        bash_command=f"""
        cd {DBT_PROJECT_DIR}
        dbt deps --profiles-dir .
        """,
    )

    run_staging = BashOperator(
        task_id="run_staging",
        bash_command=f"""
        set -e
        cd {DBT_PROJECT_DIR}
        dbt run --select stage --target snowflake --profiles-dir {DBT_PROFILES_DIR}
        """,
    )

    run_snapshot = BashOperator(
        task_id="run_snapshot",
        bash_command=f"""
        set -e
        cd {DBT_PROJECT_DIR}
        dbt snapshot --target snowflake --profiles-dir {DBT_PROFILES_DIR} 2>&1
        """,
    )

    run_dimensions = BashOperator(
        task_id="run_dimensions",
        bash_command=f"""
        set -e
        cd {DBT_PROJECT_DIR}
        dbt run --select marts.dimension --target snowflake --profiles-dir {DBT_PROFILES_DIR}
        """,
    )

    run_facts = BashOperator(
        task_id="run_facts",
        bash_command=f"""
        set -e
        cd {DBT_PROJECT_DIR}
        dbt run --select marts.fact --target snowflake --profiles-dir {DBT_PROFILES_DIR}
        """,
    )

    run_marketing = BashOperator(
        task_id="run_marketing",
        bash_command=f"""
        set -e
        cd {DBT_PROJECT_DIR}
        dbt run --select marts.marketing --target snowflake --profiles-dir {DBT_PROFILES_DIR}
        """,
    )

    run_sales = BashOperator(
        task_id="run_sales",
        bash_command=f"""
        set -e
        cd {DBT_PROJECT_DIR}
        dbt run --select marts.sales --target snowflake --profiles-dir {DBT_PROFILES_DIR}
        """,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"""
        set -e
        cd {DBT_PROJECT_DIR}
        dbt test --target snowflake --profiles-dir {DBT_PROFILES_DIR}
        """,
    )


    extract_postgres_to_s3 >> wait_for_row_count_sync >> check_dbt >> dbt_packages >> run_staging >> run_snapshot >> run_dimensions >> run_facts >> run_marketing >> run_sales >> dbt_test 
