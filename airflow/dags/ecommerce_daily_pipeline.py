from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup


SOURCE_TABLES = [
    "categories",
    "customers",
    "products",
    "coupons",
    "orders",
    "order_items",
    "payments",
    "shipments",
    "inventory_snapshots",
    "order_status_history",
]

PROJECT_ROOT = "/opt/airflow"
MANIFEST_PATH = "/tmp/ecommerce_airflow/{{ ds }}/{{ run_id | replace(':', '_') }}/extract_manifest.json"
BATCH_ID = "{{ dag_run.conf.get('batch_id', ds_nodash ~ '_' ~ run_id | replace(':', '_') | replace('+', '_')) if dag_run and dag_run.conf else ds_nodash ~ '_' ~ run_id | replace(':', '_') | replace('+', '_') }}"
START_WATERMARK = "{{ dag_run.conf.get('start_watermark', '') if dag_run and dag_run.conf else '' }}"
CURRENT_RUN_WATERMARK = "{{ dag_run.conf.get('end_watermark', data_interval_end.in_timezone('UTC').strftime('%Y-%m-%dT%H:%M:%S')) if dag_run and dag_run.conf else data_interval_end.in_timezone('UTC').strftime('%Y-%m-%dT%H:%M:%S') }}"
WATERMARK_MODE = "{{ dag_run.conf.get('watermark_mode', 'advance') if dag_run and dag_run.conf else 'advance' }}"
MART_START_DATE = "{{ dag_run.conf.get('mart_start_date', ds) if dag_run and dag_run.conf else ds }}"
MART_END_DATE = "{{ dag_run.conf.get('mart_end_date', ds) if dag_run and dag_run.conf else ds }}"
DBT_MART_VARS_JSON = '{"mart_start_date": "' + MART_START_DATE + '", "mart_end_date": "' + MART_END_DATE + '"}'


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="ecommerce_daily_pipeline",
    description="Daily ecommerce DW batch pipeline: seed, extract, raw load, dbt, quality, watermark update.",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["ecommerce", "dw", "batch"],
) as dag:
    with TaskGroup("seed") as seed:
        seed_postgres_data = BashOperator(
            task_id="seed_postgres_data",
            bash_command=(
                "python /opt/airflow/scripts/generate_data/generate_seed_data.py "
                "--reset "
                "--end-date {{ ds }} "
                "--with-incremental-changes"
            ),
            execution_timeout=timedelta(minutes=20),
        )

    with TaskGroup("extract") as extract:
        extract_postgres_to_gcs = BashOperator(
            task_id="extract_postgres_to_gcs",
            bash_command=(
                "mkdir -p $(dirname " + MANIFEST_PATH + ") && "
                "python /opt/airflow/scripts/extract/postgres_to_gcs.py "
                "--table all "
                "--batch-id " + BATCH_ID + " "
                "--extract-date {{ ds }} "
                "--start-watermark '" + START_WATERMARK + "' "
                "--current-run-watermark '" + CURRENT_RUN_WATERMARK + "' "
                "--watermark-mode '" + WATERMARK_MODE + "' "
                "--manifest-path " + MANIFEST_PATH
            ),
            execution_timeout=timedelta(minutes=30),
        )

    with TaskGroup("load_raw") as load_raw:
        load_gcs_to_bigquery_raw = BashOperator(
            task_id="load_gcs_to_bigquery_raw",
            bash_command=(
                "python /opt/airflow/scripts/load/load_manifest_to_bigquery_raw.py "
                "--manifest-path " + MANIFEST_PATH + " "
                "--project-id ${GCP_PROJECT_ID} "
                "--raw-dataset ${BQ_RAW_DATASET} "
                "--audit-dataset ${BQ_AUDIT_DATASET} "
                "--location ${BQ_LOCATION:-US}"
            ),
            execution_timeout=timedelta(minutes=30),
        )

    with TaskGroup("dbt_staging") as dbt_staging:
        run_dbt_staging = BashOperator(
            task_id="run_dbt_staging",
            bash_command="bash /opt/airflow/scripts/dbt/run_dbt.sh staging run",
            execution_timeout=timedelta(minutes=30),
        )

        test_dbt_staging = BashOperator(
            task_id="test_dbt_staging",
            bash_command="bash /opt/airflow/scripts/dbt/run_dbt.sh staging test",
            execution_timeout=timedelta(minutes=30),
        )

        run_dbt_staging >> test_dbt_staging

    with TaskGroup("dbt_core") as dbt_core:
        run_dbt_core = BashOperator(
            task_id="run_dbt_core",
            bash_command="bash /opt/airflow/scripts/dbt/run_dbt.sh core run",
            execution_timeout=timedelta(minutes=45),
        )

        test_dbt_core = BashOperator(
            task_id="test_dbt_core",
            bash_command="bash /opt/airflow/scripts/dbt/run_dbt.sh core test",
            execution_timeout=timedelta(minutes=45),
        )

        run_dbt_core >> test_dbt_core

    with TaskGroup("dbt_mart") as dbt_mart:
        run_dbt_mart = BashOperator(
            task_id="run_dbt_mart",
            bash_command="bash /opt/airflow/scripts/dbt/run_dbt.sh mart run",
            env={"DBT_VARS_JSON": DBT_MART_VARS_JSON},
            append_env=True,
            execution_timeout=timedelta(minutes=45),
        )

        test_dbt_mart = BashOperator(
            task_id="test_dbt_mart",
            bash_command="bash /opt/airflow/scripts/dbt/run_dbt.sh mart test",
            env={"DBT_VARS_JSON": DBT_MART_VARS_JSON},
            append_env=True,
            execution_timeout=timedelta(minutes=45),
        )

        run_dbt_mart >> test_dbt_mart

    with TaskGroup("quality_check") as quality_check:
        run_data_quality_checks = BashOperator(
            task_id="run_data_quality_checks",
            bash_command=(
                "python /opt/airflow/scripts/quality/run_bigquery_quality_checks.py "
                "--checks-file /opt/airflow/sql/bigquery/quality_checks.sql "
                "--project-id ${GCP_PROJECT_ID} "
                "--audit-dataset ${BQ_AUDIT_DATASET} "
                "--location ${BQ_LOCATION:-US} "
                "--execution-date {{ ds }} "
                "--batch-id " + BATCH_ID
            ),
            execution_timeout=timedelta(minutes=30),
        )

    with TaskGroup("metadata_update") as metadata_update:
        update_etl_metadata = BashOperator(
            task_id="update_etl_metadata",
            bash_command=(
                "python /opt/airflow/scripts/metadata/update_watermarks.py "
                "--manifest-path " + MANIFEST_PATH + " "
                "--watermark-mode '" + WATERMARK_MODE + "'"
            ),
            execution_timeout=timedelta(minutes=10),
        )

    seed >> extract >> load_raw >> dbt_staging >> dbt_core >> dbt_mart >> quality_check >> metadata_update
