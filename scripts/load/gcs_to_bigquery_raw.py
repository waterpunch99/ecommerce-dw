from __future__ import annotations

import argparse
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone


RAW_DATASET_ENV = "BQ_RAW_DATASET"
AUDIT_DATASET_ENV = "BQ_AUDIT_DATASET"

SOURCE_TABLES = {
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
}

GCS_URI_PATTERN = re.compile(
    r"^gs://(?P<bucket>[^/]+)/ecommerce/raw/(?P<table>[^/]+)/"
    r"extract_date=(?P<extract_date>\d{4}-\d{2}-\d{2})/"
    r"batch_id=(?P<batch_id>[^/]+)/(?P=table)\.parquet$"
)

bigquery = None


@dataclass(frozen=True)
class RawLoadTarget:
    table_name: str
    batch_id: str
    extract_date: str
    gcs_uri: str


def load_dependencies():
    global bigquery

    try:
        from google.cloud import bigquery as bigquery_module
    except ModuleNotFoundError as exc:
        raise SystemExit(
            f"Missing Python dependency: {exc.name}. "
            "Install dependencies with `pip install -r requirements.txt` "
            "or run the script inside the Airflow container."
        ) from exc

    bigquery = bigquery_module


def parse_gcs_uri(gcs_uri: str) -> RawLoadTarget:
    match = GCS_URI_PATTERN.match(gcs_uri)
    if not match:
        raise ValueError(
            "GCS URI must match "
            "gs://{bucket}/ecommerce/raw/{table}/extract_date=YYYY-MM-DD/batch_id={batch_id}/{table}.parquet"
        )

    table_name = match.group("table")
    if table_name not in SOURCE_TABLES:
        raise ValueError(f"Unsupported source table: {table_name}")

    return RawLoadTarget(
        table_name=table_name,
        batch_id=match.group("batch_id"),
        extract_date=match.group("extract_date"),
        gcs_uri=gcs_uri,
    )


def resolve_project_id(client, explicit_project: str | None) -> str:
    return explicit_project or os.getenv("GCP_PROJECT_ID") or client.project


def table_id(project_id: str, dataset_id: str, table_name: str) -> str:
    return f"{project_id}.{dataset_id}.{table_name}"


def ensure_dataset(client, project_id: str, dataset_id: str, location: str | None):
    dataset_ref = f"{project_id}.{dataset_id}"
    dataset = bigquery.Dataset(dataset_ref)
    if location:
        dataset.location = location
    client.create_dataset(dataset, exists_ok=True)


def ensure_audit_table(client, project_id: str, audit_dataset: str):
    audit_table_id = table_id(project_id, audit_dataset, "raw_load_runs")
    schema = [
        bigquery.SchemaField("batch_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("source_table", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("gcs_uri", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("target_table", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("row_count", "INT64"),
        bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("error_message", "STRING"),
        bigquery.SchemaField("started_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("finished_at", "TIMESTAMP"),
    ]
    table = bigquery.Table(audit_table_id, schema=schema)
    client.create_table(table, exists_ok=True)


def insert_audit_row(client, project_id: str, audit_dataset: str, row: dict):
    audit_table_id = table_id(project_id, audit_dataset, "raw_load_runs")
    errors = client.insert_rows_json(audit_table_id, [row])
    if errors:
        raise RuntimeError(f"Failed to insert raw load audit row: {errors}")


def delete_existing_batch(client, target_table_id: str, batch_id: str):
    query = f"""
        delete from `{target_table_id}`
        where batch_id = @batch_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("batch_id", "STRING", batch_id),
        ]
    )
    client.query(query, job_config=job_config).result()


def load_parquet(client, target: RawLoadTarget, target_table_id: str) -> int:
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    load_job = client.load_table_from_uri(target.gcs_uri, target_table_id, job_config=job_config)
    load_job.result()
    return int(load_job.output_rows or 0)


def set_loaded_at(client, target_table_id: str, batch_id: str):
    query = f"""
        update `{target_table_id}`
        set loaded_at = current_timestamp()
        where batch_id = @batch_id
          and loaded_at is null
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("batch_id", "STRING", batch_id),
        ]
    )
    client.query(query, job_config=job_config).result()


def load_one_uri(client, project_id: str, raw_dataset: str, audit_dataset: str, target: RawLoadTarget, dry_run: bool) -> int:
    target_table_id = table_id(project_id, raw_dataset, target.table_name)
    started_at = datetime.now(timezone.utc).isoformat()

    audit_base = {
        "batch_id": target.batch_id,
        "source_table": target.table_name,
        "gcs_uri": target.gcs_uri,
        "target_table": target_table_id,
        "started_at": started_at,
    }

    try:
        delete_existing_batch(client, target_table_id, target.batch_id)
        row_count = load_parquet(client, target, target_table_id)
        set_loaded_at(client, target_table_id, target.batch_id)
        insert_audit_row(
            client,
            project_id,
            audit_dataset,
            {
                **audit_base,
                "row_count": row_count,
                "status": "raw_load_success",
                "error_message": None,
                "finished_at": datetime.now(timezone.utc).isoformat(),
            },
        )
        return row_count
    except Exception as exc:
        if not dry_run:
            insert_audit_row(
                client,
                project_id,
                audit_dataset,
                {
                    **audit_base,
                    "row_count": None,
                    "status": "raw_load_failed",
                    "error_message": str(exc)[:2000],
                    "finished_at": datetime.now(timezone.utc).isoformat(),
                },
            )
        raise


def parse_args():
    parser = argparse.ArgumentParser(description="Load GCS Raw Zone Parquet files into BigQuery Raw tables.")
    parser.add_argument("--gcs-uri", action="append", required=True, help="GCS Parquet URI. Can be passed multiple times.")
    parser.add_argument("--project-id", default=None, help="GCP project id. Defaults to GCP_PROJECT_ID or ADC project.")
    parser.add_argument("--raw-dataset", default=None, help="Raw dataset. Defaults to BQ_RAW_DATASET.")
    parser.add_argument("--audit-dataset", default=None, help="Audit dataset. Defaults to BQ_AUDIT_DATASET.")
    parser.add_argument("--location", default=None, help="BigQuery dataset/job location.")
    parser.add_argument("--dry-run", action="store_true", help="Validate URI parsing and print intended operations.")
    return parser.parse_args()


def main():
    args = parse_args()
    targets = [parse_gcs_uri(uri) for uri in args.gcs_uri]
    raw_dataset = args.raw_dataset or os.getenv(RAW_DATASET_ENV, "ecommerce_raw")
    audit_dataset = args.audit_dataset or os.getenv(AUDIT_DATASET_ENV, "ecommerce_audit")

    if args.dry_run:
        project_id = args.project_id or os.getenv("GCP_PROJECT_ID", "your-gcp-project-id")
        for target in targets:
            target_table_id = table_id(project_id, raw_dataset, target.table_name)
            print(f"[dry-run] delete batch_id={target.batch_id} from {target_table_id}")
            print(f"[dry-run] load {target.gcs_uri} into {target_table_id}")
            print(
                f"{target.table_name}: batch_id={target.batch_id}, "
                f"extract_date={target.extract_date}, rows=0, "
                f"target={target_table_id}"
            )
        return

    load_dependencies()

    client = bigquery.Client(project=args.project_id or os.getenv("GCP_PROJECT_ID") or None)
    project_id = resolve_project_id(client, args.project_id)

    ensure_dataset(client, project_id, raw_dataset, args.location)
    ensure_dataset(client, project_id, audit_dataset, args.location)
    ensure_audit_table(client, project_id, audit_dataset)

    for target in targets:
        row_count = load_one_uri(client, project_id, raw_dataset, audit_dataset, target, args.dry_run)
        print(
            f"{target.table_name}: batch_id={target.batch_id}, "
            f"extract_date={target.extract_date}, rows={row_count}, "
            f"target={project_id}.{raw_dataset}.{target.table_name}"
        )


if __name__ == "__main__":
    main()
