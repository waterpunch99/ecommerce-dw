from __future__ import annotations

import argparse
import os
import tempfile
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
import json


PIPELINE_NAME = "ecommerce_daily_batch"
DEFAULT_WATERMARK = "1970-01-01T00:00:00"

SOURCE_TABLES = {
    "categories": "category_id",
    "customers": "customer_id",
    "products": "product_id",
    "coupons": "coupon_id",
    "orders": "order_id",
    "order_items": "order_item_id",
    "payments": "payment_id",
    "shipments": "shipment_id",
    "inventory_snapshots": "snapshot_date, product_id",
    "order_status_history": "order_status_history_id",
}

INT_COLUMNS = {
    "categories": ["category_id", "parent_category_id", "category_level"],
    "customers": ["customer_id"],
    "products": ["product_id", "category_id"],
    "coupons": ["coupon_id"],
    "orders": ["order_id", "customer_id", "coupon_id"],
    "order_items": ["order_item_id", "order_id", "product_id", "quantity"],
    "payments": ["payment_id", "order_id"],
    "shipments": ["shipment_id", "order_id"],
    "inventory_snapshots": ["product_id", "category_id", "stock_quantity", "reserved_quantity", "available_quantity"],
    "order_status_history": ["order_status_history_id", "order_id"],
}

pandas = None
psycopg2 = None
storage = None


@dataclass(frozen=True)
class ExtractResult:
    table_name: str
    row_count: int
    local_path: Path
    gcs_uri: str | None
    last_success_watermark: datetime
    current_run_watermark: datetime


def load_dependencies():
    global pandas, psycopg2, storage

    try:
        import pandas as pandas_module
        import psycopg2 as psycopg2_module
        from google.cloud import storage as storage_module
    except ModuleNotFoundError as exc:
        raise SystemExit(
            f"Missing Python dependency: {exc.name}. "
            "Install dependencies with `pip install -r requirements.txt` "
            "or run the script inside the Airflow container."
        ) from exc

    pandas = pandas_module
    psycopg2 = psycopg2_module
    storage = storage_module


def connect():
    return psycopg2.connect(
        host=os.getenv("SOURCE_POSTGRES_HOST", "localhost"),
        port=int(os.getenv("SOURCE_POSTGRES_PORT", "5432")),
        dbname=os.getenv("SOURCE_POSTGRES_DB", "ecommerce"),
        user=os.getenv("SOURCE_POSTGRES_USER", "ecommerce_user"),
        password=os.getenv("SOURCE_POSTGRES_PASSWORD", "ecommerce_password"),
    )


def parse_datetime(value: str) -> datetime:
    normalized = value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def quote_identifier(identifier: str) -> str:
    if identifier not in SOURCE_TABLES:
        raise ValueError(f"Unsupported source table: {identifier}")
    return f'"{identifier}"'


def ensure_watermark_row(cur, table_name: str):
    cur.execute(
        """
        insert into etl_watermarks (
            pipeline_name, source_table, last_success_watermark, status
        )
        values (%s, %s, %s, 'not_started')
        on conflict (pipeline_name, source_table) do nothing
        """,
        (PIPELINE_NAME, table_name, parse_datetime(DEFAULT_WATERMARK)),
    )


def get_last_success_watermark(cur, table_name: str, start_watermark: datetime | None = None) -> datetime:
    ensure_watermark_row(cur, table_name)
    if start_watermark is not None:
        return start_watermark

    cur.execute(
        """
        select last_success_watermark
        from etl_watermarks
        where pipeline_name = %s
          and source_table = %s
        """,
        (PIPELINE_NAME, table_name),
    )
    row = cur.fetchone()
    return row[0]


def mark_extract_started(cur, batch_id: str, table_name: str, extract_date: date, last_success: datetime, current_run: datetime):
    cur.execute(
        """
        insert into etl_batch_runs (
            batch_id, pipeline_name, source_table, extract_date,
            last_success_watermark, current_run_watermark, status
        )
        values (%s, %s, %s, %s, %s, %s, 'extract_running')
        on conflict (batch_id, pipeline_name, source_table) do update set
            extract_date = excluded.extract_date,
            last_success_watermark = excluded.last_success_watermark,
            current_run_watermark = excluded.current_run_watermark,
            gcs_uri = null,
            row_count = 0,
            status = 'extract_running',
            error_message = null,
            started_at = current_timestamp,
            finished_at = null
        """,
        (batch_id, PIPELINE_NAME, table_name, extract_date, last_success, current_run),
    )

    cur.execute(
        """
        update etl_watermarks
        set current_run_watermark = %s,
            last_run_at = current_timestamp,
            status = 'extract_running',
            row_count = 0,
            error_message = null,
            updated_at = current_timestamp
        where pipeline_name = %s
          and source_table = %s
        """,
        (current_run, PIPELINE_NAME, table_name),
    )


def mark_extract_success(cur, batch_id: str, table_name: str, gcs_uri: str | None, row_count: int):
    cur.execute(
        """
        update etl_batch_runs
        set gcs_uri = %s,
            row_count = %s,
            status = 'extract_success',
            error_message = null,
            finished_at = current_timestamp
        where batch_id = %s
          and pipeline_name = %s
          and source_table = %s
        """,
        (gcs_uri, row_count, batch_id, PIPELINE_NAME, table_name),
    )

    cur.execute(
        """
        update etl_watermarks
        set status = 'extract_success',
            row_count = %s,
            error_message = null,
            updated_at = current_timestamp
        where pipeline_name = %s
          and source_table = %s
        """,
        (row_count, PIPELINE_NAME, table_name),
    )


def mark_extract_failed(cur, batch_id: str, table_name: str, error_message: str):
    cur.execute(
        """
        update etl_batch_runs
        set status = 'extract_failed',
            error_message = %s,
            finished_at = current_timestamp
        where batch_id = %s
          and pipeline_name = %s
          and source_table = %s
        """,
        (error_message[:2000], batch_id, PIPELINE_NAME, table_name),
    )

    cur.execute(
        """
        update etl_watermarks
        set status = 'extract_failed',
            error_message = %s,
            updated_at = current_timestamp
        where pipeline_name = %s
          and source_table = %s
        """,
        (error_message[:2000], PIPELINE_NAME, table_name),
    )


def extract_dataframe(conn, table_name: str, last_success: datetime, current_run: datetime, extract_date: date, batch_id: str):
    table_identifier = quote_identifier(table_name)
    query = f"""
        select
            *,
            %s::date as extract_date,
            %s::timestamp as extracted_at,
            %s::text as source_table,
            %s::text as batch_id
        from {table_identifier}
        where updated_at > %s
          and updated_at <= %s
        order by updated_at, {SOURCE_TABLES[table_name]}
    """
    extracted_at = datetime.utcnow()
    df = pandas.read_sql_query(
        query,
        conn,
        params=(extract_date, extracted_at, table_name, batch_id, last_success, current_run),
        coerce_float=False,
    )
    for column in INT_COLUMNS.get(table_name, []):
        if column in df.columns:
            df[column] = df[column].astype("Int64")
    return df


def write_parquet(df, output_dir: Path, table_name: str, extract_date: date, batch_id: str) -> Path:
    path = output_dir / table_name / f"extract_date={extract_date.isoformat()}" / f"batch_id={batch_id}"
    path.mkdir(parents=True, exist_ok=True)
    file_path = path / f"{table_name}.parquet"
    df.to_parquet(
        file_path,
        engine="pyarrow",
        index=False,
        coerce_timestamps="us",
        allow_truncated_timestamps=True,
    )
    return file_path


def build_gcs_object_name(table_name: str, extract_date: date, batch_id: str) -> str:
    return (
        f"ecommerce/raw/{table_name}/"
        f"extract_date={extract_date.isoformat()}/"
        f"batch_id={batch_id}/{table_name}.parquet"
    )


def upload_to_gcs(local_path: Path, bucket_name: str, table_name: str, extract_date: date, batch_id: str) -> str:
    client = storage.Client(project=os.getenv("GCP_PROJECT_ID") or None)
    bucket = client.bucket(bucket_name)
    object_name = build_gcs_object_name(table_name, extract_date, batch_id)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(str(local_path))
    return f"gs://{bucket_name}/{object_name}"


def extract_table(
    conn,
    table_name: str,
    extract_date: date,
    batch_id: str,
    current_run: datetime,
    output_dir: Path,
    bucket_name: str | None,
    local_only: bool,
    start_watermark: datetime | None = None,
) -> ExtractResult:
    with conn.cursor() as cur:
        last_success = get_last_success_watermark(cur, table_name, start_watermark)
        mark_extract_started(cur, batch_id, table_name, extract_date, last_success, current_run)
    conn.commit()

    try:
        df = extract_dataframe(conn, table_name, last_success, current_run, extract_date, batch_id)
        local_path = write_parquet(df, output_dir, table_name, extract_date, batch_id)
        gcs_uri = None
        if not local_only:
            if not bucket_name:
                raise ValueError("GCS bucket name is required unless --local-only is set")
            gcs_uri = upload_to_gcs(local_path, bucket_name, table_name, extract_date, batch_id)

        with conn.cursor() as cur:
            mark_extract_success(cur, batch_id, table_name, gcs_uri, len(df))
        conn.commit()
        return ExtractResult(table_name, len(df), local_path, gcs_uri, last_success, current_run)
    except Exception as exc:
        conn.rollback()
        with conn.cursor() as cur:
            mark_extract_failed(cur, batch_id, table_name, str(exc))
        conn.commit()
        raise


def resolve_tables(table_arg: str) -> list[str]:
    if table_arg == "all":
        return list(SOURCE_TABLES)
    requested = [table.strip() for table in table_arg.split(",") if table.strip()]
    unsupported = sorted(set(requested) - set(SOURCE_TABLES))
    if unsupported:
        raise ValueError(f"Unsupported source tables: {', '.join(unsupported)}")
    return requested


def parse_args():
    parser = argparse.ArgumentParser(description="Extract Postgres source tables to GCS Raw Zone as Parquet.")
    parser.add_argument("--table", default="all", help="Source table name, comma-separated names, or all.")
    parser.add_argument("--batch-id", default=None, help="Idempotent batch id. Defaults to a generated UUID.")
    parser.add_argument("--extract-date", default=None, help="Extract date in YYYY-MM-DD. Defaults to today.")
    parser.add_argument("--start-watermark", default=None, help="Optional lower watermark override for backfill/reprocessing.")
    parser.add_argument("--current-run-watermark", default=None, help="Upper watermark timestamp. Defaults to current UTC time.")
    parser.add_argument("--output-dir", default="/tmp/ecommerce_raw_extracts", help="Local directory for Parquet files.")
    parser.add_argument("--bucket", default=None, help="GCS bucket name. Defaults to GCS_BUCKET_NAME.")
    parser.add_argument("--manifest-path", default=None, help="Write extract results as a JSON manifest.")
    parser.add_argument("--watermark-mode", choices=["advance", "preserve"], default="advance")
    parser.add_argument("--local-only", action="store_true", help="Write Parquet locally without uploading to GCS.")
    return parser.parse_args()


def main():
    args = parse_args()
    load_dependencies()

    tables = resolve_tables(args.table)
    batch_id = args.batch_id or f"batch_{uuid.uuid4().hex}"
    extract_date = date.fromisoformat(args.extract_date) if args.extract_date else date.today()
    start_watermark = parse_datetime(args.start_watermark) if args.start_watermark else None
    current_run = parse_datetime(args.current_run_watermark) if args.current_run_watermark else datetime.utcnow()
    if start_watermark is not None and start_watermark >= current_run:
        raise ValueError("start_watermark must be earlier than current_run_watermark")
    output_dir = Path(args.output_dir)
    bucket_name = args.bucket or os.getenv("GCS_BUCKET_NAME")

    results = []
    with connect() as conn:
        for table_name in tables:
            result = extract_table(
                conn=conn,
                table_name=table_name,
                extract_date=extract_date,
                batch_id=batch_id,
                current_run=current_run,
                output_dir=output_dir,
                bucket_name=bucket_name,
                local_only=args.local_only,
                start_watermark=start_watermark,
            )
            results.append(result)

    for result in results:
        target = result.gcs_uri or str(result.local_path)
        print(
            f"{result.table_name}: rows={result.row_count}, "
            f"last_success_watermark={result.last_success_watermark}, "
            f"current_run_watermark={result.current_run_watermark}, "
            f"target={target}"
        )

    if args.manifest_path:
        manifest_path = Path(args.manifest_path)
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        manifest = {
            "pipeline_name": PIPELINE_NAME,
            "batch_id": batch_id,
            "extract_date": extract_date.isoformat(),
            "watermark_mode": args.watermark_mode,
            "current_run_watermark": current_run.isoformat(),
            "tables": [
                {
                    "table_name": result.table_name,
                    "row_count": result.row_count,
                    "local_path": str(result.local_path),
                    "gcs_uri": result.gcs_uri,
                    "last_success_watermark": result.last_success_watermark.isoformat(),
                    "current_run_watermark": result.current_run_watermark.isoformat(),
                }
                for result in results
            ],
        }
        manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True))
        print(f"manifest_path={manifest_path}")


if __name__ == "__main__":
    main()
