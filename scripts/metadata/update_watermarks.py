from __future__ import annotations

import argparse
import json
import os
from datetime import datetime
from pathlib import Path


psycopg2 = None


def load_dependencies():
    global psycopg2

    try:
        import psycopg2 as psycopg2_module
    except ModuleNotFoundError as exc:
        raise SystemExit(
            f"Missing Python dependency: {exc.name}. "
            "Install dependencies with `pip install -r requirements.txt` "
            "or run the script inside the Airflow container."
        ) from exc

    psycopg2 = psycopg2_module


def connect():
    return psycopg2.connect(
        host=os.getenv("SOURCE_POSTGRES_HOST", "localhost"),
        port=int(os.getenv("SOURCE_POSTGRES_PORT", "5432")),
        dbname=os.getenv("SOURCE_POSTGRES_DB", "ecommerce"),
        user=os.getenv("SOURCE_POSTGRES_USER", "ecommerce_user"),
        password=os.getenv("SOURCE_POSTGRES_PASSWORD", "ecommerce_password"),
    )


def parse_args():
    parser = argparse.ArgumentParser(description="Update source table watermarks after full pipeline success.")
    parser.add_argument("--manifest-path", required=True)
    parser.add_argument("--watermark-mode", choices=["advance", "preserve"], default=None)
    parser.add_argument("--status", default="success")
    return parser.parse_args()


def main():
    args = parse_args()

    manifest = json.loads(Path(args.manifest_path).read_text())
    pipeline_name = manifest["pipeline_name"]
    current_run_watermark = datetime.fromisoformat(manifest["current_run_watermark"])
    watermark_mode = args.watermark_mode or manifest.get("watermark_mode", "advance")

    if watermark_mode == "preserve":
        print("watermark_mode=preserve; last_success_watermark will not be advanced")
        return

    load_dependencies()

    with connect() as conn:
        with conn.cursor() as cur:
            for table in manifest["tables"]:
                source_table = table["table_name"]
                row_count = int(table["row_count"])
                cur.execute(
                    """
                    update etl_watermarks
                    set last_success_watermark = %s,
                        current_run_watermark = %s,
                        last_run_at = current_timestamp,
                        status = %s,
                        row_count = %s,
                        error_message = null,
                        updated_at = current_timestamp
                    where pipeline_name = %s
                      and source_table = %s
                    """,
                    (
                        current_run_watermark,
                        current_run_watermark,
                        args.status,
                        row_count,
                        pipeline_name,
                        source_table,
                    ),
                )
                print(f"{source_table}: last_success_watermark={current_run_watermark}, row_count={row_count}")


if __name__ == "__main__":
    main()
