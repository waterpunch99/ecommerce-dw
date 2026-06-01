from __future__ import annotations

import argparse
import os
import re
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path


bigquery = None


@dataclass(frozen=True)
class QualityCheck:
    name: str
    check_type: str
    target_dataset: str
    target_table: str
    severity: str
    sql: str


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


def parse_quality_checks(path: Path) -> list[QualityCheck]:
    content = path.read_text()
    chunks = [chunk.strip() for chunk in re.split(r"(?m)^-- name:\s*", content) if chunk.strip()]
    checks = []

    for chunk in chunks:
        lines = chunk.splitlines()
        name = lines[0].strip()
        metadata = {"name": name}
        sql_start = 1

        for index, line in enumerate(lines[1:], start=1):
            if not line.startswith("-- "):
                sql_start = index
                break
            key, _, value = line[3:].partition(":")
            metadata[key.strip().replace("-", "_")] = value.strip()
        else:
            raise ValueError(f"Quality check {name} has no SQL body")

        sql = "\n".join(lines[sql_start:]).strip().rstrip(";")
        required_keys = ["type", "target_dataset", "target_table", "severity"]
        missing = [key for key in required_keys if not metadata.get(key)]
        if missing:
            raise ValueError(f"Quality check {name} missing metadata: {', '.join(missing)}")

        checks.append(
            QualityCheck(
                name=name,
                check_type=metadata["type"],
                target_dataset=metadata["target_dataset"],
                target_table=metadata["target_table"],
                severity=metadata["severity"].upper(),
                sql=sql,
            )
        )

    return checks


def ensure_audit_dataset_and_table(client, project_id: str, audit_dataset: str, location: str | None):
    dataset = bigquery.Dataset(f"{project_id}.{audit_dataset}")
    if location:
        dataset.location = location
    client.create_dataset(dataset, exists_ok=True)

    table_id = f"{project_id}.{audit_dataset}.data_quality_results"
    schema = [
        bigquery.SchemaField("check_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("check_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("check_type", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("target_dataset", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("target_table", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("severity", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("failed_row_count", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("checked_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("batch_id", "STRING"),
        bigquery.SchemaField("execution_date", "DATE"),
        bigquery.SchemaField("error_message", "STRING"),
    ]
    table = bigquery.Table(table_id, schema=schema)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="checked_at",
    )
    table.clustering_fields = ["target_table", "check_type", "status"]
    client.create_table(table, exists_ok=True)


def render_sql(sql: str, project_id: str) -> str:
    return sql.replace("{project_id}", project_id)


def run_check(client, check: QualityCheck, project_id: str) -> tuple[int, str | None]:
    query = render_sql(check.sql, project_id)
    rows = list(client.query(query).result())
    if not rows:
        return 0, None

    row = rows[0]
    failed_row_count = int(row["failed_row_count"] or 0)
    return failed_row_count, None


def insert_result(client, project_id: str, audit_dataset: str, result: dict):
    table_id = f"{project_id}.{audit_dataset}.data_quality_results"
    errors = client.insert_rows_json(table_id, [result])
    if errors:
        raise RuntimeError(f"Failed to insert data quality result: {errors}")


def parse_args():
    parser = argparse.ArgumentParser(description="Run SQL-based BigQuery data quality checks.")
    parser.add_argument("--checks-file", default="sql/bigquery/quality_checks.sql")
    parser.add_argument("--project-id", default=None)
    parser.add_argument("--audit-dataset", default=None)
    parser.add_argument("--location", default=None)
    parser.add_argument("--batch-id", default=None)
    parser.add_argument("--execution-date", default=None)
    parser.add_argument("--check-name", action="append", help="Run only selected check name. Can be passed multiple times.")
    parser.add_argument("--dry-run", action="store_true", help="Parse checks and print names without running BigQuery queries.")
    return parser.parse_args()


def main():
    args = parse_args()
    checks = parse_quality_checks(Path(args.checks_file))

    if args.check_name:
        selected = set(args.check_name)
        checks = [check for check in checks if check.name in selected]
        missing = selected - {check.name for check in checks}
        if missing:
            raise SystemExit(f"Unknown quality check names: {', '.join(sorted(missing))}")

    project_id = args.project_id or os.getenv("GCP_PROJECT_ID", "your-gcp-project-id")
    audit_dataset = args.audit_dataset or os.getenv("BQ_AUDIT_DATASET", "ecommerce_audit")
    execution_date = date.fromisoformat(args.execution_date) if args.execution_date else date.today()
    batch_id = args.batch_id

    if args.dry_run:
        for check in checks:
            print(f"[dry-run] {check.severity} {check.name} ({check.check_type}) -> {check.target_dataset}.{check.target_table}")
        print(f"total_checks={len(checks)}")
        return

    load_dependencies()
    client = bigquery.Client(project=args.project_id or os.getenv("GCP_PROJECT_ID") or None)
    project_id = args.project_id or os.getenv("GCP_PROJECT_ID") or client.project
    ensure_audit_dataset_and_table(client, project_id, audit_dataset, args.location)

    failed_error_checks = []
    for check in checks:
        checked_at = datetime.now(timezone.utc)
        check_id = f"{execution_date.isoformat()}_{check.name}_{uuid.uuid4().hex[:8]}"
        error_message = None

        try:
            failed_row_count, error_message = run_check(client, check, project_id)
            status = "pass" if failed_row_count == 0 else "fail"
        except Exception as exc:
            failed_row_count = -1
            status = "error"
            error_message = str(exc)[:2000]

        result = {
            "check_id": check_id,
            "check_name": check.name,
            "check_type": check.check_type,
            "target_dataset": check.target_dataset,
            "target_table": check.target_table,
            "severity": check.severity,
            "status": status,
            "failed_row_count": failed_row_count,
            "checked_at": checked_at.isoformat(),
            "batch_id": batch_id,
            "execution_date": execution_date.isoformat(),
            "error_message": error_message,
        }
        insert_result(client, project_id, audit_dataset, result)
        print(f"{check.name}: status={status}, failed_row_count={failed_row_count}, severity={check.severity}")

        if check.severity == "ERROR" and status != "pass":
            failed_error_checks.append(check.name)

    if failed_error_checks:
        raise SystemExit(f"Data quality failed: {', '.join(failed_error_checks)}")


if __name__ == "__main__":
    main()

