from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path


def parse_args():
    parser = argparse.ArgumentParser(description="Load all GCS URIs from an extract manifest into BigQuery Raw.")
    parser.add_argument("--manifest-path", required=True)
    parser.add_argument("--project-id", default=None)
    parser.add_argument("--raw-dataset", default=None)
    parser.add_argument("--audit-dataset", default=None)
    parser.add_argument("--location", default=None)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main():
    args = parse_args()
    manifest = json.loads(Path(args.manifest_path).read_text())
    gcs_uris = [
        table["gcs_uri"]
        for table in manifest.get("tables", [])
        if table.get("gcs_uri")
    ]

    if not gcs_uris:
        raise SystemExit("No GCS URIs found in manifest. Raw load requires non-local extract output.")

    loader_script = Path(__file__).resolve().parent / "gcs_to_bigquery_raw.py"
    command = [sys.executable, str(loader_script)]
    for gcs_uri in gcs_uris:
        command.extend(["--gcs-uri", gcs_uri])

    optional_args = {
        "--project-id": args.project_id,
        "--raw-dataset": args.raw_dataset,
        "--audit-dataset": args.audit_dataset,
        "--location": args.location,
    }
    for option, value in optional_args.items():
        if value:
            command.extend([option, value])

    if args.dry_run:
        command.append("--dry-run")

    subprocess.run(command, check=True)


if __name__ == "__main__":
    main()
