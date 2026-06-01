#!/usr/bin/env bash
set -euo pipefail

SELECTOR="${1:?dbt selector is required}"
COMMAND="${2:-run}"

cd /opt/airflow/dbt/ecommerce_dw

if [ ! -f profiles.yml ]; then
  cp profiles.yml.example profiles.yml
fi

if [ -n "${DBT_VARS_JSON:-}" ]; then
  dbt "${COMMAND}" --select "${SELECTOR}" --profiles-dir . --vars "${DBT_VARS_JSON}"
else
  dbt "${COMMAND}" --select "${SELECTOR}" --profiles-dir .
fi
