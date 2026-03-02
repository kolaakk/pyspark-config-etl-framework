#!/usr/bin/env bash
set -euo pipefail

JOB_NAME="${1:?job name required}"

: "${DATABRICKS_HOST:?DATABRICKS_HOST must be set}"
: "${DATABRICKS_TOKEN:?DATABRICKS_TOKEN must be set}"

echo "Locating job: ${JOB_NAME}"
JOB_ID=$(curl -sS -X GET "${DATABRICKS_HOST}/api/2.1/jobs/list?name=${JOB_NAME}" \
  -H "Authorization: Bearer ${DATABRICKS_TOKEN}" | \
  python -c "import sys,json; d=json.load(sys.stdin); print(d['jobs'][0]['job_id'] if d.get('jobs') else '')")

if [[ -z "${JOB_ID}" ]]; then
  echo "ERROR: Job not found: ${JOB_NAME}"
  exit 1
fi

echo "Triggering run-now for job_id=${JOB_ID}"
RUN_ID=$(curl -sS -X POST "${DATABRICKS_HOST}/api/2.1/jobs/run-now" \
  -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
  -H "Content-Type: application/json" \
  --data "{\"job_id\": ${JOB_ID}}" | \
  python -c "import sys,json; print(json.load(sys.stdin)['run_id'])")

echo "Started run_id=${RUN_ID}"
