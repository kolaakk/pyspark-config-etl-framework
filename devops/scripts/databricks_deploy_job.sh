#!/usr/bin/env bash
set -euo pipefail

WHEEL_LOCAL="${1:?wheel local path required}"
DBFS_WHEEL_PATH="${2:?dbfs wheel path required}"
JOB_DEF_TEMPLATE="${3:?job definition template json required}"
CONFIG_LOCAL="${4:?config file required}"
JOB_NAME="${5:?job name required}"

: "${DATABRICKS_HOST:?DATABRICKS_HOST must be set}"
: "${DATABRICKS_TOKEN:?DATABRICKS_TOKEN must be set}"

# Optional, but recommended to use existing cluster
: "${DATABRICKS_CLUSTER_ID:?DATABRICKS_CLUSTER_ID must be set (existing cluster id)}"

# Upload wheel to DBFS
echo "Uploading wheel to ${DBFS_WHEEL_PATH} ..."
curl -sS -X POST "${DATABRICKS_HOST}/api/2.0/dbfs/put" \
  -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
  -F "path=${DBFS_WHEEL_PATH#dbfs:}" \
  -F "overwrite=true" \
  -F "contents=@${WHEEL_LOCAL}" >/dev/null

# Upload config to DBFS (so cluster can access it)
CONFIG_DBFS_PATH="dbfs:/FileStore/etl/configs/$(basename "${CONFIG_LOCAL}")"
echo "Uploading config to ${CONFIG_DBFS_PATH} ..."
curl -sS -X POST "${DATABRICKS_HOST}/api/2.0/dbfs/put" \
  -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
  -F "path=${CONFIG_DBFS_PATH#dbfs:}" \
  -F "overwrite=true" \
  -F "contents=@${CONFIG_LOCAL}" >/dev/null

# Prepare job JSON by substituting placeholders
TMP_JOB_JSON="$(mktemp)"
sed \
  -e "s|__JOB_NAME__|${JOB_NAME}|g" \
  -e "s|__CLUSTER_ID__|${DATABRICKS_CLUSTER_ID}|g" \
  -e "s|__WHEEL_DBFS_PATH__|${DBFS_WHEEL_PATH}|g" \
  -e "s|__CONFIG_DBFS_PATH__|${CONFIG_DBFS_PATH}|g" \
  "${JOB_DEF_TEMPLATE}" > "${TMP_JOB_JSON}"

echo "Job definition:"
cat "${TMP_JOB_JSON}"

# Find existing job by name
echo "Checking if job exists: ${JOB_NAME}"
JOB_ID=$(curl -sS -X GET "${DATABRICKS_HOST}/api/2.1/jobs/list?name=${JOB_NAME}" \
  -H "Authorization: Bearer ${DATABRICKS_TOKEN}" | \
  python -c "import sys,json; d=json.load(sys.stdin); print(d['jobs'][0]['job_id'] if d.get('jobs') else '')" || true)

if [[ -z "${JOB_ID}" ]]; then
  echo "Creating job..."
  curl -sS -X POST "${DATABRICKS_HOST}/api/2.1/jobs/create" \
    -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
    -H "Content-Type: application/json" \
    --data @"${TMP_JOB_JSON}" | tee /dev/stderr | \
    python -c "import sys,json; print(json.load(sys.stdin)['job_id'])" >/dev/null
  echo "Job created."
else
  echo "Updating job_id=${JOB_ID}..."
  # jobs/reset expects {job_id, new_settings}
  RESET_JSON="$(mktemp)"
  python - <<PY
import json
job_id = int("${JOB_ID}")
with open("${TMP_JOB_JSON}", "r", encoding="utf-8") as f:
    new_settings = json.load(f)
payload = {"job_id": job_id, "new_settings": new_settings}
print(json.dumps(payload))
PY > "${RESET_JSON}"

  curl -sS -X POST "${DATABRICKS_HOST}/api/2.1/jobs/reset" \
    -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
    -H "Content-Type: application/json" \
    --data @"${RESET_JSON}" >/dev/null
  echo "Job updated."
fi
