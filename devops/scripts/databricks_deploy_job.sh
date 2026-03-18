#!/usr/bin/env bash
set -euo pipefail

WHEEL_LOCAL="${1:?wheel local path required}"
DBFS_WHEEL_PATH="${2:?dbfs wheel path required}"
JOB_DEF_TEMPLATE="${3:?job definition template json required}"
CONFIG_LOCAL="${4:?config file required}"
JOB_NAME="${5:?job name required}"

: "${DATABRICKS_HOST:?DATABRICKS_HOST must be set}"
: "${DATABRICKS_TOKEN:?DATABRICKS_TOKEN must be set}"
: "${DATABRICKS_CLUSTER_ID:?DATABRICKS_CLUSTER_ID must be set (existing cluster id)}"

# -------------------------------
# Upload wheel to DBFS
# -------------------------------
if [[ ! -f "${WHEEL_LOCAL}" ]]; then
  echo "ERROR: Wheel file not found: ${WHEEL_LOCAL}"
  exit 1
fi

echo "Uploading wheel to ${DBFS_WHEEL_PATH} ..."
curl -sS -X POST "${DATABRICKS_HOST}/api/2.0/dbfs/put" \
  -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
  -F "path=${DBFS_WHEEL_PATH#dbfs:}" \
  -F "overwrite=true" \
  -F "contents=@${WHEEL_LOCAL}" >/dev/null

# -------------------------------
# Upload ETL entrypoint to DBFS
# -------------------------------
ENTRYPOINT_LOCAL="$(cd "$(dirname "$0")" && pwd)/../databricks/run_etl.py"
ENTRYPOINT_DBFS="dbfs:/FileStore/etl/entrypoints/run_etl.py"

if [[ ! -f "${ENTRYPOINT_LOCAL}" ]]; then
  echo "ERROR: Entrypoint file not found: ${ENTRYPOINT_LOCAL}"
  exit 1
fi

echo "Uploading entrypoint to ${ENTRYPOINT_DBFS} ..."
curl -sS -X POST "${DATABRICKS_HOST}/api/2.0/dbfs/put" \
  -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
  -F "path=${ENTRYPOINT_DBFS#dbfs:}" \
  -F "overwrite=true" \
  -F "contents=@${ENTRYPOINT_LOCAL}" >/dev/null

# -------------------------------
# Upload config to DBFS
# -------------------------------
if [[ ! -f "${CONFIG_LOCAL}" ]]; then
  echo "ERROR: Config file not found: ${CONFIG_LOCAL}"
  exit 1
fi

# Avoid collisions across envs/jobs
CONFIG_DBFS_PATH="dbfs:/FileStore/etl/configs/${JOB_NAME}/$(basename "${CONFIG_LOCAL}")"

echo "Uploading config to ${CONFIG_DBFS_PATH} ..."
curl -sS -X POST "${DATABRICKS_HOST}/api/2.0/dbfs/put" \
  -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
  -F "path=${CONFIG_DBFS_PATH#dbfs:}" \
  -F "overwrite=true" \
  -F "contents=@${CONFIG_LOCAL}" >/dev/null

# -------------------------------
# Prepare job JSON by substituting placeholders
# -------------------------------
TMP_JOB_JSON="$(mktemp)"
sed \
  -e "s|__JOB_NAME__|${JOB_NAME}|g" \
  -e "s|__CLUSTER_ID__|${DATABRICKS_CLUSTER_ID}|g" \
  -e "s|__WHEEL_DBFS_PATH__|${DBFS_WHEEL_PATH}|g" \
  -e "s|__CONFIG_DBFS_PATH__|${CONFIG_DBFS_PATH}|g" \
  "${JOB_DEF_TEMPLATE}" > "${TMP_JOB_JSON}"

echo "Job definition (rendered):"
cat "${TMP_JOB_JSON}"

# -------------------------------
# Find existing job by name (robust)
# -------------------------------
echo "Checking if job exists: ${JOB_NAME}"
JOB_ID="$(
  curl -sS -X GET "${DATABRICKS_HOST}/api/2.1/jobs/list" \
    -H "Authorization: Bearer ${DATABRICKS_TOKEN}" | \
    python - <<PY
import sys, json
d = json.load(sys.stdin)
jobs = d.get("jobs", [])
name = "${JOB_NAME}"
m = [j for j in jobs if j.get("settings", {}).get("name") == name]
print(m[0]["job_id"] if m else "")
PY
)"

if [[ -z "${JOB_ID}" ]]; then
  echo "Creating job..."
  curl -sS -X POST "${DATABRICKS_HOST}/api/2.1/jobs/create" \
    -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
    -H "Content-Type: application/json" \
    --data @"${TMP_JOB_JSON}" >/dev/null
  echo "Job created."
else
  echo "Updating job_id=${JOB_ID}..."

  RESET_JSON="$(mktemp)"
  python - <<PY > "${RESET_JSON}"
import json
job_id = int("${JOB_ID}")
with open("${TMP_JOB_JSON}", "r", encoding="utf-8") as f:
    new_settings = json.load(f)
payload = {"job_id": job_id, "new_settings": new_settings}
print(json.dumps(payload))
PY

  curl -sS -X POST "${DATABRICKS_HOST}/api/2.1/jobs/reset" \
    -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
    -H "Content-Type: application/json" \
    --data @"${RESET_JSON}" >/dev/null
  echo "Job updated."
fi

echo "Deploy script completed successfully."
