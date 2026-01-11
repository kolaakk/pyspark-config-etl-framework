#!/usr/bin/env bash
set -euo pipefail

CONFIG_PATH="${1:-}"
if [[ -z "${CONFIG_PATH}" ]]; then
  echo "Usage: bash scripts/spark_submit.sh <config.json>"
  exit 1
fi

spark-submit -m etl_framework.runner "${CONFIG_PATH}"
