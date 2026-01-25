#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="${1:?repo_root required}"
STAGING_DIR="${2:?staging_dir required}"

mkdir -p "${STAGING_DIR}"

# Copy wheel(s)
mkdir -p "${STAGING_DIR}/dist"
cp -v "${REPO_ROOT}/dist/"*.whl "${STAGING_DIR}/dist/"

# Copy configs (all envs)
mkdir -p "${STAGING_DIR}/configs"
cp -Rv "${REPO_ROOT}/configs"/* "${STAGING_DIR}/configs/" || true

# Copy databricks deployment files
mkdir -p "${STAGING_DIR}/databricks"
cp -Rv "${REPO_ROOT}/devops/databricks"/* "${STAGING_DIR}/databricks/"

# Copy helper scripts used in release stages
mkdir -p "${STAGING_DIR}/scripts"
cp -v "${REPO_ROOT}/devops/scripts/databricks_"*.sh "${STAGING_DIR}/scripts/"

echo "Packaged artifact contents:"
find "${STAGING_DIR}" -maxdepth 3 -type f -print
