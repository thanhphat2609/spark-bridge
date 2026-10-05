#!/usr/bin/env bash
# =============================================================================
# Upload PySpark jobs + demo data to Azure (ADLS Gen2) under STORAGE_ROOT.
# Run on the client VM (or in CI on merge to main). Needs: Azure CLI (`az`).
#
#   export AZURE_STORAGE_ACCOUNT=sparkbridgedemo
#   export AZURE_STORAGE_CONTAINER=spark
#   export STORAGE_PREFIX=spark-bridge            # folder inside the container
#   export AZURE_STORAGE_KEY=...                  # or: az login + AUTH_MODE=login
#   bash scripts/deploy_jobs_azure.sh
# =============================================================================
set -euo pipefail

: "${AZURE_STORAGE_ACCOUNT:?set AZURE_STORAGE_ACCOUNT}"
CONTAINER="${AZURE_STORAGE_CONTAINER:-spark}"
PREFIX="${STORAGE_PREFIX:-spark-bridge}"
AUTH_MODE="${AUTH_MODE:-key}"                       # key | login

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

auth_args=(--account-name "$AZURE_STORAGE_ACCOUNT")
if [[ "$AUTH_MODE" == "login" ]]; then
  auth_args+=(--auth-mode login)
else
  : "${AZURE_STORAGE_KEY:?set AZURE_STORAGE_KEY (or AUTH_MODE=login)}"
  auth_args+=(--account-key "$AZURE_STORAGE_KEY")
fi

az storage container create -n "$CONTAINER" "${auth_args[@]}" -o none

echo "Uploading jobs -> abfss://$CONTAINER@$AZURE_STORAGE_ACCOUNT.dfs.core.windows.net/$PREFIX/jobs"
az storage blob upload-batch "${auth_args[@]}" --overwrite -o none \
  -d "$CONTAINER" --destination-path "$PREFIX/jobs" \
  -s "$REPO_ROOT/src/jobs" --pattern "*.py"

echo "Uploading demo data -> .../$PREFIX/data/raw"
az storage blob upload "${auth_args[@]}" --overwrite -o none \
  -c "$CONTAINER" -n "$PREFIX/data/raw/sales.csv" \
  -f "$REPO_ROOT/data/sample/sales.csv"

echo "Done. STORAGE_ROOT=abfss://$CONTAINER@$AZURE_STORAGE_ACCOUNT.dfs.core.windows.net/$PREFIX"
