#!/bin/sh
# Codespaces only: "upload" jobs + sample data into the shared volume that stands
# in for Azure Blob (STORAGE_ROOT=file:///data/shared). Runs inside the client container.
set -eu
ROOT="${STORAGE_ROOT#file://}"     # file:///data/shared -> /data/shared
mkdir -p "$ROOT/jobs" "$ROOT/data/raw"
cp /app/src/jobs/*.py "$ROOT/jobs/"
cp /app/data/sample/sales.csv "$ROOT/data/raw/sales.csv"
echo "Uploaded to $STORAGE_ROOT:"
ls -R "$ROOT/jobs" "$ROOT/data/raw"
