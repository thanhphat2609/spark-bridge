#!/usr/bin/env bash
# =============================================================================
# QA launch test on GitHub Codespaces (or any single Docker host).
#   1. build + start master (RM, History, Livy) and worker (NodeManager)
#   2. wait for Livy and for the NodeManager to register with YARN
#   3. run the client container: upload jobs/data -> submit via Livy -> PASS/FAIL
#
#   bash scripts/archive/codespaces_smoke_test.sh          # run
#   bash scripts/archive/codespaces_smoke_test.sh down     # stop and delete volumes
# =============================================================================
set -euo pipefail
cd "$(dirname "$0")/../.."
COMPOSE="docker compose -f infrastructure/codespaces/docker-compose.yml"

if ! command -v docker >/dev/null 2>&1 || ! docker info >/dev/null 2>&1; then
  echo "ERROR: Docker is not available in this environment."
  echo "  Codespaces: Ctrl/Cmd+Shift+P -> 'Codespaces: Rebuild Container' (uses .devcontainer/devcontainer.json)."
  echo "  If it still fails, check 'Codespaces: View Creation Log' for a recovery-mode error."
  exit 1
fi

if [[ "${1:-}" == "down" ]]; then
  $COMPOSE --profile client down -v
  exit 0
fi

echo ">> Building and starting the cluster (first build downloads ~1.2 GB) ..."
$COMPOSE up -d --build

wait_for() {  # wait_for <description> <command...>
  local desc="$1"; shift
  for _ in $(seq 1 60); do
    if "$@" >/dev/null 2>&1; then echo "   OK: $desc"; return 0; fi
    sleep 5
  done
  echo "   TIMEOUT: $desc"; $COMPOSE ps; $COMPOSE logs --tail=80; exit 1
}

echo ">> Waiting for services ..."
wait_for "YARN ResourceManager :8088" curl -sf http://localhost:8088/ws/v1/cluster/info
wait_for "NodeManager registered"    sh -c 'curl -sf http://localhost:8088/ws/v1/cluster/metrics | grep -q "\"activeNodes\":[1-9]"'
wait_for "Livy :8998"                curl -sf http://localhost:8998/batches
wait_for "History Server :18080"     curl -sf http://localhost:18080/api/v1/applications

echo ">> Submitting demo jobs through Livy ..."
$COMPOSE run --rm client

echo
echo "UIs (Codespaces 'Ports' tab): Livy 8998 · YARN 8088 · History 18080 · NodeManager 8042"
