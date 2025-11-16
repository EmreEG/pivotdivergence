#!/bin/bash
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")" && pwd)"
START_SCRIPT="$REPO_DIR/start.sh"

stop_group() {
  local label="$1"
  local pattern="$2"
  if ! command -v pgrep >/dev/null 2>&1; then
    echo "pgrep not found; cannot stop $label"
    return
  fi
  mapfile -t pids < <(pgrep -f "$pattern" || true)
  if [ ${#pids[@]} -eq 0 ]; then
    echo "No $label processes found"
    return
  fi
  echo "Stopping $label: ${pids[*]}"
  kill "${pids[@]}" || true
  local waited=0
  local remaining=()
  while [ $waited -lt 5 ]; do
    sleep 1
    mapfile -t remaining < <(pgrep -f "$pattern" || true)
    if [ ${#remaining[@]} -eq 0 ]; then
      echo "$label stopped"
      return
    fi
    waited=$((waited + 1))
  done
  if [ ${#remaining[@]} -gt 0 ]; then
    echo "Force killing $label: ${remaining[*]}"
    kill -9 "${remaining[@]}" || true
  fi
}

# Stop datapython supervisors (background start.sh workers before children to avoid respawn)
stop_group "datapython supervisors" "$START_SCRIPT"

# Stop datapython worker processes
stop_group "datapython ingest-live" 'datapython\.cli ingest-live'
stop_group "datapython metrics" 'datapython\.cli metrics'
stop_group "datapython gap-scan" 'datapython\.cli gap-scan'
stop_group "datapython gap-repair" 'datapython\.cli gap-repair'
stop_group "datapython reconcile" 'datapython\.cli reconcile'

# Stop API / Trading (fastapi_server.py)
stop_group "API/trading (fastapi_server.py)" 'api/fastapi_server\.py'

# Stop containers
if command -v docker &>/dev/null; then
  if docker compose version &>/dev/null; then
    COMPOSE_CMD=(docker compose)
  elif command -v docker-compose &>/dev/null; then
    COMPOSE_CMD=(docker-compose)
  else
    COMPOSE_CMD=()
  fi
else
  COMPOSE_CMD=()
fi

if [ ${#COMPOSE_CMD[@]} -gt 0 ]; then
  echo "Stopping containers: timescaledb, prometheus, grafana"
  "${COMPOSE_CMD[@]}" stop timescaledb prometheus grafana || true
fi
