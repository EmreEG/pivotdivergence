#!/bin/bash
set -euo pipefail

VENV_PYTHON="./venv/bin/python"

if [ ! -f "$VENV_PYTHON" ]; then
    echo "Error: virtual environment not found. Run ./setup.sh first"
    exit 1
fi

mkdir -p logs

# Ensure datapython package is available in the venv
DATAPYTHON_DIR="$(cd "$(dirname "$0")/../datapython" 2>/dev/null && pwd || true)"
if [ -z "$DATAPYTHON_DIR" ] || [ ! -d "$DATAPYTHON_DIR" ]; then
    echo "Error: datapython project not found next to pivotdivergence"
    exit 1
fi
if ! "$VENV_PYTHON" -c "import datapython" >/dev/null 2>&1; then
    echo "Installing datapython into venv"
    "$VENV_PYTHON" -m pip install -q -e "$DATAPYTHON_DIR"
fi

# Ensure Prometheus config exists
if [ ! -f "config/prometheus.yml" ]; then
cat > config/prometheus.yml <<'YAML'
global:
  scrape_interval: 5s
scrape_configs:
  - job_name: 'pivotdivergence'
    static_configs:
      - targets: ['host.docker.internal:9090']
YAML
fi

# Bring up dependencies
if command -v docker &>/dev/null; then
  if docker compose version &>/dev/null; then
    COMPOSE_CMD=(docker compose)
  elif command -v docker-compose &>/dev/null; then
    COMPOSE_CMD=(docker-compose)
  else
    echo "Docker Compose not found; skipping containers"
    COMPOSE_CMD=()
  fi
else
  COMPOSE_CMD=()
fi

if [ ${#COMPOSE_CMD[@]} -gt 0 ]; then
  echo "Starting containers: timescaledb, prometheus, grafana"
  "${COMPOSE_CMD[@]}" up -d timescaledb prometheus grafana
  "${COMPOSE_CMD[@]}" restart grafana >/dev/null 2>&1 || true
fi

# Wait for TimescaleDB if running
if [ ${#COMPOSE_CMD[@]} -gt 0 ]; then
  echo "Waiting for TimescaleDB on 5433"
  SECONDS=0
  until nc -z 127.0.0.1 5433 2>/dev/null || [ $SECONDS -ge 60 ]; do sleep 1; done || true

  # Initialize pivotdivergence schema only if missing (non-destructive)
  if docker ps --format '{{.Names}}' | grep -q '^pivotdivergence-db$'; then
    HAS_TRADES=$(docker exec -u postgres pivotdivergence-db psql -Atqc "SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='trades' LIMIT 1;" -d pivotdivergence 2>/dev/null || echo "")
    if [ "$HAS_TRADES" != "1" ]; then
      echo "Applying pivotdivergence schema (first run)"
      docker exec -u postgres pivotdivergence-db psql -d pivotdivergence -f /docker-entrypoint-initdb.d/schema.sql || true
    fi
  fi
fi

# Wire datapython DSN to the TimescaleDB instance on host port 5433
DB_PASS_DEFAULT="${DB_PASSWORD:-changeme}"
DB_PASS="${DB_PASS_DEFAULT}"
if docker ps --format '{{.Names}}' | grep -q '^pivotdivergence-db$'; then
  DB_PASS_IN_CONTAINER=$(docker exec -u postgres pivotdivergence-db printenv POSTGRES_PASSWORD 2>/dev/null || echo "")
  if [ -n "$DB_PASS_IN_CONTAINER" ]; then
    DB_PASS="$DB_PASS_IN_CONTAINER"
  fi
fi
export DATAPYTHON_DATABASE_DSN="postgresql://postgres:${DB_PASS}@localhost:5433/pivotdivergence"

# Apply datapython schema into landing/core schemas
echo "Applying datapython schema into pivotdivergence database"
env PYTHONPATH=. "$VENV_PYTHON" -m datapython.cli apply-schema

# Start datapython ingestion and maintenance loops in background
run_dp_ingest() {
  while true; do
    env PYTHONPATH=. "$VENV_PYTHON" -m datapython.cli ingest-live || sleep 5
  done
}

run_dp_metrics() {
  while true; do
    env PYTHONPATH=. "$VENV_PYTHON" -m datapython.cli metrics || sleep 5
  done
}

run_dp_gap_maintenance() {
  while true; do
    env PYTHONPATH=. "$VENV_PYTHON" -m datapython.cli gap-scan --window 5000 || true
    env PYTHONPATH=. "$VENV_PYTHON" -m datapython.cli gap-repair --limit 50 || true
    sleep 300
  done
}

run_dp_reconcile() {
  while true; do
    env PYTHONPATH=. "$VENV_PYTHON" -m datapython.cli reconcile --days 1 || true
    sleep 86400
  done
}

run_dp_ingest &
run_dp_metrics &
run_dp_gap_maintenance &
run_dp_reconcile &

echo "Starting API + Trading (foreground)"
echo "API:     http://localhost:8080"
echo "Metrics: http://localhost:9090/metrics"
echo "Grafana: http://localhost:3001"

# If our API process is already running, ensure it is healthy and leave it running
API_PIDS=$(ps aux | grep 'api/fastapi_server.py' | grep -v grep | awk '{print $2}' || true)
if [ -n "${API_PIDS:-}" ]; then
  if curl -fsS http://localhost:8080/health >/dev/null 2>&1; then
    echo "Pivotdivergence API already healthy on port 8080; nothing to do"
    exit 0
  else
    echo "Error: api/fastapi_server.py running but /health failed"
    exit 1
  fi
fi

# If some other process is bound to 8080, treat that as a hard conflict
if ss -ltnp 2>/dev/null | grep -q ':8080 '; then
  echo "Error: port 8080 is in use by a different process"
  exit 1
fi

# Metrics server should only be started by our API; any existing listener here is an error
if ss -ltnp 2>/dev/null | grep -q ':9090 '; then
  echo "Error: metrics server already running on port 9090"
  exit 1
fi

exec env PYTHONPATH=. "$VENV_PYTHON" api/fastapi_server.py
