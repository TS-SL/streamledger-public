#!/usr/bin/env bash
# Start/stop the local stack inside WSL (or any Linux host with Docker).
#   scripts/stack.sh up      start broker + database, create topic, apply migrations
#   scripts/stack.sh down    stop and remove containers (data volumes kept)
#   scripts/stack.sh wipe    stop and delete data volumes
#   scripts/stack.sh status  health and memory
set -euo pipefail
cd "$(dirname "$0")/.."
VENV="${STREAMLEDGER_VENV:-$HOME/.venvs/streamledger}"
ms() { date +%s%3N; }

case "${1:-status}" in
  up)
    t0=$(ms)
    docker compose up -d --wait kafka postgres
    t1=$(ms)
    docker compose run --rm -T kafka-init </dev/null
    "$VENV/bin/python" scripts/migrate.py
    echo "healthy_after_ms=$((t1 - t0)) total_ms=$(($(ms) - t0))"
    ;;
  down)
    t0=$(ms); docker compose down; echo "down_ms=$(($(ms) - t0))" ;;
  wipe)
    docker compose down -v ;;
  status)
    docker compose ps --format '{{.Service}} {{.State}} {{.Health}} {{.Ports}}'
    docker stats --no-stream --format '{{.Name}} mem={{.MemUsage}} cpu={{.CPUPerc}}' ;;
  *) echo "usage: $0 up|down|wipe|status" >&2; exit 2 ;;
esac
