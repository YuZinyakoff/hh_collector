#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

RUN_TS="${RUN_TS:-$(date -u +%Y%m%dT%H%M%SZ)}"
TRIGGERED_BY="${TRIGGERED_BY:-search-baseline-rerun-2026-04-01}"
LOG_PATH="${LOG_PATH:-.state/reports/${RUN_TS}-search-baseline-rerun.log}"
PID_PATH="${PID_PATH:-.state/reports/search-baseline-rerun.pid}"

mkdir -p "$(dirname "$LOG_PATH")"

if [[ -f "$PID_PATH" ]]; then
  EXISTING_PID="$(cat "$PID_PATH" 2>/dev/null || true)"
  if [[ -n "$EXISTING_PID" ]] && kill -0 "$EXISTING_PID" 2>/dev/null; then
    echo "search baseline rerun already active: pid=$EXISTING_PID" >&2
    exit 1
  fi
fi

nohup env PYTHONPATH=src ./.venv/bin/python -u -m hhru_platform.interfaces.cli.main run-once-v2 \
  --sync-dictionaries no \
  --detail-limit 0 \
  --detail-refresh-ttl-days 30 \
  --triggered-by "$TRIGGERED_BY" \
  > "$LOG_PATH" 2>&1 < /dev/null &

PID="$!"
echo "$PID" > "$PID_PATH"
sleep 1

echo "log=$LOG_PATH"
echo "pid=$PID"

if ! kill -0 "$PID" 2>/dev/null; then
  echo "search baseline rerun exited early" >&2
  tail -n 80 "$LOG_PATH" || true
  exit 1
fi

tail -n 20 "$LOG_PATH" || true
