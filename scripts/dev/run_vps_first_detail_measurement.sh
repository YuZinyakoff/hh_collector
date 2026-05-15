#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

RUN_TS="${RUN_TS:-$(date -u +%Y%m%dT%H%M%SZ)}"
LIMIT="${LIMIT:-100}"
INCLUDE_INACTIVE="${INCLUDE_INACTIVE:-no}"
TRIGGERED_BY="${TRIGGERED_BY:-vps-first-detail-measurement-${RUN_TS}}"
RETRY_COOLDOWN_SECONDS="${RETRY_COOLDOWN_SECONDS:-3600}"
MAX_RETRY_COOLDOWN_SECONDS="${MAX_RETRY_COOLDOWN_SECONDS:-86400}"
REPORT_DIR="${REPORT_DIR:-.state/reports/vps-first-detail-measurement/${RUN_TS}}"
PRE_REPORT_PATH="${PRE_REPORT_PATH:-${REPORT_DIR}/preflight.txt}"
DRAIN_LOG_PATH="${DRAIN_LOG_PATH:-${REPORT_DIR}/drain.txt}"
POST_REPORT_PATH="${POST_REPORT_PATH:-${REPORT_DIR}/postflight.txt}"
SUMMARY_PATH="${SUMMARY_PATH:-${REPORT_DIR}/summary.txt}"
COMPOSE="${COMPOSE:-docker compose}"

read -r -a COMPOSE_CMD <<< "$COMPOSE"

mkdir -p "$REPORT_DIR"

run_backlog_report() {
  "${COMPOSE_CMD[@]}" --profile ops run --rm --entrypoint python app \
    scripts/dev/write_detail_backlog_report.py \
    --retry-cooldown-seconds "$RETRY_COOLDOWN_SECONDS" \
    --max-retry-cooldown-seconds "$MAX_RETRY_COOLDOWN_SECONDS"
}

run_backlog_report > "$PRE_REPORT_PATH"

set +e
"${COMPOSE_CMD[@]}" --profile ops run --rm app drain-first-detail-backlog \
  --limit "$LIMIT" \
  --include-inactive "$INCLUDE_INACTIVE" \
  --triggered-by "$TRIGGERED_BY" \
  --retry-cooldown-seconds "$RETRY_COOLDOWN_SECONDS" \
  --max-retry-cooldown-seconds "$MAX_RETRY_COOLDOWN_SECONDS" 2>&1 \
  | tee "$DRAIN_LOG_PATH"
status="${PIPESTATUS[0]}"
set -e

run_backlog_report > "$POST_REPORT_PATH"

{
  echo "run_ts=$RUN_TS"
  echo "status=$status"
  echo "triggered_by=$TRIGGERED_BY"
  echo "limit=$LIMIT"
  echo "include_inactive=$INCLUDE_INACTIVE"
  echo "retry_cooldown_seconds=$RETRY_COOLDOWN_SECONDS"
  echo "max_retry_cooldown_seconds=$MAX_RETRY_COOLDOWN_SECONDS"
  echo "preflight=$PRE_REPORT_PATH"
  echo "drain=$DRAIN_LOG_PATH"
  echo "postflight=$POST_REPORT_PATH"
} > "$SUMMARY_PATH"

echo "summary=$SUMMARY_PATH"
echo "preflight=$PRE_REPORT_PATH"
echo "drain=$DRAIN_LOG_PATH"
echo "postflight=$POST_REPORT_PATH"
exit "$status"
