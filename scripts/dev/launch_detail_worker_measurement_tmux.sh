#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

SESSION_NAME="${SESSION_NAME:-hh-detail-measurement}"
RUN_TS="${RUN_TS:-$(date -u +%Y%m%dT%H%M%SZ)}"
BATCH_SIZE="${BATCH_SIZE:-100}"
MAX_TICKS="${MAX_TICKS:-1}"
INTERVAL_SECONDS="${INTERVAL_SECONDS:-0}"
INCLUDE_INACTIVE="${INCLUDE_INACTIVE:-no}"
HOST_DB_HOST="${HOST_DB_HOST:-127.0.0.1}"
TRIGGERED_BY="${TRIGGERED_BY:-detail-worker-measurement-${RUN_TS}}"
RETRY_COOLDOWN_SECONDS="${RETRY_COOLDOWN_SECONDS:-3600}"
MAX_RETRY_COOLDOWN_SECONDS="${MAX_RETRY_COOLDOWN_SECONDS:-86400}"
REPORT_DIR="${REPORT_DIR:-.state/reports/detail-worker-measurement/${RUN_TS}}"
LOG_PATH="${LOG_PATH:-${REPORT_DIR}/detail-worker.log}"
PRE_REPORT_PATH="${PRE_REPORT_PATH:-${REPORT_DIR}/preflight.txt}"
POST_REPORT_PATH="${POST_REPORT_PATH:-${REPORT_DIR}/postflight.txt}"
SUMMARY_PATH="${SUMMARY_PATH:-${REPORT_DIR}/summary.md}"
SESSION_PATH_FILE="${SESSION_PATH_FILE:-.state/reports/detail-worker-measurement.tmux-session}"
LOG_PATH_FILE="${LOG_PATH_FILE:-.state/reports/detail-worker-measurement.tmux-log}"
SUMMARY_PATH_FILE="${SUMMARY_PATH_FILE:-.state/reports/detail-worker-measurement.summary}"

mkdir -p "$REPORT_DIR"

if tmux has-session -t "$SESSION_NAME" 2>/dev/null; then
  echo "tmux session already exists: $SESSION_NAME" >&2
  exit 1
fi

set -a
source .env
set +a
export HHRU_DB_HOST="$HOST_DB_HOST"
export PYTHONPATH=src

./.venv/bin/python scripts/dev/write_detail_backlog_report.py \
  --retry-cooldown-seconds "$RETRY_COOLDOWN_SECONDS" \
  --max-retry-cooldown-seconds "$MAX_RETRY_COOLDOWN_SECONDS" \
  > "$PRE_REPORT_PATH"

TMUX_COMMAND=$(cat <<EOF
cd '$ROOT_DIR'
set -a
source .env
set +a
export HHRU_DB_HOST='$HOST_DB_HOST'
export PYTHONPATH=src
set -o pipefail
./.venv/bin/python -u -m hhru_platform.interfaces.workers.detail_worker \
  --batch-size '$BATCH_SIZE' \
  --max-ticks '$MAX_TICKS' \
  --interval-seconds '$INTERVAL_SECONDS' \
  --include-inactive '$INCLUDE_INACTIVE' \
  --triggered-by '$TRIGGERED_BY' \
  --retry-cooldown-seconds '$RETRY_COOLDOWN_SECONDS' \
  --max-retry-cooldown-seconds '$MAX_RETRY_COOLDOWN_SECONDS' 2>&1 | tee '$LOG_PATH'
status=\${PIPESTATUS[0]}

./.venv/bin/python scripts/dev/write_detail_backlog_report.py \
  --retry-cooldown-seconds '$RETRY_COOLDOWN_SECONDS' \
  --max-retry-cooldown-seconds '$MAX_RETRY_COOLDOWN_SECONDS' \
  > '$POST_REPORT_PATH'

./.venv/bin/python scripts/dev/summarize_detail_worker_measurement.py \
  --run-ts '$RUN_TS' \
  --status "\$status" \
  --triggered-by '$TRIGGERED_BY' \
  --batch-size '$BATCH_SIZE' \
  --max-ticks '$MAX_TICKS' \
  --include-inactive '$INCLUDE_INACTIVE' \
  --retry-cooldown-seconds '$RETRY_COOLDOWN_SECONDS' \
  --max-retry-cooldown-seconds '$MAX_RETRY_COOLDOWN_SECONDS' \
  --log-path '$LOG_PATH' \
  --preflight-path '$PRE_REPORT_PATH' \
  --postflight-path '$POST_REPORT_PATH' \
  --summary-path '$SUMMARY_PATH'

echo
echo "[detail-measurement] exited with status=\$status"
echo "[detail-measurement] summary=$SUMMARY_PATH"
echo "[detail-measurement] log=$LOG_PATH"
exec bash
EOF
)

tmux new-session -d -s "$SESSION_NAME" "$TMUX_COMMAND"

echo "$SESSION_NAME" > "$SESSION_PATH_FILE"
echo "$LOG_PATH" > "$LOG_PATH_FILE"
echo "$SUMMARY_PATH" > "$SUMMARY_PATH_FILE"

echo "tmux_session=$SESSION_NAME"
echo "log=$LOG_PATH"
echo "summary=$SUMMARY_PATH"
echo "preflight=$PRE_REPORT_PATH"
echo "postflight=$POST_REPORT_PATH"
echo "attach=tmux attach -t $SESSION_NAME"
echo "detach=Ctrl+b then d"
echo "tail=tail -f $LOG_PATH"
