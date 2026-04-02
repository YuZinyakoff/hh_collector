#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

SESSION_NAME="${SESSION_NAME:-hh-search-baseline}"
TRIGGERED_BY="${TRIGGERED_BY:-search-baseline-rerun-2026-04-01}"
RUN_TS="${RUN_TS:-$(date -u +%Y%m%dT%H%M%SZ)}"
LOG_PATH="${LOG_PATH:-.state/reports/${RUN_TS}-search-baseline-rerun.log}"
SESSION_PATH_FILE="${SESSION_PATH_FILE:-.state/reports/search-baseline-rerun.tmux-session}"
LOG_PATH_FILE="${LOG_PATH_FILE:-.state/reports/search-baseline-rerun.tmux-log}"

mkdir -p "$(dirname "$LOG_PATH")"

if tmux has-session -t "$SESSION_NAME" 2>/dev/null; then
  echo "tmux session already exists: $SESSION_NAME" >&2
  exit 1
fi

TMUX_COMMAND=$(cat <<EOF
cd '$ROOT_DIR'
set -a
source .env
set +a
export PYTHONPATH=src
set -o pipefail
./.venv/bin/python -u -m hhru_platform.interfaces.cli.main run-once-v2 \
  --sync-dictionaries no \
  --detail-limit 0 \
  --detail-refresh-ttl-days 30 \
  --triggered-by '$TRIGGERED_BY' 2>&1 | tee '$LOG_PATH'
status=\${PIPESTATUS[0]}
echo
echo "[search-baseline] exited with status=\$status"
echo "[search-baseline] log=$LOG_PATH"
exec bash
EOF
)

tmux new-session -d -s "$SESSION_NAME" "$TMUX_COMMAND"

echo "$SESSION_NAME" > "$SESSION_PATH_FILE"
echo "$LOG_PATH" > "$LOG_PATH_FILE"

echo "tmux_session=$SESSION_NAME"
echo "log=$LOG_PATH"
echo "attach=tmux attach -t $SESSION_NAME"
echo "detach=Ctrl+b then d"
echo "tail=tail -f $LOG_PATH"
