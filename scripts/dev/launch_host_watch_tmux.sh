#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

SESSION_NAME="${SESSION_NAME:-host-watch}"
RUN_TS="${RUN_TS:-$(date -u +%Y%m%dT%H%M%SZ)}"
INTERVAL_SECONDS="${INTERVAL_SECONDS:-60}"
LOG_PATH="${LOG_PATH:-.state/reports/${RUN_TS}-host-watch.log}"
SESSION_PATH_FILE="${SESSION_PATH_FILE:-.state/reports/host-watch.tmux-session}"
LOG_PATH_FILE="${LOG_PATH_FILE:-.state/reports/host-watch.tmux-log}"

mkdir -p "$(dirname "$LOG_PATH")"

if tmux has-session -t "$SESSION_NAME" 2>/dev/null; then
  echo "tmux session already exists: $SESSION_NAME" >&2
  exit 1
fi

TMUX_COMMAND=$(cat <<EOF
cd '$ROOT_DIR'
while true; do
  date -Is
  uptime -s
  free -h
  echo '--- docker stats ---'
  docker stats --no-stream
  echo '---'
  sleep '$INTERVAL_SECONDS'
done >> '$LOG_PATH' 2>&1
status=\$?
echo
echo "[host-watch] exited with status=\$status"
echo "[host-watch] log=$LOG_PATH"
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
