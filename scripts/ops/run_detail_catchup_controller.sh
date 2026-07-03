#!/usr/bin/env bash
set -euo pipefail

umask 077

ROOT_DIR="${HHRU_DETAIL_CATCHUP_ROOT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
LOCK_FILE="${HHRU_DETAIL_CATCHUP_LOCK_FILE:-${ROOT_DIR}/.state/locks/detail-catchup-controller.lock}"
LOG_ROOT="${HHRU_DETAIL_CATCHUP_LOG_ROOT:-${ROOT_DIR}/.state/logs/detail-catchup-controller}"
LOG_RETENTION_DAYS="${HHRU_DETAIL_CATCHUP_LOG_RETENTION_DAYS:-30}"
APPLY="${HHRU_DETAIL_CATCHUP_CONTROLLER_APPLY:-false}"
DETAIL_WORKER_SCALE="${HHRU_DETAIL_CATCHUP_WORKER_SCALE:-3}"
START_READY_THRESHOLD="${HHRU_DETAIL_CATCHUP_START_READY_THRESHOLD:-1}"
STOP_BACKLOG_THRESHOLD="${HHRU_DETAIL_CATCHUP_STOP_BACKLOG_THRESHOLD:-0}"
COMPOSE_PROJECT_SERVICE="${HHRU_DETAIL_CATCHUP_COMPOSE_SERVICE:-detail-worker}"
DB_USER="${HHRU_DB_USER:-hhru}"
DB_NAME="${HHRU_DB_NAME:-hhru_platform}"

require_non_negative_integer() {
  local name="$1"
  local value="$2"
  if [[ ! "$value" =~ ^[0-9]+$ ]]; then
    printf '%s must be a non-negative integer, got: %s\n' "$name" "$value" >&2
    exit 2
  fi
}

require_positive_integer() {
  local name="$1"
  local value="$2"
  if [[ ! "$value" =~ ^[1-9][0-9]*$ ]]; then
    printf '%s must be a positive integer, got: %s\n' "$name" "$value" >&2
    exit 2
  fi
}

require_bool() {
  local name="$1"
  local value="$2"
  case "$value" in
    true|false) ;;
    *)
      printf '%s must be true or false, got: %s\n' "$name" "$value" >&2
      exit 2
      ;;
  esac
}

require_positive_integer HHRU_DETAIL_CATCHUP_WORKER_SCALE "$DETAIL_WORKER_SCALE"
require_non_negative_integer HHRU_DETAIL_CATCHUP_START_READY_THRESHOLD "$START_READY_THRESHOLD"
require_non_negative_integer HHRU_DETAIL_CATCHUP_STOP_BACKLOG_THRESHOLD "$STOP_BACKLOG_THRESHOLD"
require_non_negative_integer HHRU_DETAIL_CATCHUP_LOG_RETENTION_DAYS "$LOG_RETENTION_DAYS"
require_bool HHRU_DETAIL_CATCHUP_CONTROLLER_APPLY "$APPLY"

cd "$ROOT_DIR"
mkdir -p "$(dirname "$LOCK_FILE")" "$LOG_ROOT"

exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  printf 'operation=detail_catchup_controller status=skipped reason=lock_held lock_file=%s\n' \
    "$LOCK_FILE"
  exit 75
fi

if (( LOG_RETENTION_DAYS > 0 )); then
  find "$LOG_ROOT" -mindepth 2 -type f -mtime +"$LOG_RETENTION_DAYS" -delete
  find "$LOG_ROOT" -mindepth 1 -type d -empty -delete
fi

RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
RUN_LOG_DIR="${LOG_ROOT}/${RUN_ID}"
mkdir -p "$RUN_LOG_DIR"

COMPOSE=(docker compose)
COMPOSE_OPS=(docker compose --profile ops)

query_scalar() {
  local sql="$1"
  "${COMPOSE[@]}" exec -T postgres psql \
    -U "$DB_USER" \
    -d "$DB_NAME" \
    -At \
    -P pager=off \
    -c "$sql" \
    | tr -d '[:space:]'
}

count_running_workers() {
  "${COMPOSE_OPS[@]}" ps --status running -q "$COMPOSE_PROJECT_SERVICE" \
    | sed '/^$/d' \
    | wc -l \
    | tr -d '[:space:]'
}

run_or_dry_run() {
  if [[ "$APPLY" == "true" ]]; then
    "$@"
  else
    printf 'dry_run_command=%q' "$1"
    shift
    printf ' %q' "$@"
    printf '\n'
  fi
}

active_runs_sql="
select count(*)
from crawl_run
where status = 'created';
"
backlog_sql="
select count(*)
from vacancy_current_state
where is_probably_inactive is false
  and not (
    (detail_fetch_status = 'succeeded' and last_detail_fetched_at is not null)
    or detail_fetch_status = 'terminal_404'
  );
"
ready_backlog_sql="
select count(*)
from vacancy_current_state
where is_probably_inactive is false
  and not (
    (detail_fetch_status = 'succeeded' and last_detail_fetched_at is not null)
    or detail_fetch_status = 'terminal_404'
  )
  and (
    first_detail_lease_expires_at is null
    or first_detail_lease_expires_at <= now()
  );
"

printf 'operation=detail_catchup_controller status=started run_id=%s log_dir=%s apply=%s worker_scale=%s\n' \
  "$RUN_ID" "$RUN_LOG_DIR" "$APPLY" "$DETAIL_WORKER_SCALE"

active_runs="$(query_scalar "$active_runs_sql")"
backlog_size="$(query_scalar "$backlog_sql")"
ready_backlog_size="$(query_scalar "$ready_backlog_sql")"
running_workers="$(count_running_workers)"

require_non_negative_integer active_runs "$active_runs"
require_non_negative_integer backlog_size "$backlog_size"
require_non_negative_integer ready_backlog_size "$ready_backlog_size"
require_non_negative_integer running_workers "$running_workers"

action="kept"
if (( active_runs > 0 )); then
  if (( running_workers > 0 )); then
    action=$([[ "$APPLY" == "true" ]] && printf 'stopped_for_active_run' || printf 'would_stop_for_active_run')
    run_or_dry_run "${COMPOSE_OPS[@]}" stop "$COMPOSE_PROJECT_SERVICE"
  else
    action="skipped_active_run"
  fi
elif (( backlog_size <= STOP_BACKLOG_THRESHOLD )); then
  if (( running_workers > 0 )); then
    action=$([[ "$APPLY" == "true" ]] && printf 'stopped_empty_backlog' || printf 'would_stop_empty_backlog')
    run_or_dry_run "${COMPOSE_OPS[@]}" stop "$COMPOSE_PROJECT_SERVICE"
  else
    action="idle_empty_backlog"
  fi
elif (( ready_backlog_size < START_READY_THRESHOLD )); then
  if (( running_workers > 0 )); then
    action=$([[ "$APPLY" == "true" ]] && printf 'stopped_no_ready_backlog' || printf 'would_stop_no_ready_backlog')
    run_or_dry_run "${COMPOSE_OPS[@]}" stop "$COMPOSE_PROJECT_SERVICE"
  else
    action="idle_no_ready_backlog"
  fi
elif (( running_workers != DETAIL_WORKER_SCALE )); then
  action=$([[ "$APPLY" == "true" ]] && printf 'started' || printf 'would_start')
  run_or_dry_run "${COMPOSE_OPS[@]}" up -d --scale "${COMPOSE_PROJECT_SERVICE}=${DETAIL_WORKER_SCALE}" "$COMPOSE_PROJECT_SERVICE"
else
  action="kept_running"
fi

printf 'operation=detail_catchup_controller status=succeeded run_id=%s action=%s active_runs=%s backlog_size=%s ready_backlog_size=%s running_workers=%s desired_worker_scale=%s apply=%s\n' \
  "$RUN_ID" \
  "$action" \
  "$active_runs" \
  "$backlog_size" \
  "$ready_backlog_size" \
  "$running_workers" \
  "$DETAIL_WORKER_SCALE" \
  "$APPLY"
