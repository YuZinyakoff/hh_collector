#!/usr/bin/env bash
set -euo pipefail

umask 077

ROOT_DIR="${HHRU_PRODUCTION_SEARCH_ROOT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
LOCK_FILE="${HHRU_PRODUCTION_SEARCH_LOCK_FILE:-${ROOT_DIR}/.state/locks/production-search-controller.lock}"
LOG_ROOT="${HHRU_PRODUCTION_SEARCH_LOG_ROOT:-${ROOT_DIR}/.state/logs/production-search-controller}"
LOG_RETENTION_DAYS="${HHRU_PRODUCTION_SEARCH_LOG_RETENTION_DAYS:-30}"
APPLY="${HHRU_PRODUCTION_SEARCH_CONTROLLER_APPLY:-false}"
FORCE="${HHRU_PRODUCTION_SEARCH_FORCE:-false}"
RUN_TYPE="${HHRU_PRODUCTION_SEARCH_RUN_TYPE:-production_weekly_sweep}"
SYNC_DICTIONARIES="${HHRU_PRODUCTION_SEARCH_SYNC_DICTIONARIES:-no}"
DETAIL_LIMIT="${HHRU_PRODUCTION_SEARCH_DETAIL_LIMIT:-0}"
DETAIL_REFRESH_TTL_DAYS="${HHRU_PRODUCTION_SEARCH_DETAIL_REFRESH_TTL_DAYS:-30}"
INTERVAL_SECONDS="${HHRU_PRODUCTION_SEARCH_INTERVAL_SECONDS:-604800}"
MAX_BACKLOG_BEFORE_SEARCH="${HHRU_PRODUCTION_SEARCH_MAX_BACKLOG_BEFORE_SEARCH:-0}"
MIN_FREE_BYTES="${HHRU_PRODUCTION_SEARCH_MIN_FREE_BYTES:-21474836480}"
TRIGGERED_BY_PREFIX="${HHRU_PRODUCTION_SEARCH_TRIGGERED_BY_PREFIX:-production-search}"
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

require_yes_no() {
  local name="$1"
  local value="$2"
  case "$value" in
    yes|no) ;;
    *)
      printf '%s must be yes or no, got: %s\n' "$name" "$value" >&2
      exit 2
      ;;
  esac
}

require_safe_label() {
  local name="$1"
  local value="$2"
  if [[ ! "$value" =~ ^[A-Za-z0-9_-]+$ ]]; then
    printf '%s must contain only letters, digits, underscore or dash, got: %s\n' \
      "$name" "$value" >&2
    exit 2
  fi
}

require_bool HHRU_PRODUCTION_SEARCH_CONTROLLER_APPLY "$APPLY"
require_bool HHRU_PRODUCTION_SEARCH_FORCE "$FORCE"
require_yes_no HHRU_PRODUCTION_SEARCH_SYNC_DICTIONARIES "$SYNC_DICTIONARIES"
require_safe_label HHRU_PRODUCTION_SEARCH_RUN_TYPE "$RUN_TYPE"
require_safe_label HHRU_PRODUCTION_SEARCH_TRIGGERED_BY_PREFIX "$TRIGGERED_BY_PREFIX"
require_non_negative_integer HHRU_PRODUCTION_SEARCH_LOG_RETENTION_DAYS "$LOG_RETENTION_DAYS"
require_non_negative_integer HHRU_PRODUCTION_SEARCH_DETAIL_LIMIT "$DETAIL_LIMIT"
require_positive_integer HHRU_PRODUCTION_SEARCH_DETAIL_REFRESH_TTL_DAYS "$DETAIL_REFRESH_TTL_DAYS"
require_positive_integer HHRU_PRODUCTION_SEARCH_INTERVAL_SECONDS "$INTERVAL_SECONDS"
require_non_negative_integer HHRU_PRODUCTION_SEARCH_MAX_BACKLOG_BEFORE_SEARCH "$MAX_BACKLOG_BEFORE_SEARCH"
require_non_negative_integer HHRU_PRODUCTION_SEARCH_MIN_FREE_BYTES "$MIN_FREE_BYTES"

cd "$ROOT_DIR"
mkdir -p "$(dirname "$LOCK_FILE")" "$LOG_ROOT"

exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  printf 'operation=production_search_controller status=skipped reason=lock_held lock_file=%s\n' \
    "$LOCK_FILE"
  exit 75
fi

if (( LOG_RETENTION_DAYS > 0 )); then
  find "$LOG_ROOT" -mindepth 2 -type f -mtime +"$LOG_RETENTION_DAYS" -delete
  find "$LOG_ROOT" -mindepth 1 -type d -empty -delete
fi

RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
RUN_LOG_DIR="${LOG_ROOT}/${RUN_ID}"
SEARCH_LOG="${RUN_LOG_DIR}/search.log"
mkdir -p "$RUN_LOG_DIR"

COMPOSE=(docker compose)
COMPOSE_OPS=(docker compose --profile ops)
TRIGGERED_BY="${HHRU_PRODUCTION_SEARCH_TRIGGERED_BY:-${TRIGGERED_BY_PREFIX}-${RUN_ID}}"

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
  "${COMPOSE_OPS[@]}" ps --status running -q detail-worker \
    | sed '/^$/d' \
    | wc -l \
    | tr -d '[:space:]'
}

count_failed_units() {
  systemctl --failed --plain --no-legend --no-pager \
    | sed '/^$/d' \
    | wc -l \
    | tr -d '[:space:]'
}

available_bytes() {
  df -PB1 "$ROOT_DIR" | awk 'NR == 2 { print $4 }'
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
last_success_epoch_sql="
select coalesce(extract(epoch from max(started_at))::bigint, 0)
from crawl_run
where run_type = '${RUN_TYPE}'
  and status = 'succeeded';
"
last_success_started_at_sql="
select coalesce(to_char(max(started_at) at time zone 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'), '-')
from crawl_run
where run_type = '${RUN_TYPE}'
  and status = 'succeeded';
"

printf 'operation=production_search_controller status=started run_id=%s log_dir=%s apply=%s force=%s run_type=%s interval_seconds=%s\n' \
  "$RUN_ID" "$RUN_LOG_DIR" "$APPLY" "$FORCE" "$RUN_TYPE" "$INTERVAL_SECONDS"

active_runs="$(query_scalar "$active_runs_sql")"
backlog_size="$(query_scalar "$backlog_sql")"
last_success_epoch="$(query_scalar "$last_success_epoch_sql")"
last_success_started_at="$(query_scalar "$last_success_started_at_sql")"
running_detail_workers="$(count_running_workers)"
failed_units="$(count_failed_units)"
free_bytes="$(available_bytes)"
now_epoch="$(date -u +%s)"
due_after_epoch=$((last_success_epoch + INTERVAL_SECONDS))

require_non_negative_integer active_runs "$active_runs"
require_non_negative_integer backlog_size "$backlog_size"
require_non_negative_integer last_success_epoch "$last_success_epoch"
require_non_negative_integer running_detail_workers "$running_detail_workers"
require_non_negative_integer failed_units "$failed_units"
require_non_negative_integer free_bytes "$free_bytes"

due="no"
if [[ "$FORCE" == "true" ]] || (( last_success_epoch == 0 )) || (( now_epoch >= due_after_epoch )); then
  due="yes"
fi

finish_skipped() {
  local action="$1"
  local reason="$2"
  printf 'operation=production_search_controller status=succeeded run_id=%s action=%s reason=%s due=%s active_runs=%s backlog_size=%s running_detail_workers=%s failed_units=%s free_bytes=%s last_success_started_at=%s apply=%s\n' \
    "$RUN_ID" \
    "$action" \
    "$reason" \
    "$due" \
    "$active_runs" \
    "$backlog_size" \
    "$running_detail_workers" \
    "$failed_units" \
    "$free_bytes" \
    "$last_success_started_at" \
    "$APPLY"
}

if [[ "$due" == "no" ]]; then
  due_after_utc="$(date -u -d "@${due_after_epoch}" +%Y-%m-%dT%H:%M:%SZ)"
  finish_skipped "skipped_not_due" "next_due_at=${due_after_utc}"
  exit 0
fi

if (( active_runs > 0 )); then
  finish_skipped "skipped_active_run" "active_crawl_run_exists"
  exit 0
fi

if (( backlog_size > MAX_BACKLOG_BEFORE_SEARCH )); then
  finish_skipped "skipped_backlog_not_empty" "first_detail_backlog_above_threshold"
  exit 0
fi

if (( failed_units > 0 )); then
  finish_skipped "skipped_failed_units" "systemd_failed_units_present"
  exit 0
fi

if (( free_bytes < MIN_FREE_BYTES )); then
  finish_skipped "skipped_low_disk" "free_bytes_below_threshold"
  exit 0
fi

if (( running_detail_workers > 0 )); then
  run_or_dry_run "${COMPOSE_OPS[@]}" stop detail-worker
fi

search_command=(
  "${COMPOSE_OPS[@]}" run --rm app trigger-run-now
  --sync-dictionaries "$SYNC_DICTIONARIES"
  --detail-limit "$DETAIL_LIMIT"
  --detail-refresh-ttl-days "$DETAIL_REFRESH_TTL_DAYS"
  --run-type "$RUN_TYPE"
  --triggered-by "$TRIGGERED_BY"
)

if [[ "$APPLY" != "true" ]]; then
  run_or_dry_run "${search_command[@]}"
  finish_skipped "would_start_search" "dry_run"
  exit 0
fi

if "${search_command[@]}" 2>&1 | tee "$SEARCH_LOG"; then
  search_status="$(awk -F= '$1 == "status" { value = $2 } END { print value }' "$SEARCH_LOG")"
  search_run_id="$(awk -F= '$1 == "run_id" { value = $2 } END { print value }' "$SEARCH_LOG")"
  printf 'operation=production_search_controller status=succeeded run_id=%s action=started_search search_status=%s search_run_id=%s active_runs=%s backlog_size=%s running_detail_workers=%s failed_units=%s free_bytes=%s last_success_started_at=%s apply=%s\n' \
    "$RUN_ID" \
    "${search_status:-unknown}" \
    "${search_run_id:--}" \
    "$active_runs" \
    "$backlog_size" \
    "$running_detail_workers" \
    "$failed_units" \
    "$free_bytes" \
    "$last_success_started_at" \
    "$APPLY"
else
  exit_code=$?
  printf 'operation=production_search_controller status=failed run_id=%s action=started_search exit_code=%s log=%s\n' \
    "$RUN_ID" "$exit_code" "$SEARCH_LOG" >&2
  exit "$exit_code"
fi
