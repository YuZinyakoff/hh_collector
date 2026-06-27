#!/usr/bin/env bash
set -euo pipefail

umask 077

ROOT_DIR="${HHRU_BACKUP_OFFSITE_CLEANUP_ROOT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
LOCK_FILE="${HHRU_BACKUP_OFFSITE_CLEANUP_LOCK_FILE:-${ROOT_DIR}/.state/locks/backup-offsite-cleanup.lock}"
HEAVY_OPS_LOCK_FILE="${HHRU_HEAVY_OPS_LOCK_FILE:-${ROOT_DIR}/.state/locks/heavy-ops.lock}"
HEAVY_OPS_LOCK_WAIT_SECONDS="${HHRU_HEAVY_OPS_LOCK_WAIT_SECONDS:-21600}"
LOG_ROOT="${HHRU_BACKUP_OFFSITE_CLEANUP_LOG_ROOT:-${ROOT_DIR}/.state/logs/backup-offsite-cleanup}"
LOG_RETENTION_DAYS="${HHRU_BACKUP_OFFSITE_CLEANUP_LOG_RETENTION_DAYS:-90}"
KEEP_LATEST="${HHRU_BACKUP_OFFSITE_CLEANUP_KEEP_LATEST:-2}"
KEEP_WEEKLY="${HHRU_BACKUP_OFFSITE_CLEANUP_KEEP_WEEKLY:-0}"
APPLY="${HHRU_BACKUP_OFFSITE_CLEANUP_APPLY:-false}"
REQUIRE_RECENT_RESTORE_DRILL="${HHRU_BACKUP_OFFSITE_CLEANUP_REQUIRE_RECENT_RESTORE_DRILL:-false}"
RESTORE_LOG_ROOT="${HHRU_BACKUP_OFFSITE_CLEANUP_RESTORE_LOG_ROOT:-${ROOT_DIR}/.state/logs/backup-restore-drill}"
RESTORE_MAX_AGE_HOURS="${HHRU_BACKUP_OFFSITE_CLEANUP_RESTORE_MAX_AGE_HOURS:-72}"

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

normalize_bool() {
  local name="$1"
  local value="$2"
  case "${value,,}" in
    true|yes|y|1)
      printf 'yes'
      ;;
    false|no|n|0|'')
      printf 'no'
      ;;
    *)
      printf '%s must be boolean, got: %s\n' "$name" "$value" >&2
      exit 2
      ;;
  esac
}

require_non_negative_integer HHRU_BACKUP_OFFSITE_CLEANUP_LOG_RETENTION_DAYS "$LOG_RETENTION_DAYS"
require_positive_integer HHRU_BACKUP_OFFSITE_CLEANUP_KEEP_LATEST "$KEEP_LATEST"
require_non_negative_integer HHRU_BACKUP_OFFSITE_CLEANUP_KEEP_WEEKLY "$KEEP_WEEKLY"
require_positive_integer HHRU_BACKUP_OFFSITE_CLEANUP_RESTORE_MAX_AGE_HOURS "$RESTORE_MAX_AGE_HOURS"
require_positive_integer HHRU_HEAVY_OPS_LOCK_WAIT_SECONDS "$HEAVY_OPS_LOCK_WAIT_SECONDS"

APPLY_NORMALIZED="$(normalize_bool HHRU_BACKUP_OFFSITE_CLEANUP_APPLY "$APPLY")"
REQUIRE_RECENT_RESTORE_DRILL_NORMALIZED="$(
  normalize_bool \
    HHRU_BACKUP_OFFSITE_CLEANUP_REQUIRE_RECENT_RESTORE_DRILL \
    "$REQUIRE_RECENT_RESTORE_DRILL"
)"

cd "$ROOT_DIR"
mkdir -p "$(dirname "$LOCK_FILE")" "$(dirname "$HEAVY_OPS_LOCK_FILE")" "$LOG_ROOT"

exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  printf 'status=skipped\nreason=backup_offsite_cleanup_lock_held\nlock_file=%s\n' "$LOCK_FILE"
  exit 75
fi

exec 8>"$HEAVY_OPS_LOCK_FILE"
if ! flock -w "$HEAVY_OPS_LOCK_WAIT_SECONDS" 8; then
  printf 'status=failed\nreason=heavy_ops_lock_timeout\nlock_file=%s\n' \
    "$HEAVY_OPS_LOCK_FILE" >&2
  exit 1
fi

if (( LOG_RETENTION_DAYS > 0 )); then
  find "$LOG_ROOT" -mindepth 2 -type f -mtime +"$LOG_RETENTION_DAYS" -delete
  find "$LOG_ROOT" -mindepth 1 -type d -empty -delete
fi

RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
TRIGGER_PREFIX="weekly-production-backup-offsite-cleanup-${RUN_ID}"
RUN_LOG_DIR="${LOG_ROOT}/${RUN_ID}"
mkdir -p "$RUN_LOG_DIR"

COMPOSE=(docker compose --profile ops run --rm app)

run_step() {
  local step="$1"
  shift
  local step_log="${RUN_LOG_DIR}/${step}.log"

  printf 'step=%s status=started log=%s\n' "$step" "$step_log"
  if "$@" >"$step_log" 2>&1; then
    printf 'step=%s status=succeeded log=%s\n' "$step" "$step_log"
    return 0
  else
    local exit_code=$?
    printf 'step=%s status=failed exit_code=%s log=%s\n' \
      "$step" "$exit_code" "$step_log" >&2
    tail -40 "$step_log" >&2 || true
    return "$exit_code"
  fi
}

summary_value() {
  local key="$1"
  local log_file="$2"
  awk -F= -v key="$key" '$1 == key { value = $2 } END { print value }' "$log_file"
}

require_recent_restore_drill_success() {
  if [[ "$REQUIRE_RECENT_RESTORE_DRILL_NORMALIZED" != "yes" ]]; then
    return 0
  fi
  if [[ ! -d "$RESTORE_LOG_ROOT" ]]; then
    printf 'operation=weekly_backup_offsite_cleanup status=failed reason=restore_log_root_not_found restore_log_root=%s\n' \
      "$RESTORE_LOG_ROOT" >&2
    exit 1
  fi

  local latest_marker
  latest_marker="$(
    find "$RESTORE_LOG_ROOT" -mindepth 2 -maxdepth 2 -type f -name success.env -printf '%T@ %p\n' \
      | sort -nr \
      | sed -n '1p' \
      | cut -d' ' -f2-
  )"
  if [[ -z "$latest_marker" ]]; then
    printf 'operation=weekly_backup_offsite_cleanup status=failed reason=no_restore_drill_success_marker restore_log_root=%s\n' \
      "$RESTORE_LOG_ROOT" >&2
    exit 1
  fi
  if ! grep -Fxq 'status=succeeded' "$latest_marker"; then
    printf 'operation=weekly_backup_offsite_cleanup status=failed reason=invalid_restore_drill_success_marker marker=%s\n' \
      "$latest_marker" >&2
    exit 1
  fi

  local marker_epoch
  local now_epoch
  local max_age_seconds
  marker_epoch="$(stat -c %Y "$latest_marker")"
  now_epoch="$(date +%s)"
  max_age_seconds=$((RESTORE_MAX_AGE_HOURS * 3600))
  if (( now_epoch - marker_epoch > max_age_seconds )); then
    printf 'operation=weekly_backup_offsite_cleanup status=failed reason=restore_drill_success_marker_stale marker=%s max_age_hours=%s\n' \
      "$latest_marker" "$RESTORE_MAX_AGE_HOURS" >&2
    exit 1
  fi

  printf 'restore_drill_success_marker=%s\n' "$latest_marker"
}

cleanup_args=(
  cleanup-backup-offsite
  --keep-latest "$KEEP_LATEST"
  --keep-weekly "$KEEP_WEEKLY"
  --triggered-by "${TRIGGER_PREFIX}-cleanup"
)
if [[ "$APPLY_NORMALIZED" == "yes" ]]; then
  cleanup_args+=(--apply)
fi

printf 'operation=weekly_backup_offsite_cleanup status=started run_id=%s apply=%s keep_latest=%s keep_weekly=%s log_dir=%s\n' \
  "$RUN_ID" "$APPLY_NORMALIZED" "$KEEP_LATEST" "$KEEP_WEEKLY" "$RUN_LOG_DIR"

require_recent_restore_drill_success

run_step cleanup "${COMPOSE[@]}" "${cleanup_args[@]}"

cleanup_log="${RUN_LOG_DIR}/cleanup.log"
cleanup_status="$(summary_value status "$cleanup_log")"
if [[ "$cleanup_status" != "succeeded" ]]; then
  printf 'operation=weekly_backup_offsite_cleanup status=failed reason=unexpected_cleanup_status cleanup_status=%s\n' \
    "${cleanup_status:-missing}" >&2
  exit 1
fi

printf 'operation=weekly_backup_offsite_cleanup status=succeeded run_id=%s apply=%s keep_latest=%s keep_weekly=%s log_dir=%s\n' \
  "$RUN_ID" "$APPLY_NORMALIZED" "$KEEP_LATEST" "$KEEP_WEEKLY" "$RUN_LOG_DIR"
