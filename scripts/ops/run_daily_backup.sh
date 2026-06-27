#!/usr/bin/env bash
set -euo pipefail

umask 077

ROOT_DIR="${HHRU_BACKUP_DAILY_ROOT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
LOCK_FILE="${HHRU_BACKUP_DAILY_LOCK_FILE:-${ROOT_DIR}/.state/locks/backup-daily.lock}"
HEAVY_OPS_LOCK_FILE="${HHRU_HEAVY_OPS_LOCK_FILE:-${ROOT_DIR}/.state/locks/heavy-ops.lock}"
HEAVY_OPS_LOCK_WAIT_SECONDS="${HHRU_HEAVY_OPS_LOCK_WAIT_SECONDS:-21600}"
LOG_ROOT="${HHRU_BACKUP_DAILY_LOG_ROOT:-${ROOT_DIR}/.state/logs/backup-daily}"
LOG_RETENTION_DAYS="${HHRU_BACKUP_DAILY_LOG_RETENTION_DAYS:-30}"
LOCAL_BACKUP_RETENTION_DAYS="${HHRU_BACKUP_DAILY_LOCAL_RETENTION_DAYS:-1}"

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

require_non_negative_integer HHRU_BACKUP_DAILY_LOG_RETENTION_DAYS "$LOG_RETENTION_DAYS"
require_positive_integer HHRU_BACKUP_DAILY_LOCAL_RETENTION_DAYS "$LOCAL_BACKUP_RETENTION_DAYS"
require_positive_integer HHRU_HEAVY_OPS_LOCK_WAIT_SECONDS "$HEAVY_OPS_LOCK_WAIT_SECONDS"
export HHRU_BACKUP_RETENTION_DAYS="$LOCAL_BACKUP_RETENTION_DAYS"

cd "$ROOT_DIR"
mkdir -p "$(dirname "$LOCK_FILE")" "$(dirname "$HEAVY_OPS_LOCK_FILE")" "$LOG_ROOT"

exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  printf 'status=skipped\nreason=backup_daily_lock_held\nlock_file=%s\n' "$LOCK_FILE"
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
TRIGGER_PREFIX="daily-production-backup-${RUN_ID}"
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

printf 'operation=daily_backup status=started run_id=%s log_dir=%s\n' \
  "$RUN_ID" "$RUN_LOG_DIR"

run_step create-backup \
  "${COMPOSE[@]}" run-backup \
  --triggered-by "${TRIGGER_PREFIX}-create"

backup_file="$(summary_value backup_file "${RUN_LOG_DIR}/create-backup.log")"
if [[ -z "$backup_file" ]]; then
  printf 'operation=daily_backup status=failed reason=missing_backup_file\n' >&2
  exit 1
fi
printf 'step=create-backup backup_file=%s\n' "$backup_file"

run_step local-verify \
  "${COMPOSE[@]}" verify-backup-file \
  --backup-file "$backup_file"

run_step offsite-sync \
  "${COMPOSE[@]}" sync-backup-offsite \
  --limit 1 \
  --triggered-by "${TRIGGER_PREFIX}-offsite-sync"

scanned_backup_count="$(
  summary_value scanned_backup_count "${RUN_LOG_DIR}/offsite-sync.log"
)"
if [[ "$scanned_backup_count" != "1" ]]; then
  printf 'operation=daily_backup status=failed reason=unexpected_sync_scan_count count=%s\n' \
    "${scanned_backup_count:-missing}" >&2
  exit 1
fi
if ! grep -Fq "$(basename "$backup_file")" "${RUN_LOG_DIR}/offsite-sync.log"; then
  printf 'operation=daily_backup status=failed reason=created_backup_not_synced backup_file=%s\n' \
    "$backup_file" >&2
  exit 1
fi

run_step offsite-verify \
  "${COMPOSE[@]}" verify-backup-offsite \
  --backup-file "$backup_file" \
  --triggered-by "${TRIGGER_PREFIX}-offsite-verify"

printf 'operation=daily_backup status=succeeded run_id=%s backup_file=%s log_dir=%s\n' \
  "$RUN_ID" "$backup_file" "$RUN_LOG_DIR"
