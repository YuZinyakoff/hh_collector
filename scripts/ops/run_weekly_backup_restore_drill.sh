#!/usr/bin/env bash
set -euo pipefail

umask 077

ROOT_DIR="${HHRU_BACKUP_RESTORE_DRILL_ROOT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
LOCK_FILE="${HHRU_BACKUP_RESTORE_DRILL_LOCK_FILE:-${ROOT_DIR}/.state/locks/backup-restore-drill.lock}"
HEAVY_OPS_LOCK_FILE="${HHRU_HEAVY_OPS_LOCK_FILE:-${ROOT_DIR}/.state/locks/heavy-ops.lock}"
HEAVY_OPS_LOCK_WAIT_SECONDS="${HHRU_HEAVY_OPS_LOCK_WAIT_SECONDS:-21600}"
LOG_ROOT="${HHRU_BACKUP_RESTORE_DRILL_LOG_ROOT:-${ROOT_DIR}/.state/logs/backup-restore-drill}"
LOG_RETENTION_DAYS="${HHRU_BACKUP_RESTORE_DRILL_LOG_RETENTION_DAYS:-90}"
BACKUP_DIR="${HHRU_BACKUP_RESTORE_DRILL_BACKUP_DIR:-${ROOT_DIR}/.state/backups}"
TARGET_DB="${HHRU_BACKUP_RESTORE_DRILL_TARGET_DB:-hhru_platform_restore_drill}"

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

require_non_negative_integer HHRU_BACKUP_RESTORE_DRILL_LOG_RETENTION_DAYS "$LOG_RETENTION_DAYS"
require_positive_integer HHRU_HEAVY_OPS_LOCK_WAIT_SECONDS "$HEAVY_OPS_LOCK_WAIT_SECONDS"
if [[ ! "$TARGET_DB" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]]; then
  printf 'invalid restore drill target database: %s\n' "$TARGET_DB" >&2
  exit 2
fi
case "$TARGET_DB" in
  postgres|template0|template1|hhru_platform)
    printf 'refusing unsafe restore drill target database: %s\n' "$TARGET_DB" >&2
    exit 2
    ;;
esac

cd "$ROOT_DIR"
mkdir -p "$(dirname "$LOCK_FILE")" "$(dirname "$HEAVY_OPS_LOCK_FILE")" "$LOG_ROOT"

exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  printf 'status=skipped\nreason=backup_restore_drill_lock_held\nlock_file=%s\n' "$LOCK_FILE"
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
TRIGGER_PREFIX="weekly-production-backup-restore-drill-${RUN_ID}"
RUN_LOG_DIR="${LOG_ROOT}/${RUN_ID}"
mkdir -p "$RUN_LOG_DIR"

COMPOSE=(docker compose --profile ops run --rm app)
POSTGRES=(docker compose exec -T postgres)

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

drop_target_db() {
  "${POSTGRES[@]}" sh -ceu \
    'dropdb --username "$POSTGRES_USER" --if-exists "$1"' \
    sh "$TARGET_DB"
}

if [[ ! -d "$BACKUP_DIR" ]]; then
  printf 'operation=weekly_backup_restore_drill status=failed reason=backup_dir_not_found backup_dir=%s\n' \
    "$BACKUP_DIR" >&2
  exit 1
fi
BACKUP_DIR="$(cd "$BACKUP_DIR" && pwd)"

latest_receipt="$(
  find "$BACKUP_DIR" -type f -name '*.dump.offsite.verified.json' -printf '%T@ %p\n' \
    | sort -nr \
    | sed -n '1p' \
    | cut -d' ' -f2-
)"
if [[ -z "$latest_receipt" ]]; then
  printf 'operation=weekly_backup_restore_drill status=failed reason=no_verified_backup_receipt\n' >&2
  exit 1
fi
backup_file_host="${latest_receipt%.offsite.verified.json}"
if [[ ! -f "${backup_file_host}.manifest.json" ]]; then
  printf 'operation=weekly_backup_restore_drill status=failed reason=missing_backup_manifest backup_file=%s\n' \
    "$backup_file_host" >&2
  exit 1
fi
case "$backup_file_host" in
  "${ROOT_DIR}/"*)
    backup_file="${backup_file_host#"${ROOT_DIR}/"}"
    ;;
  *)
    printf 'operation=weekly_backup_restore_drill status=failed reason=backup_outside_repo_mount backup_file=%s\n' \
      "$backup_file_host" >&2
    exit 1
    ;;
esac

printf 'operation=weekly_backup_restore_drill status=started run_id=%s backup_file=%s target_db=%s log_dir=%s\n' \
  "$RUN_ID" "$backup_file" "$TARGET_DB" "$RUN_LOG_DIR"

cleanup_pending=yes
cleanup_on_exit() {
  if [[ "$cleanup_pending" == "yes" ]]; then
    drop_target_db >>"${RUN_LOG_DIR}/cleanup-on-exit.log" 2>&1 || true
  fi
}
trap cleanup_on_exit EXIT

run_step offsite-restore-drill \
  "${COMPOSE[@]}" run-backup-offsite-restore-drill \
  --backup-file "$backup_file" \
  --target-db "$TARGET_DB" \
  --triggered-by "${TRIGGER_PREFIX}-restore"

run_step cleanup-restore-db drop_target_db
cleanup_pending=no
trap - EXIT

printf 'operation=weekly_backup_restore_drill status=succeeded run_id=%s backup_file=%s target_db=%s log_dir=%s\n' \
  "$RUN_ID" "$backup_file" "$TARGET_DB" "$RUN_LOG_DIR"
