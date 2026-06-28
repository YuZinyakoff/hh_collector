#!/usr/bin/env bash
set -euo pipefail

umask 077

ROOT_DIR="${HHRU_RESEARCH_ARCHIVE_DAILY_ROOT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
LOCK_FILE="${HHRU_RESEARCH_ARCHIVE_DAILY_LOCK_FILE:-${ROOT_DIR}/.state/locks/research-archive-daily.lock}"
HEAVY_OPS_LOCK_FILE="${HHRU_HEAVY_OPS_LOCK_FILE:-${ROOT_DIR}/.state/locks/heavy-ops.lock}"
HEAVY_OPS_LOCK_WAIT_SECONDS="${HHRU_HEAVY_OPS_LOCK_WAIT_SECONDS:-21600}"
LOG_ROOT="${HHRU_RESEARCH_ARCHIVE_DAILY_LOG_ROOT:-${ROOT_DIR}/.state/logs/research-archive-daily}"
LOG_RETENTION_DAYS="${HHRU_RESEARCH_ARCHIVE_DAILY_LOG_RETENTION_DAYS:-30}"
MAX_EXPORT_BATCHES="${HHRU_RESEARCH_ARCHIVE_DAILY_MAX_EXPORT_BATCHES:-20}"
LIMIT_PER_DATASET="${HHRU_RESEARCH_ARCHIVE_DAILY_LIMIT_PER_DATASET:-100000}"
CHUNK_SIZE="${HHRU_RESEARCH_ARCHIVE_DAILY_CHUNK_SIZE:-100000}"
BATCH_SIZE="${HHRU_RESEARCH_ARCHIVE_DAILY_BATCH_SIZE:-1000}"
SETTLED_DELAY_HOURS="${HHRU_RESEARCH_ARCHIVE_DAILY_SETTLED_DELAY_HOURS:-24}"
READBACK_LIMIT="${HHRU_RESEARCH_ARCHIVE_DAILY_READBACK_LIMIT:-2}"
HOUSEKEEPING_APPLY="${HHRU_RESEARCH_ARCHIVE_DAILY_HOUSEKEEPING_APPLY:-false}"
HOUSEKEEPING_RAW_API_PAYLOAD_RETENTION_DAYS="${HHRU_RESEARCH_ARCHIVE_DAILY_RAW_API_PAYLOAD_RETENTION_DAYS:-${HHRU_HOUSEKEEPING_RAW_API_PAYLOAD_RETENTION_DAYS:-}}"
HOUSEKEEPING_VACANCY_SNAPSHOT_RETENTION_DAYS="${HHRU_RESEARCH_ARCHIVE_DAILY_VACANCY_SNAPSHOT_RETENTION_DAYS:-${HHRU_HOUSEKEEPING_VACANCY_SNAPSHOT_RETENTION_DAYS:-}}"
HOUSEKEEPING_DETAIL_FETCH_ATTEMPT_RETENTION_DAYS="${HHRU_RESEARCH_ARCHIVE_DAILY_DETAIL_FETCH_ATTEMPT_RETENTION_DAYS:-${HHRU_HOUSEKEEPING_DETAIL_FETCH_ATTEMPT_RETENTION_DAYS:-}}"
HOUSEKEEPING_FINISHED_CRAWL_RUN_RETENTION_DAYS="${HHRU_RESEARCH_ARCHIVE_DAILY_FINISHED_CRAWL_RUN_RETENTION_DAYS:-${HHRU_HOUSEKEEPING_FINISHED_CRAWL_RUN_RETENTION_DAYS:-}}"
HOUSEKEEPING_DELETE_LIMIT_PER_TARGET="${HHRU_RESEARCH_ARCHIVE_DAILY_DELETE_LIMIT_PER_TARGET:-${HHRU_HOUSEKEEPING_DELETE_LIMIT_PER_TARGET:-}}"

require_positive_integer() {
  local name="$1"
  local value="$2"
  if [[ ! "$value" =~ ^[1-9][0-9]*$ ]]; then
    printf '%s must be a positive integer, got: %s\n' "$name" "$value" >&2
    exit 2
  fi
}

require_non_negative_integer() {
  local name="$1"
  local value="$2"
  if [[ ! "$value" =~ ^[0-9]+$ ]]; then
    printf '%s must be a non-negative integer, got: %s\n' "$name" "$value" >&2
    exit 2
  fi
}

require_optional_non_negative_integer() {
  local name="$1"
  local value="$2"
  if [[ -n "$value" ]]; then
    require_non_negative_integer "$name" "$value"
  fi
}

require_optional_positive_integer() {
  local name="$1"
  local value="$2"
  if [[ -n "$value" ]]; then
    require_positive_integer "$name" "$value"
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

require_positive_integer HHRU_RESEARCH_ARCHIVE_DAILY_MAX_EXPORT_BATCHES "$MAX_EXPORT_BATCHES"
require_positive_integer HHRU_RESEARCH_ARCHIVE_DAILY_LIMIT_PER_DATASET "$LIMIT_PER_DATASET"
require_positive_integer HHRU_RESEARCH_ARCHIVE_DAILY_CHUNK_SIZE "$CHUNK_SIZE"
require_positive_integer HHRU_RESEARCH_ARCHIVE_DAILY_BATCH_SIZE "$BATCH_SIZE"
require_positive_integer HHRU_HEAVY_OPS_LOCK_WAIT_SECONDS "$HEAVY_OPS_LOCK_WAIT_SECONDS"
require_non_negative_integer HHRU_RESEARCH_ARCHIVE_DAILY_LOG_RETENTION_DAYS "$LOG_RETENTION_DAYS"
require_non_negative_integer HHRU_RESEARCH_ARCHIVE_DAILY_SETTLED_DELAY_HOURS "$SETTLED_DELAY_HOURS"
require_non_negative_integer HHRU_RESEARCH_ARCHIVE_DAILY_READBACK_LIMIT "$READBACK_LIMIT"
require_optional_non_negative_integer \
  HHRU_RESEARCH_ARCHIVE_DAILY_RAW_API_PAYLOAD_RETENTION_DAYS \
  "$HOUSEKEEPING_RAW_API_PAYLOAD_RETENTION_DAYS"
require_optional_non_negative_integer \
  HHRU_RESEARCH_ARCHIVE_DAILY_VACANCY_SNAPSHOT_RETENTION_DAYS \
  "$HOUSEKEEPING_VACANCY_SNAPSHOT_RETENTION_DAYS"
require_optional_non_negative_integer \
  HHRU_RESEARCH_ARCHIVE_DAILY_DETAIL_FETCH_ATTEMPT_RETENTION_DAYS \
  "$HOUSEKEEPING_DETAIL_FETCH_ATTEMPT_RETENTION_DAYS"
require_optional_non_negative_integer \
  HHRU_RESEARCH_ARCHIVE_DAILY_FINISHED_CRAWL_RUN_RETENTION_DAYS \
  "$HOUSEKEEPING_FINISHED_CRAWL_RUN_RETENTION_DAYS"
require_optional_positive_integer \
  HHRU_RESEARCH_ARCHIVE_DAILY_DELETE_LIMIT_PER_TARGET \
  "$HOUSEKEEPING_DELETE_LIMIT_PER_TARGET"
HOUSEKEEPING_APPLY_NORMALIZED="$(
  normalize_bool \
    HHRU_RESEARCH_ARCHIVE_DAILY_HOUSEKEEPING_APPLY \
    "$HOUSEKEEPING_APPLY"
)"

HOUSEKEEPING_RETENTION_ARGS=()
append_housekeeping_arg() {
  local flag="$1"
  local value="$2"
  if [[ -n "$value" ]]; then
    HOUSEKEEPING_RETENTION_ARGS+=("$flag" "$value")
  fi
}

append_housekeeping_arg \
  --raw-api-payload-retention-days \
  "$HOUSEKEEPING_RAW_API_PAYLOAD_RETENTION_DAYS"
append_housekeeping_arg \
  --vacancy-snapshot-retention-days \
  "$HOUSEKEEPING_VACANCY_SNAPSHOT_RETENTION_DAYS"
append_housekeeping_arg \
  --detail-fetch-attempt-retention-days \
  "$HOUSEKEEPING_DETAIL_FETCH_ATTEMPT_RETENTION_DAYS"
append_housekeeping_arg \
  --finished-crawl-run-retention-days \
  "$HOUSEKEEPING_FINISHED_CRAWL_RUN_RETENTION_DAYS"
append_housekeeping_arg \
  --delete-limit-per-target \
  "$HOUSEKEEPING_DELETE_LIMIT_PER_TARGET"

cd "$ROOT_DIR"
mkdir -p "$(dirname "$LOCK_FILE")" "$(dirname "$HEAVY_OPS_LOCK_FILE")" "$LOG_ROOT"

exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  printf 'status=skipped\nreason=research_archive_daily_lock_held\nlock_file=%s\n' "$LOCK_FILE"
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
TRIGGER_PREFIX="daily-production-archive-${RUN_ID}"
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

printf 'operation=daily_research_archive status=started run_id=%s log_dir=%s\n' \
  "$RUN_ID" "$RUN_LOG_DIR"

export_complete=no
for batch_number in $(seq 1 "$MAX_EXPORT_BATCHES"); do
  step="export-${batch_number}"
  run_step "$step" \
    "${COMPOSE[@]}" export-research-archive \
    --incremental \
    --settled-delay-hours "$SETTLED_DELAY_HOURS" \
    --limit-per-dataset "$LIMIT_PER_DATASET" \
    --chunk-size "$CHUNK_SIZE" \
    --batch-size "$BATCH_SIZE" \
    --archive-kind production \
    --triggered-by "${TRIGGER_PREFIX}-${step}"

  total_row_count="$(
    awk -F= '$1 == "total_row_count" { value = $2 } END { print value }' \
      "${RUN_LOG_DIR}/${step}.log"
  )"
  if [[ ! "$total_row_count" =~ ^[0-9]+$ ]]; then
    printf 'step=%s status=failed reason=missing_total_row_count\n' "$step" >&2
    exit 1
  fi
  printf 'step=%s total_row_count=%s\n' "$step" "$total_row_count"

  if (( total_row_count == 0 )); then
    export_complete=yes
    break
  fi
done

if [[ "$export_complete" != yes ]]; then
  printf 'operation=daily_research_archive status=failed reason=max_export_batches_exhausted max_export_batches=%s\n' \
    "$MAX_EXPORT_BATCHES" >&2
  exit 1
fi

run_step local-verify \
  "${COMPOSE[@]}" verify-research-archive \
  --triggered-by "${TRIGGER_PREFIX}-local-verify"

run_step offsite-sync \
  "${COMPOSE[@]}" sync-research-archive-offsite \
  --triggered-by "${TRIGGER_PREFIX}-offsite-sync"

run_step offsite-verify \
  "${COMPOSE[@]}" verify-research-archive-offsite \
  --readback-limit "$READBACK_LIMIT" \
  --triggered-by "${TRIGGER_PREFIX}-offsite-verify"

run_step coverage-audit \
  "${COMPOSE[@]}" audit-research-archive-coverage \
  --archive-kind production \
  --triggered-by "${TRIGGER_PREFIX}-coverage-audit"

run_step housekeeping-preview \
  "${COMPOSE[@]}" preview-research-archive-housekeeping \
  --archive-kind production \
  "${HOUSEKEEPING_RETENTION_ARGS[@]}" \
  --triggered-by "${TRIGGER_PREFIX}-housekeeping-preview"

if [[ "$HOUSEKEEPING_APPLY_NORMALIZED" == "yes" ]]; then
  run_step housekeeping-apply \
    "${COMPOSE[@]}" apply-research-archive-housekeeping \
    --archive-kind production \
    --apply \
    "${HOUSEKEEPING_RETENTION_ARGS[@]}" \
    --triggered-by "${TRIGGER_PREFIX}-housekeeping-apply"
fi

printf 'operation=daily_research_archive status=succeeded run_id=%s log_dir=%s\n' \
  "$RUN_ID" "$RUN_LOG_DIR"
