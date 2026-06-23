#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
RUN_ID=""
START_TS="2026-06-23T12:00:00+00:00"
LOG_FILE=""
MAX_TREE_ROWS="80"

usage() {
  cat <<'USAGE'
Usage:
  scripts/ops/inspect_collection_run.sh --run-id UUID [--start-ts ISO_TS] [--log FILE] [--max-tree-rows N]

Examples:
  scripts/ops/inspect_collection_run.sh --run-id bcf9ef54-27b0-4a90-bd33-728775053ea4 --start-ts 2026-06-23T12:00:00+00:00 --log /tmp/hhru-production-search-20260623.log
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --run-id)
      RUN_ID="${2:-}"
      shift 2
      ;;
    --start-ts)
      START_TS="${2:-}"
      shift 2
      ;;
    --log)
      LOG_FILE="${2:-}"
      shift 2
      ;;
    --max-tree-rows)
      MAX_TREE_ROWS="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      printf 'unknown argument: %s\n' "$1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ ! "$RUN_ID" =~ ^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$ ]]; then
  printf 'RUN_ID must be a UUID, got: %s\n' "$RUN_ID" >&2
  exit 2
fi

if [[ -z "$START_TS" ]]; then
  printf 'START_TS must be non-empty\n' >&2
  exit 2
fi

if [[ ! "$MAX_TREE_ROWS" =~ ^[1-9][0-9]*$ ]]; then
  printf 'MAX_TREE_ROWS must be a positive integer, got: %s\n' "$MAX_TREE_ROWS" >&2
  exit 2
fi

cd "$ROOT_DIR"

PSQL=(
  docker compose exec -T postgres
  psql
  -U "${HHRU_DB_USER:-hhru}"
  -d "${HHRU_DB_NAME:-hhru_platform}"
  -P pager=off
  -v ON_ERROR_STOP=1
)

print_section() {
  printf '\n=== %s ===\n' "$1"
}

run_sql() {
  "${PSQL[@]}" -c "$1"
}

print_section "inspect collection run"
printf 'repo_root=%s\n' "$ROOT_DIR"
printf 'run_id=%s\n' "$RUN_ID"
printf 'start_ts=%s\n' "$START_TS"
if [[ -n "$LOG_FILE" ]]; then
  printf 'log_file=%s\n' "$LOG_FILE"
fi

print_section "collection containers"
docker compose ps scheduler detail-worker

if [[ -n "$LOG_FILE" ]]; then
  print_section "log tail"
  if [[ -f "$LOG_FILE" ]]; then
    tail -120 "$LOG_FILE"
  else
    printf 'log_file_not_found=%s\n' "$LOG_FILE"
  fi

  print_section "log signal tail"
  if [[ -f "$LOG_FILE" ]]; then
    PATTERN='failed|error|exception|traceback|captcha|forbidden|429|403|502|completed|summary|status'
    grep -Ei "$PATTERN" "$LOG_FILE" | tail -120 || true
  else
    printf 'log_file_not_found=%s\n' "$LOG_FILE"
  fi
fi

print_section "run state"
run_sql "select id, run_type, status, triggered_by, started_at, finished_at from crawl_run where id = '$RUN_ID'::uuid;"

print_section "partition state"
run_sql "select status, coverage_status, count(*) from crawl_partition where crawl_run_id = '$RUN_ID'::uuid group by status, coverage_status order by status, coverage_status;"

print_section "fresh row counts"
run_sql "select 'raw_payloads_since_start' as metric, count(*) from raw_api_payload where received_at >= '$START_TS'::timestamptz union all select 'seen_events_since_start', count(*) from vacancy_seen_event where seen_at >= '$START_TS'::timestamptz union all select 'new_vacancies_since_start', count(*) from vacancy_current_state where first_seen_at >= '$START_TS'::timestamptz union all select 'short_snapshots_since_start', count(*) from vacancy_snapshot where snapshot_type = 'short' and captured_at >= '$START_TS'::timestamptz union all select 'detail_snapshots_since_start', count(*) from vacancy_snapshot where snapshot_type = 'detail' and captured_at >= '$START_TS'::timestamptz union all select 'detail_attempts_since_start', count(*) from detail_fetch_attempt where requested_at >= '$START_TS'::timestamptz;"

print_section "fresh timestamp ranges"
run_sql "select 'raw_payloads' as metric, min(received_at), max(received_at) from raw_api_payload where received_at >= '$START_TS'::timestamptz union all select 'seen_events', min(seen_at), max(seen_at) from vacancy_seen_event where seen_at >= '$START_TS'::timestamptz union all select 'new_vacancies', min(first_seen_at), max(first_seen_at) from vacancy_current_state where first_seen_at >= '$START_TS'::timestamptz union all select 'short_snapshots', min(captured_at), max(captured_at) from vacancy_snapshot where snapshot_type = 'short' and captured_at >= '$START_TS'::timestamptz union all select 'detail_snapshots', min(captured_at), max(captured_at) from vacancy_snapshot where snapshot_type = 'detail' and captured_at >= '$START_TS'::timestamptz union all select 'detail_attempts', min(requested_at), max(requested_at) from detail_fetch_attempt where requested_at >= '$START_TS'::timestamptz;"

print_section "run coverage"
docker compose --profile ops run --rm app show-run-coverage --run-id "$RUN_ID"

print_section "run tree"
docker compose --profile ops run --rm app show-run-tree --run-id "$RUN_ID" --max-rows "$MAX_TREE_ROWS"
