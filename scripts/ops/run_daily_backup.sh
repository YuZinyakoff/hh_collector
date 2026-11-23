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
PRUNE_VERIFIED_LOCAL_DUMPS="${HHRU_BACKUP_DAILY_PRUNE_VERIFIED_LOCAL_DUMPS:-true}"
RESERVE_BYTES="${HHRU_BACKUP_DAILY_RESERVE_BYTES:-10737418240}"
BACKUP_DIR=".state/backups"

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

require_non_negative_integer HHRU_BACKUP_DAILY_LOG_RETENTION_DAYS "$LOG_RETENTION_DAYS"
require_positive_integer HHRU_BACKUP_DAILY_LOCAL_RETENTION_DAYS "$LOCAL_BACKUP_RETENTION_DAYS"
require_positive_integer HHRU_HEAVY_OPS_LOCK_WAIT_SECONDS "$HEAVY_OPS_LOCK_WAIT_SECONDS"
require_positive_integer HHRU_BACKUP_DAILY_RESERVE_BYTES "$RESERVE_BYTES"
if (( ${#RESERVE_BYTES} > 17 )); then
  printf 'invalid backup reserve byte count\n' >&2
  exit 2
fi
PRUNE_VERIFIED_LOCAL_DUMPS_NORMALIZED="$(
  normalize_bool \
    HHRU_BACKUP_DAILY_PRUNE_VERIFIED_LOCAL_DUMPS \
    "$PRUNE_VERIFIED_LOCAL_DUMPS"
)"
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

COMPOSE_RUN=(docker compose --profile ops run --rm)
COMPOSE=("${COMPOSE_RUN[@]}" app)

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

has_verified_receipts() {
  local dump="$1"
  [[ -s "$dump.manifest.json" && -s "$dump.offsite.json" &&
     -s "$dump.offsite.verified.json" ]] || return 1
  python3 - "$dump" <<'PY'
import hashlib
import json
import sys
from pathlib import Path

dump = Path(sys.argv[1])
try:
    raw = Path(f"{dump}.manifest.json").read_bytes()
    manifest = json.loads(raw)
    upload = json.loads(Path(f"{dump}.offsite.json").read_bytes())
    verified = json.loads(Path(f"{dump}.offsite.verified.json").read_bytes())
    facts = {
        "backup_size_bytes": dump.stat().st_size,
        "backup_sha256": manifest["backup_sha256"],
        "manifest_sha256": hashlib.sha256(raw).hexdigest(),
        "chunk_size_bytes": manifest["chunk_size_bytes"],
        "part_count": len(manifest["parts"]),
    }
    matches = (
        manifest["backup_file"] == str(dump.relative_to(".state/backups"))
        and manifest["backup_size_bytes"] == facts["backup_size_bytes"]
        and facts["part_count"] > 0
        and all(r[k] == v for r in (upload, verified) for k, v in facts.items())
        and all(upload[k] == verified[k] for k in (
            "offsite_url", "offsite_root", "remote_backup_path", "remote_manifest_path",
        ))
        and verified["verified_object_count"] == facts["part_count"] + 1
    )
except (OSError, ValueError, TypeError, KeyError):
    matches = False
raise SystemExit(0 if matches else 1)
PY
}

prepare_dump_budget() {
  local latest_size required_bytes available_bytes
  latest_size="$(python3 - "$BACKUP_DIR" <<'PY'
import json
import sys
from pathlib import Path

receipts = list(Path(sys.argv[1]).glob("*.dump.offsite.verified.json"))
if not receipts:
    print(1024**3)
else:
    latest = max(receipts, key=lambda path: path.stat().st_mtime_ns)
    size = json.loads(latest.read_bytes())["backup_size_bytes"]
    if type(size) is not int or not 0 < size < 10**17:
        raise SystemExit("invalid backup_size_bytes in latest verification receipt")
    print(size)
PY
)" || return 1
  available_bytes="$(df -PB1 "$BACKUP_DIR" | awk 'NR == 2 { print $4 }')"
  require_positive_integer available_bytes "$available_bytes"
  required_bytes=$((latest_size + (latest_size + 9) / 10 + RESERVE_BYTES))
  if (( available_bytes < required_bytes )); then
    printf 'operation=daily_backup status=failed reason=insufficient_disk_space_preflight available_bytes=%s required_bytes=%s reserve_bytes=%s\n' \
      "$available_bytes" "$required_bytes" "$RESERVE_BYTES" >&2
    return 1
  fi
  max_dump_bytes=$((available_bytes - RESERVE_BYTES))
  printf 'disk_preflight_status=passed available_bytes=%s required_bytes=%s max_dump_bytes=%s reserve_bytes=%s\n' \
    "$available_bytes" "$required_bytes" "$max_dump_bytes" "$RESERVE_BYTES"
}

prune_verified_local_dumps() {
  local dump="$1"
  local dump_size

  if [[ "$PRUNE_VERIFIED_LOCAL_DUMPS_NORMALIZED" != "yes" ]]; then
    printf 'local_prune_enabled=no\n'
    printf 'local_deleted_dump_count=0\n'
    printf 'local_deleted_dump_bytes=0\n'
    return 0
  fi

  if ! has_verified_receipts "$dump"; then
    printf 'local_prune_error=unmatched_verification_receipts backup_file=%s\n' "$dump" >&2
    return 1
  fi

  dump_size="$(stat -c %s "$dump")"
  rm -f -- "$dump" "$dump.local-ready"
  printf 'local_deleted_dump=%s size_bytes=%s\n' "$dump" "$dump_size"
  printf 'local_prune_enabled=yes\n'
  printf 'local_deleted_dump_count=1\n'
  printf 'local_deleted_dump_bytes=%s\n' "$dump_size"
}

printf 'operation=daily_backup status=started run_id=%s log_dir=%s\n' \
  "$RUN_ID" "$RUN_LOG_DIR"

mkdir -p "$BACKUP_DIR"
shopt -s globstar nullglob dotglob
pending_dumps=()
for dump in "$BACKUP_DIR"/**/*.dump; do
  if [[ -L "$dump" || ! -f "$dump" ]] || ! has_verified_receipts "$dump"; then
    pending_dumps+=("$dump")
  fi
done
if (( ${#pending_dumps[@]} > 1 )); then
  printf 'operation=daily_backup status=failed reason=multiple_unverified_dumps count=%s\n' \
    "${#pending_dumps[@]}" >&2
  exit 1
elif (( ${#pending_dumps[@]} == 1 )); then
  backup_file="${pending_dumps[0]}"
  if [[ ! -f "$backup_file" || -L "$backup_file" ||
        ! -s "$backup_file.local-ready" || -L "$backup_file.local-ready" ]]; then
    printf 'operation=daily_backup status=failed reason=unconfirmed_local_dump backup_file=%s\n' \
      "$backup_file" >&2
    exit 1
  fi
  expected_sha="$(< "$backup_file.local-ready")"
  if [[ ! "$expected_sha" =~ ^[0-9a-f]{64}$ ]]; then
    printf 'operation=daily_backup status=failed reason=invalid_local_backup_proof\n' >&2
    exit 1
  fi
  printf 'operation=daily_backup action=resume backup_file=%s\n' "$backup_file"
else
  expected_sha=""
  prepare_dump_budget
  run_step create-backup \
    "${COMPOSE_RUN[@]}" -e "HHRU_BACKUP_MAX_DUMP_BYTES=$max_dump_bytes" app run-backup \
    --triggered-by "${TRIGGER_PREFIX}-create"

  backup_file="$(summary_value backup_file "${RUN_LOG_DIR}/create-backup.log")"
  if [[ "${backup_file%/*}" != "$BACKUP_DIR" || "$backup_file" != *.dump ||
        ! -f "$backup_file" || -L "$backup_file" ]]; then
    printf 'operation=daily_backup status=failed reason=unexpected_backup_file\n' >&2
    exit 1
  fi
  printf 'step=create-backup backup_file=%s\n' "$backup_file"
fi

run_step local-verify \
  "${COMPOSE[@]}" verify-backup-file \
  --backup-file "$backup_file"

local_sha="$(summary_value backup_sha256 "$RUN_LOG_DIR/local-verify.log")"
if [[ ! "$local_sha" =~ ^[0-9a-f]{64}$ ||
      ( -n "$expected_sha" && "$local_sha" != "$expected_sha" ) ]]; then
  printf 'operation=daily_backup status=failed reason=local_backup_hash_mismatch\n' >&2
  exit 1
fi
if [[ -z "$expected_sha" ]]; then
  ready_tmp="$(mktemp "$backup_file.local-ready.XXXXXXXX")"
  trap 'rm -f -- "$ready_tmp"' EXIT
  printf '%s\n' "$local_sha" >"$ready_tmp"
  mv -T -- "$ready_tmp" "$backup_file.local-ready"
  trap - EXIT
fi

run_step offsite-sync \
  "${COMPOSE[@]}" sync-backup-offsite \
  --backup-file "$backup_file" --limit 1 \
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

if [[ "$(summary_value backup_sha256 "$RUN_LOG_DIR/offsite-verify.log")" != "$local_sha" ]]; then
  printf 'operation=daily_backup status=failed reason=offsite_backup_hash_mismatch\n' >&2
  exit 1
fi

run_step prune-local-verified-dumps prune_verified_local_dumps "$backup_file"

printf 'operation=daily_backup status=succeeded run_id=%s backup_file=%s log_dir=%s\n' \
  "$RUN_ID" "$backup_file" "$RUN_LOG_DIR"
