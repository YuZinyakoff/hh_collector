#!/usr/bin/env bash
set -euo pipefail

umask 077

backup_dir="${HHRU_BACKUP_DIR:-.state/backups}"
backup_prefix="${HHRU_BACKUP_PREFIX:-hhru-platform}"
backup_retention_days="${HHRU_BACKUP_RETENTION_DAYS:-7}"
db_host="${HHRU_DB_HOST:-localhost}"
db_port="${HHRU_DB_PORT:-5432}"
db_name="${HHRU_DB_NAME:?HHRU_DB_NAME is required}"
db_user="${HHRU_DB_USER:?HHRU_DB_USER is required}"
db_password="${HHRU_DB_PASSWORD:?HHRU_DB_PASSWORD is required}"
timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
backup_file="${backup_dir%/}/${backup_prefix}_${db_name}_${timestamp}.dump"
max_dump_bytes="${HHRU_BACKUP_MAX_DUMP_BYTES:-}"

dump_args=(
  --host="${db_host}"
  --port="${db_port}"
  --username="${db_user}"
  --dbname="${db_name}"
  --format=custom
  --compress=9
  --clean
  --if-exists
  --create
  --no-owner
  --no-privileges
)
export PGPASSWORD="${db_password}"

if [[ "$#" == 1 && "${1:-}" == --stdout ]]; then
  exec pg_dump "${dump_args[@]}"
elif (( $# != 0 )); then
  printf 'usage: backup_postgres.sh [--stdout]\n' >&2
  exit 2
fi

mkdir -p "${backup_dir}"

if [[ -n "$max_dump_bytes" ]] &&
   { [[ ! "$max_dump_bytes" =~ ^[1-9][0-9]{0,17}$ ]] || (( max_dump_bytes < 1024 )); }; then
  printf 'HHRU_BACKUP_MAX_DUMP_BYTES must be an integer of at least 1024 bytes\n' >&2
  exit 2
fi

partial_file="$(mktemp "${backup_file}.partial.XXXXXXXX")"
trap 'rm -f -- "$partial_file"' EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

(
  if [[ -n "$max_dump_bytes" ]]; then
    ulimit -f "$((max_dump_bytes / 1024))"
  fi
  exec pg_dump "${dump_args[@]}" --file="${partial_file}"
)

# Publish only a completed dump; the hard link also refuses a name collision.
ln -T -- "$partial_file" "$backup_file"
rm -f -- "$partial_file"

if [[ "${backup_retention_days}" =~ ^[0-9]+$ ]] && (( backup_retention_days > 0 )); then
  find "${backup_dir}" \
    -maxdepth 1 \
    -type f \
    -name "${backup_prefix}_${db_name}_*.dump" \
    -mtime +"${backup_retention_days}" \
    -delete
fi

printf 'backup_file=%s\n' "${backup_file}"
