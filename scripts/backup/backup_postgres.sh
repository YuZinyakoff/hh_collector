#!/usr/bin/env bash
set -euo pipefail

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

mkdir -p "${backup_dir}"

export PGPASSWORD="${db_password}"

pg_dump \
  --host="${db_host}" \
  --port="${db_port}" \
  --username="${db_user}" \
  --dbname="${db_name}" \
  --format=custom \
  --compress=9 \
  --clean \
  --if-exists \
  --create \
  --no-owner \
  --no-privileges \
  --file="${backup_file}"

if [[ "${backup_retention_days}" =~ ^[0-9]+$ ]] && (( backup_retention_days > 0 )); then
  find "${backup_dir}" \
    -maxdepth 1 \
    -type f \
    -name "${backup_prefix}_${db_name}_*.dump" \
    -mtime +"${backup_retention_days}" \
    -delete
fi

printf 'backup_file=%s\n' "${backup_file}"
