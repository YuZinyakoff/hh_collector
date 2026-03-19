#!/usr/bin/env bash
set -euo pipefail

restore_file="${1:-${HHRU_RESTORE_FILE:-}}"
db_host="${HHRU_DB_HOST:-localhost}"
db_port="${HHRU_DB_PORT:-5432}"
db_user="${HHRU_DB_USER:?HHRU_DB_USER is required}"
db_password="${HHRU_DB_PASSWORD:?HHRU_DB_PASSWORD is required}"
restore_confirm="${HHRU_RESTORE_CONFIRM:-}"

if [[ -z "${restore_file}" ]]; then
  echo "restore file is required via first argument or HHRU_RESTORE_FILE" >&2
  exit 1
fi

if [[ ! -f "${restore_file}" ]]; then
  echo "restore file not found: ${restore_file}" >&2
  exit 1
fi

if [[ "${restore_confirm}" != "yes" ]]; then
  echo "restore is destructive; set HHRU_RESTORE_CONFIRM=yes to continue" >&2
  exit 1
fi

export PGPASSWORD="${db_password}"

pg_restore \
  --host="${db_host}" \
  --port="${db_port}" \
  --username="${db_user}" \
  --dbname=postgres \
  --clean \
  --if-exists \
  --create \
  --no-owner \
  --no-privileges \
  "${restore_file}"

printf 'restored_from=%s\n' "${restore_file}"
