#!/usr/bin/env bash
set -euo pipefail

restore_file="${1:-${HHRU_RESTORE_FILE:-}}"
db_host="${HHRU_DB_HOST:-localhost}"
db_port="${HHRU_DB_PORT:-5432}"
db_name="${HHRU_DB_NAME:-hhru_platform}"
db_user="${HHRU_DB_USER:?HHRU_DB_USER is required}"
db_password="${HHRU_DB_PASSWORD:?HHRU_DB_PASSWORD is required}"
restore_confirm="${HHRU_RESTORE_CONFIRM:-}"
restore_target_db="${HHRU_RESTORE_TARGET_DB:-}"
restore_drop_target_db="${HHRU_RESTORE_DROP_TARGET_DB:-yes}"

if [[ -z "${restore_file}" ]]; then
  echo "restore file is required via first argument or HHRU_RESTORE_FILE" >&2
  exit 1
fi

if [[ ! -f "${restore_file}" ]]; then
  echo "restore file not found: ${restore_file}" >&2
  exit 1
fi

pg_restore --list "${restore_file}" >/dev/null

if [[ "${restore_confirm}" != "yes" ]]; then
  echo "restore requires explicit confirmation; set HHRU_RESTORE_CONFIRM=yes to continue" >&2
  exit 1
fi

export PGPASSWORD="${db_password}"

if [[ -n "${restore_target_db}" ]]; then
  if [[ "${restore_drop_target_db}" == "yes" ]]; then
    dropdb \
      --if-exists \
      --host="${db_host}" \
      --port="${db_port}" \
      --username="${db_user}" \
      "${restore_target_db}"
  fi

  createdb \
    --host="${db_host}" \
    --port="${db_port}" \
    --username="${db_user}" \
    "${restore_target_db}"

  pg_restore \
    --host="${db_host}" \
    --port="${db_port}" \
    --username="${db_user}" \
    --dbname="${restore_target_db}" \
    --clean \
    --if-exists \
    --no-owner \
    --no-privileges \
    "${restore_file}"

  printf 'restore_mode=%s\n' "target_db"
  printf 'restored_from=%s\n' "${restore_file}"
  printf 'restored_to_db=%s\n' "${restore_target_db}"
  exit 0
fi

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

printf 'restore_mode=%s\n' "replace"
printf 'restored_from=%s\n' "${restore_file}"
printf 'restored_to_db=%s\n' "${db_name}"
