#!/usr/bin/env bash
set -euo pipefail

UNIT_NAME="${1:-}"
WEBHOOK_URL="${HHRU_OPS_FAILURE_WEBHOOK_URL:-http://127.0.0.1:8010/alertmanager}"
TIMEOUT_SECONDS="${HHRU_OPS_FAILURE_TIMEOUT_SECONDS:-10}"

if [[ ! "$UNIT_NAME" =~ ^[A-Za-z0-9@_.:-]+$ ]]; then
  printf 'invalid or missing systemd unit name: %s\n' "$UNIT_NAME" >&2
  exit 2
fi
if [[ ! "$TIMEOUT_SECONDS" =~ ^[1-9][0-9]*$ ]]; then
  printf 'HHRU_OPS_FAILURE_TIMEOUT_SECONDS must be a positive integer\n' >&2
  exit 2
fi

HOST_NAME="$(hostname | tr -cd 'A-Za-z0-9_.-')"
HOST_NAME="${HOST_NAME:-unknown-host}"
STARTED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
SUMMARY="systemd unit failed: ${UNIT_NAME}"
ACTION="Run systemctl status ${UNIT_NAME} and journalctl -u ${UNIT_NAME} on ${HOST_NAME}"

PAYLOAD="$(
  printf '{"status":"firing","groupLabels":{"alertname":"HHRUPlatformSystemdUnitFailed","severity":"critical","unit":"%s"},"commonLabels":{"alertname":"HHRUPlatformSystemdUnitFailed","severity":"critical","unit":"%s","instance":"%s"},"commonAnnotations":{"summary":"%s","action":"%s"},"alerts":[{"status":"firing","labels":{"alertname":"HHRUPlatformSystemdUnitFailed","severity":"critical","unit":"%s","instance":"%s"},"annotations":{"summary":"%s","action":"%s"},"startsAt":"%s"}]}' \
    "$UNIT_NAME" "$UNIT_NAME" "$HOST_NAME" "$SUMMARY" "$ACTION" \
    "$UNIT_NAME" "$HOST_NAME" "$SUMMARY" "$ACTION" "$STARTED_AT"
)"

curl --fail --silent --show-error \
  --max-time "$TIMEOUT_SECONDS" \
  --header 'Content-Type: application/json' \
  --data-binary "$PAYLOAD" \
  "$WEBHOOK_URL"

printf 'operation=notify_systemd_failure status=succeeded unit=%s webhook_url=%s\n' \
  "$UNIT_NAME" "$WEBHOOK_URL"
