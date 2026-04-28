from __future__ import annotations

import argparse
import json
import logging
import urllib.error
import urllib.request
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from hhru_platform.config.logging import configure_logging
from hhru_platform.config.settings import get_settings

LOGGER = logging.getLogger(__name__)
MAX_TELEGRAM_MESSAGE_LENGTH = 3900


@dataclass(slots=True, frozen=True)
class TelegramConfig:
    bot_token: str
    chat_id: str
    disable_notification: bool = False


def main() -> int:
    configure_logging()
    settings = get_settings()
    parser = argparse.ArgumentParser(
        prog="hhru-alert-webhook",
        description="Receive Alertmanager webhooks and forward them to configured channels.",
    )
    parser.add_argument("--host", default=settings.alert_webhook_host)
    parser.add_argument("--port", type=int, default=settings.alert_webhook_port)
    args = parser.parse_args()

    telegram_config = _telegram_config_from_settings(settings)
    handler = _build_handler(telegram_config=telegram_config)
    server = ThreadingHTTPServer((str(args.host), int(args.port)), handler)
    LOGGER.info(
        "alert webhook server started",
        extra={
            "host": str(args.host),
            "port": int(args.port),
            "telegram_configured": telegram_config is not None,
        },
    )
    try:
        server.serve_forever()
    finally:
        server.server_close()
    return 0


def format_alertmanager_message(payload: dict[str, Any], *, max_alerts: int = 5) -> str:
    status = str(payload.get("status") or "unknown")
    group_labels = _string_map(payload.get("groupLabels"))
    common_labels = _string_map(payload.get("commonLabels"))
    common_annotations = _string_map(payload.get("commonAnnotations"))
    alerts = [alert for alert in payload.get("alerts", []) if isinstance(alert, dict)]

    severity = common_labels.get("severity", "-")
    alert_name = common_labels.get("alertname") or group_labels.get("alertname") or "-"
    summary = common_annotations.get("summary", "-")
    action = common_annotations.get("action")

    lines = [
        f"[{status.upper()}] {alert_name}",
        f"severity: {severity}",
        f"summary: {summary}",
    ]
    if action:
        lines.append(f"action: {action}")
    if group_labels:
        lines.append("group: " + _format_labels(group_labels))
    lines.append(f"alerts: {len(alerts)}")

    for index, alert in enumerate(alerts[:max_alerts], start=1):
        labels = _string_map(alert.get("labels"))
        annotations = _string_map(alert.get("annotations"))
        starts_at = str(alert.get("startsAt") or "-")
        instance = labels.get("instance", "-")
        alert_summary = annotations.get("summary") or summary
        lines.append(f"{index}. {labels.get('alertname', alert_name)} instance={instance}")
        lines.append(f"   starts_at={starts_at}")
        lines.append(f"   summary={alert_summary}")

    omitted = len(alerts) - max_alerts
    if omitted > 0:
        lines.append(f"... omitted {omitted} alert(s)")

    message = "\n".join(lines)
    if len(message) > MAX_TELEGRAM_MESSAGE_LENGTH:
        return message[: MAX_TELEGRAM_MESSAGE_LENGTH - 20] + "\n... truncated"
    return message


def send_telegram_message(config: TelegramConfig, message: str) -> None:
    request = urllib.request.Request(
        url=f"https://api.telegram.org/bot{config.bot_token}/sendMessage",
        data=json.dumps(
            {
                "chat_id": config.chat_id,
                "text": message,
                "disable_notification": config.disable_notification,
            }
        ).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        response.read()


def _build_handler(
    *,
    telegram_config: TelegramConfig | None,
) -> type[BaseHTTPRequestHandler]:
    class AlertWebhookHandler(BaseHTTPRequestHandler):
        server_version = "hhru-alert-webhook/1.0"

        def do_GET(self) -> None:
            if self.path != "/healthz":
                self.send_error(HTTPStatus.NOT_FOUND)
                return
            self._write_response(HTTPStatus.OK, {"status": "ok"})

        def do_POST(self) -> None:
            if self.path not in {"/", "/alertmanager"}:
                self.send_error(HTTPStatus.NOT_FOUND)
                return

            try:
                payload = self._read_json_body()
            except ValueError as error:
                LOGGER.warning("invalid alert webhook payload: %s", error)
                self._write_response(HTTPStatus.BAD_REQUEST, {"error": str(error)})
                return

            alerts = payload.get("alerts", [])
            alert_count = len(alerts) if isinstance(alerts, list) else 0
            message = format_alertmanager_message(payload)
            LOGGER.warning(
                "alertmanager webhook received",
                extra={
                    "alert_status": payload.get("status"),
                    "alert_count": alert_count,
                    "telegram_configured": telegram_config is not None,
                    "alert_message": message,
                },
            )
            if telegram_config is not None:
                try:
                    send_telegram_message(telegram_config, message)
                except (OSError, urllib.error.URLError, TimeoutError) as error:
                    LOGGER.exception("telegram alert delivery failed: %s", error)
                    self._write_response(
                        HTTPStatus.BAD_GATEWAY,
                        {"status": "telegram_delivery_failed"},
                    )
                    return

            self._write_response(HTTPStatus.OK, {"status": "accepted"})

        def log_message(self, format: str, *args: Any) -> None:
            LOGGER.debug("alert webhook access: " + format, *args)

        def _read_json_body(self) -> dict[str, Any]:
            content_length = int(self.headers.get("Content-Length", "0"))
            if content_length <= 0:
                raise ValueError("empty request body")
            body = self.rfile.read(content_length)
            payload = json.loads(body.decode("utf-8"))
            if not isinstance(payload, dict):
                raise ValueError("request body must be a JSON object")
            return payload

        def _write_response(self, status: HTTPStatus, payload: dict[str, str]) -> None:
            body = json.dumps(payload).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    return AlertWebhookHandler


def _telegram_config_from_settings(settings: Any) -> TelegramConfig | None:
    bot_token = settings.alert_telegram_bot_token
    chat_id = settings.alert_telegram_chat_id
    if not bot_token or not chat_id:
        return None
    return TelegramConfig(
        bot_token=str(bot_token),
        chat_id=str(chat_id),
        disable_notification=bool(settings.alert_telegram_disable_notification),
    )


def _string_map(value: Any) -> dict[str, str]:
    if not isinstance(value, dict):
        return {}
    return {str(key): str(item) for key, item in value.items()}


def _format_labels(labels: dict[str, str]) -> str:
    return ", ".join(f"{key}={value}" for key, value in sorted(labels.items()))


if __name__ == "__main__":
    raise SystemExit(main())
