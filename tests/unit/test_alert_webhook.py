from __future__ import annotations

import threading
import urllib.request
from typing import Any

from hhru_platform.interfaces.workers.alert_webhook import (
    TelegramConfig,
    TelegramDeliveryDispatcher,
    _queue_telegram_message,
    format_alertmanager_message,
    send_telegram_message,
)


def test_format_alertmanager_message_includes_action_and_alerts() -> None:
    payload = {
        "status": "firing",
        "groupLabels": {"alertname": "HHRUPlatformBackupStale"},
        "commonLabels": {
            "alertname": "HHRUPlatformBackupStale",
            "severity": "warning",
        },
        "commonAnnotations": {
            "summary": "PostgreSQL backup is stale",
            "action": "Run run-backup and verify the archive.",
        },
        "alerts": [
            {
                "labels": {
                    "alertname": "HHRUPlatformBackupStale",
                    "instance": "metrics:8001",
                },
                "annotations": {"summary": "PostgreSQL backup is stale"},
                "startsAt": "2026-04-28T10:00:00Z",
            }
        ],
    }

    message = format_alertmanager_message(payload)

    assert "[FIRING] HHRUPlatformBackupStale" in message
    assert "severity: warning" in message
    assert "action: Run run-backup and verify the archive." in message
    assert "alerts: 1" in message
    assert "1. HHRUPlatformBackupStale instance=metrics:8001" in message


def test_format_alertmanager_message_limits_alert_details() -> None:
    payload = {
        "status": "firing",
        "commonLabels": {"alertname": "ManyAlerts", "severity": "critical"},
        "commonAnnotations": {"summary": "many alerts"},
        "alerts": [
            {
                "labels": {"alertname": f"Alert{index}", "instance": f"node-{index}"},
                "annotations": {"summary": f"summary-{index}"},
                "startsAt": "2026-04-28T10:00:00Z",
            }
            for index in range(7)
        ],
    }

    message = format_alertmanager_message(payload, max_alerts=3)

    assert "alerts: 7" in message
    assert "1. Alert0 instance=node-0" in message
    assert "3. Alert2 instance=node-2" in message
    assert "Alert3" not in message
    assert "... omitted 4 alert(s)" in message


def test_telegram_dispatcher_uses_bounded_non_blocking_queue() -> None:
    sender_started = threading.Event()
    release_sender = threading.Event()

    def blocking_sender(config: TelegramConfig, message: str) -> None:
        sender_started.set()
        release_sender.wait(timeout=5)

    dispatcher = TelegramDeliveryDispatcher(
        TelegramConfig(bot_token="token", chat_id="chat"),
        queue_size=2,
        sender=blocking_sender,
    )
    dispatcher.start()

    assert dispatcher.enqueue("first") is True
    assert sender_started.wait(timeout=1)
    assert dispatcher.enqueue("second") is True
    assert dispatcher.enqueue("third") is True
    assert dispatcher.enqueue("dropped") is False
    release_sender.set()


def test_alert_webhook_queues_without_waiting_for_telegram_delivery() -> None:
    dispatcher = _FakeDispatcher(accepted=True)
    result = _queue_telegram_message(dispatcher, "message")

    assert result == "queued"
    assert dispatcher.messages == ["message"]


def test_alert_webhook_does_not_retry_when_telegram_queue_is_full() -> None:
    dispatcher = _FakeDispatcher(accepted=False)
    result = _queue_telegram_message(dispatcher, "message")

    assert result == "dropped_queue_full"
    assert dispatcher.messages == ["message"]


def test_alert_webhook_accepts_when_telegram_is_disabled() -> None:
    assert _queue_telegram_message(None, "message") == "disabled"


def test_send_telegram_message_uses_configured_proxy_and_timeout(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    class FakeResponse:
        def __enter__(self) -> FakeResponse:
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def read(self) -> bytes:
            return b"{}"

    class FakeOpener:
        def open(self, request: urllib.request.Request, timeout: float) -> FakeResponse:
            captured["request"] = request
            captured["timeout"] = timeout
            return FakeResponse()

    def fake_build_opener(*handlers: urllib.request.BaseHandler) -> FakeOpener:
        captured["handlers"] = handlers
        return FakeOpener()

    monkeypatch.setattr(urllib.request, "build_opener", fake_build_opener)

    send_telegram_message(
        TelegramConfig(
            bot_token="token",
            chat_id="chat",
            timeout_seconds=3.5,
            proxy_url="http://proxy.example.test:3128",
        ),
        "message",
    )

    proxy_handler = captured["handlers"][0]
    assert isinstance(proxy_handler, urllib.request.ProxyHandler)
    assert proxy_handler.proxies["https"] == "http://proxy.example.test:3128"
    assert captured["timeout"] == 3.5


class _FakeDispatcher:
    def __init__(self, *, accepted: bool) -> None:
        self._accepted = accepted
        self.messages: list[str] = []

    def enqueue(self, message: str) -> bool:
        self.messages.append(message)
        return self._accepted
