from __future__ import annotations

from hhru_platform.interfaces.workers.alert_webhook import format_alertmanager_message


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
