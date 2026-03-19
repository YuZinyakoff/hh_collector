from __future__ import annotations

import json
import logging
from datetime import UTC, date, datetime
from enum import Enum
from pathlib import Path
from uuid import UUID

_RESERVED_LOG_RECORD_FIELDS = frozenset(logging.makeLogRecord({}).__dict__.keys())


class ServiceContextFilter(logging.Filter):
    def __init__(self, *, service_name: str, env: str) -> None:
        super().__init__()
        self._service_name = service_name
        self._env = env

    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, "service_name"):
            record.service_name = self._service_name
        if not hasattr(record, "env"):
            record.env = self._env
        return True


class JsonLogFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": datetime.fromtimestamp(record.created, tz=UTC).isoformat(),
            "level": record.levelname.lower(),
            "logger": record.name,
            "message": record.getMessage(),
            "service_name": getattr(record, "service_name", "hhru-platform"),
            "env": getattr(record, "env", "local"),
        }
        payload.update(_extra_fields(record))
        if record.exc_info is not None:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=True, sort_keys=True, default=_json_default)


def log_event(
    logger: logging.Logger,
    level: int,
    event: str,
    **fields: object,
) -> None:
    extra_fields: dict[str, object] = {"event": event}
    for key, value in fields.items():
        if key in _RESERVED_LOG_RECORD_FIELDS or key in {"message", "asctime"}:
            extra_fields[f"field_{key}"] = value
        else:
            extra_fields[key] = value
    logger.log(level, event, extra=extra_fields)


def _extra_fields(record: logging.LogRecord) -> dict[str, object]:
    return {
        key: value
        for key, value in record.__dict__.items()
        if key not in _RESERVED_LOG_RECORD_FIELDS and not key.startswith("_")
    }


def _json_default(value: object) -> str | float | int | bool | None:
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, UUID | Path | Enum):
        return str(value)
    return str(value)
