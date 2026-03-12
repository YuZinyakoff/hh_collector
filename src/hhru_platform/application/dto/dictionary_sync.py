from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

SUPPORTED_DICTIONARY_NAMES: tuple[str, ...] = ("areas", "professional_roles")


@dataclass(slots=True, frozen=True)
class DictionaryFetchResponse:
    dictionary_name: str
    endpoint: str
    method: str
    params_json: dict[str, object]
    request_headers_json: dict[str, str]
    status_code: int
    headers: dict[str, str]
    latency_ms: int
    requested_at: datetime
    response_received_at: datetime | None
    payload_json: Any | None
    error_type: str | None = None
    error_message: str | None = None

    @property
    def etag(self) -> str | None:
        return self.headers.get("etag")


@dataclass(slots=True, frozen=True)
class DictionaryPersistSummary:
    created_count: int
    updated_count: int
    deactivated_count: int = 0
