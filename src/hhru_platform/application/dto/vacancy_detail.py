from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(slots=True, frozen=True)
class VacancyDetailResponse:
    endpoint: str
    method: str
    params_json: dict[str, object]
    request_headers_json: dict[str, str]
    status_code: int
    headers: dict[str, str]
    latency_ms: int
    requested_at: datetime
    response_received_at: datetime | None
    payload_json: object | None
    error_type: str | None = None
    error_message: str | None = None


@dataclass(slots=True, frozen=True)
class NormalizedVacancyDetail:
    hh_vacancy_id: str
    name_current: str
    area_hh_id: str | None
    published_at: datetime | None
    created_at_hh: datetime | None
    alternate_url: str | None
    employment_type_code: str | None
    schedule_type_code: str | None
    experience_code: str | None
    normalized_json: dict[str, object]
    detail_hash: str
