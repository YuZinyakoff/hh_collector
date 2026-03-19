from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from hhru_platform.application.dto.enrichment import NormalizedEmployerReference


@dataclass(slots=True, frozen=True)
class VacancySearchResponse:
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
class NormalizedVacancyShortRecord:
    hh_vacancy_id: str
    name_current: str
    area_hh_id: str | None
    published_at: datetime | None
    created_at_hh: datetime | None
    alternate_url: str | None
    employment_type_code: str | None
    schedule_type_code: str | None
    experience_code: str | None
    employer: NormalizedEmployerReference | None
    professional_role_hh_ids: tuple[str, ...]
    short_hash: str
    list_position: int


@dataclass(slots=True, frozen=True)
class NormalizedVacancySearchPage:
    page: int
    pages: int | None
    per_page: int
    found: int | None
    items: list[NormalizedVacancyShortRecord]


@dataclass(slots=True, frozen=True)
class StoredVacancyReference:
    id: UUID
    hh_vacancy_id: str
    name_current: str


@dataclass(slots=True, frozen=True)
class VacancyUpsertResult:
    created_count: int
    vacancies: list[StoredVacancyReference]


@dataclass(slots=True, frozen=True)
class ObservedVacancyRecord:
    vacancy_id: UUID
    hh_vacancy_id: str
    list_position: int
    short_hash: str
