from dataclasses import dataclass
from datetime import datetime
from uuid import UUID


@dataclass(slots=True)
class VacancyCurrentState:
    vacancy_id: UUID
    first_seen_at: datetime
    last_seen_at: datetime
    seen_count: int
    consecutive_missing_runs: int
    is_probably_inactive: bool
    last_seen_run_id: UUID | None
    last_short_hash: str | None
    last_detail_hash: str | None
    last_detail_fetched_at: datetime | None
    detail_fetch_status: str
    updated_at: datetime


@dataclass(slots=True, frozen=True)
class VacancyCurrentStateReconciliationUpdate:
    vacancy_id: UUID
    consecutive_missing_runs: int
    is_probably_inactive: bool
    last_seen_run_id: UUID | None
