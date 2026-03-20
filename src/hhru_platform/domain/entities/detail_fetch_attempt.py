from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from uuid import UUID


@dataclass(slots=True)
class DetailFetchAttempt:
    id: int
    vacancy_id: UUID
    crawl_run_id: UUID | None
    reason: str
    attempt: int
    status: str
    requested_at: datetime
    finished_at: datetime | None
    error_message: str | None
