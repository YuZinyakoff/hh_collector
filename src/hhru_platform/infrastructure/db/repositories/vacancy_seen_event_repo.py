from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from uuid import UUID

from sqlalchemy import insert
from sqlalchemy.orm import Session

from hhru_platform.application.dto import ObservedVacancyRecord
from hhru_platform.infrastructure.db.models.vacancy_seen_event import VacancySeenEvent


class SqlAlchemyVacancySeenEventRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def add_many(
        self,
        *,
        crawl_run_id: UUID,
        crawl_partition_id: UUID,
        seen_at: datetime,
        short_payload_ref_id: int | None,
        observations: Sequence[ObservedVacancyRecord],
    ) -> int:
        if not observations:
            return 0

        values = [
            {
                "vacancy_id": observation.vacancy_id,
                "crawl_run_id": crawl_run_id,
                "crawl_partition_id": crawl_partition_id,
                "seen_at": seen_at,
                "list_position": observation.list_position,
                "short_hash": observation.short_hash,
                "short_payload_ref_id": short_payload_ref_id,
            }
            for observation in observations
        ]
        self._session.execute(insert(VacancySeenEvent), values)
        self._session.flush()
        return len(values)
