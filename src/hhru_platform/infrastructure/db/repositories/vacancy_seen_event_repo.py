from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from uuid import UUID

from sqlalchemy import insert, select
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

    def list_distinct_vacancy_ids_by_run(self, crawl_run_id: UUID) -> list[UUID]:
        statement = (
            select(VacancySeenEvent.vacancy_id)
            .where(VacancySeenEvent.crawl_run_id == crawl_run_id)
            .distinct()
            .order_by(VacancySeenEvent.vacancy_id)
        )
        return list(self._session.scalars(statement))

    def list_latest_short_hashes_before_run(
        self,
        *,
        crawl_run_id: UUID,
        vacancy_ids: list[UUID],
    ) -> dict[UUID, str]:
        if not vacancy_ids:
            return {}

        statement = (
            select(VacancySeenEvent.vacancy_id, VacancySeenEvent.short_hash)
            .where(
                VacancySeenEvent.vacancy_id.in_(vacancy_ids),
                VacancySeenEvent.crawl_run_id != crawl_run_id,
            )
            .distinct(VacancySeenEvent.vacancy_id)
            .order_by(
                VacancySeenEvent.vacancy_id,
                VacancySeenEvent.seen_at.desc(),
                VacancySeenEvent.id.desc(),
            )
        )
        rows = self._session.execute(statement)
        return {vacancy_id: short_hash for vacancy_id, short_hash in rows}
