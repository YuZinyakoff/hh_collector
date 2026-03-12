from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from uuid import UUID

from sqlalchemy import func
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from hhru_platform.application.dto import ObservedVacancyRecord
from hhru_platform.infrastructure.db.models.vacancy_current_state import VacancyCurrentState

UPSERT_BATCH_SIZE = 1000


class SqlAlchemyVacancyCurrentStateRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def observe_many(
        self,
        *,
        crawl_run_id: UUID,
        observed_at: datetime,
        observations: Sequence[ObservedVacancyRecord],
    ) -> int:
        if not observations:
            return 0

        for observation_batch in _batched(observations, UPSERT_BATCH_SIZE):
            insert_values = [
                {
                    "vacancy_id": observation.vacancy_id,
                    "first_seen_at": observed_at,
                    "last_seen_at": observed_at,
                    "seen_count": 1,
                    "consecutive_missing_runs": 0,
                    "is_probably_inactive": False,
                    "last_seen_run_id": crawl_run_id,
                    "last_short_hash": observation.short_hash,
                }
                for observation in observation_batch
            ]
            insert_statement = insert(VacancyCurrentState).values(insert_values)
            upsert_statement = insert_statement.on_conflict_do_update(
                index_elements=[VacancyCurrentState.vacancy_id],
                set_={
                    "last_seen_at": insert_statement.excluded.last_seen_at,
                    "seen_count": VacancyCurrentState.seen_count + 1,
                    "consecutive_missing_runs": 0,
                    "is_probably_inactive": False,
                    "last_seen_run_id": insert_statement.excluded.last_seen_run_id,
                    "last_short_hash": insert_statement.excluded.last_short_hash,
                    "updated_at": func.now(),
                },
            )
            self._session.execute(upsert_statement)

        self._session.flush()
        return len(observations)


def _batched(
    observations: Sequence[ObservedVacancyRecord],
    batch_size: int,
) -> list[Sequence[ObservedVacancyRecord]]:
    return [
        observations[index : index + batch_size]
        for index in range(0, len(observations), batch_size)
    ]
