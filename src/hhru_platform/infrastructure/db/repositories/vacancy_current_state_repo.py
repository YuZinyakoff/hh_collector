from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from hhru_platform.application.dto import ObservedVacancyRecord
from hhru_platform.domain.entities.vacancy_current_state import (
    VacancyCurrentState as VacancyCurrentStateEntity,
)
from hhru_platform.domain.entities.vacancy_current_state import (
    VacancyCurrentStateReconciliationUpdate,
)
from hhru_platform.infrastructure.db.models.vacancy_current_state import (
    VacancyCurrentState as VacancyCurrentStateModel,
)

UPSERT_BATCH_SIZE = 1000


class SqlAlchemyVacancyCurrentStateRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def list_last_short_hashes(
        self,
        *,
        vacancy_ids: list[UUID],
    ) -> dict[UUID, str]:
        if not vacancy_ids:
            return {}

        statement = select(
            VacancyCurrentStateModel.vacancy_id,
            VacancyCurrentStateModel.last_short_hash,
        ).where(VacancyCurrentStateModel.vacancy_id.in_(tuple(vacancy_ids)))
        rows = self._session.execute(statement)
        return {
            vacancy_id: last_short_hash
            for vacancy_id, last_short_hash in rows
            if last_short_hash is not None
        }

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
                    "updated_at": observed_at,
                }
                for observation in observation_batch
            ]
            insert_statement = insert(VacancyCurrentStateModel).values(insert_values)
            upsert_statement = insert_statement.on_conflict_do_update(
                index_elements=[VacancyCurrentStateModel.vacancy_id],
                set_={
                    "last_seen_at": insert_statement.excluded.last_seen_at,
                    "seen_count": VacancyCurrentStateModel.seen_count + 1,
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

    def record_detail_fetch(
        self,
        *,
        vacancy_id: UUID,
        recorded_at: datetime,
        detail_hash: str | None,
        detail_fetch_status: str,
    ) -> None:
        insert_statement = insert(VacancyCurrentStateModel).values(
            [
                {
                    "vacancy_id": vacancy_id,
                    "first_seen_at": recorded_at,
                    "last_seen_at": recorded_at,
                    "seen_count": 1,
                    "consecutive_missing_runs": 0,
                    "is_probably_inactive": False,
                    "last_detail_hash": detail_hash,
                    "last_detail_fetched_at": recorded_at if detail_hash is not None else None,
                    "detail_fetch_status": detail_fetch_status,
                    "updated_at": recorded_at,
                }
            ]
        )
        upsert_statement = insert_statement.on_conflict_do_update(
            index_elements=[VacancyCurrentStateModel.vacancy_id],
            set_={
                "last_detail_hash": (
                    insert_statement.excluded.last_detail_hash
                    if detail_hash is not None
                    else VacancyCurrentStateModel.last_detail_hash
                ),
                "last_detail_fetched_at": (
                    insert_statement.excluded.last_detail_fetched_at
                    if detail_hash is not None
                    else VacancyCurrentStateModel.last_detail_fetched_at
                ),
                "detail_fetch_status": insert_statement.excluded.detail_fetch_status,
                "updated_at": func.now(),
            },
        )
        self._session.execute(upsert_statement)
        self._session.flush()

    def list_all(self) -> list[VacancyCurrentStateEntity]:
        statement = select(VacancyCurrentStateModel).order_by(VacancyCurrentStateModel.vacancy_id)
        return [self._to_entity(model) for model in self._session.scalars(statement)]

    def list_by_last_seen_run_id(self, crawl_run_id: UUID) -> list[VacancyCurrentStateEntity]:
        statement = (
            select(VacancyCurrentStateModel)
            .where(VacancyCurrentStateModel.last_seen_run_id == crawl_run_id)
            .order_by(VacancyCurrentStateModel.vacancy_id)
        )
        return [self._to_entity(model) for model in self._session.scalars(statement)]

    def apply_reconciliation_updates(
        self,
        *,
        updated_at: datetime,
        updates: Sequence[VacancyCurrentStateReconciliationUpdate],
    ) -> int:
        for update in updates:
            model = self._session.get(VacancyCurrentStateModel, update.vacancy_id)
            if model is None:
                raise LookupError(f"vacancy_current_state not found: {update.vacancy_id}")

            model.consecutive_missing_runs = update.consecutive_missing_runs
            model.is_probably_inactive = update.is_probably_inactive
            model.last_seen_run_id = update.last_seen_run_id
            model.updated_at = updated_at
            self._session.add(model)

        self._session.flush()
        return len(updates)

    @staticmethod
    def _to_entity(model: VacancyCurrentStateModel) -> VacancyCurrentStateEntity:
        return VacancyCurrentStateEntity(
            vacancy_id=model.vacancy_id,
            first_seen_at=model.first_seen_at,
            last_seen_at=model.last_seen_at,
            seen_count=model.seen_count,
            consecutive_missing_runs=model.consecutive_missing_runs,
            is_probably_inactive=model.is_probably_inactive,
            last_seen_run_id=model.last_seen_run_id,
            last_short_hash=model.last_short_hash,
            last_detail_hash=model.last_detail_hash,
            last_detail_fetched_at=model.last_detail_fetched_at,
            detail_fetch_status=model.detail_fetch_status,
            updated_at=model.updated_at,
        )


def _batched(
    observations: Sequence[ObservedVacancyRecord],
    batch_size: int,
) -> list[Sequence[ObservedVacancyRecord]]:
    return [
        observations[index : index + batch_size]
        for index in range(0, len(observations), batch_size)
    ]
