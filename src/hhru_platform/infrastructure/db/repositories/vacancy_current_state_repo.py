from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import and_, bindparam, func, not_, or_, select, text, true
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session
from sqlalchemy.sql.elements import ColumnElement
from sqlalchemy.sql.selectable import Subquery

from hhru_platform.application.dto import ObservedVacancyRecord
from hhru_platform.domain.entities.vacancy_current_state import (
    VacancyCurrentState as VacancyCurrentStateEntity,
)
from hhru_platform.domain.entities.vacancy_current_state import (
    VacancyCurrentStateReconciliationUpdate,
)
from hhru_platform.domain.value_objects.enums import DetailFetchStatus
from hhru_platform.infrastructure.db.models.detail_fetch_attempt import DetailFetchAttempt
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
        should_record_fetch_time = (
            detail_hash is not None
            or detail_fetch_status == DetailFetchStatus.TERMINAL_404.value
        )
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
                    "last_detail_fetched_at": recorded_at if should_record_fetch_time else None,
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
                    if should_record_fetch_time
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

    def count_first_detail_backlog(self, *, include_inactive: bool) -> int:
        statement = (
            select(func.count())
            .select_from(VacancyCurrentStateModel)
            .where(*_first_detail_backlog_filters(include_inactive=include_inactive))
        )
        return int(self._session.scalar(statement) or 0)

    def count_first_detail_backlog_ready(
        self,
        *,
        include_inactive: bool,
        retry_cooldown_seconds: int = 0,
        max_retry_cooldown_seconds: int = 0,
        now: datetime | None = None,
    ) -> int:
        latest_failed_attempts = _latest_failed_detail_attempts_subquery()
        statement = (
            select(func.count())
            .select_from(VacancyCurrentStateModel)
            .outerjoin(
                latest_failed_attempts,
                VacancyCurrentStateModel.vacancy_id
                == latest_failed_attempts.c.vacancy_id,
            )
            .where(
                *_first_detail_backlog_filters(include_inactive=include_inactive),
                _detail_retry_ready_filter(
                    latest_failed_attempts=latest_failed_attempts,
                    retry_cooldown_seconds=retry_cooldown_seconds,
                    max_retry_cooldown_seconds=max_retry_cooldown_seconds,
                    now=now or datetime.now(UTC),
                ),
            )
        )
        return int(self._session.scalar(statement) or 0)

    def list_first_detail_backlog(
        self,
        *,
        limit: int,
        include_inactive: bool,
        retry_cooldown_seconds: int = 0,
        max_retry_cooldown_seconds: int = 0,
        now: datetime | None = None,
    ) -> list[VacancyCurrentStateEntity]:
        if limit <= 0:
            return []

        latest_failed_attempts = _latest_failed_detail_attempts_subquery()
        statement = (
            select(VacancyCurrentStateModel)
            .outerjoin(
                latest_failed_attempts,
                VacancyCurrentStateModel.vacancy_id
                == latest_failed_attempts.c.vacancy_id,
            )
            .where(*_first_detail_backlog_filters(include_inactive=include_inactive))
            .where(
                _detail_retry_ready_filter(
                    latest_failed_attempts=latest_failed_attempts,
                    retry_cooldown_seconds=retry_cooldown_seconds,
                    max_retry_cooldown_seconds=max_retry_cooldown_seconds,
                    now=now or datetime.now(UTC),
                )
            )
            .order_by(
                VacancyCurrentStateModel.first_seen_at,
                VacancyCurrentStateModel.last_seen_at.desc(),
                VacancyCurrentStateModel.vacancy_id,
            )
            .limit(limit)
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


def _first_detail_backlog_filters(*, include_inactive: bool) -> list[ColumnElement[bool]]:
    closed_succeeded = and_(
        VacancyCurrentStateModel.detail_fetch_status == DetailFetchStatus.SUCCEEDED.value,
        VacancyCurrentStateModel.last_detail_fetched_at.is_not(None),
    )
    closed_terminal_404 = (
        VacancyCurrentStateModel.detail_fetch_status == DetailFetchStatus.TERMINAL_404.value
    )
    filters = [
        not_(or_(closed_succeeded, closed_terminal_404)),
    ]
    if not include_inactive:
        filters.append(VacancyCurrentStateModel.is_probably_inactive.is_(False))
    return filters


def _latest_failed_detail_attempts_subquery() -> Subquery:
    ranked_attempts = (
        select(
            DetailFetchAttempt.vacancy_id.label("vacancy_id"),
            DetailFetchAttempt.attempt.label("attempt"),
            func.coalesce(
                DetailFetchAttempt.finished_at,
                DetailFetchAttempt.requested_at,
            ).label("attempted_at"),
            func.row_number()
            .over(
                partition_by=DetailFetchAttempt.vacancy_id,
                order_by=(
                    DetailFetchAttempt.requested_at.desc(),
                    DetailFetchAttempt.id.desc(),
                ),
            )
            .label("rank"),
        )
        .where(DetailFetchAttempt.status == DetailFetchStatus.FAILED.value)
        .subquery("latest_detail_attempt_ranked")
    )
    return (
        select(
            ranked_attempts.c.vacancy_id,
            ranked_attempts.c.attempt,
            ranked_attempts.c.attempted_at,
        )
        .where(ranked_attempts.c.rank == 1)
        .subquery("latest_detail_attempt")
    )


def _detail_retry_ready_filter(
    *,
    latest_failed_attempts: Subquery,
    retry_cooldown_seconds: int,
    max_retry_cooldown_seconds: int,
    now: datetime,
) -> ColumnElement[bool]:
    if retry_cooldown_seconds <= 0:
        return true()

    capped_retry_exponent = func.least(
        func.greatest(latest_failed_attempts.c.attempt - 1, 0),
        16,
    )
    cooldown_seconds = func.least(
        bindparam("max_retry_cooldown_seconds", max_retry_cooldown_seconds),
        bindparam("retry_cooldown_seconds", retry_cooldown_seconds)
        * func.power(2, capped_retry_exponent),
    )
    return or_(
        latest_failed_attempts.c.vacancy_id.is_(None),
        latest_failed_attempts.c.attempted_at
        <= bindparam("detail_retry_now", now)
        - (cooldown_seconds * text("INTERVAL '1 second'")),
    )
