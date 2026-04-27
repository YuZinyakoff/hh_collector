from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session

from hhru_platform.domain.entities.detail_fetch_attempt import (
    DetailFetchAttempt as DetailFetchAttemptEntity,
)
from hhru_platform.infrastructure.db.models.detail_fetch_attempt import DetailFetchAttempt


class SqlAlchemyDetailFetchAttemptRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def start(
        self,
        *,
        vacancy_id: UUID,
        crawl_run_id: UUID | None,
        reason: str,
        attempt: int,
        requested_at: datetime,
        status: str,
    ) -> int:
        fetch_attempt = DetailFetchAttempt(
            vacancy_id=vacancy_id,
            crawl_run_id=crawl_run_id,
            reason=reason,
            attempt=attempt,
            requested_at=requested_at or datetime.now(UTC),
            status=status,
        )
        self._session.add(fetch_attempt)
        self._session.flush()
        return fetch_attempt.id

    def finish(
        self,
        *,
        detail_fetch_attempt_id: int,
        status: str,
        finished_at: datetime,
        error_message: str | None,
    ) -> int:
        fetch_attempt = self._session.get(DetailFetchAttempt, detail_fetch_attempt_id)
        if fetch_attempt is None:
            raise LookupError(f"detail_fetch_attempt not found: {detail_fetch_attempt_id}")

        fetch_attempt.status = status
        fetch_attempt.finished_at = finished_at
        fetch_attempt.error_message = error_message
        self._session.flush()
        return fetch_attempt.id

    def list_repair_backlog_by_run_id(self, crawl_run_id: UUID) -> list[DetailFetchAttemptEntity]:
        latest_attempts = (
            select(DetailFetchAttempt)
            .where(DetailFetchAttempt.crawl_run_id == crawl_run_id)
            .distinct(DetailFetchAttempt.vacancy_id)
            .order_by(
                DetailFetchAttempt.vacancy_id,
                DetailFetchAttempt.requested_at.desc(),
                DetailFetchAttempt.id.desc(),
            )
        )
        return [
            self._to_entity(model)
            for model in self._session.scalars(latest_attempts)
            if model.status == "failed"
        ]

    def latest_attempt_numbers_by_vacancy_ids(
        self,
        vacancy_ids: list[UUID],
    ) -> dict[UUID, int]:
        if not vacancy_ids:
            return {}

        latest_attempts = (
            select(DetailFetchAttempt.vacancy_id, DetailFetchAttempt.attempt)
            .where(DetailFetchAttempt.vacancy_id.in_(tuple(vacancy_ids)))
            .distinct(DetailFetchAttempt.vacancy_id)
            .order_by(
                DetailFetchAttempt.vacancy_id,
                DetailFetchAttempt.requested_at.desc(),
                DetailFetchAttempt.id.desc(),
            )
        )
        return {
            vacancy_id: attempt
            for vacancy_id, attempt in self._session.execute(latest_attempts)
        }

    @staticmethod
    def _to_entity(model: DetailFetchAttempt) -> DetailFetchAttemptEntity:
        return DetailFetchAttemptEntity(
            id=model.id,
            vacancy_id=model.vacancy_id,
            crawl_run_id=model.crawl_run_id,
            reason=model.reason,
            attempt=model.attempt,
            status=model.status,
            requested_at=model.requested_at,
            finished_at=model.finished_at,
            error_message=model.error_message,
        )
