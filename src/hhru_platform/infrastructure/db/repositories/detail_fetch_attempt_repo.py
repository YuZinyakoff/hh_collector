from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy.orm import Session

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
