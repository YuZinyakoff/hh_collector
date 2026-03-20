from __future__ import annotations

from collections.abc import Callable

from sqlalchemy import text
from sqlalchemy.orm import Session

from hhru_platform.domain.entities.crawl_run import CrawlRun
from hhru_platform.domain.value_objects.enums import CrawlRunStatus
from hhru_platform.infrastructure.db.repositories.crawl_run_repo import (
    SqlAlchemyCrawlRunRepository,
)
from hhru_platform.infrastructure.db.session import SessionLocal

COLLECTION_RUN_ADVISORY_LOCK_KEY = 4_204_424_204


class PostgresCollectionRunAdmissionLease:
    def __init__(self, session: Session, *, advisory_lock_key: int) -> None:
        self._session = session
        self._advisory_lock_key = advisory_lock_key
        self._released = False

    def get_active_run(self) -> CrawlRun | None:
        return SqlAlchemyCrawlRunRepository(self._session).get_latest_by_statuses(
            (CrawlRunStatus.CREATED.value,)
        )

    def release(self) -> None:
        if self._released:
            return
        try:
            self._session.execute(
                text("SELECT pg_advisory_unlock(:lock_key)"),
                {"lock_key": self._advisory_lock_key},
            )
        finally:
            self._session.close()
            self._released = True


class PostgresCollectionRunAdmissionController:
    def __init__(
        self,
        session_factory: Callable[[], Session] = SessionLocal,
        *,
        advisory_lock_key: int = COLLECTION_RUN_ADVISORY_LOCK_KEY,
    ) -> None:
        self._session_factory = session_factory
        self._advisory_lock_key = advisory_lock_key

    def acquire(self) -> PostgresCollectionRunAdmissionLease | None:
        session = self._session_factory()
        try:
            acquired = bool(
                session.execute(
                    text("SELECT pg_try_advisory_lock(:lock_key)"),
                    {"lock_key": self._advisory_lock_key},
                ).scalar_one()
            )
        except Exception:
            session.close()
            raise

        if not acquired:
            session.close()
            return None

        return PostgresCollectionRunAdmissionLease(
            session,
            advisory_lock_key=self._advisory_lock_key,
        )
