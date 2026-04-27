from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import OperationalError

from hhru_platform.infrastructure.db.repositories import (
    SqlAlchemyDetailFetchAttemptRepository,
    SqlAlchemyVacancyCurrentStateRepository,
)
from hhru_platform.infrastructure.db.session import (
    create_engine_from_settings,
    create_session_factory,
    session_scope,
)


def _database_is_available() -> bool:
    engine = create_engine_from_settings()
    try:
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
    except OperationalError:
        return False
    finally:
        engine.dispose()

    return True


pytestmark = pytest.mark.skipif(
    not _database_is_available(),
    reason="PostgreSQL is not available for integration tests.",
)


def test_first_detail_backlog_repository_lists_active_missing_detail_rows() -> None:
    engine = create_engine_from_settings()
    session_factory = create_session_factory(engine)
    now = datetime(2000, 1, 1, 12, 0, tzinfo=UTC)
    active_missing_id = uuid4()
    active_failed_id = uuid4()
    active_succeeded_id = uuid4()
    active_terminal_404_id = uuid4()
    inactive_missing_id = uuid4()
    vacancy_ids = (
        active_missing_id,
        active_failed_id,
        active_succeeded_id,
        active_terminal_404_id,
        inactive_missing_id,
    )

    try:
        with session_scope(session_factory) as session:
            state_repository = SqlAlchemyVacancyCurrentStateRepository(session)
            active_backlog_before = state_repository.count_first_detail_backlog(
                include_inactive=False
            )
            full_backlog_before = state_repository.count_first_detail_backlog(
                include_inactive=True
            )

        with engine.begin() as connection:
            for vacancy_id in vacancy_ids:
                connection.execute(
                    text(
                        """
                        INSERT INTO vacancy (
                            id,
                            hh_vacancy_id,
                            name_current,
                            source_type
                        )
                        VALUES (
                            :vacancy_id,
                            :hh_vacancy_id,
                            'Pytest first detail backlog vacancy',
                            'hh_api'
                        )
                        """
                    ),
                    {
                        "vacancy_id": vacancy_id,
                        "hh_vacancy_id": f"pytest-first-detail-{vacancy_id}",
                    },
                )

            _insert_current_state(
                connection,
                vacancy_id=active_missing_id,
                first_seen_at=now - timedelta(days=3),
                detail_fetch_status="not_requested",
                last_detail_fetched_at=None,
                is_probably_inactive=False,
            )
            _insert_current_state(
                connection,
                vacancy_id=active_failed_id,
                first_seen_at=now - timedelta(days=2),
                detail_fetch_status="failed",
                last_detail_fetched_at=None,
                is_probably_inactive=False,
            )
            _insert_current_state(
                connection,
                vacancy_id=active_succeeded_id,
                first_seen_at=now - timedelta(days=1),
                detail_fetch_status="succeeded",
                last_detail_fetched_at=now - timedelta(hours=1),
                is_probably_inactive=False,
            )
            _insert_current_state(
                connection,
                vacancy_id=active_terminal_404_id,
                first_seen_at=now - timedelta(hours=12),
                detail_fetch_status="terminal_404",
                last_detail_fetched_at=None,
                is_probably_inactive=False,
            )
            _insert_current_state(
                connection,
                vacancy_id=inactive_missing_id,
                first_seen_at=now,
                detail_fetch_status="not_requested",
                last_detail_fetched_at=None,
                is_probably_inactive=True,
            )
            _insert_attempt(
                connection,
                vacancy_id=active_missing_id,
                attempt=1,
                requested_at=now - timedelta(minutes=20),
            )
            _insert_attempt(
                connection,
                vacancy_id=active_missing_id,
                attempt=2,
                requested_at=now - timedelta(minutes=10),
            )
            _insert_attempt(
                connection,
                vacancy_id=active_failed_id,
                attempt=5,
                requested_at=now - timedelta(minutes=5),
            )

        with session_scope(session_factory) as session:
            state_repository = SqlAlchemyVacancyCurrentStateRepository(session)
            attempt_repository = SqlAlchemyDetailFetchAttemptRepository(session)

            assert state_repository.count_first_detail_backlog(
                include_inactive=False
            ) == active_backlog_before + 2
            assert state_repository.count_first_detail_backlog(
                include_inactive=True
            ) == full_backlog_before + 3
            assert [
                state.vacancy_id
                for state in state_repository.list_first_detail_backlog(
                    limit=2,
                    include_inactive=False,
                )
            ] == [active_missing_id, active_failed_id]
            assert attempt_repository.latest_attempt_numbers_by_vacancy_ids(
                [active_missing_id, active_failed_id, active_succeeded_id]
            ) == {
                active_missing_id: 2,
                active_failed_id: 5,
            }
    finally:
        with engine.begin() as connection:
            connection.execute(
                text("DELETE FROM detail_fetch_attempt WHERE vacancy_id = ANY(:vacancy_ids)"),
                {"vacancy_ids": list(vacancy_ids)},
            )
            connection.execute(
                text("DELETE FROM vacancy_current_state WHERE vacancy_id = ANY(:vacancy_ids)"),
                {"vacancy_ids": list(vacancy_ids)},
            )
            connection.execute(
                text("DELETE FROM vacancy WHERE id = ANY(:vacancy_ids)"),
                {"vacancy_ids": list(vacancy_ids)},
            )
        engine.dispose()


def test_first_detail_backlog_repository_skips_recent_retryable_failures() -> None:
    engine = create_engine_from_settings()
    session_factory = create_session_factory(engine)
    now = datetime(2000, 1, 2, 12, 0, tzinfo=UTC)
    no_attempt_id = uuid4()
    recent_failed_id = uuid4()
    old_failed_id = uuid4()
    vacancy_ids = (no_attempt_id, recent_failed_id, old_failed_id)

    try:
        with engine.begin() as connection:
            for vacancy_id in vacancy_ids:
                connection.execute(
                    text(
                        """
                        INSERT INTO vacancy (
                            id,
                            hh_vacancy_id,
                            name_current,
                            source_type
                        )
                        VALUES (
                            :vacancy_id,
                            :hh_vacancy_id,
                            'Pytest first detail cooldown vacancy',
                            'hh_api'
                        )
                        """
                    ),
                    {
                        "vacancy_id": vacancy_id,
                        "hh_vacancy_id": f"pytest-first-detail-cooldown-{vacancy_id}",
                    },
                )

            _insert_current_state(
                connection,
                vacancy_id=no_attempt_id,
                first_seen_at=now - timedelta(days=3),
                detail_fetch_status="not_requested",
                last_detail_fetched_at=None,
                is_probably_inactive=False,
            )
            _insert_current_state(
                connection,
                vacancy_id=recent_failed_id,
                first_seen_at=now - timedelta(days=2),
                detail_fetch_status="failed",
                last_detail_fetched_at=None,
                is_probably_inactive=False,
            )
            _insert_current_state(
                connection,
                vacancy_id=old_failed_id,
                first_seen_at=now - timedelta(days=1),
                detail_fetch_status="failed",
                last_detail_fetched_at=None,
                is_probably_inactive=False,
            )
            _insert_attempt(
                connection,
                vacancy_id=recent_failed_id,
                attempt=1,
                requested_at=now - timedelta(minutes=30),
            )
            _insert_attempt(
                connection,
                vacancy_id=old_failed_id,
                attempt=2,
                requested_at=now - timedelta(hours=3),
            )

        with session_scope(session_factory) as session:
            state_repository = SqlAlchemyVacancyCurrentStateRepository(session)

            assert (
                state_repository.count_first_detail_backlog_ready(
                    include_inactive=False,
                    retry_cooldown_seconds=3600,
                    max_retry_cooldown_seconds=86400,
                    now=now,
                )
                >= 2
            )
            ready_ids = [
                state.vacancy_id
                for state in state_repository.list_first_detail_backlog(
                    limit=10,
                    include_inactive=False,
                    retry_cooldown_seconds=3600,
                    max_retry_cooldown_seconds=86400,
                    now=now,
                )
                if state.vacancy_id in vacancy_ids
            ]
            assert ready_ids == [no_attempt_id, old_failed_id]
    finally:
        with engine.begin() as connection:
            connection.execute(
                text("DELETE FROM detail_fetch_attempt WHERE vacancy_id = ANY(:vacancy_ids)"),
                {"vacancy_ids": list(vacancy_ids)},
            )
            connection.execute(
                text("DELETE FROM vacancy_current_state WHERE vacancy_id = ANY(:vacancy_ids)"),
                {"vacancy_ids": list(vacancy_ids)},
            )
            connection.execute(
                text("DELETE FROM vacancy WHERE id = ANY(:vacancy_ids)"),
                {"vacancy_ids": list(vacancy_ids)},
            )
        engine.dispose()


def _insert_current_state(
    connection,
    *,
    vacancy_id,
    first_seen_at: datetime,
    detail_fetch_status: str,
    last_detail_fetched_at: datetime | None,
    is_probably_inactive: bool,
) -> None:
    connection.execute(
        text(
            """
            INSERT INTO vacancy_current_state (
                vacancy_id,
                first_seen_at,
                last_seen_at,
                seen_count,
                consecutive_missing_runs,
                is_probably_inactive,
                last_short_hash,
                last_detail_hash,
                last_detail_fetched_at,
                detail_fetch_status,
                updated_at
            )
            VALUES (
                :vacancy_id,
                :first_seen_at,
                :last_seen_at,
                1,
                0,
                :is_probably_inactive,
                :last_short_hash,
                :last_detail_hash,
                :last_detail_fetched_at,
                :detail_fetch_status,
                :updated_at
            )
            """
        ),
        {
            "vacancy_id": vacancy_id,
            "first_seen_at": first_seen_at,
            "last_seen_at": first_seen_at + timedelta(hours=1),
            "is_probably_inactive": is_probably_inactive,
            "last_short_hash": f"short-{vacancy_id}",
            "last_detail_hash": (
                f"detail-{vacancy_id}"
                if last_detail_fetched_at is not None
                else None
            ),
            "last_detail_fetched_at": last_detail_fetched_at,
            "detail_fetch_status": detail_fetch_status,
            "updated_at": first_seen_at,
        },
    )


def _insert_attempt(
    connection,
    *,
    vacancy_id,
    attempt: int,
    requested_at: datetime,
) -> None:
    connection.execute(
        text(
            """
            INSERT INTO detail_fetch_attempt (
                vacancy_id,
                crawl_run_id,
                reason,
                attempt,
                status,
                requested_at,
                finished_at,
                error_message
            )
            VALUES (
                :vacancy_id,
                NULL,
                'first_detail_backlog',
                :attempt,
                'failed',
                :requested_at,
                :finished_at,
                'pytest failure'
            )
            """
        ),
        {
            "vacancy_id": vacancy_id,
            "attempt": attempt,
            "requested_at": requested_at,
            "finished_at": requested_at + timedelta(seconds=1),
        },
    )
