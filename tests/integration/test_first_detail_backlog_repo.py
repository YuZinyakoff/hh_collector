from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import pytest
from sqlalchemy import event, text
from sqlalchemy.exc import OperationalError

from hhru_platform.infrastructure.db.repositories import (
    SqlAlchemyDetailFetchAttemptRepository,
    SqlAlchemyVacancyCurrentStateRepository,
)
from hhru_platform.infrastructure.db.repositories import (
    detail_fetch_attempt_repo as detail_attempt_repo_module,
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


def test_latest_attempt_numbers_by_vacancy_ids_chunks_lookup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        detail_attempt_repo_module,
        "DETAIL_ATTEMPT_LOOKUP_CHUNK_SIZE",
        2,
    )
    engine = create_engine_from_settings()
    session_factory = create_session_factory(engine)
    now = datetime(2000, 1, 2, 8, 0, tzinfo=UTC)
    vacancy_ids = tuple(uuid4() for _ in range(6))
    vacancy_ids_with_attempts = vacancy_ids[:-1]
    expected_attempts = {
        vacancy_id: attempt
        for vacancy_id, attempt in zip(
            vacancy_ids_with_attempts,
            range(11, 16),
            strict=True,
        )
    }
    lookup_param_counts: list[int] = []

    def capture_lookup_query(
        conn: Any,
        cursor: Any,
        statement: str,
        parameters: Any,
        context: Any,
        executemany: bool,
    ) -> None:
        del conn, cursor, context, executemany
        if (
            "FROM detail_fetch_attempt" not in statement
            or "ORDER BY detail_fetch_attempt.vacancy_id" not in statement
        ):
            return
        if isinstance(parameters, dict):
            lookup_param_counts.append(
                sum(1 for key in parameters if str(key).startswith("vacancy_id_"))
            )
        else:
            lookup_param_counts.append(len(parameters or ()))

    event.listen(engine, "before_cursor_execute", capture_lookup_query)
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
                            'Pytest chunked detail attempt lookup vacancy',
                            'hh_api'
                        )
                        """
                    ),
                    {
                        "vacancy_id": vacancy_id,
                        "hh_vacancy_id": f"pytest-chunked-attempt-{vacancy_id}",
                    },
                )

            for vacancy_id, latest_attempt in expected_attempts.items():
                _insert_attempt(
                    connection,
                    vacancy_id=vacancy_id,
                    attempt=1,
                    requested_at=now - timedelta(hours=1),
                )
                _insert_attempt(
                    connection,
                    vacancy_id=vacancy_id,
                    attempt=latest_attempt,
                    requested_at=now + timedelta(seconds=latest_attempt),
                )

        with session_scope(session_factory) as session:
            attempt_repository = SqlAlchemyDetailFetchAttemptRepository(session)

            assert (
                attempt_repository.latest_attempt_numbers_by_vacancy_ids(
                    list(vacancy_ids)
                )
                == expected_attempts
            )

        assert len(lookup_param_counts) == 3
        assert max(lookup_param_counts) <= 2
    finally:
        event.remove(engine, "before_cursor_execute", capture_lookup_query)
        with engine.begin() as connection:
            connection.execute(
                text("DELETE FROM detail_fetch_attempt WHERE vacancy_id = ANY(:vacancy_ids)"),
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


def test_first_detail_backlog_claim_uses_lease_to_prevent_duplicate_batches() -> None:
    engine = create_engine_from_settings()
    session_factory = create_session_factory(engine)
    now = datetime(2000, 1, 3, 12, 0, tzinfo=UTC)
    first_id = uuid4()
    second_id = uuid4()
    third_id = uuid4()
    vacancy_ids = (first_id, second_id, third_id)

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
                            'Pytest first detail claim vacancy',
                            'hh_api'
                        )
                        """
                    ),
                    {
                        "vacancy_id": vacancy_id,
                        "hh_vacancy_id": f"pytest-first-detail-claim-{vacancy_id}",
                    },
                )

            _insert_current_state(
                connection,
                vacancy_id=first_id,
                first_seen_at=now - timedelta(days=3),
                detail_fetch_status="not_requested",
                last_detail_fetched_at=None,
                is_probably_inactive=False,
            )
            _insert_current_state(
                connection,
                vacancy_id=second_id,
                first_seen_at=now - timedelta(days=2),
                detail_fetch_status="not_requested",
                last_detail_fetched_at=None,
                is_probably_inactive=False,
            )
            _insert_current_state(
                connection,
                vacancy_id=third_id,
                first_seen_at=now - timedelta(days=1),
                detail_fetch_status="not_requested",
                last_detail_fetched_at=None,
                is_probably_inactive=False,
            )

        with session_scope(session_factory) as session:
            state_repository = SqlAlchemyVacancyCurrentStateRepository(session)
            first_claim = state_repository.claim_first_detail_backlog(
                limit=2,
                include_inactive=False,
                retry_cooldown_seconds=3600,
                max_retry_cooldown_seconds=86400,
                now=now,
                lease_owner="pytest-worker-a",
                lease_expires_at=now + timedelta(hours=1),
            )

        with session_scope(session_factory) as session:
            state_repository = SqlAlchemyVacancyCurrentStateRepository(session)
            second_claim = state_repository.claim_first_detail_backlog(
                limit=2,
                include_inactive=False,
                retry_cooldown_seconds=3600,
                max_retry_cooldown_seconds=86400,
                now=now,
                lease_owner="pytest-worker-b",
                lease_expires_at=now + timedelta(hours=1),
            )

        assert [state.vacancy_id for state in first_claim] == [first_id, second_id]
        assert [state.vacancy_id for state in second_claim] == [third_id]

        with engine.begin() as connection:
            lease_rows = connection.execute(
                text(
                    """
                    SELECT vacancy_id, detail_fetch_status, first_detail_lease_owner
                    FROM vacancy_current_state
                    WHERE vacancy_id = ANY(:vacancy_ids)
                    ORDER BY first_seen_at
                    """
                ),
                {"vacancy_ids": list(vacancy_ids)},
            ).all()
            assert [
                (row.detail_fetch_status, row.first_detail_lease_owner)
                for row in lease_rows
            ] == [
                ("running", "pytest-worker-a"),
                ("running", "pytest-worker-a"),
                ("running", "pytest-worker-b"),
            ]

            connection.execute(
                text(
                    """
                    UPDATE vacancy_current_state
                    SET first_detail_lease_expires_at = :expired_at
                    WHERE vacancy_id = ANY(:vacancy_ids)
                    """
                ),
                {
                    "expired_at": now - timedelta(seconds=1),
                    "vacancy_ids": [first_id, second_id],
                },
            )

        with session_scope(session_factory) as session:
            state_repository = SqlAlchemyVacancyCurrentStateRepository(session)
            reclaimed = state_repository.claim_first_detail_backlog(
                limit=10,
                include_inactive=False,
                retry_cooldown_seconds=3600,
                max_retry_cooldown_seconds=86400,
                now=now,
                lease_owner="pytest-worker-c",
                lease_expires_at=now + timedelta(hours=1),
            )

        assert [state.vacancy_id for state in reclaimed] == [first_id, second_id]
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
    connection: Any,
    *,
    vacancy_id: UUID,
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
    connection: Any,
    *,
    vacancy_id: UUID,
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
