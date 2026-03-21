from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import OperationalError

from hhru_platform.application.commands.fetch_vacancy_detail import (
    FetchVacancyDetailCommand,
    FetchVacancyDetailResult,
)
from hhru_platform.application.commands.retry_failed_details import (
    RetryFailedDetailsCommand,
    retry_failed_details,
)
from hhru_platform.infrastructure.db.repositories import (
    SqlAlchemyCrawlRunRepository,
    SqlAlchemyDetailFetchAttemptRepository,
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


def test_retry_failed_details_rebuilds_backlog_from_latest_attempts_and_promotes_run() -> None:
    engine = create_engine_from_settings()
    session_factory = create_session_factory(engine)
    run_id = uuid4()
    vacancy_id = uuid4()

    try:
        with session_scope(session_factory) as session:
            session.execute(
                text(
                    """
                    INSERT INTO crawl_run (
                        id,
                        run_type,
                        status,
                        started_at,
                        finished_at,
                        triggered_by,
                        config_snapshot_json,
                        partitions_total,
                        partitions_done,
                        partitions_failed,
                        notes
                    )
                    VALUES (
                        :run_id,
                        'weekly_sweep',
                        'completed_with_detail_errors',
                        :started_at,
                        :finished_at,
                        'pytest-repair',
                        '{}'::jsonb,
                        1,
                        1,
                        0,
                        '1 detail fetch failed'
                    )
                    """
                ),
                {
                    "run_id": run_id,
                    "started_at": datetime(2026, 3, 20, 11, 0, tzinfo=UTC),
                    "finished_at": datetime(2026, 3, 20, 11, 30, tzinfo=UTC),
                },
            )
            session.execute(
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
                        'Pytest Vacancy',
                        'hh_api'
                    )
                    """
                ),
                {
                    "vacancy_id": vacancy_id,
                    "hh_vacancy_id": f"pytest-repair-{vacancy_id}",
                },
            )
            session.execute(
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
                        :run_id,
                        'first_seen',
                        1,
                        'failed',
                        :requested_at,
                        :finished_at,
                        'pytest initial failure'
                    )
                    """
                ),
                {
                    "vacancy_id": vacancy_id,
                    "run_id": run_id,
                    "requested_at": datetime(2026, 3, 20, 11, 5, tzinfo=UTC),
                    "finished_at": datetime(2026, 3, 20, 11, 5, 30, tzinfo=UTC),
                },
            )

            crawl_run_repository = SqlAlchemyCrawlRunRepository(session)
            detail_fetch_attempt_repository = SqlAlchemyDetailFetchAttemptRepository(session)

            def fetch_vacancy_detail_step(
                command: FetchVacancyDetailCommand,
            ) -> FetchVacancyDetailResult:
                attempt_id = detail_fetch_attempt_repository.start(
                    vacancy_id=command.vacancy_id,
                    crawl_run_id=command.crawl_run_id,
                    reason=command.reason,
                    attempt=command.attempt,
                    requested_at=datetime(2026, 3, 20, 12, 0, tzinfo=UTC),
                    status="running",
                )
                detail_fetch_attempt_repository.finish(
                    detail_fetch_attempt_id=attempt_id,
                    status="succeeded",
                    finished_at=datetime(2026, 3, 20, 12, 0, 10, tzinfo=UTC),
                    error_message=None,
                )
                return FetchVacancyDetailResult(
                    vacancy_id=command.vacancy_id,
                    hh_vacancy_id=f"hh-{command.vacancy_id}",
                    detail_fetch_status="succeeded",
                    snapshot_id=1,
                    request_log_id=1,
                    raw_payload_id=1,
                    detail_fetch_attempt_id=attempt_id,
                    error_message=None,
                )

            result = retry_failed_details(
                RetryFailedDetailsCommand(
                    crawl_run_id=run_id,
                    triggered_by="pytest-repair-backlog",
                ),
                crawl_run_repository=crawl_run_repository,
                detail_fetch_attempt_repository=detail_fetch_attempt_repository,
                fetch_vacancy_detail_step=fetch_vacancy_detail_step,
            )

        assert result.status == "succeeded"
        assert result.backlog_size == 1
        assert result.retried_count == 1
        assert result.repaired_count == 1
        assert result.remaining_backlog_count == 0

        with engine.connect() as connection:
            run_row = (
                connection.execute(
                    text(
                        """
                        SELECT status, notes
                        FROM crawl_run
                        WHERE id = :run_id
                        """
                    ),
                    {"run_id": run_id},
                )
                .mappings()
                .one()
            )
            attempt_rows = (
                connection.execute(
                    text(
                        """
                        SELECT attempt, reason, status
                        FROM detail_fetch_attempt
                        WHERE crawl_run_id = :run_id
                        ORDER BY attempt
                        """
                    ),
                    {"run_id": run_id},
                )
                .mappings()
                .all()
            )

        assert run_row["status"] == "succeeded"
        assert "cleared" in run_row["notes"]
        assert attempt_rows == [
            {"attempt": 1, "reason": "first_seen", "status": "failed"},
            {"attempt": 2, "reason": "repair_backlog", "status": "succeeded"},
        ]
    finally:
        with engine.begin() as connection:
            connection.execute(
                text("DELETE FROM vacancy WHERE id = :vacancy_id"),
                {"vacancy_id": vacancy_id},
            )
            connection.execute(
                text("DELETE FROM crawl_run WHERE id = :run_id"),
                {"run_id": run_id},
            )
        engine.dispose()
