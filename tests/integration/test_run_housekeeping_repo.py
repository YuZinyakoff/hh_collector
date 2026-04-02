from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import OperationalError

from hhru_platform.infrastructure.db.repositories import SqlAlchemyHousekeepingRepository
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


def _cleanup_test_rows(engine) -> None:
    with engine.begin() as connection:
        connection.execute(text("DELETE FROM detail_fetch_attempt WHERE id IN (401, 402, 403)"))
        connection.execute(text("DELETE FROM vacancy_snapshot WHERE id IN (301, 302, 303, 304)"))
        connection.execute(text("DELETE FROM raw_api_payload WHERE id IN (201, 202, 203, 204)"))
        connection.execute(text("DELETE FROM api_request_log WHERE id IN (101, 102, 103)"))
        connection.execute(
            text(
                """
                DELETE FROM crawl_partition
                WHERE crawl_run_id IN (
                    SELECT id
                    FROM crawl_run
                    WHERE triggered_by IN (
                        'pytest-housekeeping-old',
                        'pytest-housekeeping-active'
                    )
                )
                """
            )
        )
        connection.execute(
            text(
                """
                DELETE FROM vacancy
                WHERE hh_vacancy_id LIKE 'pytest-housekeeping-%'
                """
            )
        )
        connection.execute(
            text(
                """
                DELETE FROM crawl_run
                WHERE triggered_by IN (
                    'pytest-housekeeping-old',
                    'pytest-housekeeping-active'
                )
                """
            )
        )


def test_housekeeping_repository_counts_only_safe_retention_candidates() -> None:
    engine = create_engine_from_settings()
    session_factory = create_session_factory(engine)
    old_run_id = uuid4()
    active_run_id = uuid4()
    vacancy_one_id = uuid4()
    vacancy_two_id = uuid4()

    _cleanup_test_rows(engine)

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
                    VALUES
                        (
                            :old_run_id,
                            'weekly_sweep',
                            'succeeded',
                            :old_started_at,
                            :old_finished_at,
                            'pytest-housekeeping-old',
                            '{}'::jsonb,
                            1,
                            1,
                            0,
                            NULL
                        ),
                        (
                            :active_run_id,
                            'weekly_sweep',
                            'created',
                            :active_started_at,
                            NULL,
                            'pytest-housekeeping-active',
                            '{}'::jsonb,
                            1,
                            0,
                            0,
                            NULL
                        )
                    """
                ),
                {
                    "old_run_id": old_run_id,
                    "old_started_at": datetime(2025, 12, 1, 10, 0, tzinfo=UTC),
                    "old_finished_at": datetime(2025, 12, 1, 11, 0, tzinfo=UTC),
                    "active_run_id": active_run_id,
                    "active_started_at": datetime(2026, 3, 21, 10, 0, tzinfo=UTC),
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
                    VALUES
                        (:vacancy_one_id, :vacancy_one_hh_id, 'Pytest Vacancy One', 'hh_api'),
                        (:vacancy_two_id, :vacancy_two_hh_id, 'Pytest Vacancy Two', 'hh_api')
                    """
                ),
                {
                    "vacancy_one_id": vacancy_one_id,
                    "vacancy_one_hh_id": f"pytest-housekeeping-{vacancy_one_id}",
                    "vacancy_two_id": vacancy_two_id,
                    "vacancy_two_hh_id": f"pytest-housekeeping-{vacancy_two_id}",
                },
            )
            session.execute(
                text(
                    """
                    INSERT INTO crawl_partition (
                        id,
                        crawl_run_id,
                        parent_partition_id,
                        partition_key,
                        scope_key,
                        params_json,
                        status,
                        depth,
                        planner_policy_version,
                        is_terminal,
                        is_saturated,
                        coverage_status,
                        pages_processed,
                        items_seen,
                        retry_count,
                        created_at
                    )
                    VALUES
                        (
                            :old_partition_id,
                            :old_run_id,
                            NULL,
                            'area:113',
                            'area:113',
                            '{}'::jsonb,
                            'done',
                            0,
                            'v2',
                            true,
                            false,
                            'covered',
                            1,
                            10,
                            0,
                            :old_created_at
                        ),
                        (
                            :active_partition_id,
                            :active_run_id,
                            NULL,
                            'area:1',
                            'area:1',
                            '{}'::jsonb,
                            'pending',
                            0,
                            'v2',
                            true,
                            false,
                            'unassessed',
                            0,
                            0,
                            0,
                            :active_created_at
                        )
                    """
                ),
                {
                    "old_partition_id": uuid4(),
                    "old_run_id": old_run_id,
                    "old_created_at": datetime(2025, 12, 1, 10, 0, tzinfo=UTC),
                    "active_partition_id": uuid4(),
                    "active_run_id": active_run_id,
                    "active_created_at": datetime(2026, 3, 21, 10, 0, tzinfo=UTC),
                },
            )
            session.execute(
                text(
                    """
                    INSERT INTO api_request_log (
                        id,
                        crawl_run_id,
                        crawl_partition_id,
                        request_type,
                        endpoint,
                        method,
                        params_json,
                        status_code,
                        latency_ms,
                        attempt,
                        requested_at,
                        response_received_at,
                        error_type,
                        error_message
                    )
                    VALUES
                        (
                            101,
                            :old_run_id,
                            NULL,
                            'list_page',
                            '/vacancies',
                            'GET',
                            '{}'::jsonb,
                            200,
                            100,
                            1,
                            :old_requested_at,
                            :old_requested_at,
                            NULL,
                            NULL
                        ),
                        (
                            102,
                            :old_run_id,
                            NULL,
                            'detail',
                            '/vacancies/1',
                            'GET',
                            '{}'::jsonb,
                            200,
                            100,
                            1,
                            :old_requested_at,
                            :old_requested_at,
                            NULL,
                            NULL
                        ),
                        (
                            103,
                            :active_run_id,
                            NULL,
                            'list_page',
                            '/vacancies',
                            'GET',
                            '{}'::jsonb,
                            200,
                            100,
                            1,
                            :active_requested_at,
                            :active_requested_at,
                            NULL,
                            NULL
                        )
                    """
                ),
                {
                    "old_run_id": old_run_id,
                    "old_requested_at": datetime(2025, 12, 1, 10, 5, tzinfo=UTC),
                    "active_run_id": active_run_id,
                    "active_requested_at": datetime(2026, 3, 21, 10, 5, tzinfo=UTC),
                },
            )
            session.execute(
                text(
                    """
                    INSERT INTO raw_api_payload (
                        id,
                        api_request_log_id,
                        endpoint_type,
                        entity_hh_id,
                        payload_json,
                        payload_hash,
                        received_at
                    )
                    VALUES
                        (
                            201,
                            101,
                            'list_page',
                            NULL,
                            '{}'::jsonb,
                            'raw-delete-me',
                            :old_received_at
                        ),
                        (
                            202,
                            102,
                            'vacancy_detail',
                            'hh-1',
                            '{}'::jsonb,
                            'raw-protected',
                            :old_received_at
                        ),
                        (
                            203,
                            103,
                            'list_page',
                            NULL,
                            '{}'::jsonb,
                            'raw-active',
                            :active_received_at
                        ),
                        (
                            204,
                            101,
                            'list_page',
                            NULL,
                            '{}'::jsonb,
                            'raw-v2-short-snapshot',
                            :old_received_at
                        )
                    """
                ),
                {
                    "old_received_at": datetime(2025, 12, 1, 10, 5, tzinfo=UTC),
                    "active_received_at": datetime(2026, 3, 21, 10, 5, tzinfo=UTC),
                },
            )
            session.execute(
                text(
                    """
                    INSERT INTO vacancy_snapshot (
                        id,
                        vacancy_id,
                        snapshot_type,
                        captured_at,
                        crawl_run_id,
                        short_hash,
                        detail_hash,
                        short_payload_ref_id,
                        detail_payload_ref_id,
                        normalized_json,
                        change_reason
                    )
                    VALUES
                        (
                            301,
                            :vacancy_one_id,
                            'detail',
                            :older_snapshot_at,
                            :old_run_id,
                            NULL,
                            'hash-old',
                            NULL,
                            NULL,
                            '{}'::jsonb,
                            'older_snapshot'
                        ),
                        (
                            302,
                            :vacancy_one_id,
                            'detail',
                            :latest_snapshot_at,
                            :old_run_id,
                            NULL,
                            'hash-latest',
                            NULL,
                            202,
                            '{}'::jsonb,
                            'latest_snapshot'
                        ),
                        (
                            303,
                            :vacancy_one_id,
                            'short',
                            :latest_short_snapshot_at,
                            :old_run_id,
                            'short-hash-latest',
                            NULL,
                            204,
                            NULL,
                            CAST(:latest_short_snapshot_json AS jsonb),
                            'latest_short_snapshot'
                        ),
                        (
                            304,
                            :vacancy_one_id,
                            'short',
                            :older_short_snapshot_at,
                            :old_run_id,
                            'short-hash-old',
                            NULL,
                            NULL,
                            NULL,
                            CAST(:older_short_snapshot_json AS jsonb),
                            'older_short_snapshot'
                        )
                    """
                ),
                {
                    "vacancy_one_id": vacancy_one_id,
                    "old_run_id": old_run_id,
                    "older_snapshot_at": datetime(2025, 12, 1, 10, 10, tzinfo=UTC),
                    "latest_snapshot_at": datetime(2025, 12, 2, 10, 10, tzinfo=UTC),
                    "older_short_snapshot_at": datetime(2025, 12, 1, 10, 20, tzinfo=UTC),
                    "latest_short_snapshot_at": datetime(2025, 12, 2, 10, 20, tzinfo=UTC),
                    "latest_short_snapshot_json": (
                        '{"schema_version": 2, "source": "short", "payload": {"id": "hh-1"}}'
                    ),
                    "older_short_snapshot_json": (
                        '{"schema_version": 2, "source": "short", "payload": {"id": "hh-1-old"}}'
                    ),
                },
            )
            session.execute(
                text(
                    """
                    INSERT INTO detail_fetch_attempt (
                        id,
                        vacancy_id,
                        crawl_run_id,
                        reason,
                        attempt,
                        status,
                        requested_at,
                        finished_at,
                        error_message
                    )
                    VALUES
                        (
                            401,
                            :vacancy_one_id,
                            :old_run_id,
                            'first_seen',
                            1,
                            'failed',
                            :older_attempt_at,
                            :older_attempt_at,
                            'old failure'
                        ),
                        (
                            402,
                            :vacancy_one_id,
                            :old_run_id,
                            'ttl_refresh',
                            2,
                            'succeeded',
                            :latest_attempt_at,
                            :latest_attempt_at,
                            NULL
                        ),
                        (
                            403,
                            :vacancy_two_id,
                            :active_run_id,
                            'first_seen',
                            1,
                            'running',
                            :active_attempt_at,
                            NULL,
                            NULL
                        )
                    """
                ),
                {
                    "vacancy_one_id": vacancy_one_id,
                    "old_run_id": old_run_id,
                    "older_attempt_at": datetime(2025, 12, 1, 10, 15, tzinfo=UTC),
                    "latest_attempt_at": datetime(2025, 12, 2, 10, 15, tzinfo=UTC),
                    "vacancy_two_id": vacancy_two_id,
                    "active_run_id": active_run_id,
                    "active_attempt_at": datetime(2026, 3, 21, 10, 15, tzinfo=UTC),
                },
            )

            repository = SqlAlchemyHousekeepingRepository(session)
            cutoff = datetime(2026, 2, 1, tzinfo=UTC)

            assert repository.count_raw_api_payload_candidates(cutoff=cutoff) == 2
            assert repository.list_raw_api_payload_ids_for_retention(cutoff=cutoff, limit=10) == [
                201,
                204,
            ]
            assert repository.count_vacancy_snapshot_candidates(cutoff=cutoff) == 2
            assert repository.list_vacancy_snapshot_ids_for_retention(
                cutoff=cutoff,
                limit=10,
            ) == [301, 304]
            assert repository.count_detail_fetch_attempt_candidates(cutoff=cutoff) == 1
            assert repository.list_detail_fetch_attempt_ids_for_retention(
                cutoff=cutoff,
                limit=10,
            ) == [401]
            assert repository.count_finished_crawl_run_candidates(cutoff=cutoff) == 1
            assert repository.list_finished_crawl_run_ids_for_retention(
                cutoff=cutoff,
                limit=10,
            ) == [old_run_id]
            assert repository.count_crawl_partition_candidates_for_finished_runs(
                cutoff=cutoff
            ) == 1
            assert repository.count_crawl_partitions_for_run_ids([old_run_id]) == 1
    finally:
        _cleanup_test_rows(engine)
        engine.dispose()
