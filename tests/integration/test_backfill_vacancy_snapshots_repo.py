from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import OperationalError

from hhru_platform.application.commands.backfill_vacancy_snapshots import (
    BackfillVacancySnapshotsCommand,
    backfill_vacancy_snapshots,
)
from hhru_platform.application.commands.create_crawl_run import (
    CreateCrawlRunCommand,
    create_crawl_run,
)
from hhru_platform.infrastructure.db.repositories import (
    SqlAlchemyCrawlPartitionRepository,
    SqlAlchemyCrawlRunRepository,
    SqlAlchemyVacancySnapshotBackfillRepository,
)
from hhru_platform.infrastructure.db.session import (
    create_engine_from_settings,
    create_session_factory,
    session_scope,
)
from hhru_platform.infrastructure.normalization.vacancy_short_normalizer import (
    normalize_vacancy_search_page,
)

TEST_TRIGGERED_BY = "pytest-backfill-vacancy-snapshots"
TEST_VACANCY_HH_ID = "pytest-backfill-vacancy"


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


def test_backfill_vacancy_snapshots_upgrades_legacy_detail_rows_and_creates_short_rows() -> None:
    engine = create_engine_from_settings()
    session_factory = create_session_factory(engine)
    created_run_id: UUID | None = None
    created_partition_id: UUID | None = None
    vacancy_id = uuid4()
    short_payload_json = {
        "items": [
            {
                "id": TEST_VACANCY_HH_ID,
                "name": "Pytest Backfill Search Vacancy",
                "alternate_url": f"https://hh.ru/vacancy/{TEST_VACANCY_HH_ID}",
            }
        ],
        "page": 0,
        "pages": 1,
        "per_page": 20,
    }
    detail_payload_json = {
        "id": TEST_VACANCY_HH_ID,
        "name": "Pytest Backfill Detail Vacancy",
        "description": "Backfilled detail text",
        "alternate_url": f"https://hh.ru/vacancy/{TEST_VACANCY_HH_ID}",
    }
    short_hash = normalize_vacancy_search_page(short_payload_json).items[0].short_hash

    try:
        with session_scope(session_factory) as session:
            crawl_run_repository = SqlAlchemyCrawlRunRepository(session)
            crawl_partition_repository = SqlAlchemyCrawlPartitionRepository(session)
            crawl_run = create_crawl_run(
                CreateCrawlRunCommand(
                    run_type="weekly_sweep",
                    triggered_by=TEST_TRIGGERED_BY,
                ),
                crawl_run_repository,
            )
            created_run_id = crawl_run.id
            crawl_partition = crawl_partition_repository.add(
                crawl_run_id=crawl_run.id,
                partition_key="pytest-backfill-partition",
                status="done",
                params_json={"params": {"text": "pytest backfill"}},
            )
            created_partition_id = crawl_partition.id

            session.execute(
                text(
                    """
                    INSERT INTO vacancy (
                        id,
                        hh_vacancy_id,
                        name_current,
                        source_type
                    )
                    VALUES (:vacancy_id, :hh_vacancy_id, 'Legacy Vacancy', 'hh_api')
                    """
                ),
                {
                    "vacancy_id": vacancy_id,
                    "hh_vacancy_id": TEST_VACANCY_HH_ID,
                },
            )
            session.execute(
                text(
                    """
                    INSERT INTO vacancy_current_state (
                        vacancy_id,
                        first_seen_at,
                        last_seen_at,
                        seen_count,
                        consecutive_missing_runs,
                        is_probably_inactive,
                        last_seen_run_id,
                        last_short_hash,
                        last_detail_hash,
                        last_detail_fetched_at,
                        detail_fetch_status
                    )
                    VALUES (
                        :vacancy_id,
                        :seen_at,
                        :seen_at,
                        1,
                        0,
                        FALSE,
                        :crawl_run_id,
                        :short_hash,
                        'legacy-detail-hash',
                        :detail_seen_at,
                        'succeeded'
                    )
                    """
                ),
                {
                    "vacancy_id": vacancy_id,
                    "seen_at": datetime(2026, 3, 20, 12, 0, tzinfo=UTC),
                    "detail_seen_at": datetime(2026, 3, 20, 12, 5, tzinfo=UTC),
                    "crawl_run_id": created_run_id,
                    "short_hash": short_hash,
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
                            9901,
                            :crawl_run_id,
                            :crawl_partition_id,
                            'vacancy_search',
                            '/vacancies',
                            'GET',
                            '{}'::jsonb,
                            200,
                            10,
                            1,
                            :seen_at,
                            :seen_at,
                            NULL,
                            NULL
                        ),
                        (
                            9902,
                            :crawl_run_id,
                            NULL,
                            'vacancy_detail',
                            '/vacancies/pytest-backfill-vacancy',
                            'GET',
                            '{}'::jsonb,
                            200,
                            10,
                            1,
                            :detail_seen_at,
                            :detail_seen_at,
                            NULL,
                            NULL
                        )
                    """
                ),
                {
                    "crawl_run_id": created_run_id,
                    "crawl_partition_id": created_partition_id,
                    "seen_at": datetime(2026, 3, 20, 12, 0, tzinfo=UTC),
                    "detail_seen_at": datetime(2026, 3, 20, 12, 5, tzinfo=UTC),
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
                            9911,
                            9901,
                            'vacancies.search',
                            NULL,
                            CAST(:short_payload_json AS jsonb),
                            'pytest-short-payload',
                            :seen_at
                        ),
                        (
                            9912,
                            9902,
                            'vacancies.detail',
                            :hh_vacancy_id,
                            CAST(:detail_payload_json AS jsonb),
                            'pytest-detail-payload',
                            :detail_seen_at
                        )
                    """
                ),
                {
                    "short_payload_json": text_repr(short_payload_json),
                    "detail_payload_json": text_repr(detail_payload_json),
                    "seen_at": datetime(2026, 3, 20, 12, 0, tzinfo=UTC),
                    "detail_seen_at": datetime(2026, 3, 20, 12, 5, tzinfo=UTC),
                    "hh_vacancy_id": TEST_VACANCY_HH_ID,
                },
            )
            session.execute(
                text(
                    """
                    INSERT INTO vacancy_seen_event (
                        vacancy_id,
                        crawl_run_id,
                        crawl_partition_id,
                        seen_at,
                        list_position,
                        short_hash,
                        short_payload_ref_id
                    )
                    VALUES (
                        :vacancy_id,
                        :crawl_run_id,
                        :crawl_partition_id,
                        :seen_at,
                        0,
                        :short_hash,
                        9911
                    )
                    """
                ),
                {
                    "vacancy_id": vacancy_id,
                    "crawl_run_id": created_run_id,
                    "crawl_partition_id": created_partition_id,
                    "seen_at": datetime(2026, 3, 20, 12, 0, tzinfo=UTC),
                    "short_hash": short_hash,
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
                    VALUES (
                        9921,
                        :vacancy_id,
                        'detail',
                        :detail_seen_at,
                        :crawl_run_id,
                        NULL,
                        'legacy-detail-hash',
                        NULL,
                        9912,
                        '{}'::jsonb,
                        'legacy_snapshot'
                    )
                    """
                ),
                {
                    "vacancy_id": vacancy_id,
                    "detail_seen_at": datetime(2026, 3, 20, 12, 5, tzinfo=UTC),
                    "crawl_run_id": created_run_id,
                },
            )

            result = backfill_vacancy_snapshots(
                BackfillVacancySnapshotsCommand(batch_size=50, triggered_by=TEST_TRIGGERED_BY),
                repository=SqlAlchemyVacancySnapshotBackfillRepository(session),
            )

        assert result.status == "succeeded"
        assert result.detail_snapshots_updated >= 1
        assert result.short_snapshots_created >= 1

        with engine.connect() as connection:
            detail_snapshot_row = (
                connection.execute(
                    text(
                        """
                        SELECT detail_hash, normalized_json
                        FROM vacancy_snapshot
                        WHERE id = 9921
                        """
                    )
                )
                .mappings()
                .one()
            )
            short_snapshot_row = (
                connection.execute(
                    text(
                        """
                        SELECT snapshot_type, short_hash, short_payload_ref_id, normalized_json
                        FROM vacancy_snapshot
                        WHERE snapshot_type = 'short' AND vacancy_id = :vacancy_id
                        """
                    ),
                    {"vacancy_id": vacancy_id},
                )
                .mappings()
                .one()
            )
            current_state_row = (
                connection.execute(
                    text(
                        """
                        SELECT last_detail_hash
                        FROM vacancy_current_state
                        WHERE vacancy_id = :vacancy_id
                        """
                    ),
                    {"vacancy_id": vacancy_id},
                )
                .mappings()
                .one()
            )

        assert detail_snapshot_row["detail_hash"] != "legacy-detail-hash"
        assert (
            detail_snapshot_row["normalized_json"]["payload"]["description"]
            == "Backfilled detail text"
        )
        assert short_snapshot_row["snapshot_type"] == "short"
        assert short_snapshot_row["short_hash"] == short_hash
        assert short_snapshot_row["short_payload_ref_id"] == 9911
        assert short_snapshot_row["normalized_json"]["payload"]["id"] == TEST_VACANCY_HH_ID
        assert current_state_row["last_detail_hash"] == detail_snapshot_row["detail_hash"]
    finally:
        with engine.begin() as connection:
            connection.execute(text("DELETE FROM api_request_log WHERE id IN (9901, 9902)"))
            connection.execute(
                text("DELETE FROM vacancy WHERE id = :vacancy_id"),
                {"vacancy_id": vacancy_id},
            )
            if created_run_id is not None:
                connection.execute(
                    text("DELETE FROM crawl_run WHERE id = :crawl_run_id"),
                    {"crawl_run_id": created_run_id},
                )
        engine.dispose()


def text_repr(payload: dict[str, object]) -> str:
    import json

    return json.dumps(payload, ensure_ascii=False)
