from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID

import pytest
from sqlalchemy import text
from sqlalchemy.exc import OperationalError

from hhru_platform.application.commands.create_crawl_run import (
    CreateCrawlRunCommand,
    create_crawl_run,
)
from hhru_platform.application.commands.process_list_page import (
    ProcessListPageCommand,
    process_list_page,
)
from hhru_platform.application.dto import VacancySearchResponse
from hhru_platform.infrastructure.db.repositories import (
    SqlAlchemyApiRequestLogRepository,
    SqlAlchemyCrawlPartitionRepository,
    SqlAlchemyCrawlRunRepository,
    SqlAlchemyRawApiPayloadRepository,
    SqlAlchemyVacancyCurrentStateRepository,
    SqlAlchemyVacancyRepository,
    SqlAlchemyVacancySeenEventRepository,
)
from hhru_platform.infrastructure.db.session import (
    create_engine_from_settings,
    create_session_factory,
    session_scope,
)

TEST_TRIGGERED_BY = "pytest-process-list-page"
TEST_USER_AGENT = "pytest-process-list-page"
TEST_PARTITION_KEY = "pytest-process-list-page-partition"
TEST_AREA_HH_ID = "pytest-process-list-area"
TEST_VACANCY_IDS = ("pytest-process-list-vacancy-1", "pytest-process-list-vacancy-2")


class StaticVacancySearchApiClient:
    def search_vacancies(self, params_json: dict[str, object]) -> VacancySearchResponse:
        assert params_json["page"] == 0
        assert params_json["per_page"] == 2
        assert params_json["text"] == "pytest process list"
        return VacancySearchResponse(
            endpoint="/vacancies",
            method="GET",
            params_json=dict(params_json),
            request_headers_json={
                "Accept": "application/json",
                "User-Agent": TEST_USER_AGENT,
            },
            status_code=200,
            headers={"x-request-id": "pytest-process-list-page"},
            latency_ms=17,
            requested_at=datetime(2026, 3, 12, 12, 0, tzinfo=UTC),
            response_received_at=datetime(2026, 3, 12, 12, 0, 1, tzinfo=UTC),
            payload_json={
                "items": [
                    {
                        "id": TEST_VACANCY_IDS[0],
                        "name": "Python Engineer",
                        "area": {"id": TEST_AREA_HH_ID, "name": "Test Area"},
                        "created_at": "2026-03-12T09:30:00+0300",
                        "published_at": "2026-03-12T10:00:00+0300",
                        "alternate_url": "https://hh.ru/vacancy/pytest-process-list-vacancy-1",
                        "employment": {"id": "full", "name": "Full"},
                        "schedule": {"id": "remote", "name": "Remote"},
                        "experience": {"id": "between1And3", "name": "1-3 years"},
                    },
                    {
                        "id": TEST_VACANCY_IDS[1],
                        "name": "Data Engineer",
                        "area": {"id": TEST_AREA_HH_ID, "name": "Test Area"},
                        "created_at": "2026-03-12T09:35:00+0300",
                        "published_at": "2026-03-12T10:05:00+0300",
                        "alternate_url": "https://hh.ru/vacancy/pytest-process-list-vacancy-2",
                        "employment": {"id": "part", "name": "Part time"},
                        "schedule": {"id": "fullDay", "name": "Full day"},
                        "experience": {"id": "noExperience", "name": "No experience"},
                    },
                ],
                "found": 7,
                "page": 0,
                "pages": 4,
                "per_page": 2,
            },
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


def test_process_list_page_persists_vacancies_seen_events_current_state_and_logs() -> None:
    engine = create_engine_from_settings()
    session_factory = create_session_factory(engine)
    created_run_id: UUID | None = None
    created_partition_id: UUID | None = None

    try:
        with session_scope(session_factory) as session:
            session.execute(
                text(
                    """
                    INSERT INTO area (hh_area_id, name, level, path_text, is_active)
                    VALUES (:hh_area_id, :name, 0, :path_text, TRUE)
                    ON CONFLICT (hh_area_id) DO NOTHING
                    """
                ),
                {
                    "hh_area_id": TEST_AREA_HH_ID,
                    "name": "Test Area",
                    "path_text": "Test Area",
                },
            )

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
                partition_key=TEST_PARTITION_KEY,
                status="pending",
                params_json={"params": {"text": "pytest process list", "per_page": 2}},
            )
            created_partition_id = crawl_partition.id

            result = process_list_page(
                ProcessListPageCommand(partition_id=crawl_partition.id),
                crawl_partition_repository=crawl_partition_repository,
                api_client=StaticVacancySearchApiClient(),
                api_request_log_repository=SqlAlchemyApiRequestLogRepository(session),
                raw_api_payload_repository=SqlAlchemyRawApiPayloadRepository(session),
                vacancy_repository=SqlAlchemyVacancyRepository(session),
                vacancy_seen_event_repository=SqlAlchemyVacancySeenEventRepository(session),
                vacancy_current_state_repository=SqlAlchemyVacancyCurrentStateRepository(session),
            )

        assert result.partition_status == "done"
        assert result.vacancies_processed == 2
        assert result.vacancies_created == 2
        assert result.seen_events_created == 2

        with engine.connect() as connection:
            vacancy_rows = connection.execute(
                text(
                    """
                    SELECT v.hh_vacancy_id,
                           v.name_current,
                           v.employment_type_code,
                           v.schedule_type_code,
                           v.experience_code,
                           a.hh_area_id AS area_hh_id
                    FROM vacancy AS v
                    LEFT JOIN area AS a ON a.id = v.area_id
                    WHERE v.hh_vacancy_id IN (:vacancy_one, :vacancy_two)
                    ORDER BY v.hh_vacancy_id
                    """
                ),
                {
                    "vacancy_one": TEST_VACANCY_IDS[0],
                    "vacancy_two": TEST_VACANCY_IDS[1],
                },
            ).mappings().all()
            seen_event_rows = connection.execute(
                text(
                    """
                    SELECT list_position, short_payload_ref_id
                    FROM vacancy_seen_event
                    WHERE crawl_partition_id = :crawl_partition_id
                    ORDER BY list_position
                    """
                ),
                {"crawl_partition_id": created_partition_id},
            ).mappings().all()
            current_state_rows = connection.execute(
                text(
                    """
                    SELECT seen_count,
                           consecutive_missing_runs,
                           is_probably_inactive,
                           detail_fetch_status,
                           last_seen_run_id
                    FROM vacancy_current_state
                    WHERE vacancy_id IN (
                        SELECT id FROM vacancy WHERE hh_vacancy_id IN (:vacancy_one, :vacancy_two)
                    )
                    ORDER BY vacancy_id
                    """
                ),
                {
                    "vacancy_one": TEST_VACANCY_IDS[0],
                    "vacancy_two": TEST_VACANCY_IDS[1],
                },
            ).mappings().all()
            request_log_row = connection.execute(
                text(
                    """
                    SELECT id, status_code
                    FROM api_request_log
                    WHERE request_type = 'vacancy_search'
                      AND request_headers_json ->> 'User-Agent' = :user_agent
                    """
                ),
                {"user_agent": TEST_USER_AGENT},
            ).mappings().one()
            raw_payload_row = connection.execute(
                text(
                    """
                    SELECT id, endpoint_type
                    FROM raw_api_payload
                    WHERE api_request_log_id = :api_request_log_id
                    """
                ),
                {"api_request_log_id": request_log_row["id"]},
            ).mappings().one()
            partition_row = connection.execute(
                text(
                    """
                    SELECT status, pages_total_expected, pages_processed, items_seen
                    FROM crawl_partition
                    WHERE id = :crawl_partition_id
                    """
                ),
                {"crawl_partition_id": created_partition_id},
            ).mappings().one()

        assert [row["hh_vacancy_id"] for row in vacancy_rows] == list(TEST_VACANCY_IDS)
        assert all(row["area_hh_id"] == TEST_AREA_HH_ID for row in vacancy_rows)
        assert vacancy_rows[0]["employment_type_code"] == "full"
        assert vacancy_rows[0]["schedule_type_code"] == "remote"
        assert vacancy_rows[0]["experience_code"] == "between1And3"
        assert vacancy_rows[1]["employment_type_code"] == "part"
        assert len(seen_event_rows) == 2
        assert [row["list_position"] for row in seen_event_rows] == [0, 1]
        assert all(row["short_payload_ref_id"] == raw_payload_row["id"] for row in seen_event_rows)
        assert len(current_state_rows) == 2
        assert all(row["seen_count"] == 1 for row in current_state_rows)
        assert all(row["consecutive_missing_runs"] == 0 for row in current_state_rows)
        assert all(row["is_probably_inactive"] is False for row in current_state_rows)
        assert all(row["detail_fetch_status"] == "not_requested" for row in current_state_rows)
        assert all(row["last_seen_run_id"] == created_run_id for row in current_state_rows)
        assert request_log_row["status_code"] == 200
        assert raw_payload_row["endpoint_type"] == "vacancies.search"
        assert partition_row["status"] == "done"
        assert partition_row["pages_total_expected"] == 4
        assert partition_row["pages_processed"] == 1
        assert partition_row["items_seen"] == 2
    finally:
        with engine.begin() as connection:
            connection.execute(
                text(
                    """
                    DELETE FROM api_request_log
                    WHERE request_headers_json ->> 'User-Agent' = :user_agent
                    """
                ),
                {"user_agent": TEST_USER_AGENT},
            )
            connection.execute(
                text(
                    """
                    DELETE FROM crawl_run
                    WHERE triggered_by = :triggered_by
                    """
                ),
                {"triggered_by": TEST_TRIGGERED_BY},
            )
            connection.execute(
                text(
                    """
                    DELETE FROM vacancy
                    WHERE hh_vacancy_id IN (:vacancy_one, :vacancy_two)
                    """
                ),
                {
                    "vacancy_one": TEST_VACANCY_IDS[0],
                    "vacancy_two": TEST_VACANCY_IDS[1],
                },
            )
            connection.execute(
                text(
                    """
                    DELETE FROM area
                    WHERE hh_area_id = :hh_area_id
                    """
                ),
                {"hh_area_id": TEST_AREA_HH_ID},
            )
        engine.dispose()
