from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import OperationalError

from hhru_platform.application.commands.fetch_vacancy_detail import (
    FetchVacancyDetailCommand,
    fetch_vacancy_detail,
)
from hhru_platform.application.dto import VacancyDetailResponse
from hhru_platform.infrastructure.db.repositories import (
    SqlAlchemyApiRequestLogRepository,
    SqlAlchemyDetailFetchAttemptRepository,
    SqlAlchemyRawApiPayloadRepository,
    SqlAlchemyVacancyCurrentStateRepository,
    SqlAlchemyVacancyRepository,
    SqlAlchemyVacancySnapshotRepository,
)
from hhru_platform.infrastructure.db.session import (
    create_engine_from_settings,
    create_session_factory,
    session_scope,
)

TEST_USER_AGENT = "pytest-fetch-vacancy-detail"
TEST_AREA_HH_ID = "pytest-detail-area"
TEST_VACANCY_HH_ID = "pytest-detail-vacancy"
TEST_EMPLOYER_HH_ID = "pytest-detail-employer"
TEST_ROLE_HH_IDS = ("pytest-detail-role-python", "pytest-detail-role-data")


class StaticVacancyDetailApiClient:
    def fetch_vacancy_detail(self, hh_vacancy_id: str) -> VacancyDetailResponse:
        assert hh_vacancy_id == TEST_VACANCY_HH_ID
        return VacancyDetailResponse(
            endpoint=f"/vacancies/{hh_vacancy_id}",
            method="GET",
            params_json={},
            request_headers_json={
                "Accept": "application/json",
                "User-Agent": TEST_USER_AGENT,
            },
            status_code=200,
            headers={"x-request-id": "pytest-detail-request"},
            latency_ms=19,
            requested_at=datetime(2026, 3, 12, 12, 0, tzinfo=UTC),
            response_received_at=datetime(2026, 3, 12, 12, 0, 1, tzinfo=UTC),
            payload_json={
                "id": TEST_VACANCY_HH_ID,
                "name": "Lead Python Engineer",
                "description": "Detailed vacancy description from pytest",
                "alternate_url": f"https://hh.ru/vacancy/{TEST_VACANCY_HH_ID}",
                "archived": False,
                "area": {"id": TEST_AREA_HH_ID, "name": "Pytest Area"},
                "created_at": "2026-03-12T09:30:00+0300",
                "initial_created_at": "2026-03-11T09:00:00+0300",
                "employer": {
                    "id": TEST_EMPLOYER_HH_ID,
                    "name": "Pytest Detail Employer",
                    "alternate_url": "https://hh.ru/employer/pytest-detail-employer",
                    "trusted": True,
                },
                "employment": {"id": "full", "name": "Full employment"},
                "experience": {"id": "between1And3", "name": "1-3 years"},
                "key_skills": [{"name": "Python"}, {"name": "PostgreSQL"}],
                "professional_roles": [
                    {"id": TEST_ROLE_HH_IDS[0], "name": "Python Developer"},
                    {"id": TEST_ROLE_HH_IDS[1], "name": "Data Engineer"},
                ],
                "published_at": "2026-03-12T10:00:00+0300",
                "salary": {"currency": "RUR", "from": 200000, "to": 300000, "gross": False},
                "schedule": {"id": "remote", "name": "Remote"},
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


def _cleanup_test_rows(engine) -> None:
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                DELETE FROM vacancy_snapshot
                WHERE vacancy_id IN (
                    SELECT id
                    FROM vacancy
                    WHERE hh_vacancy_id = :hh_vacancy_id
                )
                """
            ),
            {"hh_vacancy_id": TEST_VACANCY_HH_ID},
        )
        connection.execute(
            text(
                """
                DELETE FROM detail_fetch_attempt
                WHERE vacancy_id IN (
                    SELECT id
                    FROM vacancy
                    WHERE hh_vacancy_id = :hh_vacancy_id
                )
                """
            ),
            {"hh_vacancy_id": TEST_VACANCY_HH_ID},
        )
        connection.execute(
            text(
                """
                DELETE FROM vacancy_current_state
                WHERE vacancy_id IN (
                    SELECT id
                    FROM vacancy
                    WHERE hh_vacancy_id = :hh_vacancy_id
                )
                """
            ),
            {"hh_vacancy_id": TEST_VACANCY_HH_ID},
        )
        connection.execute(
            text(
                """
                DELETE FROM raw_api_payload
                WHERE id IN (
                    SELECT raw.id
                    FROM raw_api_payload AS raw
                    JOIN api_request_log AS request_log
                      ON request_log.id = raw.api_request_log_id
                    WHERE request_log.request_headers_json ->> 'User-Agent' = :user_agent
                )
                """
            ),
            {"user_agent": TEST_USER_AGENT},
        )
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
                DELETE FROM employer
                WHERE hh_employer_id = :hh_employer_id
                """
            ),
            {"hh_employer_id": TEST_EMPLOYER_HH_ID},
        )
        connection.execute(
            text(
                """
                DELETE FROM professional_role
                WHERE hh_professional_role_id IN (:role_one, :role_two)
                """
            ),
            {
                "role_one": TEST_ROLE_HH_IDS[0],
                "role_two": TEST_ROLE_HH_IDS[1],
            },
        )
        connection.execute(
            text(
                """
                DELETE FROM vacancy
                WHERE hh_vacancy_id = :hh_vacancy_id
                """
            ),
            {"hh_vacancy_id": TEST_VACANCY_HH_ID},
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


def test_fetch_vacancy_detail_persists_attempt_snapshot_current_state_and_logs() -> None:
    engine = create_engine_from_settings()
    session_factory = create_session_factory(engine)
    area_id = uuid4()
    created_vacancy_id = uuid4()

    try:
        _cleanup_test_rows(engine)
        with session_scope(session_factory) as session:
            session.execute(
                text(
                    """
                    INSERT INTO area (id, hh_area_id, name, level, path_text, is_active)
                    VALUES (:id, :hh_area_id, :name, 0, :path_text, TRUE)
                    ON CONFLICT (hh_area_id) DO NOTHING
                    """
                ),
                {
                    "id": area_id,
                    "hh_area_id": TEST_AREA_HH_ID,
                    "name": "Pytest Area",
                    "path_text": "Pytest Area",
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
                        :id,
                        :hh_vacancy_id,
                        :name_current,
                        'hh_api'
                    )
                    """
                ),
                {
                    "id": created_vacancy_id,
                    "hh_vacancy_id": TEST_VACANCY_HH_ID,
                    "name_current": "Old vacancy name",
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
                        detail_fetch_status,
                        updated_at
                    )
                    VALUES (
                        :vacancy_id,
                        :first_seen_at,
                        :last_seen_at,
                        1,
                        0,
                        FALSE,
                        'not_requested',
                        :updated_at
                    )
                    """
                ),
                {
                    "vacancy_id": created_vacancy_id,
                    "first_seen_at": datetime(2026, 3, 12, 11, 0, tzinfo=UTC),
                    "last_seen_at": datetime(2026, 3, 12, 11, 0, tzinfo=UTC),
                    "updated_at": datetime(2026, 3, 12, 11, 0, tzinfo=UTC),
                },
            )
            session.execute(
                text(
                    """
                    INSERT INTO professional_role (
                        hh_professional_role_id,
                        name,
                        category_name,
                        is_active
                    )
                    VALUES
                        (:role_one, :role_one_name, 'Pytest Category', TRUE),
                        (:role_two, :role_two_name, 'Pytest Category', TRUE)
                    ON CONFLICT (hh_professional_role_id) DO NOTHING
                    """
                ),
                {
                    "role_one": TEST_ROLE_HH_IDS[0],
                    "role_one_name": "Python Developer",
                    "role_two": TEST_ROLE_HH_IDS[1],
                    "role_two_name": "Data Engineer",
                },
            )

            result = fetch_vacancy_detail(
                FetchVacancyDetailCommand(vacancy_id=created_vacancy_id),
                vacancy_repository=SqlAlchemyVacancyRepository(session),
                api_client=StaticVacancyDetailApiClient(),
                detail_fetch_attempt_repository=SqlAlchemyDetailFetchAttemptRepository(session),
                api_request_log_repository=SqlAlchemyApiRequestLogRepository(session),
                raw_api_payload_repository=SqlAlchemyRawApiPayloadRepository(session),
                vacancy_snapshot_repository=SqlAlchemyVacancySnapshotRepository(session),
                vacancy_current_state_repository=SqlAlchemyVacancyCurrentStateRepository(session),
            )

        assert result.detail_fetch_status == "succeeded"
        assert result.snapshot_id is not None
        assert result.request_log_id > 0
        assert result.raw_payload_id is not None

        with engine.connect() as connection:
            vacancy_row = (
                connection.execute(
                    text(
                        """
                    SELECT v.hh_vacancy_id,
                           v.name_current,
                           v.alternate_url,
                           e.hh_employer_id AS employer_hh_id,
                           e.name AS employer_name,
                           v.employment_type_code,
                           v.schedule_type_code,
                           v.experience_code,
                           a.hh_area_id AS area_hh_id
                    FROM vacancy AS v
                    LEFT JOIN area AS a ON a.id = v.area_id
                    LEFT JOIN employer AS e ON e.id = v.employer_id
                    WHERE v.id = :vacancy_id
                    """
                    ),
                    {"vacancy_id": created_vacancy_id},
                )
                .mappings()
                .one()
            )
            employer_row = (
                connection.execute(
                    text(
                        """
                    SELECT hh_employer_id, name, alternate_url, is_trusted
                    FROM employer
                    WHERE hh_employer_id = :hh_employer_id
                    """
                    ),
                    {"hh_employer_id": TEST_EMPLOYER_HH_ID},
                )
                .mappings()
                .one()
            )
            vacancy_role_rows = (
                connection.execute(
                    text(
                        """
                    SELECT pr.hh_professional_role_id
                    FROM vacancy_professional_role AS vpr
                    JOIN professional_role AS pr ON pr.id = vpr.professional_role_id
                    WHERE vpr.vacancy_id = :vacancy_id
                    ORDER BY pr.hh_professional_role_id
                    """
                    ),
                    {"vacancy_id": created_vacancy_id},
                )
                .mappings()
                .all()
            )
            attempt_row = (
                connection.execute(
                    text(
                        """
                    SELECT reason, attempt, status, error_message
                    FROM detail_fetch_attempt
                    WHERE vacancy_id = :vacancy_id
                    """
                    ),
                    {"vacancy_id": created_vacancy_id},
                )
                .mappings()
                .one()
            )
            request_log_row = (
                connection.execute(
                    text(
                        """
                    SELECT id, status_code, endpoint
                    FROM api_request_log
                    WHERE request_type = 'vacancy_detail'
                      AND request_headers_json ->> 'User-Agent' = :user_agent
                    """
                    ),
                    {"user_agent": TEST_USER_AGENT},
                )
                .mappings()
                .one()
            )
            raw_payload_row = (
                connection.execute(
                    text(
                        """
                    SELECT id, endpoint_type, entity_hh_id
                    FROM raw_api_payload
                    WHERE api_request_log_id = :api_request_log_id
                    """
                    ),
                    {"api_request_log_id": request_log_row["id"]},
                )
                .mappings()
                .one()
            )
            snapshot_row = (
                connection.execute(
                    text(
                        """
                    SELECT snapshot_type, change_reason, detail_hash, detail_payload_ref_id
                    FROM vacancy_snapshot
                    WHERE vacancy_id = :vacancy_id
                    """
                    ),
                    {"vacancy_id": created_vacancy_id},
                )
                .mappings()
                .one()
            )
            current_state_row = (
                connection.execute(
                    text(
                        """
                    SELECT last_detail_hash, last_detail_fetched_at, detail_fetch_status
                    FROM vacancy_current_state
                    WHERE vacancy_id = :vacancy_id
                    """
                    ),
                    {"vacancy_id": created_vacancy_id},
                )
                .mappings()
                .one()
            )

        assert vacancy_row["hh_vacancy_id"] == TEST_VACANCY_HH_ID
        assert vacancy_row["name_current"] == "Lead Python Engineer"
        assert vacancy_row["alternate_url"] == f"https://hh.ru/vacancy/{TEST_VACANCY_HH_ID}"
        assert vacancy_row["employer_hh_id"] == TEST_EMPLOYER_HH_ID
        assert vacancy_row["employer_name"] == "Pytest Detail Employer"
        assert vacancy_row["employment_type_code"] == "full"
        assert vacancy_row["schedule_type_code"] == "remote"
        assert vacancy_row["experience_code"] == "between1And3"
        assert vacancy_row["area_hh_id"] == TEST_AREA_HH_ID
        assert employer_row["hh_employer_id"] == TEST_EMPLOYER_HH_ID
        assert employer_row["name"] == "Pytest Detail Employer"
        assert employer_row["alternate_url"] == "https://hh.ru/employer/pytest-detail-employer"
        assert employer_row["is_trusted"] is True
        assert [row["hh_professional_role_id"] for row in vacancy_role_rows] == [
            TEST_ROLE_HH_IDS[1],
            TEST_ROLE_HH_IDS[0],
        ]
        assert attempt_row["reason"] == "manual_refetch"
        assert attempt_row["attempt"] == 1
        assert attempt_row["status"] == "succeeded"
        assert attempt_row["error_message"] is None
        assert request_log_row["status_code"] == 200
        assert request_log_row["endpoint"] == f"/vacancies/{TEST_VACANCY_HH_ID}"
        assert raw_payload_row["endpoint_type"] == "vacancies.detail"
        assert raw_payload_row["entity_hh_id"] == TEST_VACANCY_HH_ID
        assert snapshot_row["snapshot_type"] == "detail"
        assert snapshot_row["change_reason"] == "manual_refetch"
        assert snapshot_row["detail_hash"] is not None
        assert snapshot_row["detail_payload_ref_id"] == raw_payload_row["id"]
        assert current_state_row["last_detail_hash"] == snapshot_row["detail_hash"]
        assert current_state_row["last_detail_fetched_at"] is not None
        assert current_state_row["detail_fetch_status"] == "succeeded"
    finally:
        _cleanup_test_rows(engine)
        engine.dispose()
