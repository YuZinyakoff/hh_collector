from __future__ import annotations

from datetime import UTC, datetime

import pytest
from sqlalchemy import text
from sqlalchemy.exc import OperationalError

from hhru_platform.application.commands.sync_dictionary import (
    SyncDictionaryCommand,
    sync_dictionary,
)
from hhru_platform.application.dto import DictionaryFetchResponse
from hhru_platform.infrastructure.db.repositories import (
    SqlAlchemyApiRequestLogRepository,
    SqlAlchemyAreaRepository,
    SqlAlchemyDictionaryStore,
    SqlAlchemyDictionarySyncRunRepository,
    SqlAlchemyProfessionalRoleRepository,
    SqlAlchemyRawApiPayloadRepository,
)
from hhru_platform.infrastructure.db.session import (
    create_engine_from_settings,
    create_session_factory,
    session_scope,
)

TEST_USER_AGENT = "pytest-dictionary-sync"
TEST_AREAS_ETAG = "pytest-dictionary-sync-areas-etag"
TEST_ROLES_ETAG = "pytest-dictionary-sync-roles-etag"
TEST_AREA_IDS = ("pytest-root-area", "pytest-child-area")
TEST_ROLE_IDS = ("pytest-role-1", "pytest-role-2")


class StaticDictionaryApiClient:
    def fetch_dictionary(self, dictionary_name: str) -> DictionaryFetchResponse:
        if dictionary_name == "areas":
            return DictionaryFetchResponse(
                dictionary_name="areas",
                endpoint="/areas",
                method="GET",
                params_json={},
                request_headers_json={
                    "Accept": "application/json",
                    "User-Agent": TEST_USER_AGENT,
                },
                status_code=200,
                headers={"etag": TEST_AREAS_ETAG},
                latency_ms=18,
                requested_at=datetime(2026, 3, 12, 12, 0, tzinfo=UTC),
                response_received_at=datetime(2026, 3, 12, 12, 0, 1, tzinfo=UTC),
                payload_json=[
                    {
                        "id": TEST_AREA_IDS[0],
                        "name": "Test Root Area",
                        "parent_id": None,
                        "areas": [
                            {
                                "id": TEST_AREA_IDS[1],
                                "name": "Test Child Area",
                                "parent_id": TEST_AREA_IDS[0],
                                "areas": [],
                                "utc_offset": "+03:00",
                            }
                        ],
                    }
                ],
            )

        if dictionary_name == "professional_roles":
            return DictionaryFetchResponse(
                dictionary_name="professional_roles",
                endpoint="/professional_roles",
                method="GET",
                params_json={},
                request_headers_json={
                    "Accept": "application/json",
                    "User-Agent": TEST_USER_AGENT,
                },
                status_code=200,
                headers={"etag": TEST_ROLES_ETAG},
                latency_ms=22,
                requested_at=datetime(2026, 3, 12, 12, 1, tzinfo=UTC),
                response_received_at=datetime(2026, 3, 12, 12, 1, 1, tzinfo=UTC),
                payload_json={
                    "categories": [
                        {
                            "id": "pytest-category",
                            "name": "Test Category",
                            "roles": [
                                {
                                    "id": TEST_ROLE_IDS[0],
                                    "name": "Test Role One",
                                    "accept_incomplete_resumes": True,
                                    "is_default": True,
                                    "search_deprecated": False,
                                    "select_deprecated": False,
                                },
                                {
                                    "id": TEST_ROLE_IDS[1],
                                    "name": "Test Role Two",
                                    "accept_incomplete_resumes": False,
                                    "is_default": False,
                                    "search_deprecated": False,
                                    "select_deprecated": False,
                                },
                            ],
                        }
                    ]
                },
            )

        raise AssertionError(f"unexpected dictionary: {dictionary_name}")


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


def test_sync_dictionary_persists_runs_logs_raw_payloads_and_reference_rows() -> None:
    engine = create_engine_from_settings()
    session_factory = create_session_factory(engine)
    api_client = StaticDictionaryApiClient()

    try:
        with session_scope(session_factory) as session:
            sync_run_repository = SqlAlchemyDictionarySyncRunRepository(session)
            api_request_log_repository = SqlAlchemyApiRequestLogRepository(session)
            raw_api_payload_repository = SqlAlchemyRawApiPayloadRepository(session)
            dictionary_store = SqlAlchemyDictionaryStore(
                area_repository=SqlAlchemyAreaRepository(session),
                professional_role_repository=SqlAlchemyProfessionalRoleRepository(session),
            )

            areas_result = sync_dictionary(
                SyncDictionaryCommand(dictionary_name="areas"),
                api_client=api_client,
                sync_run_repository=sync_run_repository,
                api_request_log_repository=api_request_log_repository,
                raw_api_payload_repository=raw_api_payload_repository,
                dictionary_store=dictionary_store,
            )
            roles_result = sync_dictionary(
                SyncDictionaryCommand(dictionary_name="professional_roles"),
                api_client=api_client,
                sync_run_repository=sync_run_repository,
                api_request_log_repository=api_request_log_repository,
                raw_api_payload_repository=raw_api_payload_repository,
                dictionary_store=dictionary_store,
            )

        assert areas_result.status == "succeeded"
        assert roles_result.status == "succeeded"

        with engine.connect() as connection:
            sync_run_rows = (
                connection.execute(
                    text(
                        """
                    SELECT dictionary_name, status, source_status_code
                    FROM dictionary_sync_run
                    WHERE etag IN (:areas_etag, :roles_etag)
                    ORDER BY dictionary_name
                    """
                    ),
                    {"areas_etag": TEST_AREAS_ETAG, "roles_etag": TEST_ROLES_ETAG},
                )
                .mappings()
                .all()
            )
            child_area_row = (
                connection.execute(
                    text(
                        """
                    SELECT child.name, child.level, child.path_text, child.is_active,
                           parent.hh_area_id AS parent_hh_area_id
                    FROM area AS child
                    LEFT JOIN area AS parent ON parent.id = child.parent_area_id
                    WHERE child.hh_area_id = :child_hh_area_id
                    """
                    ),
                    {"child_hh_area_id": TEST_AREA_IDS[1]},
                )
                .mappings()
                .one()
            )
            professional_role_rows = (
                connection.execute(
                    text(
                        """
                    SELECT hh_professional_role_id, name, category_name, is_active
                    FROM professional_role
                    WHERE hh_professional_role_id IN (:role_one, :role_two)
                    ORDER BY hh_professional_role_id
                    """
                    ),
                    {"role_one": TEST_ROLE_IDS[0], "role_two": TEST_ROLE_IDS[1]},
                )
                .mappings()
                .all()
            )
            request_log_count = connection.execute(
                text(
                    """
                    SELECT count(*)
                    FROM api_request_log
                    WHERE request_type = 'dictionary_sync'
                      AND request_headers_json ->> 'User-Agent' = :user_agent
                    """
                ),
                {"user_agent": TEST_USER_AGENT},
            ).scalar_one()
            raw_payload_count = connection.execute(
                text(
                    """
                    SELECT count(*)
                    FROM raw_api_payload
                    WHERE api_request_log_id IN (
                        SELECT id
                        FROM api_request_log
                        WHERE request_headers_json ->> 'User-Agent' = :user_agent
                    )
                    """
                ),
                {"user_agent": TEST_USER_AGENT},
            ).scalar_one()

        assert [row["dictionary_name"] for row in sync_run_rows] == ["areas", "professional_roles"]
        assert all(row["status"] == "succeeded" for row in sync_run_rows)
        assert child_area_row["name"] == "Test Child Area"
        assert child_area_row["level"] == 1
        assert child_area_row["path_text"] == "Test Root Area / Test Child Area"
        assert child_area_row["is_active"] is True
        assert child_area_row["parent_hh_area_id"] == TEST_AREA_IDS[0]
        assert len(professional_role_rows) == 2
        assert all(row["category_name"] == "Test Category" for row in professional_role_rows)
        assert all(row["is_active"] is True for row in professional_role_rows)
        assert request_log_count == 2
        assert raw_payload_count == 2
    finally:
        with engine.begin() as connection:
            connection.execute(
                text(
                    """
                    DELETE FROM raw_api_payload
                    WHERE api_request_log_id IN (
                        SELECT id
                        FROM api_request_log
                        WHERE request_headers_json ->> 'User-Agent' = :user_agent
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
                    DELETE FROM dictionary_sync_run
                    WHERE etag IN (:areas_etag, :roles_etag)
                    """
                ),
                {"areas_etag": TEST_AREAS_ETAG, "roles_etag": TEST_ROLES_ETAG},
            )
            connection.execute(
                text(
                    """
                    DELETE FROM professional_role
                    WHERE hh_professional_role_id IN (:role_one, :role_two)
                    """
                ),
                {"role_one": TEST_ROLE_IDS[0], "role_two": TEST_ROLE_IDS[1]},
            )
            connection.execute(
                text(
                    """
                    DELETE FROM area
                    WHERE hh_area_id IN (:root_hh_area_id, :child_hh_area_id)
                    """
                ),
                {
                    "root_hh_area_id": TEST_AREA_IDS[0],
                    "child_hh_area_id": TEST_AREA_IDS[1],
                },
            )
        engine.dispose()
