from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID

import pytest
import sqlalchemy as sa
from sqlalchemy import text
from sqlalchemy.exc import OperationalError

from hhru_platform.application.commands.create_crawl_run import (
    CreateCrawlRunCommand,
    create_crawl_run,
)
from hhru_platform.application.commands.plan_sweep_v2 import PlanRunV2Command, plan_sweep_v2
from hhru_platform.application.commands.split_partition import (
    SplitPartitionCommand,
    split_partition,
)
from hhru_platform.application.commands.sync_dictionary import (
    SyncDictionaryCommand,
    sync_dictionary,
)
from hhru_platform.application.dto import DictionaryFetchResponse
from hhru_platform.infrastructure.db.repositories import (
    SqlAlchemyApiRequestLogRepository,
    SqlAlchemyAreaRepository,
    SqlAlchemyCrawlPartitionRepository,
    SqlAlchemyCrawlRunRepository,
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

TEST_USER_AGENT = "pytest-planner-v2"
TEST_AREAS_ETAG = "pytest-planner-v2-areas-etag"
TEST_TRIGGERED_BY = "pytest-plan-run-v2"
TEST_AREA_IDS = (
    "pytest-root-russia",
    "pytest-root-kazakhstan",
    "pytest-area-moscow",
    "pytest-area-spb",
    "pytest-area-zelenograd",
)


class StaticAreasDictionaryApiClient:
    def fetch_dictionary(self, dictionary_name: str) -> DictionaryFetchResponse:
        if dictionary_name != "areas":
            raise AssertionError(f"unexpected dictionary: {dictionary_name}")

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
            latency_ms=12,
            requested_at=datetime(2026, 3, 19, 12, 0, tzinfo=UTC),
            response_received_at=datetime(2026, 3, 19, 12, 0, 1, tzinfo=UTC),
            payload_json=[
                {
                    "id": TEST_AREA_IDS[0],
                    "name": "Pytest Russia",
                    "parent_id": None,
                    "areas": [
                        {
                            "id": TEST_AREA_IDS[2],
                            "name": "Pytest Moscow",
                            "parent_id": TEST_AREA_IDS[0],
                            "areas": [
                                {
                                    "id": TEST_AREA_IDS[4],
                                    "name": "Pytest Zelenograd",
                                    "parent_id": TEST_AREA_IDS[2],
                                    "areas": [],
                                    "utc_offset": "+03:00",
                                }
                            ],
                            "utc_offset": "+03:00",
                        },
                        {
                            "id": TEST_AREA_IDS[3],
                            "name": "Pytest Saint Petersburg",
                            "parent_id": TEST_AREA_IDS[0],
                            "areas": [],
                            "utc_offset": "+03:00",
                        },
                    ],
                },
                {
                    "id": TEST_AREA_IDS[1],
                    "name": "Pytest Kazakhstan",
                    "parent_id": None,
                    "areas": [],
                },
            ],
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


def test_plan_sweep_v2_creates_area_tree_roots_and_split_children() -> None:
    engine = create_engine_from_settings()
    session_factory = create_session_factory(engine)
    api_client = StaticAreasDictionaryApiClient()
    created_run_id: UUID | None = None
    root_partition_id: UUID | None = None

    try:
        with session_scope(session_factory) as session:
            sync_dictionary(
                SyncDictionaryCommand(dictionary_name="areas"),
                api_client=api_client,
                sync_run_repository=SqlAlchemyDictionarySyncRunRepository(session),
                api_request_log_repository=SqlAlchemyApiRequestLogRepository(session),
                raw_api_payload_repository=SqlAlchemyRawApiPayloadRepository(session),
                dictionary_store=SqlAlchemyDictionaryStore(
                    area_repository=SqlAlchemyAreaRepository(session),
                    professional_role_repository=SqlAlchemyProfessionalRoleRepository(session),
                ),
            )

            crawl_run_repository = SqlAlchemyCrawlRunRepository(session)
            crawl_partition_repository = SqlAlchemyCrawlPartitionRepository(session)
            area_repository = SqlAlchemyAreaRepository(session)
            crawl_run = create_crawl_run(
                CreateCrawlRunCommand(
                    run_type="weekly_sweep",
                    triggered_by=TEST_TRIGGERED_BY,
                ),
                crawl_run_repository,
            )
            created_run_id = crawl_run.id

            plan_result = plan_sweep_v2(
                PlanRunV2Command(crawl_run_id=crawl_run.id),
                crawl_run_repository=crawl_run_repository,
                crawl_partition_repository=crawl_partition_repository,
                area_repository=area_repository,
            )
            assert len(plan_result.created_partitions) == 2

            root_partition = next(
                partition
                for partition in plan_result.partitions
                if partition.partition_key == f"area:{TEST_AREA_IDS[0]}"
            )
            root_partition_id = root_partition.id

            first_split_result = split_partition(
                SplitPartitionCommand(partition_id=root_partition.id),
                crawl_partition_repository=crawl_partition_repository,
                crawl_run_repository=crawl_run_repository,
                area_repository=area_repository,
            )
            second_split_result = split_partition(
                SplitPartitionCommand(partition_id=root_partition.id),
                crawl_partition_repository=crawl_partition_repository,
                crawl_run_repository=crawl_run_repository,
                area_repository=area_repository,
            )

        assert created_run_id is not None
        assert root_partition_id is not None
        assert first_split_result.parent_partition.status == "split_done"
        assert len(first_split_result.created_children) == 2
        assert len(second_split_result.created_children) == 0

        with session_scope(session_factory) as session:
            crawl_run_repository = SqlAlchemyCrawlRunRepository(session)
            crawl_partition_repository = SqlAlchemyCrawlPartitionRepository(session)
            stored_run = crawl_run_repository.get(created_run_id)
            stored_partitions = crawl_partition_repository.list_by_run_id(created_run_id)
            stored_children = crawl_partition_repository.list_children(root_partition_id)

        assert stored_run is not None
        assert stored_run.partitions_total == 4
        assert len(stored_partitions) == 4
        assert len({partition.scope_key for partition in stored_partitions}) == 4
        assert len(stored_children) == 2

        root_partitions = [
            partition for partition in stored_partitions if partition.parent_partition_id is None
        ]
        assert {partition.partition_key for partition in root_partitions} == {
            f"area:{TEST_AREA_IDS[0]}",
            f"area:{TEST_AREA_IDS[1]}",
        }
        assert all(partition.depth == 0 for partition in root_partitions)
        assert all(partition.planner_policy_version == "v2" for partition in root_partitions)

        stored_root_partition = next(
            partition for partition in stored_partitions if partition.id == root_partition_id
        )
        assert stored_root_partition.status == "split_done"
        assert stored_root_partition.is_terminal is False
        assert stored_root_partition.is_saturated is True
        assert stored_root_partition.coverage_status == "split"

        assert {child.partition_key for child in stored_children} == {
            f"area:{TEST_AREA_IDS[2]}",
            f"area:{TEST_AREA_IDS[3]}",
        }
        assert all(child.parent_partition_id == root_partition_id for child in stored_children)
        assert all(child.depth == 1 for child in stored_children)
        assert all(child.status == "pending" for child in stored_children)
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
                    WHERE etag = :etag
                    """
                ),
                {"etag": TEST_AREAS_ETAG},
            )
            connection.execute(
                text(
                    """
                    DELETE FROM area
                    WHERE hh_area_id IN :hh_area_ids
                    """
                ).bindparams(sa.bindparam("hh_area_ids", expanding=True)),
                {"hh_area_ids": TEST_AREA_IDS},
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
        engine.dispose()
