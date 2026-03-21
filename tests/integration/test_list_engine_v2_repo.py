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
from hhru_platform.application.commands.process_list_page import process_list_page
from hhru_platform.application.commands.process_partition_v2 import (
    ProcessPartitionV2Command,
    process_partition_v2,
)
from hhru_platform.application.commands.run_list_engine_v2 import (
    RunListEngineV2Command,
    run_list_engine_v2,
)
from hhru_platform.application.commands.split_partition import split_partition
from hhru_platform.application.dto import VacancySearchResponse
from hhru_platform.application.policies.list_engine import PartitionSaturationPolicyV1
from hhru_platform.infrastructure.db.repositories import (
    SqlAlchemyApiRequestLogRepository,
    SqlAlchemyAreaRepository,
    SqlAlchemyCrawlPartitionRepository,
    SqlAlchemyCrawlRunRepository,
    SqlAlchemyRawApiPayloadRepository,
    SqlAlchemyVacancyCurrentStateRepository,
    SqlAlchemyVacancyRepository,
    SqlAlchemyVacancySeenEventRepository,
    SqlAlchemyVacancySnapshotRepository,
)
from hhru_platform.infrastructure.db.session import (
    create_engine_from_settings,
    create_session_factory,
    session_scope,
)

TEST_MULTI_TRIGGERED_BY = "pytest-list-engine-v2-multipage"
TEST_MULTI_USER_AGENT = "pytest-list-engine-v2-multipage"
TEST_MULTI_AREA_ID = "pytest-list-engine-v2-area"
TEST_MULTI_VACANCY_IDS = (
    "pytest-list-engine-v2-vacancy-1",
    "pytest-list-engine-v2-vacancy-2",
    "pytest-list-engine-v2-vacancy-3",
)

TEST_TREE_TRIGGERED_BY = "pytest-list-engine-v2-tree"
TEST_TREE_USER_AGENT = "pytest-list-engine-v2-tree"
TEST_TREE_ROOT_AREA_ID = "pytest-list-engine-v2-root"
TEST_TREE_CHILD_AREA_IDS = (
    "pytest-list-engine-v2-child-1",
    "pytest-list-engine-v2-child-2",
)
TEST_TREE_VACANCY_IDS = (
    "pytest-list-engine-v2-root-vacancy",
    "pytest-list-engine-v2-child-1-vacancy",
    "pytest-list-engine-v2-child-2-vacancy",
)


class MultiPageVacancySearchApiClient:
    def search_vacancies(self, params_json: dict[str, object]) -> VacancySearchResponse:
        assert params_json["area"] == TEST_MULTI_AREA_ID
        assert params_json["per_page"] == 2
        page = int(params_json["page"])
        payload = {
            0: {
                "items": [
                    _build_search_item(
                        hh_vacancy_id=TEST_MULTI_VACANCY_IDS[0],
                        area_hh_id=TEST_MULTI_AREA_ID,
                        name="Pytest Multi 1",
                    ),
                    _build_search_item(
                        hh_vacancy_id=TEST_MULTI_VACANCY_IDS[1],
                        area_hh_id=TEST_MULTI_AREA_ID,
                        name="Pytest Multi 2",
                    ),
                ],
                "found": 3,
                "page": 0,
                "pages": 2,
                "per_page": 2,
            },
            1: {
                "items": [
                    _build_search_item(
                        hh_vacancy_id=TEST_MULTI_VACANCY_IDS[2],
                        area_hh_id=TEST_MULTI_AREA_ID,
                        name="Pytest Multi 3",
                    )
                ],
                "found": 3,
                "page": 1,
                "pages": 2,
                "per_page": 2,
            },
        }[page]
        return _build_search_response(
            params_json=params_json,
            payload_json=payload,
            user_agent=TEST_MULTI_USER_AGENT,
            request_id=f"pytest-list-engine-v2-multipage-{page}",
        )


class TreeVacancySearchApiClient:
    def search_vacancies(self, params_json: dict[str, object]) -> VacancySearchResponse:
        page = int(params_json["page"])
        assert page == 0
        area_hh_id = str(params_json["area"])

        if area_hh_id == TEST_TREE_ROOT_AREA_ID:
            payload = {
                "items": [
                    _build_search_item(
                        hh_vacancy_id=TEST_TREE_VACANCY_IDS[0],
                        area_hh_id=TEST_TREE_ROOT_AREA_ID,
                        name="Pytest Tree Root",
                    )
                ],
                "found": 2000,
                "page": 0,
                "pages": 100,
                "per_page": int(params_json["per_page"]),
            }
        elif area_hh_id == TEST_TREE_CHILD_AREA_IDS[0]:
            payload = {
                "items": [
                    _build_search_item(
                        hh_vacancy_id=TEST_TREE_VACANCY_IDS[1],
                        area_hh_id=TEST_TREE_CHILD_AREA_IDS[0],
                        name="Pytest Tree Child 1",
                    )
                ],
                "found": 1,
                "page": 0,
                "pages": 1,
                "per_page": int(params_json["per_page"]),
            }
        elif area_hh_id == TEST_TREE_CHILD_AREA_IDS[1]:
            payload = {
                "items": [
                    _build_search_item(
                        hh_vacancy_id=TEST_TREE_VACANCY_IDS[2],
                        area_hh_id=TEST_TREE_CHILD_AREA_IDS[1],
                        name="Pytest Tree Child 2",
                    )
                ],
                "found": 1,
                "page": 0,
                "pages": 1,
                "per_page": int(params_json["per_page"]),
            }
        else:
            raise AssertionError(f"unexpected area={area_hh_id}")

        return _build_search_response(
            params_json=params_json,
            payload_json=payload,
            user_agent=TEST_TREE_USER_AGENT,
            request_id=f"pytest-list-engine-v2-tree-{area_hh_id}",
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


def test_process_partition_v2_reads_all_pages_and_marks_leaf_covered() -> None:
    engine = create_engine_from_settings()
    session_factory = create_session_factory(engine)
    created_run_id: UUID | None = None
    created_partition_id: UUID | None = None

    try:
        with session_scope(session_factory) as session:
            _insert_root_area(
                session,
                hh_area_id=TEST_MULTI_AREA_ID,
                name="Pytest Multi Root",
                path_text="Pytest Multi Root",
            )

            crawl_run_repository = SqlAlchemyCrawlRunRepository(session)
            crawl_partition_repository = SqlAlchemyCrawlPartitionRepository(session)
            crawl_run = create_crawl_run(
                CreateCrawlRunCommand(
                    run_type="weekly_sweep",
                    triggered_by=TEST_MULTI_TRIGGERED_BY,
                ),
                crawl_run_repository,
            )
            created_run_id = crawl_run.id
            crawl_partition = _add_area_partition(
                crawl_partition_repository,
                crawl_run_id=crawl_run.id,
                hh_area_id=TEST_MULTI_AREA_ID,
                area_name="Pytest Multi Root",
            )
            created_partition_id = crawl_partition.id

            api_request_log_repository = SqlAlchemyApiRequestLogRepository(session)
            raw_api_payload_repository = SqlAlchemyRawApiPayloadRepository(session)
            vacancy_repository = SqlAlchemyVacancyRepository(session)
            vacancy_seen_event_repository = SqlAlchemyVacancySeenEventRepository(session)
            vacancy_current_state_repository = SqlAlchemyVacancyCurrentStateRepository(session)
            vacancy_snapshot_repository = SqlAlchemyVacancySnapshotRepository(session)
            api_client = MultiPageVacancySearchApiClient()

            result = process_partition_v2(
                ProcessPartitionV2Command(partition_id=crawl_partition.id),
                crawl_partition_repository=crawl_partition_repository,
                process_list_page_step=lambda step_command: process_list_page(
                    step_command,
                    crawl_partition_repository=crawl_partition_repository,
                    api_client=api_client,
                    api_request_log_repository=api_request_log_repository,
                    raw_api_payload_repository=raw_api_payload_repository,
                    vacancy_repository=vacancy_repository,
                    vacancy_seen_event_repository=vacancy_seen_event_repository,
                    vacancy_current_state_repository=vacancy_current_state_repository,
                    vacancy_snapshot_repository=vacancy_snapshot_repository,
                ),
                split_partition_step=lambda step_command: split_partition(
                    step_command,
                    crawl_partition_repository=crawl_partition_repository,
                    crawl_run_repository=crawl_run_repository,
                    area_repository=SqlAlchemyAreaRepository(session),
                ),
                saturation_policy=PartitionSaturationPolicyV1(pages_threshold=100),
            )

        assert result.status == "succeeded"
        assert result.final_partition_status == "done"
        assert result.final_coverage_status == "covered"
        assert result.saturated is False
        assert result.pages_attempted == 2
        assert result.pages_processed == 2
        assert result.vacancies_found == 3
        assert result.vacancies_created == 3
        assert result.seen_events_created == 3

        assert created_run_id is not None
        assert created_partition_id is not None
        with engine.connect() as connection:
            partition_row = (
                connection.execute(
                    text(
                        """
                        SELECT status,
                               coverage_status,
                               pages_total_expected,
                               pages_processed,
                               items_seen,
                               is_terminal,
                               is_saturated
                        FROM crawl_partition
                        WHERE id = :partition_id
                        """
                    ),
                    {"partition_id": created_partition_id},
                )
                .mappings()
                .one()
            )
            request_logs_count = int(
                connection.scalar(
                    text(
                        """
                        SELECT COUNT(*)
                        FROM api_request_log
                        WHERE request_headers_json ->> 'User-Agent' = :user_agent
                        """
                    ),
                    {"user_agent": TEST_MULTI_USER_AGENT},
                )
                or 0
            )
            raw_payloads_count = int(
                connection.scalar(
                    text(
                        """
                        SELECT COUNT(*)
                        FROM raw_api_payload
                        WHERE api_request_log_id IN (
                            SELECT id
                            FROM api_request_log
                            WHERE request_headers_json ->> 'User-Agent' = :user_agent
                        )
                        """
                    ),
                    {"user_agent": TEST_MULTI_USER_AGENT},
                )
                or 0
            )
            vacancy_count = int(
                connection.scalar(
                    text(
                        """
                        SELECT COUNT(*)
                        FROM vacancy
                        WHERE hh_vacancy_id IN :vacancy_ids
                        """
                    ).bindparams(sa.bindparam("vacancy_ids", expanding=True)),
                    {"vacancy_ids": TEST_MULTI_VACANCY_IDS},
                )
                or 0
            )
            seen_event_count = int(
                connection.scalar(
                    text(
                        """
                        SELECT COUNT(*)
                        FROM vacancy_seen_event
                        WHERE crawl_run_id = :crawl_run_id
                        """
                    ),
                    {"crawl_run_id": created_run_id},
                )
                or 0
            )

        assert partition_row["status"] == "done"
        assert partition_row["coverage_status"] == "covered"
        assert partition_row["pages_total_expected"] == 2
        assert partition_row["pages_processed"] == 2
        assert partition_row["items_seen"] == 3
        assert partition_row["is_terminal"] is True
        assert partition_row["is_saturated"] is False
        assert request_logs_count == 2
        assert raw_payloads_count == 2
        assert vacancy_count == 3
        assert seen_event_count == 3
    finally:
        _cleanup_test_rows(
            engine,
            user_agent=TEST_MULTI_USER_AGENT,
            triggered_by=TEST_MULTI_TRIGGERED_BY,
            area_ids=(TEST_MULTI_AREA_ID,),
            vacancy_ids=TEST_MULTI_VACANCY_IDS,
        )
        engine.dispose()


def test_run_list_engine_v2_splits_saturated_parent_and_covers_children() -> None:
    engine = create_engine_from_settings()
    session_factory = create_session_factory(engine)
    created_run_id: UUID | None = None

    try:
        with session_scope(session_factory) as session:
            root_area_id = _insert_root_area(
                session,
                hh_area_id=TEST_TREE_ROOT_AREA_ID,
                name="Pytest Tree Root",
                path_text="Pytest Tree Root",
            )
            _insert_child_area(
                session,
                hh_area_id=TEST_TREE_CHILD_AREA_IDS[0],
                name="Pytest Tree Child 1",
                path_text="Pytest Tree Root / Pytest Tree Child 1",
                parent_area_id=root_area_id,
            )
            _insert_child_area(
                session,
                hh_area_id=TEST_TREE_CHILD_AREA_IDS[1],
                name="Pytest Tree Child 2",
                path_text="Pytest Tree Root / Pytest Tree Child 2",
                parent_area_id=root_area_id,
            )

            crawl_run_repository = SqlAlchemyCrawlRunRepository(session)
            crawl_partition_repository = SqlAlchemyCrawlPartitionRepository(session)
            area_repository = SqlAlchemyAreaRepository(session)
            crawl_run = create_crawl_run(
                CreateCrawlRunCommand(
                    run_type="weekly_sweep",
                    triggered_by=TEST_TREE_TRIGGERED_BY,
                ),
                crawl_run_repository,
            )
            created_run_id = crawl_run.id
            root_partition = _add_area_partition(
                crawl_partition_repository,
                crawl_run_id=crawl_run.id,
                hh_area_id=TEST_TREE_ROOT_AREA_ID,
                area_name="Pytest Tree Root",
            )

            api_request_log_repository = SqlAlchemyApiRequestLogRepository(session)
            raw_api_payload_repository = SqlAlchemyRawApiPayloadRepository(session)
            vacancy_repository = SqlAlchemyVacancyRepository(session)
            vacancy_seen_event_repository = SqlAlchemyVacancySeenEventRepository(session)
            vacancy_current_state_repository = SqlAlchemyVacancyCurrentStateRepository(session)
            vacancy_snapshot_repository = SqlAlchemyVacancySnapshotRepository(session)
            api_client = TreeVacancySearchApiClient()

            result = run_list_engine_v2(
                RunListEngineV2Command(crawl_run_id=crawl_run.id),
                crawl_run_repository=crawl_run_repository,
                crawl_partition_repository=crawl_partition_repository,
                process_partition_v2_step=lambda step_command: process_partition_v2(
                    step_command,
                    crawl_partition_repository=crawl_partition_repository,
                    process_list_page_step=lambda page_command: process_list_page(
                        page_command,
                        crawl_partition_repository=crawl_partition_repository,
                        api_client=api_client,
                        api_request_log_repository=api_request_log_repository,
                        raw_api_payload_repository=raw_api_payload_repository,
                        vacancy_repository=vacancy_repository,
                        vacancy_seen_event_repository=vacancy_seen_event_repository,
                        vacancy_current_state_repository=vacancy_current_state_repository,
                        vacancy_snapshot_repository=vacancy_snapshot_repository,
                    ),
                    split_partition_step=lambda split_command: split_partition(
                        split_command,
                        crawl_partition_repository=crawl_partition_repository,
                        crawl_run_repository=crawl_run_repository,
                        area_repository=area_repository,
                    ),
                    saturation_policy=PartitionSaturationPolicyV1(pages_threshold=100),
                ),
            )

        assert result.status == "succeeded"
        assert result.partitions_attempted == 3
        assert result.partitions_completed == 3
        assert result.partitions_failed == 0
        assert result.pages_attempted == 3
        assert result.pages_processed == 3
        assert result.vacancies_found == 3
        assert result.vacancies_created == 3
        assert result.seen_events_created == 3
        assert result.saturated_partitions == 1
        assert result.children_created_total == 2
        assert result.remaining_pending_terminal_count == 0
        assert result.partition_results[0].partition_id == root_partition.id
        assert result.partition_results[0].final_partition_status == "split_done"
        assert result.partition_results[0].final_coverage_status == "split"

        assert created_run_id is not None
        with session_scope(session_factory) as session:
            crawl_run_repository = SqlAlchemyCrawlRunRepository(session)
            crawl_partition_repository = SqlAlchemyCrawlPartitionRepository(session)
            rerun_result = run_list_engine_v2(
                RunListEngineV2Command(crawl_run_id=created_run_id),
                crawl_run_repository=crawl_run_repository,
                crawl_partition_repository=crawl_partition_repository,
                process_partition_v2_step=lambda _: (_ for _ in ()).throw(
                    AssertionError("unexpected partition execution on fully covered tree")
                ),
            )

        assert rerun_result.status == "succeeded"
        assert rerun_result.partitions_attempted == 0
        assert rerun_result.remaining_pending_terminal_count == 0

        with engine.connect() as connection:
            partition_rows = (
                connection.execute(
                    text(
                        """
                        SELECT partition_key,
                               parent_partition_id,
                               status,
                               coverage_status,
                               pages_total_expected,
                               pages_processed,
                               is_terminal,
                               is_saturated
                        FROM crawl_partition
                        WHERE crawl_run_id = :crawl_run_id
                        ORDER BY depth, partition_key
                        """
                    ),
                    {"crawl_run_id": created_run_id},
                )
                .mappings()
                .all()
            )
            stored_run = (
                connection.execute(
                    text(
                        """
                        SELECT partitions_total
                        FROM crawl_run
                        WHERE id = :crawl_run_id
                        """
                    ),
                    {"crawl_run_id": created_run_id},
                )
                .mappings()
                .one()
            )

        assert stored_run["partitions_total"] == 3
        assert len(partition_rows) == 3

        root_row = partition_rows[0]
        child_rows = partition_rows[1:]
        assert root_row["partition_key"] == f"area:{TEST_TREE_ROOT_AREA_ID}"
        assert root_row["parent_partition_id"] is None
        assert root_row["status"] == "split_done"
        assert root_row["coverage_status"] == "split"
        assert root_row["pages_total_expected"] == 100
        assert root_row["pages_processed"] == 1
        assert root_row["is_terminal"] is False
        assert root_row["is_saturated"] is True

        assert {row["partition_key"] for row in child_rows} == {
            f"area:{TEST_TREE_CHILD_AREA_IDS[0]}",
            f"area:{TEST_TREE_CHILD_AREA_IDS[1]}",
        }
        assert all(row["parent_partition_id"] == root_partition.id for row in child_rows)
        assert all(row["status"] == "done" for row in child_rows)
        assert all(row["coverage_status"] == "covered" for row in child_rows)
        assert all(row["pages_total_expected"] == 1 for row in child_rows)
        assert all(row["pages_processed"] == 1 for row in child_rows)
        assert all(row["is_terminal"] is True for row in child_rows)
        assert all(row["is_saturated"] is False for row in child_rows)
    finally:
        _cleanup_test_rows(
            engine,
            user_agent=TEST_TREE_USER_AGENT,
            triggered_by=TEST_TREE_TRIGGERED_BY,
            area_ids=(TEST_TREE_ROOT_AREA_ID, *TEST_TREE_CHILD_AREA_IDS),
            vacancy_ids=TEST_TREE_VACANCY_IDS,
        )
        engine.dispose()


def _add_area_partition(
    crawl_partition_repository: SqlAlchemyCrawlPartitionRepository,
    *,
    crawl_run_id: UUID,
    hh_area_id: str,
    area_name: str,
    parent_partition_id: UUID | None = None,
    depth: int = 0,
):
    return crawl_partition_repository.add(
        crawl_run_id=crawl_run_id,
        partition_key=f"area:{hh_area_id}",
        status="pending",
        params_json={
            "planner_policy": "area_exhaustive_v2",
            "planner_policy_version": "v2",
            "scope": {
                "dimension": "area",
                "scope_key": f"area:{hh_area_id}",
                "hh_area_id": hh_area_id,
                "area_name": area_name,
                "path_text": area_name,
                "depth": depth,
            },
            "params": {
                "area": hh_area_id,
                "page": 0,
                "per_page": 2,
            },
            "run_type": "weekly_sweep",
        },
        parent_partition_id=parent_partition_id,
        depth=depth,
        split_dimension="area",
        split_value=hh_area_id,
        scope_key=f"area:{hh_area_id}",
        planner_policy_version="v2",
        is_terminal=True,
        is_saturated=False,
        coverage_status="unassessed",
    )


def _insert_root_area(
    session,
    *,
    hh_area_id: str,
    name: str,
    path_text: str,
) -> UUID:
    session.execute(
        text(
            """
            INSERT INTO area (hh_area_id, name, level, path_text, is_active)
            VALUES (:hh_area_id, :name, 0, :path_text, TRUE)
            ON CONFLICT (hh_area_id) DO UPDATE
            SET name = EXCLUDED.name,
                level = EXCLUDED.level,
                path_text = EXCLUDED.path_text,
                is_active = EXCLUDED.is_active
            """
        ),
        {
            "hh_area_id": hh_area_id,
            "name": name,
            "path_text": path_text,
        },
    )
    return session.scalar(
        text("SELECT id FROM area WHERE hh_area_id = :hh_area_id"),
        {"hh_area_id": hh_area_id},
    )


def _insert_child_area(
    session,
    *,
    hh_area_id: str,
    name: str,
    path_text: str,
    parent_area_id: UUID,
) -> None:
    session.execute(
        text(
            """
            INSERT INTO area (hh_area_id, name, parent_area_id, level, path_text, is_active)
            VALUES (:hh_area_id, :name, :parent_area_id, 1, :path_text, TRUE)
            ON CONFLICT (hh_area_id) DO UPDATE
            SET name = EXCLUDED.name,
                parent_area_id = EXCLUDED.parent_area_id,
                level = EXCLUDED.level,
                path_text = EXCLUDED.path_text,
                is_active = EXCLUDED.is_active
            """
        ),
        {
            "hh_area_id": hh_area_id,
            "name": name,
            "parent_area_id": parent_area_id,
            "path_text": path_text,
        },
    )


def _build_search_response(
    *,
    params_json: dict[str, object],
    payload_json: dict[str, object],
    user_agent: str,
    request_id: str,
) -> VacancySearchResponse:
    return VacancySearchResponse(
        endpoint="/vacancies",
        method="GET",
        params_json=dict(params_json),
        request_headers_json={
            "Accept": "application/json",
            "User-Agent": user_agent,
        },
        status_code=200,
        headers={"x-request-id": request_id},
        latency_ms=17,
        requested_at=datetime(2026, 3, 19, 12, 0, tzinfo=UTC),
        response_received_at=datetime(2026, 3, 19, 12, 0, 1, tzinfo=UTC),
        payload_json=payload_json,
    )


def _build_search_item(
    *,
    hh_vacancy_id: str,
    area_hh_id: str,
    name: str,
) -> dict[str, object]:
    return {
        "id": hh_vacancy_id,
        "name": name,
        "area": {"id": area_hh_id, "name": area_hh_id},
        "created_at": "2026-03-19T09:30:00+0300",
        "published_at": "2026-03-19T10:00:00+0300",
        "alternate_url": f"https://hh.ru/vacancy/{hh_vacancy_id}",
    }


def _cleanup_test_rows(
    engine,
    *,
    user_agent: str,
    triggered_by: str,
    area_ids: tuple[str, ...],
    vacancy_ids: tuple[str, ...],
) -> None:
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
            {"user_agent": user_agent},
        )
        connection.execute(
            text(
                """
                DELETE FROM api_request_log
                WHERE request_headers_json ->> 'User-Agent' = :user_agent
                """
            ),
            {"user_agent": user_agent},
        )
        connection.execute(
            text(
                """
                DELETE FROM vacancy_seen_event
                WHERE crawl_run_id IN (
                    SELECT id
                    FROM crawl_run
                    WHERE triggered_by = :triggered_by
                )
                """
            ),
            {"triggered_by": triggered_by},
        )
        connection.execute(
            text(
                """
                DELETE FROM vacancy_current_state
                WHERE vacancy_id IN (
                    SELECT id
                    FROM vacancy
                    WHERE hh_vacancy_id IN :vacancy_ids
                )
                """
            ).bindparams(sa.bindparam("vacancy_ids", expanding=True)),
            {"vacancy_ids": vacancy_ids},
        )
        connection.execute(
            text(
                """
                DELETE FROM crawl_partition
                WHERE crawl_run_id IN (
                    SELECT id
                    FROM crawl_run
                    WHERE triggered_by = :triggered_by
                )
                """
            ),
            {"triggered_by": triggered_by},
        )
        connection.execute(
            text(
                """
                DELETE FROM crawl_run
                WHERE triggered_by = :triggered_by
                """
            ),
            {"triggered_by": triggered_by},
        )
        connection.execute(
            text(
                """
                DELETE FROM vacancy
                WHERE hh_vacancy_id IN :vacancy_ids
                """
            ).bindparams(sa.bindparam("vacancy_ids", expanding=True)),
            {"vacancy_ids": vacancy_ids},
        )
        connection.execute(
            text(
                """
                DELETE FROM area
                WHERE hh_area_id IN :area_ids
                """
            ).bindparams(sa.bindparam("area_ids", expanding=True)),
            {"area_ids": area_ids},
        )
