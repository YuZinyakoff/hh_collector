from __future__ import annotations

from uuid import UUID

import pytest
from sqlalchemy import text
from sqlalchemy.exc import OperationalError

from hhru_platform.application.commands.create_crawl_run import (
    CreateCrawlRunCommand,
    create_crawl_run,
)
from hhru_platform.application.commands.plan_sweep import PlanRunCommand, plan_sweep
from hhru_platform.application.policies.planner import SinglePartitionPlannerPolicyV1
from hhru_platform.infrastructure.db.repositories.crawl_partition_repo import (
    SqlAlchemyCrawlPartitionRepository,
)
from hhru_platform.infrastructure.db.repositories.crawl_run_repo import (
    SqlAlchemyCrawlRunRepository,
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


def test_plan_sweep_persists_partitions_for_existing_run() -> None:
    engine = create_engine_from_settings()
    session_factory = create_session_factory(engine)
    triggered_by = "pytest-plan-run"
    created_run_id: UUID | None = None

    try:
        with session_scope(session_factory) as session:
            crawl_run_repository = SqlAlchemyCrawlRunRepository(session)
            crawl_partition_repository = SqlAlchemyCrawlPartitionRepository(session)
            crawl_run = create_crawl_run(
                CreateCrawlRunCommand(
                    run_type="weekly_sweep",
                    triggered_by=triggered_by,
                ),
                crawl_run_repository,
            )
            created_run_id = crawl_run.id

            result = plan_sweep(
                PlanRunCommand(crawl_run_id=created_run_id),
                crawl_run_repository,
                crawl_partition_repository,
                SinglePartitionPlannerPolicyV1(),
            )

        assert created_run_id is not None
        assert len(result.created_partitions) == 1

        with session_scope(session_factory) as session:
            crawl_run_repository = SqlAlchemyCrawlRunRepository(session)
            crawl_partition_repository = SqlAlchemyCrawlPartitionRepository(session)
            stored_run = crawl_run_repository.get(created_run_id)
            stored_partitions = crawl_partition_repository.list_by_run_id(created_run_id)

        assert stored_run is not None
        assert stored_run.partitions_total == 1
        assert len(stored_partitions) == 1
        assert stored_partitions[0].partition_key == "global-default"
        assert stored_partitions[0].status == "pending"
        assert stored_partitions[0].params_json["planner_policy"] == "single_partition_v1"
    finally:
        with engine.begin() as connection:
            connection.execute(
                text("DELETE FROM crawl_run WHERE triggered_by = :triggered_by"),
                {"triggered_by": triggered_by},
            )
        engine.dispose()
