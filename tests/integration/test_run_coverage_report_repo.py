from __future__ import annotations

import pytest
from sqlalchemy import text
from sqlalchemy.exc import OperationalError

from hhru_platform.application.commands.create_crawl_run import (
    CreateCrawlRunCommand,
    create_crawl_run,
)
from hhru_platform.application.commands.report_run_coverage import (
    ReportRunCoverageCommand,
    report_run_coverage,
)
from hhru_platform.infrastructure.db.repositories import (
    SqlAlchemyCrawlPartitionRepository,
    SqlAlchemyCrawlRunRepository,
)
from hhru_platform.infrastructure.db.session import (
    create_engine_from_settings,
    create_session_factory,
    session_scope,
)

TEST_TRIGGERED_BY = "pytest-run-coverage-report"


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


def test_report_run_coverage_builds_summary_and_tree_from_partition_rows() -> None:
    engine = create_engine_from_settings()
    session_factory = create_session_factory(engine)

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

            split_root = crawl_partition_repository.add(
                crawl_run_id=crawl_run.id,
                partition_key="area:113",
                status="split_done",
                params_json={"params": {"area": "113"}},
                scope_key="area:113",
                planner_policy_version="v2",
                is_terminal=False,
                is_saturated=True,
                coverage_status="split",
            )
            crawl_partition_repository.add(
                crawl_run_id=crawl_run.id,
                partition_key="area:1",
                status="done",
                params_json={"params": {"area": "1"}},
                parent_partition_id=split_root.id,
                depth=1,
                scope_key="area:1",
                planner_policy_version="v2",
                is_terminal=True,
                is_saturated=False,
                coverage_status="covered",
            )
            crawl_partition_repository.add(
                crawl_run_id=crawl_run.id,
                partition_key="area:2",
                status="pending",
                params_json={"params": {"area": "2"}},
                parent_partition_id=split_root.id,
                depth=1,
                scope_key="area:2",
                planner_policy_version="v2",
                is_terminal=True,
                is_saturated=False,
                coverage_status="unassessed",
            )
            crawl_partition_repository.add(
                crawl_run_id=crawl_run.id,
                partition_key="area:120",
                status="unresolved",
                params_json={"params": {"area": "120"}},
                scope_key="area:120",
                planner_policy_version="v2",
                is_terminal=True,
                is_saturated=True,
                coverage_status="unresolved",
            )
            crawl_partition_repository.add(
                crawl_run_id=crawl_run.id,
                partition_key="area:130",
                status="failed",
                params_json={"params": {"area": "130"}},
                scope_key="area:130",
                planner_policy_version="v2",
                is_terminal=True,
                is_saturated=False,
                coverage_status="unassessed",
            )
            report = report_run_coverage(
                ReportRunCoverageCommand(crawl_run_id=crawl_run.id),
                crawl_run_repository=crawl_run_repository,
                crawl_partition_repository=crawl_partition_repository,
            )

        assert report.summary.total_partitions == 5
        assert report.summary.root_partitions == 3
        assert report.summary.terminal_partitions == 4
        assert report.summary.covered_terminal_partitions == 1
        assert report.summary.pending_partitions == 1
        assert report.summary.pending_terminal_partitions == 1
        assert report.summary.running_partitions == 0
        assert report.summary.split_partitions == 1
        assert report.summary.unresolved_partitions == 1
        assert report.summary.failed_partitions == 1
        assert report.summary.coverage_ratio == 0.25
        assert [row.scope_key for row in report.tree_rows] == [
            "area:113",
            "area:1",
            "area:2",
            "area:120",
            "area:130",
        ]
    finally:
        with engine.begin() as connection:
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
                {"triggered_by": TEST_TRIGGERED_BY},
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
