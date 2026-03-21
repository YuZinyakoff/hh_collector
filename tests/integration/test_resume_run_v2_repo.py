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
from hhru_platform.application.commands.finalize_crawl_run import (
    FinalizeCrawlRunCommand,
    finalize_crawl_run,
)
from hhru_platform.application.commands.process_partition_v2 import (
    ProcessPartitionV2Result,
)
from hhru_platform.application.commands.reconcile_run import (
    ReconcileRunCommand,
    reconcile_run,
)
from hhru_platform.application.commands.report_run_coverage import report_run_coverage
from hhru_platform.application.commands.resume_run_v2 import (
    ResumeRunV2Command,
    resume_run_v2,
)
from hhru_platform.application.commands.run_list_engine_v2 import RunListEngineV2Result
from hhru_platform.application.policies.reconciliation import (
    MissingRunsReconciliationPolicyV1,
)
from hhru_platform.infrastructure.db.repositories import (
    SqlAlchemyCrawlPartitionRepository,
    SqlAlchemyCrawlRunRepository,
    SqlAlchemyVacancyCurrentStateRepository,
    SqlAlchemyVacancySeenEventRepository,
)
from hhru_platform.infrastructure.db.session import (
    create_engine_from_settings,
    create_session_factory,
    session_scope,
)

TEST_TRIGGERED_BY = "pytest-resume-run-v2"


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


def test_resume_run_v2_requeues_unresolved_partition_and_completes_existing_run() -> None:
    engine = create_engine_from_settings()
    session_factory = create_session_factory(engine)
    created_run_id: UUID | None = None
    created_partition_id: UUID | None = None

    try:
        with session_scope(session_factory) as session:
            crawl_run_repository = SqlAlchemyCrawlRunRepository(session)
            crawl_partition_repository = SqlAlchemyCrawlPartitionRepository(session)
            vacancy_seen_event_repository = SqlAlchemyVacancySeenEventRepository(session)
            vacancy_current_state_repository = SqlAlchemyVacancyCurrentStateRepository(session)
            crawl_run = create_crawl_run(
                CreateCrawlRunCommand(
                    run_type="weekly_sweep",
                    triggered_by=TEST_TRIGGERED_BY,
                ),
                crawl_run_repository,
            )
            created_run_id = crawl_run.id
            unresolved_partition = crawl_partition_repository.add(
                crawl_run_id=crawl_run.id,
                partition_key="area:113",
                status="unresolved",
                params_json={"params": {"area": "113"}},
                scope_key="area:113",
                planner_policy_version="v2",
                is_terminal=True,
                is_saturated=True,
                coverage_status="unresolved",
            )
            created_partition_id = unresolved_partition.id
            crawl_run_repository.complete(
                run_id=crawl_run.id,
                status="completed_with_unresolved",
                finished_at=datetime(2026, 3, 20, 10, 30, tzinfo=UTC),
                partitions_done=0,
                partitions_failed=1,
                notes="initial unresolved tree",
            )

            def run_list_engine_v2_step(command) -> RunListEngineV2Result:
                partition = crawl_partition_repository.get(unresolved_partition.id)
                assert partition is not None
                assert partition.status == "pending"
                assert partition.coverage_status == "unassessed"
                assert partition.retry_count == 1
                covered_partition = crawl_partition_repository.mark_covered(partition.id)
                return RunListEngineV2Result(
                    status="succeeded",
                    crawl_run_id=command.crawl_run_id,
                    partition_results=(
                        ProcessPartitionV2Result(
                            partition_id=covered_partition.id,
                            crawl_run_id=covered_partition.crawl_run_id,
                            final_partition_status=covered_partition.status,
                            final_coverage_status=covered_partition.coverage_status,
                            saturated=False,
                            page_results=(),
                            split_result=None,
                            saturation_reason=None,
                            error_message=None,
                        ),
                    ),
                    remaining_pending_terminal_partitions=(),
                )

            result = resume_run_v2(
                ResumeRunV2Command(
                    crawl_run_id=crawl_run.id,
                    detail_limit=0,
                    triggered_by="pytest-resume",
                ),
                crawl_run_repository=crawl_run_repository,
                crawl_partition_repository=crawl_partition_repository,
                run_list_engine_v2_step=run_list_engine_v2_step,
                report_run_coverage_step=lambda step_command: report_run_coverage(
                    step_command,
                    crawl_run_repository=crawl_run_repository,
                    crawl_partition_repository=crawl_partition_repository,
                ),
                select_detail_candidates_step=lambda step_command: (_ for _ in ()).throw(
                    AssertionError(
                        f"unexpected detail selection {step_command.crawl_run_id}"
                    )
                ),
                fetch_vacancy_detail_step=lambda step_command: (_ for _ in ()).throw(
                    AssertionError(f"unexpected detail fetch {step_command.vacancy_id}")
                ),
                reconcile_run_step=lambda step_command: reconcile_run(
                    ReconcileRunCommand(
                        crawl_run_id=step_command.crawl_run_id,
                        final_run_status=step_command.final_run_status,
                        notes=step_command.notes,
                    ),
                    crawl_run_repository=crawl_run_repository,
                    crawl_partition_repository=crawl_partition_repository,
                    vacancy_seen_event_repository=vacancy_seen_event_repository,
                    vacancy_current_state_repository=vacancy_current_state_repository,
                    reconciliation_policy=MissingRunsReconciliationPolicyV1(),
                ),
                finalize_crawl_run_step=lambda step_command: finalize_crawl_run(
                    FinalizeCrawlRunCommand(
                        crawl_run_id=step_command.crawl_run_id,
                        final_status=step_command.final_status,
                        notes=step_command.notes,
                    ),
                    crawl_run_repository=crawl_run_repository,
                    crawl_partition_repository=crawl_partition_repository,
                ),
            )

        assert result.status == "succeeded"
        assert result.initial_run_status == "completed_with_unresolved"
        assert result.unresolved_before_resume == 1
        assert result.resumed_unresolved_partitions == 1
        assert result.covered_terminal_partitions == 1
        assert result.reconciliation_status == "succeeded"

        assert created_run_id is not None
        assert created_partition_id is not None
        with engine.connect() as connection:
            run_row = (
                connection.execute(
                    text(
                        """
                        SELECT status, finished_at, notes
                        FROM crawl_run
                        WHERE id = :crawl_run_id
                        """
                    ),
                    {"crawl_run_id": created_run_id},
                )
                .mappings()
                .one()
            )
            partition_row = (
                connection.execute(
                    text(
                        """
                        SELECT status, coverage_status, retry_count
                        FROM crawl_partition
                        WHERE id = :partition_id
                        """
                    ),
                    {"partition_id": created_partition_id},
                )
                .mappings()
                .one()
            )

        assert run_row["status"] == "succeeded"
        assert run_row["finished_at"] is not None
        assert run_row["notes"] is None
        assert partition_row["status"] == "done"
        assert partition_row["coverage_status"] == "covered"
        assert partition_row["retry_count"] == 1
    finally:
        if created_run_id is not None:
            with engine.begin() as connection:
                connection.execute(
                    text("DELETE FROM crawl_run WHERE id = :crawl_run_id"),
                    {"crawl_run_id": created_run_id},
                )
        engine.dispose()
