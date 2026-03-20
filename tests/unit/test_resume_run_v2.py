from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest

from hhru_platform.application.commands.fetch_vacancy_detail import (
    FetchVacancyDetailResult,
)
from hhru_platform.application.commands.finalize_crawl_run import (
    FinalizeCrawlRunResult,
)
from hhru_platform.application.commands.process_partition_v2 import (
    ProcessPartitionV2Result,
)
from hhru_platform.application.commands.reconcile_run import ReconcileRunResult
from hhru_platform.application.commands.report_run_coverage import (
    RunCoverageReport,
    RunCoverageSummary,
)
from hhru_platform.application.commands.resume_run_v2 import (
    ResumeRunV2Command,
    ResumeRunV2NotAllowedError,
    resume_run_v2,
)
from hhru_platform.application.commands.run_list_engine_v2 import RunListEngineV2Result
from hhru_platform.domain.entities.crawl_partition import CrawlPartition
from hhru_platform.domain.entities.crawl_run import CrawlRun


class InMemoryCrawlRunRepository:
    def __init__(self, crawl_run: CrawlRun) -> None:
        self.crawl_run = crawl_run
        self.reopen_calls: list[tuple[UUID, str]] = []

    def get(self, run_id: UUID) -> CrawlRun | None:
        if self.crawl_run.id != run_id:
            return None
        return self.crawl_run

    def reopen(self, *, run_id: UUID, status: str = "created") -> CrawlRun:
        assert self.crawl_run.id == run_id
        self.reopen_calls.append((run_id, status))
        self.crawl_run.status = status
        self.crawl_run.finished_at = None
        return self.crawl_run


class RecordingCrawlPartitionRepository:
    def __init__(self, partitions: list[CrawlPartition]) -> None:
        self.partitions = partitions
        self.calls: list[UUID] = []

    def requeue_unresolved_by_run_id(self, run_id: UUID) -> list[CrawlPartition]:
        self.calls.append(run_id)
        for partition in self.partitions:
            partition.status = "pending"
            partition.coverage_status = "unassessed"
            partition.finished_at = None
            partition.last_error_message = None
            partition.retry_count += 1
        return list(self.partitions)


class RecordingResumeMetricsRecorder:
    def __init__(self) -> None:
        self.resume_attempts: list[dict[str, object]] = []
        self.backlog_updates: list[dict[str, object]] = []

    def record_resume_attempt(self, **kwargs) -> None:
        self.resume_attempts.append(kwargs)

    def set_detail_repair_backlog(self, **kwargs) -> None:
        self.backlog_updates.append(kwargs)


def test_resume_run_v2_requeues_unresolved_branches_and_reconciles_run() -> None:
    run_id = uuid4()
    partition_id = uuid4()
    events: list[tuple[object, ...]] = []
    crawl_run = _build_crawl_run(
        run_id=run_id,
        run_type="weekly_sweep",
        status="completed_with_unresolved",
    )
    crawl_run.finished_at = datetime(2026, 3, 20, 10, 30, tzinfo=UTC)
    unresolved_partition = _build_partition(
        crawl_run_id=run_id,
        partition_id=partition_id,
        partition_key="area:113",
        status="unresolved",
        coverage_status="unresolved",
    )
    coverage_reports = iter(
        (
            _build_coverage_report(
                run_id=run_id,
                total_partitions=1,
                terminal_partitions=1,
                covered_terminal_partitions=0,
                pending_partitions=0,
                pending_terminal_partitions=0,
                unresolved_partitions=1,
                run_status="completed_with_unresolved",
            ),
            _build_coverage_report(
                run_id=run_id,
                total_partitions=1,
                terminal_partitions=1,
                covered_terminal_partitions=0,
                pending_partitions=1,
                pending_terminal_partitions=1,
                run_status="created",
            ),
            _build_coverage_report(
                run_id=run_id,
                total_partitions=1,
                terminal_partitions=1,
                covered_terminal_partitions=1,
                pending_partitions=0,
                pending_terminal_partitions=0,
                coverage_ratio=1.0,
                run_status="created",
            ),
            _build_coverage_report(
                run_id=run_id,
                total_partitions=1,
                terminal_partitions=1,
                covered_terminal_partitions=1,
                pending_partitions=0,
                pending_terminal_partitions=0,
                coverage_ratio=1.0,
                run_status="succeeded",
            ),
        )
    )

    def report_run_coverage_step(command) -> RunCoverageReport:
        events.append(("coverage", command.crawl_run_id))
        return next(coverage_reports)

    def run_list_engine_v2_step(command) -> RunListEngineV2Result:
        events.append(("list_engine", command.crawl_run_id))
        return RunListEngineV2Result(
            status="succeeded",
            crawl_run_id=command.crawl_run_id,
            partition_results=(
                _build_partition_result(
                    crawl_run_id=command.crawl_run_id,
                    partition_id=partition_id,
                    final_partition_status="done",
                    final_coverage_status="covered",
                ),
            ),
            remaining_pending_terminal_partitions=(),
        )

    def reconcile_run_step(command) -> ReconcileRunResult:
        events.append(("reconcile", command.crawl_run_id, command.final_run_status))
        assert command.final_run_status == "succeeded"
        return ReconcileRunResult(
            crawl_run_id=command.crawl_run_id,
            observed_in_run_count=0,
            missing_updated_count=0,
            marked_inactive_count=0,
            run_status="succeeded",
        )

    metrics_recorder = RecordingResumeMetricsRecorder()
    result = resume_run_v2(
        ResumeRunV2Command(
            crawl_run_id=run_id,
            detail_limit=0,
            triggered_by="cli",
        ),
        crawl_run_repository=InMemoryCrawlRunRepository(crawl_run),
        crawl_partition_repository=RecordingCrawlPartitionRepository([unresolved_partition]),
        run_list_engine_v2_step=run_list_engine_v2_step,
        report_run_coverage_step=report_run_coverage_step,
        select_detail_candidates_step=lambda command: (_ for _ in ()).throw(
            AssertionError(f"unexpected detail selection {command.crawl_run_id}")
        ),
        fetch_vacancy_detail_step=lambda command: (_ for _ in ()).throw(
            AssertionError(f"unexpected detail fetch {command.vacancy_id}")
        ),
        reconcile_run_step=reconcile_run_step,
        finalize_crawl_run_step=lambda command: (_ for _ in ()).throw(
            AssertionError(f"unexpected finalize {command.crawl_run_id}")
        ),
        metrics_recorder=metrics_recorder,
    )

    assert events == [
        ("coverage", run_id),
        ("coverage", run_id),
        ("list_engine", run_id),
        ("coverage", run_id),
        ("reconcile", run_id, "succeeded"),
        ("coverage", run_id),
    ]
    assert result.status == "succeeded"
    assert result.initial_run_status == "completed_with_unresolved"
    assert result.unresolved_before_resume == 1
    assert result.resumed_unresolved_partitions == 1
    assert result.covered_terminal_partitions == 1
    assert result.coverage_ratio == 1.0
    assert result.list_stage_status == "completed"
    assert result.detail_stage_status == "skipped"
    assert result.reconciliation_status == "succeeded"
    assert result.completed_steps == (
        "requeue_unresolved_partitions",
        "reopen_crawl_run",
        "run_list_engine_v2",
        "reconcile_run",
    )
    assert result.skipped_steps == ()
    assert metrics_recorder.resume_attempts == [
        {"run_type": "weekly_sweep", "outcome": "succeeded"}
    ]
    assert metrics_recorder.backlog_updates == [
        {
            "run_id": str(run_id),
            "run_type": "weekly_sweep",
            "backlog_size": 0,
        }
    ]


def test_resume_run_v2_returns_completed_with_unresolved_when_tree_is_still_unresolved() -> None:
    run_id = uuid4()
    partition_id = uuid4()
    events: list[tuple[object, ...]] = []
    crawl_run = _build_crawl_run(
        run_id=run_id,
        run_type="weekly_sweep",
        status="completed_with_unresolved",
    )
    crawl_run.finished_at = datetime(2026, 3, 20, 10, 30, tzinfo=UTC)
    unresolved_partition = _build_partition(
        crawl_run_id=run_id,
        partition_id=partition_id,
        partition_key="area:113",
        status="unresolved",
        coverage_status="unresolved",
    )
    coverage_reports = iter(
        (
            _build_coverage_report(
                run_id=run_id,
                total_partitions=1,
                terminal_partitions=1,
                covered_terminal_partitions=0,
                pending_partitions=0,
                pending_terminal_partitions=0,
                unresolved_partitions=1,
                run_status="completed_with_unresolved",
            ),
            _build_coverage_report(
                run_id=run_id,
                total_partitions=1,
                terminal_partitions=1,
                covered_terminal_partitions=0,
                pending_partitions=1,
                pending_terminal_partitions=1,
                run_status="created",
            ),
            _build_coverage_report(
                run_id=run_id,
                total_partitions=1,
                terminal_partitions=1,
                covered_terminal_partitions=0,
                pending_partitions=0,
                pending_terminal_partitions=0,
                unresolved_partitions=1,
                run_status="created",
            ),
            _build_coverage_report(
                run_id=run_id,
                total_partitions=1,
                terminal_partitions=1,
                covered_terminal_partitions=0,
                pending_partitions=0,
                pending_terminal_partitions=0,
                unresolved_partitions=1,
                run_status="completed_with_unresolved",
            ),
        )
    )

    def report_run_coverage_step(command) -> RunCoverageReport:
        events.append(("coverage", command.crawl_run_id))
        return next(coverage_reports)

    def run_list_engine_v2_step(command) -> RunListEngineV2Result:
        events.append(("list_engine", command.crawl_run_id))
        return RunListEngineV2Result(
            status="succeeded",
            crawl_run_id=command.crawl_run_id,
            partition_results=(
                _build_partition_result(
                    crawl_run_id=command.crawl_run_id,
                    partition_id=partition_id,
                    final_partition_status="unresolved",
                    final_coverage_status="unresolved",
                    error_message="still unresolved after retry",
                ),
            ),
            remaining_pending_terminal_partitions=(),
        )

    def finalize_crawl_run_step(command) -> FinalizeCrawlRunResult:
        events.append(("finalize", command.crawl_run_id, command.final_status))
        assert command.final_status == "completed_with_unresolved"
        return FinalizeCrawlRunResult(
            crawl_run_id=command.crawl_run_id,
            run_status=command.final_status,
            partitions_done=0,
            partitions_failed=1,
        )

    metrics_recorder = RecordingResumeMetricsRecorder()
    result = resume_run_v2(
        ResumeRunV2Command(
            crawl_run_id=run_id,
            detail_limit=0,
            triggered_by="cli",
        ),
        crawl_run_repository=InMemoryCrawlRunRepository(crawl_run),
        crawl_partition_repository=RecordingCrawlPartitionRepository([unresolved_partition]),
        run_list_engine_v2_step=run_list_engine_v2_step,
        report_run_coverage_step=report_run_coverage_step,
        select_detail_candidates_step=lambda command: (_ for _ in ()).throw(
            AssertionError(f"unexpected detail selection {command.crawl_run_id}")
        ),
        fetch_vacancy_detail_step=lambda command: (_ for _ in ()).throw(
            AssertionError(f"unexpected detail fetch {command.vacancy_id}")
        ),
        reconcile_run_step=lambda command: (_ for _ in ()).throw(
            AssertionError(f"unexpected reconcile {command.crawl_run_id}")
        ),
        finalize_crawl_run_step=finalize_crawl_run_step,
        metrics_recorder=metrics_recorder,
    )

    assert events == [
        ("coverage", run_id),
        ("coverage", run_id),
        ("list_engine", run_id),
        ("coverage", run_id),
        ("finalize", run_id, "completed_with_unresolved"),
        ("coverage", run_id),
    ]
    assert result.status == "completed_with_unresolved"
    assert result.initial_run_status == "completed_with_unresolved"
    assert result.unresolved_before_resume == 1
    assert result.resumed_unresolved_partitions == 1
    assert result.reconciliation_status == "skipped"
    assert result.completed_steps == (
        "requeue_unresolved_partitions",
        "reopen_crawl_run",
        "run_list_engine_v2",
        "finalize_crawl_run",
    )
    assert result.skipped_steps == ("reconcile_run",)
    assert metrics_recorder.resume_attempts == [
        {"run_type": "weekly_sweep", "outcome": "completed_with_unresolved"}
    ]
    assert metrics_recorder.backlog_updates == []


def test_resume_run_v2_rejects_failed_runs() -> None:
    run_id = uuid4()
    crawl_run = _build_crawl_run(run_id=run_id, run_type="weekly_sweep", status="failed")

    with pytest.raises(ResumeRunV2NotAllowedError, match="status=failed"):
        resume_run_v2(
            ResumeRunV2Command(crawl_run_id=run_id),
            crawl_run_repository=InMemoryCrawlRunRepository(crawl_run),
            crawl_partition_repository=RecordingCrawlPartitionRepository([]),
            run_list_engine_v2_step=lambda command: (_ for _ in ()).throw(
                AssertionError(f"unexpected list engine {command.crawl_run_id}")
            ),
            report_run_coverage_step=lambda command: _build_coverage_report(
                run_id=command.crawl_run_id,
                total_partitions=1,
                terminal_partitions=1,
                covered_terminal_partitions=0,
                pending_partitions=0,
                pending_terminal_partitions=0,
                failed_partitions=1,
                run_status="failed",
            ),
            select_detail_candidates_step=lambda command: (_ for _ in ()).throw(
                AssertionError(f"unexpected detail selection {command.crawl_run_id}")
            ),
            fetch_vacancy_detail_step=lambda command: (_ for _ in ()).throw(
                AssertionError(f"unexpected detail fetch {command.vacancy_id}")
            ),
            reconcile_run_step=lambda command: (_ for _ in ()).throw(
                AssertionError(f"unexpected reconcile {command.crawl_run_id}")
            ),
            finalize_crawl_run_step=lambda command: (_ for _ in ()).throw(
                AssertionError(f"unexpected finalize {command.crawl_run_id}")
            ),
        )


def _build_crawl_run(*, run_id: UUID, run_type: str, status: str) -> CrawlRun:
    return CrawlRun(
        id=run_id,
        run_type=run_type,
        status=status,
        started_at=datetime(2026, 3, 20, 10, 0, tzinfo=UTC),
        finished_at=None,
        triggered_by="pytest",
        config_snapshot_json={},
        partitions_total=1,
        partitions_done=0,
        partitions_failed=0,
        notes=None,
    )


def _build_partition(
    *,
    crawl_run_id: UUID,
    partition_id: UUID,
    partition_key: str,
    status: str,
    coverage_status: str,
) -> CrawlPartition:
    return CrawlPartition(
        id=partition_id,
        crawl_run_id=crawl_run_id,
        partition_key=partition_key,
        params_json={"params": {"area": partition_key}},
        status=status,
        pages_total_expected=100,
        pages_processed=1,
        items_seen=20,
        retry_count=0,
        started_at=datetime(2026, 3, 20, 10, 5, tzinfo=UTC),
        finished_at=datetime(2026, 3, 20, 10, 10, tzinfo=UTC),
        last_error_message="unresolved before resume",
        created_at=datetime(2026, 3, 20, 10, 1, tzinfo=UTC),
        parent_partition_id=None,
        depth=0,
        split_dimension="area",
        split_value=partition_key.split(":")[-1],
        scope_key=partition_key,
        planner_policy_version="v2",
        is_terminal=True,
        is_saturated=True,
        coverage_status=coverage_status,
    )


def _build_partition_result(
    *,
    crawl_run_id: UUID,
    partition_id: UUID,
    final_partition_status: str,
    final_coverage_status: str,
    error_message: str | None = None,
) -> ProcessPartitionV2Result:
    return ProcessPartitionV2Result(
        partition_id=partition_id,
        crawl_run_id=crawl_run_id,
        final_partition_status=final_partition_status,
        final_coverage_status=final_coverage_status,
        saturated=final_partition_status == "unresolved",
        page_results=(),
        split_result=None,
        saturation_reason=None,
        error_message=error_message,
    )


def _build_coverage_report(
    *,
    run_id: UUID,
    total_partitions: int,
    terminal_partitions: int,
    covered_terminal_partitions: int,
    pending_partitions: int,
    pending_terminal_partitions: int,
    running_partitions: int = 0,
    split_partitions: int = 0,
    unresolved_partitions: int = 0,
    failed_partitions: int = 0,
    coverage_ratio: float | None = None,
    run_status: str = "created",
) -> RunCoverageReport:
    resolved_coverage_ratio = coverage_ratio
    if resolved_coverage_ratio is None:
        resolved_coverage_ratio = (
            covered_terminal_partitions / terminal_partitions
            if terminal_partitions > 0
            else 0.0
        )
    return RunCoverageReport(
        crawl_run=_build_crawl_run(
            run_id=run_id,
            run_type="weekly_sweep",
            status=run_status,
        ),
        summary=RunCoverageSummary(
            crawl_run_id=run_id,
            run_type="weekly_sweep",
            run_status=run_status,
            total_partitions=total_partitions,
            root_partitions=1,
            terminal_partitions=terminal_partitions,
            covered_terminal_partitions=covered_terminal_partitions,
            pending_partitions=pending_partitions,
            pending_terminal_partitions=pending_terminal_partitions,
            running_partitions=running_partitions,
            split_partitions=split_partitions,
            unresolved_partitions=unresolved_partitions,
            failed_partitions=failed_partitions,
            coverage_ratio=resolved_coverage_ratio,
        ),
        tree_rows=(),
    )


def _build_detail_result(
    vacancy_id: UUID,
    *,
    error_message: str | None = None,
) -> FetchVacancyDetailResult:
    detail_fetch_status = "failed" if error_message is not None else "succeeded"
    return FetchVacancyDetailResult(
        vacancy_id=vacancy_id,
        hh_vacancy_id=f"hh-{vacancy_id}",
        detail_fetch_status=detail_fetch_status,
        snapshot_id=100 if error_message is None else None,
        request_log_id=200,
        raw_payload_id=300,
        detail_fetch_attempt_id=400,
        error_message=error_message,
    )
