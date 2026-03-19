from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID, uuid4

from hhru_platform.application.commands.fetch_vacancy_detail import (
    FetchVacancyDetailResult,
)
from hhru_platform.application.commands.plan_sweep import PlanRunResult
from hhru_platform.application.commands.process_partition_v2 import (
    ProcessPartitionV2Result,
)
from hhru_platform.application.commands.reconcile_run import ReconcileRunResult
from hhru_platform.application.commands.report_run_coverage import (
    RunCoverageReport,
    RunCoverageSummary,
)
from hhru_platform.application.commands.run_collection_once_v2 import (
    RunCollectionOnceV2Command,
    run_collection_once_v2,
)
from hhru_platform.application.commands.run_list_engine_v2 import RunListEngineV2Result
from hhru_platform.application.commands.select_detail_candidates import (
    DetailFetchCandidate,
    SelectDetailCandidatesResult,
)
from hhru_platform.domain.entities.crawl_partition import CrawlPartition
from hhru_platform.domain.entities.crawl_run import CrawlRun


def test_run_collection_once_v2_sequences_tree_aware_flow_and_succeeds() -> None:
    run_id = uuid4()
    root_partition_id = uuid4()
    vacancy_one = uuid4()
    vacancy_two = uuid4()
    events: list[tuple[object, ...]] = []

    def create_crawl_run_step(command) -> CrawlRun:
        events.append(("create", command.run_type, command.triggered_by))
        return _build_crawl_run(run_id=run_id, run_type=command.run_type)

    def plan_run_v2_step(command) -> PlanRunResult:
        events.append(("plan_v2", command.crawl_run_id))
        root_partition = _build_partition(
            crawl_run_id=command.crawl_run_id,
            partition_id=root_partition_id,
            partition_key="area:113",
            status="pending",
            coverage_status="unassessed",
        )
        return PlanRunResult(
            crawl_run_id=command.crawl_run_id,
            created_partitions=[root_partition],
            partitions=[root_partition],
        )

    coverage_reports = iter(
        (
            _build_coverage_report(
                run_id=run_id,
                total_partitions=1,
                terminal_partitions=1,
                covered_terminal_partitions=0,
                pending_partitions=1,
                pending_terminal_partitions=1,
            ),
            _build_coverage_report(
                run_id=run_id,
                total_partitions=1,
                terminal_partitions=1,
                covered_terminal_partitions=1,
                pending_partitions=0,
                pending_terminal_partitions=0,
                coverage_ratio=1.0,
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
                    partition_id=root_partition_id,
                    final_partition_status="done",
                    final_coverage_status="covered",
                ),
            ),
            remaining_pending_terminal_partitions=(),
        )

    def select_detail_candidates_step(command) -> SelectDetailCandidatesResult:
        events.append(
            (
                "select_detail",
                command.crawl_run_id,
                command.limit,
                command.detail_refresh_ttl_days,
            )
        )
        return SelectDetailCandidatesResult(
            crawl_run_id=command.crawl_run_id,
            observed_vacancy_count=4,
            eligible_candidates_count=2,
            selected_candidates=(
                DetailFetchCandidate(vacancy_id=vacancy_one, reason="first_seen"),
                DetailFetchCandidate(vacancy_id=vacancy_two, reason="short_changed"),
            ),
            skipped_due_to_limit=0,
            first_seen_candidates=1,
            short_changed_candidates=1,
            ttl_refresh_candidates=0,
        )

    def fetch_vacancy_detail_step(command) -> FetchVacancyDetailResult:
        events.append(("detail", command.vacancy_id, command.reason))
        return _build_detail_result(command.vacancy_id)

    def reconcile_run_step(command) -> ReconcileRunResult:
        events.append(("reconcile", command.crawl_run_id))
        return ReconcileRunResult(
            crawl_run_id=command.crawl_run_id,
            observed_in_run_count=4,
            missing_updated_count=1,
            marked_inactive_count=0,
            run_status="completed",
        )

    result = run_collection_once_v2(
        RunCollectionOnceV2Command(
            sync_dictionaries=False,
            detail_limit=2,
            detail_refresh_ttl_days=14,
            run_type="weekly_sweep",
            triggered_by="cli",
        ),
        sync_dictionary_step=lambda command: (_ for _ in ()).throw(
            AssertionError(f"unexpected dictionary sync {command.dictionary_name}")
        ),
        create_crawl_run_step=create_crawl_run_step,
        plan_run_v2_step=plan_run_v2_step,
        run_list_engine_v2_step=run_list_engine_v2_step,
        report_run_coverage_step=report_run_coverage_step,
        select_detail_candidates_step=select_detail_candidates_step,
        fetch_vacancy_detail_step=fetch_vacancy_detail_step,
        reconcile_run_step=reconcile_run_step,
    )

    assert events == [
        ("create", "weekly_sweep", "cli"),
        ("plan_v2", run_id),
        ("coverage", run_id),
        ("list_engine", run_id),
        ("coverage", run_id),
        ("select_detail", run_id, 2, 14),
        ("detail", vacancy_one, "first_seen"),
        ("detail", vacancy_two, "short_changed"),
        ("reconcile", run_id),
    ]
    assert result.status == "succeeded"
    assert result.run_id == run_id
    assert result.list_stage_status == "completed"
    assert result.detail_stage_status == "completed"
    assert result.reconciliation_status == "completed"
    assert result.total_partitions == 1
    assert result.covered_terminal_partitions == 1
    assert result.pending_terminal_partitions == 0
    assert result.unresolved_partitions == 0
    assert result.failed_partitions == 0
    assert result.coverage_ratio == 1.0
    assert result.detail_candidates_selected == 2
    assert result.detail_fetch_attempted == 2
    assert result.detail_fetch_failed == 0
    assert result.completed_steps == (
        "create_crawl_run",
        "plan_sweep_v2",
        "run_list_engine_v2",
        "fetch_vacancy_detail",
        "reconcile_run",
    )
    assert result.skipped_steps == ()


def test_run_collection_once_v2_returns_completed_with_unresolved() -> None:
    run_id = uuid4()
    root_partition_id = uuid4()
    events: list[tuple[object, ...]] = []

    def create_crawl_run_step(command) -> CrawlRun:
        events.append(("create", command.run_type, command.triggered_by))
        return _build_crawl_run(run_id=run_id, run_type=command.run_type)

    def plan_run_v2_step(command) -> PlanRunResult:
        events.append(("plan_v2", command.crawl_run_id))
        root_partition = _build_partition(
            crawl_run_id=command.crawl_run_id,
            partition_id=root_partition_id,
            partition_key="area:113",
            status="pending",
            coverage_status="unassessed",
        )
        return PlanRunResult(
            crawl_run_id=command.crawl_run_id,
            created_partitions=[root_partition],
            partitions=[root_partition],
        )

    coverage_reports = iter(
        (
            _build_coverage_report(
                run_id=run_id,
                total_partitions=1,
                terminal_partitions=1,
                covered_terminal_partitions=0,
                pending_partitions=1,
                pending_terminal_partitions=1,
            ),
            _build_coverage_report(
                run_id=run_id,
                total_partitions=1,
                terminal_partitions=1,
                covered_terminal_partitions=0,
                pending_partitions=0,
                pending_terminal_partitions=0,
                unresolved_partitions=1,
                coverage_ratio=0.0,
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
                    partition_id=root_partition_id,
                    final_partition_status="unresolved",
                    final_coverage_status="unresolved",
                    saturated=True,
                    error_message="root area cannot be split further",
                ),
            ),
            remaining_pending_terminal_partitions=(),
        )

    result = run_collection_once_v2(
        RunCollectionOnceV2Command(
            detail_limit=5,
            run_type="weekly_sweep",
            triggered_by="cli",
        ),
        sync_dictionary_step=lambda command: (_ for _ in ()).throw(
            AssertionError(f"unexpected dictionary sync {command.dictionary_name}")
        ),
        create_crawl_run_step=create_crawl_run_step,
        plan_run_v2_step=plan_run_v2_step,
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
    )

    assert events == [
        ("create", "weekly_sweep", "cli"),
        ("plan_v2", run_id),
        ("coverage", run_id),
        ("list_engine", run_id),
        ("coverage", run_id),
    ]
    assert result.status == "completed_with_unresolved"
    assert result.run_id == run_id
    assert result.list_stage_status == "completed_with_unresolved"
    assert result.detail_stage_status == "skipped"
    assert result.reconciliation_status == "skipped"
    assert result.unresolved_partitions == 1
    assert result.coverage_ratio == 0.0
    assert result.failed_step is None
    assert "unresolved partition" in (result.error_message or "")
    assert result.completed_steps == (
        "create_crawl_run",
        "plan_sweep_v2",
        "run_list_engine_v2",
    )
    assert result.skipped_steps == ("fetch_vacancy_detail", "reconcile_run")


def test_run_collection_once_v2_fails_when_tree_coverage_has_failed_partitions() -> None:
    run_id = uuid4()
    root_partition_id = uuid4()
    events: list[tuple[object, ...]] = []

    def create_crawl_run_step(command) -> CrawlRun:
        events.append(("create", command.run_type, command.triggered_by))
        return _build_crawl_run(run_id=run_id, run_type=command.run_type)

    def plan_run_v2_step(command) -> PlanRunResult:
        events.append(("plan_v2", command.crawl_run_id))
        root_partition = _build_partition(
            crawl_run_id=command.crawl_run_id,
            partition_id=root_partition_id,
            partition_key="area:113",
            status="pending",
            coverage_status="unassessed",
        )
        return PlanRunResult(
            crawl_run_id=command.crawl_run_id,
            created_partitions=[root_partition],
            partitions=[root_partition],
        )

    coverage_reports = iter(
        (
            _build_coverage_report(
                run_id=run_id,
                total_partitions=1,
                terminal_partitions=1,
                covered_terminal_partitions=0,
                pending_partitions=1,
                pending_terminal_partitions=1,
            ),
            _build_coverage_report(
                run_id=run_id,
                total_partitions=1,
                terminal_partitions=1,
                covered_terminal_partitions=0,
                pending_partitions=0,
                pending_terminal_partitions=0,
                failed_partitions=1,
                coverage_ratio=0.0,
            ),
        )
    )

    def report_run_coverage_step(command) -> RunCoverageReport:
        events.append(("coverage", command.crawl_run_id))
        return next(coverage_reports)

    def run_list_engine_v2_step(command) -> RunListEngineV2Result:
        events.append(("list_engine", command.crawl_run_id))
        return RunListEngineV2Result(
            status="failed",
            crawl_run_id=command.crawl_run_id,
            partition_results=(
                _build_partition_result(
                    crawl_run_id=command.crawl_run_id,
                    partition_id=root_partition_id,
                    final_partition_status="failed",
                    final_coverage_status="unassessed",
                    error_message="upstream search request failed",
                ),
            ),
            remaining_pending_terminal_partitions=(),
        )

    result = run_collection_once_v2(
        RunCollectionOnceV2Command(
            detail_limit=5,
            run_type="weekly_sweep",
            triggered_by="cli",
        ),
        sync_dictionary_step=lambda command: (_ for _ in ()).throw(
            AssertionError(f"unexpected dictionary sync {command.dictionary_name}")
        ),
        create_crawl_run_step=create_crawl_run_step,
        plan_run_v2_step=plan_run_v2_step,
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
    )

    assert events == [
        ("create", "weekly_sweep", "cli"),
        ("plan_v2", run_id),
        ("coverage", run_id),
        ("list_engine", run_id),
        ("coverage", run_id),
    ]
    assert result.status == "failed"
    assert result.list_stage_status == "failed"
    assert result.detail_stage_status == "skipped"
    assert result.reconciliation_status == "skipped"
    assert result.failed_step == "run_list_engine_v2"
    assert "failed partition" in (result.error_message or "")
    assert result.failed_partitions == 1
    assert result.skipped_steps == ("fetch_vacancy_detail", "reconcile_run")


def test_run_collection_once_v2_marks_final_status_failed_when_detail_stage_has_failures() -> None:
    run_id = uuid4()
    root_partition_id = uuid4()
    vacancy_one = uuid4()
    vacancy_two = uuid4()
    events: list[tuple[object, ...]] = []

    def create_crawl_run_step(command) -> CrawlRun:
        events.append(("create", command.run_type, command.triggered_by))
        return _build_crawl_run(run_id=run_id, run_type=command.run_type)

    def plan_run_v2_step(command) -> PlanRunResult:
        events.append(("plan_v2", command.crawl_run_id))
        root_partition = _build_partition(
            crawl_run_id=command.crawl_run_id,
            partition_id=root_partition_id,
            partition_key="area:113",
            status="pending",
            coverage_status="unassessed",
        )
        return PlanRunResult(
            crawl_run_id=command.crawl_run_id,
            created_partitions=[root_partition],
            partitions=[root_partition],
        )

    coverage_reports = iter(
        (
            _build_coverage_report(
                run_id=run_id,
                total_partitions=1,
                terminal_partitions=1,
                covered_terminal_partitions=0,
                pending_partitions=1,
                pending_terminal_partitions=1,
            ),
            _build_coverage_report(
                run_id=run_id,
                total_partitions=1,
                terminal_partitions=1,
                covered_terminal_partitions=1,
                pending_partitions=0,
                pending_terminal_partitions=0,
                coverage_ratio=1.0,
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
                    partition_id=root_partition_id,
                    final_partition_status="done",
                    final_coverage_status="covered",
                ),
            ),
            remaining_pending_terminal_partitions=(),
        )

    def select_detail_candidates_step(command) -> SelectDetailCandidatesResult:
        events.append(("select_detail", command.crawl_run_id))
        return SelectDetailCandidatesResult(
            crawl_run_id=command.crawl_run_id,
            observed_vacancy_count=2,
            eligible_candidates_count=2,
            selected_candidates=(
                DetailFetchCandidate(vacancy_id=vacancy_one, reason="first_seen"),
                DetailFetchCandidate(vacancy_id=vacancy_two, reason="ttl_refresh"),
            ),
            skipped_due_to_limit=0,
            first_seen_candidates=1,
            short_changed_candidates=0,
            ttl_refresh_candidates=1,
        )

    def fetch_vacancy_detail_step(command) -> FetchVacancyDetailResult:
        events.append(("detail", command.vacancy_id, command.reason))
        if command.vacancy_id == vacancy_two:
            return _build_detail_result(command.vacancy_id, error_message="detail fetch failed")
        return _build_detail_result(command.vacancy_id)

    def reconcile_run_step(command) -> ReconcileRunResult:
        events.append(("reconcile", command.crawl_run_id))
        return ReconcileRunResult(
            crawl_run_id=command.crawl_run_id,
            observed_in_run_count=2,
            missing_updated_count=0,
            marked_inactive_count=0,
            run_status="completed",
        )

    result = run_collection_once_v2(
        RunCollectionOnceV2Command(
            detail_limit=2,
            run_type="weekly_sweep",
            triggered_by="cli",
        ),
        sync_dictionary_step=lambda command: (_ for _ in ()).throw(
            AssertionError(f"unexpected dictionary sync {command.dictionary_name}")
        ),
        create_crawl_run_step=create_crawl_run_step,
        plan_run_v2_step=plan_run_v2_step,
        run_list_engine_v2_step=run_list_engine_v2_step,
        report_run_coverage_step=report_run_coverage_step,
        select_detail_candidates_step=select_detail_candidates_step,
        fetch_vacancy_detail_step=fetch_vacancy_detail_step,
        reconcile_run_step=reconcile_run_step,
    )

    assert events == [
        ("create", "weekly_sweep", "cli"),
        ("plan_v2", run_id),
        ("coverage", run_id),
        ("list_engine", run_id),
        ("coverage", run_id),
        ("select_detail", run_id),
        ("detail", vacancy_one, "first_seen"),
        ("detail", vacancy_two, "ttl_refresh"),
        ("reconcile", run_id),
    ]
    assert result.status == "failed"
    assert result.list_stage_status == "completed"
    assert result.detail_stage_status == "completed_with_failures"
    assert result.reconciliation_status == "completed"
    assert result.failed_step == "fetch_vacancy_detail"
    assert result.error_message == "1 detail fetch(es) failed"
    assert result.detail_fetch_attempted == 2
    assert result.detail_fetch_failed == 1
    assert result.completed_steps == (
        "create_crawl_run",
        "plan_sweep_v2",
        "run_list_engine_v2",
        "fetch_vacancy_detail",
        "reconcile_run",
    )
    assert result.skipped_steps == ()


def _build_crawl_run(*, run_id: UUID, run_type: str) -> CrawlRun:
    return CrawlRun(
        id=run_id,
        run_type=run_type,
        status="created",
        started_at=datetime(2026, 3, 19, 10, 0, tzinfo=UTC),
        finished_at=None,
        triggered_by="pytest",
        config_snapshot_json={},
        partitions_total=0,
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
        pages_total_expected=None,
        pages_processed=0,
        items_seen=0,
        retry_count=0,
        started_at=None,
        finished_at=None,
        last_error_message=None,
        created_at=datetime(2026, 3, 19, 10, 1, tzinfo=UTC),
        parent_partition_id=None,
        depth=0,
        split_dimension="area",
        split_value=partition_key.split(":")[-1],
        scope_key=partition_key,
        planner_policy_version="v2",
        is_terminal=True,
        is_saturated=False,
        coverage_status=coverage_status,
    )


def _build_partition_result(
    *,
    crawl_run_id: UUID,
    partition_id: UUID,
    final_partition_status: str,
    final_coverage_status: str,
    saturated: bool = False,
    error_message: str | None = None,
) -> ProcessPartitionV2Result:
    return ProcessPartitionV2Result(
        partition_id=partition_id,
        crawl_run_id=crawl_run_id,
        final_partition_status=final_partition_status,
        final_coverage_status=final_coverage_status,
        saturated=saturated,
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
) -> RunCoverageReport:
    resolved_coverage_ratio = coverage_ratio
    if resolved_coverage_ratio is None:
        resolved_coverage_ratio = (
            covered_terminal_partitions / terminal_partitions
            if terminal_partitions > 0
            else 0.0
        )

    crawl_run = _build_crawl_run(run_id=run_id, run_type="weekly_sweep")
    return RunCoverageReport(
        crawl_run=crawl_run,
        summary=RunCoverageSummary(
            crawl_run_id=run_id,
            run_type="weekly_sweep",
            run_status=crawl_run.status,
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
