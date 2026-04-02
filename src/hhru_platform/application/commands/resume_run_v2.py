from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol
from uuid import UUID

from hhru_platform.application.commands.fetch_vacancy_detail import (
    FetchVacancyDetailCommand,
    FetchVacancyDetailResult,
)
from hhru_platform.application.commands.finalize_crawl_run import (
    FinalizeCrawlRunCommand,
    FinalizeCrawlRunResult,
)
from hhru_platform.application.commands.reconcile_run import (
    ReconcileRunCommand,
    ReconcileRunResult,
)
from hhru_platform.application.commands.report_run_coverage import (
    ReportRunCoverageCommand,
    RunCoverageReport,
)
from hhru_platform.application.commands.run_collection_once_v2 import (
    DETAIL_STAGE_STATUS_COMPLETED,
    DETAIL_STAGE_STATUS_COMPLETED_WITH_FAILURES,
    DETAIL_STAGE_STATUS_SKIPPED,
    LIST_STAGE_STATUS_COMPLETED,
    LIST_STAGE_STATUS_COMPLETED_WITH_UNRESOLVED,
    LIST_STAGE_STATUS_FAILED,
    _build_run_list_engine_failure_message,
)
from hhru_platform.application.commands.run_list_engine_v2 import (
    RunListEngineV2Command,
    RunListEngineV2Result,
)
from hhru_platform.application.commands.select_detail_candidates import (
    SelectDetailCandidatesCommand,
    SelectDetailCandidatesResult,
)
from hhru_platform.domain.entities.crawl_partition import CrawlPartition
from hhru_platform.domain.entities.crawl_run import CrawlRun
from hhru_platform.domain.value_objects.enums import CrawlRunStatus
from hhru_platform.infrastructure.observability.operations import (
    log_operation_started,
    record_operation_failed,
    record_operation_succeeded,
)

LOGGER = logging.getLogger(__name__)

RESUME_RUN_V2_STATUS_SUCCEEDED = CrawlRunStatus.SUCCEEDED.value
RESUME_RUN_V2_STATUS_COMPLETED_WITH_DETAIL_ERRORS = (
    CrawlRunStatus.COMPLETED_WITH_DETAIL_ERRORS.value
)
RESUME_RUN_V2_STATUS_COMPLETED_WITH_UNRESOLVED = (
    CrawlRunStatus.COMPLETED_WITH_UNRESOLVED.value
)
RESUME_RUN_V2_STATUS_FAILED = CrawlRunStatus.FAILED.value
RESUMABLE_RUN_STATUSES = {
    CrawlRunStatus.CREATED.value,
    CrawlRunStatus.COMPLETED_WITH_UNRESOLVED.value,
}


class ResumeRunV2NotAllowedError(ValueError):
    def __init__(self, crawl_run_id: UUID, message: str) -> None:
        super().__init__(f"crawl_run {crawl_run_id} cannot be resumed: {message}")
        self.crawl_run_id = crawl_run_id


class ResumeRunV2StepError(RuntimeError):
    def __init__(
        self,
        *,
        step: str,
        message: str,
        run_id: UUID,
        final_coverage_report: RunCoverageReport | None = None,
        list_engine_results: tuple[RunListEngineV2Result, ...] = (),
        detail_selection_result: SelectDetailCandidatesResult | None = None,
        detail_results: tuple[FetchVacancyDetailResult, ...] = (),
        list_stage_status: str = LIST_STAGE_STATUS_FAILED,
        detail_stage_status: str = DETAIL_STAGE_STATUS_SKIPPED,
    ) -> None:
        super().__init__(message)
        self.step = step
        self.run_id = run_id
        self.final_coverage_report = final_coverage_report
        self.list_engine_results = list_engine_results
        self.detail_selection_result = detail_selection_result
        self.detail_results = detail_results
        self.list_stage_status = list_stage_status
        self.detail_stage_status = detail_stage_status


@dataclass(slots=True, frozen=True)
class ResumeRunV2Command:
    crawl_run_id: UUID
    detail_limit: int = 100
    detail_refresh_ttl_days: int = 30
    triggered_by: str = "resume-run-v2"

    def __post_init__(self) -> None:
        normalized_triggered_by = self.triggered_by.strip()
        if not normalized_triggered_by:
            raise ValueError("triggered_by must not be empty")
        if self.detail_limit < 0:
            raise ValueError("detail_limit must be greater than or equal to zero")
        if self.detail_refresh_ttl_days < 1:
            raise ValueError("detail_refresh_ttl_days must be greater than or equal to one")

        object.__setattr__(self, "triggered_by", normalized_triggered_by)


@dataclass(slots=True, frozen=True)
class ResumeRunV2Result:
    status: str
    run_id: UUID
    run_type: str
    triggered_by: str
    initial_run_status: str
    initial_coverage_report: RunCoverageReport
    final_coverage_report: RunCoverageReport | None
    resumed_partitions: tuple[CrawlPartition, ...]
    list_engine_results: tuple[RunListEngineV2Result, ...]
    list_stage_status: str
    detail_selection_result: SelectDetailCandidatesResult | None
    detail_results: tuple[FetchVacancyDetailResult, ...]
    detail_stage_status: str
    reconciliation_result: ReconcileRunResult | None
    failed_step: str | None = None
    error_message: str | None = None
    completed_steps: tuple[str, ...] = ()
    skipped_steps: tuple[str, ...] = ()

    @property
    def unresolved_before_resume(self) -> int:
        return self.initial_coverage_report.summary.unresolved_partitions

    @property
    def pending_before_resume(self) -> int:
        return self.initial_coverage_report.summary.pending_terminal_partitions

    @property
    def covered_before_resume(self) -> int:
        return self.initial_coverage_report.summary.covered_terminal_partitions

    @property
    def coverage_ratio_before_resume(self) -> float:
        return self.initial_coverage_report.summary.coverage_ratio

    @property
    def resumed_unresolved_partitions(self) -> int:
        return len(self.resumed_partitions)

    @property
    def list_engine_iterations(self) -> int:
        return len(self.list_engine_results)

    @property
    def detail_candidates_selected(self) -> int:
        if self.detail_selection_result is None:
            return 0
        return len(self.detail_selection_result.selected_candidates)

    @property
    def detail_fetch_attempted(self) -> int:
        return len(self.detail_results)

    @property
    def detail_fetch_succeeded(self) -> int:
        return sum(1 for result in self.detail_results if result.error_message is None)

    @property
    def detail_fetch_failed(self) -> int:
        return self.detail_fetch_attempted - self.detail_fetch_succeeded

    @property
    def total_partitions(self) -> int:
        if self.final_coverage_report is None:
            return 0
        return self.final_coverage_report.summary.total_partitions

    @property
    def covered_terminal_partitions(self) -> int:
        if self.final_coverage_report is None:
            return 0
        return self.final_coverage_report.summary.covered_terminal_partitions

    @property
    def pending_terminal_partitions(self) -> int:
        if self.final_coverage_report is None:
            return 0
        return self.final_coverage_report.summary.pending_terminal_partitions

    @property
    def split_partitions(self) -> int:
        if self.final_coverage_report is None:
            return 0
        return self.final_coverage_report.summary.split_partitions

    @property
    def unresolved_partitions(self) -> int:
        if self.final_coverage_report is None:
            return 0
        return self.final_coverage_report.summary.unresolved_partitions

    @property
    def failed_partitions(self) -> int:
        if self.final_coverage_report is None:
            return 0
        return self.final_coverage_report.summary.failed_partitions

    @property
    def coverage_ratio(self) -> float:
        if self.final_coverage_report is None:
            return 0.0
        return self.final_coverage_report.summary.coverage_ratio

    @property
    def reconciliation_status(self) -> str:
        if self.reconciliation_result is None:
            return "skipped"
        return self.reconciliation_result.run_status


class CrawlRunRepository(Protocol):
    def get(self, run_id: UUID) -> CrawlRun | None:
        """Return a crawl run by id."""

    def reopen(self, *, run_id: UUID, status: str = CrawlRunStatus.CREATED.value) -> CrawlRun:
        """Reopen a crawl run for continued execution."""


class CrawlPartitionRepository(Protocol):
    def requeue_unresolved_by_run_id(self, run_id: UUID) -> list[CrawlPartition]:
        """Reset unresolved terminal partitions back to pending."""


ReportRunCoverageStep = Callable[[ReportRunCoverageCommand], RunCoverageReport]
RunListEngineV2Step = Callable[[RunListEngineV2Command], RunListEngineV2Result]
SelectDetailCandidatesStep = Callable[
    [SelectDetailCandidatesCommand],
    SelectDetailCandidatesResult,
]
FetchVacancyDetailStep = Callable[[FetchVacancyDetailCommand], FetchVacancyDetailResult]
ReconcileRunStep = Callable[[ReconcileRunCommand], ReconcileRunResult]
FinalizeCrawlRunStep = Callable[[FinalizeCrawlRunCommand], FinalizeCrawlRunResult]


class ResumeRunV2MetricsRecorder(Protocol):
    def record_resume_attempt(
        self,
        *,
        run_type: str,
        outcome: str,
    ) -> None:
        """Persist one resume-run-v2 outcome."""

    def set_detail_repair_backlog(
        self,
        *,
        run_id: str,
        run_type: str,
        backlog_size: int,
    ) -> None:
        """Persist the current detail repair backlog size for a crawl_run."""


def resume_run_v2(
    command: ResumeRunV2Command,
    *,
    crawl_run_repository: CrawlRunRepository,
    crawl_partition_repository: CrawlPartitionRepository,
    run_list_engine_v2_step: RunListEngineV2Step,
    report_run_coverage_step: ReportRunCoverageStep,
    select_detail_candidates_step: SelectDetailCandidatesStep,
    fetch_vacancy_detail_step: FetchVacancyDetailStep,
    reconcile_run_step: ReconcileRunStep,
    finalize_crawl_run_step: FinalizeCrawlRunStep,
    metrics_recorder: ResumeRunV2MetricsRecorder | None = None,
) -> ResumeRunV2Result:
    started_at = log_operation_started(
        LOGGER,
        operation="resume_run_v2",
        run_id=command.crawl_run_id,
        detail_limit=command.detail_limit,
        detail_refresh_ttl_days=command.detail_refresh_ttl_days,
        triggered_by=command.triggered_by,
    )
    crawl_run = crawl_run_repository.get(command.crawl_run_id)
    if crawl_run is None:
        if metrics_recorder is not None:
            metrics_recorder.record_resume_attempt(
                run_type="unknown",
                outcome="not_found",
            )
        record_operation_failed(
            LOGGER,
            operation="resume_run_v2",
            started_at=started_at,
            error_type="LookupError",
            error_message=f"crawl_run not found: {command.crawl_run_id}",
            run_id=command.crawl_run_id,
        )
        raise LookupError(f"crawl_run not found: {command.crawl_run_id}")
    initial_run_status = crawl_run.status
    try:
        initial_coverage_report = _report_resume_coverage(
            crawl_run.id,
            report_run_coverage_step,
        )
    except ResumeRunV2StepError as error:
        if metrics_recorder is not None:
            metrics_recorder.record_resume_attempt(
                run_type=crawl_run.run_type,
                outcome=RESUME_RUN_V2_STATUS_FAILED,
            )
        record_operation_failed(
            LOGGER,
            operation="resume_run_v2",
            started_at=started_at,
            error_type="ResumeRunV2StepError",
            error_message=str(error),
            run_id=crawl_run.id,
        )
        raise
    if crawl_run.status not in RESUMABLE_RUN_STATUSES:
        message = f"status={crawl_run.status}"
        if metrics_recorder is not None:
            metrics_recorder.record_resume_attempt(
                run_type=crawl_run.run_type,
                outcome="not_allowed",
            )
        record_operation_failed(
            LOGGER,
            operation="resume_run_v2",
            started_at=started_at,
            error_type="ResumeRunV2NotAllowedError",
            error_message=message,
            run_id=crawl_run.id,
        )
        raise ResumeRunV2NotAllowedError(crawl_run.id, message)
    if initial_coverage_report.summary.failed_partitions > 0:
        message = (
            f"failed_partitions={initial_coverage_report.summary.failed_partitions}; "
            "resume-run-v2 only supports pending or unresolved branches"
        )
        if metrics_recorder is not None:
            metrics_recorder.record_resume_attempt(
                run_type=crawl_run.run_type,
                outcome="not_allowed",
            )
        record_operation_failed(
            LOGGER,
            operation="resume_run_v2",
            started_at=started_at,
            error_type="ResumeRunV2NotAllowedError",
            error_message=message,
            run_id=crawl_run.id,
        )
        raise ResumeRunV2NotAllowedError(crawl_run.id, message)

    resumed_partitions: tuple[CrawlPartition, ...] = ()
    list_engine_results: tuple[RunListEngineV2Result, ...] = ()
    final_coverage_report: RunCoverageReport | None = initial_coverage_report
    detail_selection_result: SelectDetailCandidatesResult | None = None
    detail_results: tuple[FetchVacancyDetailResult, ...] = ()
    list_stage_status = LIST_STAGE_STATUS_FAILED
    detail_stage_status = DETAIL_STAGE_STATUS_SKIPPED
    reconciliation_result: ReconcileRunResult | None = None
    completed_steps: list[str] = []
    run_finalized = False

    try:
        if initial_coverage_report.summary.unresolved_partitions > 0:
            resumed_partitions = tuple(
                crawl_partition_repository.requeue_unresolved_by_run_id(crawl_run.id)
            )
            completed_steps.append("requeue_unresolved_partitions")

        if (
            crawl_run.status != CrawlRunStatus.CREATED.value
            or crawl_run.finished_at is not None
        ):
            crawl_run = crawl_run_repository.reopen(run_id=crawl_run.id)
            completed_steps.append("reopen_crawl_run")

        (
            list_engine_results,
            final_coverage_report,
            list_stage_status,
            list_stage_error_message,
        ) = _run_resume_list_stage(
            crawl_run_id=crawl_run.id,
            run_list_engine_v2_step=run_list_engine_v2_step,
            report_run_coverage_step=report_run_coverage_step,
        )
        completed_steps.append("run_list_engine_v2")

        if list_stage_status == LIST_STAGE_STATUS_FAILED:
            raise ResumeRunV2StepError(
                step="run_list_engine_v2",
                message=list_stage_error_message or "list stage failed",
                run_id=crawl_run.id,
                final_coverage_report=final_coverage_report,
                list_engine_results=list_engine_results,
                list_stage_status=list_stage_status,
            )

        if list_stage_status == LIST_STAGE_STATUS_COMPLETED_WITH_UNRESOLVED:
            finalize_crawl_run_step(
                FinalizeCrawlRunCommand(
                    crawl_run_id=crawl_run.id,
                    final_status=RESUME_RUN_V2_STATUS_COMPLETED_WITH_UNRESOLVED,
                    notes=list_stage_error_message,
                )
            )
            run_finalized = True
            completed_steps.append("finalize_crawl_run")
            final_coverage_report = _report_resume_coverage(
                crawl_run.id,
                report_run_coverage_step,
            )
            result = _build_resume_result(
                status=RESUME_RUN_V2_STATUS_COMPLETED_WITH_UNRESOLVED,
                command=command,
                crawl_run=crawl_run,
                initial_run_status=initial_run_status,
                initial_coverage_report=initial_coverage_report,
                final_coverage_report=final_coverage_report,
                resumed_partitions=resumed_partitions,
                list_engine_results=list_engine_results,
                list_stage_status=list_stage_status,
                detail_selection_result=None,
                detail_results=(),
                detail_stage_status=DETAIL_STAGE_STATUS_SKIPPED,
                reconciliation_result=None,
                failed_step=None,
                error_message=list_stage_error_message,
                completed_steps=tuple(completed_steps),
            )
            if metrics_recorder is not None:
                metrics_recorder.record_resume_attempt(
                    run_type=result.run_type,
                    outcome=result.status,
                )
            record_operation_failed(
                LOGGER,
                operation="resume_run_v2",
                started_at=started_at,
                error_type="ResumeRunV2CoverageUnresolved",
                error_message=result.error_message or "run coverage is unresolved",
                run_id=result.run_id,
                run_type=result.run_type,
                triggered_by=result.triggered_by,
                initial_run_status=result.initial_run_status,
                unresolved_before_resume=result.unresolved_before_resume,
                resumed_unresolved_partitions=result.resumed_unresolved_partitions,
                covered_before_resume=result.covered_before_resume,
                covered_terminal_partitions=result.covered_terminal_partitions,
                pending_terminal_partitions=result.pending_terminal_partitions,
                split_partitions=result.split_partitions,
                unresolved_partitions=result.unresolved_partitions,
                failed_partitions=result.failed_partitions,
                coverage_ratio_before_resume=result.coverage_ratio_before_resume,
                coverage_ratio=result.coverage_ratio,
            )
            return result

        if command.detail_limit > 0:
            try:
                detail_selection_result = select_detail_candidates_step(
                    SelectDetailCandidatesCommand(
                        crawl_run_id=crawl_run.id,
                        limit=command.detail_limit,
                        detail_refresh_ttl_days=command.detail_refresh_ttl_days,
                    )
                )
            except Exception as error:
                raise ResumeRunV2StepError(
                    step="select_detail_candidates",
                    message=str(error),
                    run_id=crawl_run.id,
                    final_coverage_report=final_coverage_report,
                    list_engine_results=list_engine_results,
                    list_stage_status=list_stage_status,
                ) from error

            try:
                detail_results = tuple(
                    fetch_vacancy_detail_step(
                        FetchVacancyDetailCommand(
                            vacancy_id=candidate.vacancy_id,
                            reason=candidate.reason,
                            attempt=1,
                            crawl_run_id=crawl_run.id,
                        )
                    )
                    for candidate in detail_selection_result.selected_candidates
                )
            except Exception as error:
                raise ResumeRunV2StepError(
                    step="fetch_vacancy_detail",
                    message=str(error),
                    run_id=crawl_run.id,
                    final_coverage_report=final_coverage_report,
                    list_engine_results=list_engine_results,
                    detail_selection_result=detail_selection_result,
                    detail_results=detail_results,
                    list_stage_status=list_stage_status,
                ) from error
            completed_steps.append("fetch_vacancy_detail")
            detail_stage_status = (
                DETAIL_STAGE_STATUS_COMPLETED_WITH_FAILURES
                if any(result.error_message is not None for result in detail_results)
                else DETAIL_STAGE_STATUS_COMPLETED
            )

        failed_detail_fetch_count = sum(
            1 for result in detail_results if result.error_message is not None
        )
        try:
            reconciliation_result = reconcile_run_step(
                ReconcileRunCommand(
                    crawl_run_id=crawl_run.id,
                    final_run_status=(
                        RESUME_RUN_V2_STATUS_COMPLETED_WITH_DETAIL_ERRORS
                        if detail_stage_status == DETAIL_STAGE_STATUS_COMPLETED_WITH_FAILURES
                        else RESUME_RUN_V2_STATUS_SUCCEEDED
                    ),
                    notes=(
                        f"{failed_detail_fetch_count} detail fetch(es) failed after resume"
                        if detail_stage_status == DETAIL_STAGE_STATUS_COMPLETED_WITH_FAILURES
                        else None
                    ),
                )
            )
        except Exception as error:
            raise ResumeRunV2StepError(
                step="reconcile_run",
                message=str(error),
                run_id=crawl_run.id,
                final_coverage_report=final_coverage_report,
                list_engine_results=list_engine_results,
                detail_selection_result=detail_selection_result,
                detail_results=detail_results,
                list_stage_status=list_stage_status,
                detail_stage_status=detail_stage_status,
            ) from error
        completed_steps.append("reconcile_run")
        run_finalized = True
        final_coverage_report = _report_resume_coverage(
            crawl_run.id,
            report_run_coverage_step,
        )
    except ResumeRunV2StepError as error:
        if not run_finalized:
            try:
                finalize_crawl_run_step(
                    FinalizeCrawlRunCommand(
                        crawl_run_id=error.run_id,
                        final_status=RESUME_RUN_V2_STATUS_FAILED,
                        notes=str(error),
                    )
                )
                run_finalized = True
                completed_steps.append("finalize_crawl_run")
                final_coverage_report = _report_resume_coverage_safely(
                    error.run_id,
                    report_run_coverage_step,
                    fallback_report=error.final_coverage_report or final_coverage_report,
                )
            except Exception:
                final_coverage_report = error.final_coverage_report or final_coverage_report
        result = _build_resume_result(
            status=RESUME_RUN_V2_STATUS_FAILED,
            command=command,
            crawl_run=crawl_run,
            initial_run_status=initial_run_status,
            initial_coverage_report=initial_coverage_report,
            final_coverage_report=error.final_coverage_report or final_coverage_report,
            resumed_partitions=resumed_partitions,
            list_engine_results=error.list_engine_results or list_engine_results,
            list_stage_status=error.list_stage_status,
            detail_selection_result=error.detail_selection_result or detail_selection_result,
            detail_results=error.detail_results or detail_results,
            detail_stage_status=error.detail_stage_status,
            reconciliation_result=None,
            failed_step=error.step,
            error_message=str(error),
            completed_steps=tuple(completed_steps),
        )
        if metrics_recorder is not None:
            metrics_recorder.record_resume_attempt(
                run_type=result.run_type,
                outcome=result.status,
            )
        record_operation_failed(
            LOGGER,
            operation="resume_run_v2",
            started_at=started_at,
            error_type="ResumeRunV2StepError",
            error_message=result.error_message or "resume failed",
            run_id=result.run_id,
            run_type=result.run_type,
            triggered_by=result.triggered_by,
            initial_run_status=result.initial_run_status,
            failed_step=result.failed_step,
            unresolved_before_resume=result.unresolved_before_resume,
            resumed_unresolved_partitions=result.resumed_unresolved_partitions,
            covered_before_resume=result.covered_before_resume,
            covered_terminal_partitions=result.covered_terminal_partitions,
            pending_terminal_partitions=result.pending_terminal_partitions,
            split_partitions=result.split_partitions,
            unresolved_partitions=result.unresolved_partitions,
            failed_partitions=result.failed_partitions,
            detail_fetch_attempted=result.detail_fetch_attempted,
            detail_fetch_failed=result.detail_fetch_failed,
            reconciliation_status=result.reconciliation_status,
            completed_steps=",".join(result.completed_steps) or None,
            skipped_steps=",".join(result.skipped_steps) or None,
        )
        return result
    except Exception as error:
        record_operation_failed(
            LOGGER,
            operation="resume_run_v2",
            started_at=started_at,
            error_type=error.__class__.__name__,
            error_message=str(error),
            run_id=crawl_run.id,
            triggered_by=command.triggered_by,
        )
        raise

    final_status = (
        RESUME_RUN_V2_STATUS_COMPLETED_WITH_DETAIL_ERRORS
        if detail_stage_status == DETAIL_STAGE_STATUS_COMPLETED_WITH_FAILURES
        else RESUME_RUN_V2_STATUS_SUCCEEDED
    )
    error_message = None
    if final_status == RESUME_RUN_V2_STATUS_COMPLETED_WITH_DETAIL_ERRORS:
        failed_detail_count = sum(
            1 for result in detail_results if result.error_message is not None
        )
        error_message = (
            f"{failed_detail_count} detail fetch(es) failed after resume"
        )

    result = _build_resume_result(
        status=final_status,
        command=command,
        crawl_run=crawl_run,
        initial_run_status=initial_run_status,
        initial_coverage_report=initial_coverage_report,
        final_coverage_report=final_coverage_report,
        resumed_partitions=resumed_partitions,
        list_engine_results=list_engine_results,
        list_stage_status=list_stage_status,
        detail_selection_result=detail_selection_result,
        detail_results=detail_results,
        detail_stage_status=detail_stage_status,
        reconciliation_result=reconciliation_result,
        failed_step=None,
        error_message=error_message,
        completed_steps=tuple(completed_steps),
    )
    if metrics_recorder is not None:
        metrics_recorder.record_resume_attempt(
            run_type=result.run_type,
            outcome=result.status,
        )
        metrics_recorder.set_detail_repair_backlog(
            run_id=str(result.run_id),
            run_type=result.run_type,
            backlog_size=result.detail_fetch_failed,
        )
    record_operation_succeeded(
        LOGGER,
        operation="resume_run_v2",
        started_at=started_at,
        run_id=result.run_id,
        run_type=result.run_type,
        triggered_by=result.triggered_by,
        initial_run_status=result.initial_run_status,
        unresolved_before_resume=result.unresolved_before_resume,
        pending_before_resume=result.pending_before_resume,
        covered_before_resume=result.covered_before_resume,
        resumed_unresolved_partitions=result.resumed_unresolved_partitions,
        list_engine_iterations=result.list_engine_iterations,
        total_partitions=result.total_partitions,
        covered_terminal_partitions=result.covered_terminal_partitions,
        pending_terminal_partitions=result.pending_terminal_partitions,
        split_partitions=result.split_partitions,
        unresolved_partitions=result.unresolved_partitions,
        failed_partitions=result.failed_partitions,
        coverage_ratio_before_resume=result.coverage_ratio_before_resume,
        coverage_ratio=result.coverage_ratio,
        list_stage_status=result.list_stage_status,
        final_status=result.status,
        detail_stage_status=result.detail_stage_status,
        detail_fetch_attempted=result.detail_fetch_attempted,
        detail_fetch_failed=result.detail_fetch_failed,
        reconciliation_status=result.reconciliation_status,
        error_message=result.error_message,
    )
    return result


def _build_resume_result(
    *,
    status: str,
    command: ResumeRunV2Command,
    crawl_run: CrawlRun,
    initial_run_status: str,
    initial_coverage_report: RunCoverageReport,
    final_coverage_report: RunCoverageReport | None,
    resumed_partitions: tuple[CrawlPartition, ...],
    list_engine_results: tuple[RunListEngineV2Result, ...],
    list_stage_status: str,
    detail_selection_result: SelectDetailCandidatesResult | None,
    detail_results: tuple[FetchVacancyDetailResult, ...],
    detail_stage_status: str,
    reconciliation_result: ReconcileRunResult | None,
    failed_step: str | None,
    error_message: str | None,
    completed_steps: tuple[str, ...],
) -> ResumeRunV2Result:
    return ResumeRunV2Result(
        status=status,
        run_id=crawl_run.id,
        run_type=crawl_run.run_type,
        triggered_by=command.triggered_by,
        initial_run_status=initial_run_status,
        initial_coverage_report=initial_coverage_report,
        final_coverage_report=final_coverage_report,
        resumed_partitions=resumed_partitions,
        list_engine_results=list_engine_results,
        list_stage_status=list_stage_status,
        detail_selection_result=detail_selection_result,
        detail_results=detail_results,
        detail_stage_status=detail_stage_status,
        reconciliation_result=reconciliation_result,
        failed_step=failed_step,
        error_message=error_message,
        completed_steps=completed_steps,
        skipped_steps=tuple(
            step
            for step in _expected_steps(command)
            if step not in completed_steps and step != failed_step
        ),
    )


def _expected_steps(command: ResumeRunV2Command) -> tuple[str, ...]:
    steps: list[str] = [
        "requeue_unresolved_partitions",
        "reopen_crawl_run",
        "run_list_engine_v2",
    ]
    if command.detail_limit > 0:
        steps.append("fetch_vacancy_detail")
    steps.append("reconcile_run")
    return tuple(steps)


def _report_resume_coverage(
    crawl_run_id: UUID,
    report_run_coverage_step: ReportRunCoverageStep,
) -> RunCoverageReport:
    try:
        return report_run_coverage_step(ReportRunCoverageCommand(crawl_run_id=crawl_run_id))
    except Exception as error:
        raise ResumeRunV2StepError(
            step="report_run_coverage",
            message=str(error),
            run_id=crawl_run_id,
        ) from error


def _report_resume_coverage_safely(
    crawl_run_id: UUID,
    report_run_coverage_step: ReportRunCoverageStep,
    *,
    fallback_report: RunCoverageReport | None,
) -> RunCoverageReport | None:
    try:
        return _report_resume_coverage(crawl_run_id, report_run_coverage_step)
    except ResumeRunV2StepError:
        return fallback_report


def _run_resume_list_stage(
    *,
    crawl_run_id: UUID,
    run_list_engine_v2_step: RunListEngineV2Step,
    report_run_coverage_step: ReportRunCoverageStep,
) -> tuple[
    tuple[RunListEngineV2Result, ...],
    RunCoverageReport,
    str,
    str | None,
]:
    coverage_report = _report_resume_coverage(crawl_run_id, report_run_coverage_step)
    list_engine_results: list[RunListEngineV2Result] = []

    while True:
        if coverage_report.summary.is_fully_covered:
            return (
                tuple(list_engine_results),
                coverage_report,
                LIST_STAGE_STATUS_COMPLETED,
                None,
            )
        if coverage_report.summary.failed_partitions > 0:
            return (
                tuple(list_engine_results),
                coverage_report,
                LIST_STAGE_STATUS_FAILED,
                (
                    f"tree coverage failed with {coverage_report.summary.failed_partitions} "
                    "failed partition(s)"
                ),
            )
        if coverage_report.summary.unresolved_partitions > 0:
            return (
                tuple(list_engine_results),
                coverage_report,
                LIST_STAGE_STATUS_COMPLETED_WITH_UNRESOLVED,
                (
                    "tree coverage stopped with "
                    f"{coverage_report.summary.unresolved_partitions} unresolved partition(s)"
                ),
            )
        if (
            coverage_report.summary.pending_terminal_partitions == 0
            and coverage_report.summary.running_partitions == 0
        ):
            return (
                tuple(list_engine_results),
                coverage_report,
                LIST_STAGE_STATUS_FAILED,
                (
                    "tree coverage is incomplete but there are no pending or running "
                    "terminal partitions left to execute"
                ),
            )

        try:
            run_result = run_list_engine_v2_step(
                RunListEngineV2Command(
                    crawl_run_id=crawl_run_id,
                    partition_limit=1,
                )
            )
        except Exception as error:
            raise ResumeRunV2StepError(
                step="run_list_engine_v2",
                message=str(error),
                run_id=crawl_run_id,
                final_coverage_report=coverage_report,
                list_engine_results=tuple(list_engine_results),
            ) from error
        list_engine_results.append(_compact_run_list_engine_result(run_result))
        coverage_report = _report_resume_coverage(crawl_run_id, report_run_coverage_step)

        if run_result.status == LIST_STAGE_STATUS_FAILED:
            return (
                tuple(list_engine_results),
                coverage_report,
                LIST_STAGE_STATUS_FAILED,
                _build_run_list_engine_failure_message(
                    run_result,
                    coverage_report=coverage_report,
                ),
            )

        if (
            run_result.partitions_attempted == 0
            and coverage_report.summary.pending_terminal_partitions > 0
        ):
            return (
                tuple(list_engine_results),
                coverage_report,
                LIST_STAGE_STATUS_FAILED,
                "run_list_engine_v2 made no progress while pending terminal partitions remained",
            )


def _compact_run_list_engine_result(
    result: RunListEngineV2Result,
) -> RunListEngineV2Result:
    return RunListEngineV2Result(
        status=result.status,
        crawl_run_id=result.crawl_run_id,
        partition_results=(),
        remaining_pending_terminal_count=result.remaining_pending_terminal_count,
        search_transport_failures_total=result.search_transport_failures_total,
        search_captcha_failures_total=result.search_captcha_failures_total,
        first_failure_error_type=result.first_failure_error_type,
        first_failure_error_message=result.first_failure_error_message,
    )
