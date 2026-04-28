from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Protocol
from uuid import UUID

from hhru_platform.application.commands.create_crawl_run import CreateCrawlRunCommand
from hhru_platform.application.commands.fetch_vacancy_detail import (
    FetchVacancyDetailCommand,
    FetchVacancyDetailResult,
)
from hhru_platform.application.commands.finalize_crawl_run import (
    FinalizeCrawlRunCommand,
    FinalizeCrawlRunResult,
)
from hhru_platform.application.commands.plan_sweep import PlanRunResult
from hhru_platform.application.commands.plan_sweep_v2 import PlanRunV2Command
from hhru_platform.application.commands.reconcile_run import (
    ReconcileRunCommand,
    ReconcileRunResult,
)
from hhru_platform.application.commands.report_run_coverage import (
    ReportRunCoverageCommand,
    RunCoverageReport,
)
from hhru_platform.application.commands.run_list_engine_v2 import (
    RunListEngineV2Command,
    RunListEngineV2Result,
)
from hhru_platform.application.commands.select_detail_candidates import (
    SelectDetailCandidatesCommand,
    SelectDetailCandidatesResult,
)
from hhru_platform.application.commands.sync_dictionary import (
    SyncDictionaryCommand,
    SyncDictionaryResult,
)
from hhru_platform.application.dto import SUPPORTED_DICTIONARY_NAMES
from hhru_platform.domain.entities.crawl_run import CrawlRun
from hhru_platform.domain.value_objects.enums import DictionarySyncStatus
from hhru_platform.infrastructure.observability.operations import (
    log_operation_started,
    record_operation_failed,
    record_operation_succeeded,
)

LOGGER = logging.getLogger(__name__)

DEFAULT_SEARCH_TRANSPORT_CONSECUTIVE_FAILURE_LIMIT = 3
DEFAULT_SEARCH_TRANSPORT_TOTAL_FAILURE_LIMIT = 5
LIST_STAGE_STATUS_COMPLETED = "completed"
LIST_STAGE_STATUS_COMPLETED_WITH_UNRESOLVED = "completed_with_unresolved"
LIST_STAGE_STATUS_FAILED = "failed"
DETAIL_STAGE_STATUS_SKIPPED = "skipped"
DETAIL_STAGE_STATUS_COMPLETED = "completed"
DETAIL_STAGE_STATUS_COMPLETED_WITH_FAILURES = "completed_with_failures"
RUN_COLLECTION_ONCE_V2_STATUS_SUCCEEDED = "succeeded"
RUN_COLLECTION_ONCE_V2_STATUS_COMPLETED_WITH_DETAIL_ERRORS = "completed_with_detail_errors"
RUN_COLLECTION_ONCE_V2_STATUS_COMPLETED_WITH_UNRESOLVED = "completed_with_unresolved"
RUN_COLLECTION_ONCE_V2_STATUS_FAILED = "failed"


class RunCollectionOnceV2StepError(RuntimeError):
    def __init__(
        self,
        *,
        step: str,
        message: str,
        run_id: UUID | None = None,
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
class RunCollectionOnceV2Command:
    sync_dictionaries: bool = False
    detail_limit: int = 100
    detail_refresh_ttl_days: int = 30
    run_type: str = "weekly_sweep"
    triggered_by: str = "run-once-v2"
    dictionary_names: tuple[str, ...] = SUPPORTED_DICTIONARY_NAMES
    search_transport_consecutive_failure_limit: int = (
        DEFAULT_SEARCH_TRANSPORT_CONSECUTIVE_FAILURE_LIMIT
    )
    search_transport_total_failure_limit: int = DEFAULT_SEARCH_TRANSPORT_TOTAL_FAILURE_LIMIT

    def __post_init__(self) -> None:
        normalized_run_type = self.run_type.strip()
        normalized_triggered_by = self.triggered_by.strip()
        normalized_dictionary_names = tuple(name.strip() for name in self.dictionary_names)

        if not normalized_run_type:
            raise ValueError("run_type must not be empty")
        if not normalized_triggered_by:
            raise ValueError("triggered_by must not be empty")
        if self.detail_limit < 0:
            raise ValueError("detail_limit must be greater than or equal to zero")
        if self.detail_refresh_ttl_days < 1:
            raise ValueError("detail_refresh_ttl_days must be greater than or equal to one")
        if self.search_transport_consecutive_failure_limit < 1:
            raise ValueError(
                "search_transport_consecutive_failure_limit must be greater than or equal to one"
            )
        if self.search_transport_total_failure_limit < 1:
            raise ValueError(
                "search_transport_total_failure_limit must be greater than or equal to one"
            )
        if any(name not in SUPPORTED_DICTIONARY_NAMES for name in normalized_dictionary_names):
            supported = ", ".join(SUPPORTED_DICTIONARY_NAMES)
            raise ValueError(f"dictionary_names must be drawn from: {supported}")

        object.__setattr__(self, "run_type", normalized_run_type)
        object.__setattr__(self, "triggered_by", normalized_triggered_by)
        object.__setattr__(self, "dictionary_names", normalized_dictionary_names)


@dataclass(slots=True, frozen=True)
class RunCollectionOnceV2Result:
    status: str
    run_id: UUID | None
    run_type: str
    triggered_by: str
    dictionary_results: tuple[SyncDictionaryResult, ...]
    planned_partition_ids: tuple[UUID, ...]
    list_engine_results: tuple[RunListEngineV2Result, ...]
    final_coverage_report: RunCoverageReport | None
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
    def partitions_planned(self) -> int:
        return len(self.planned_partition_ids)

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

    @property
    def search_transport_failures_total(self) -> int:
        return sum(result.search_transport_failures_total for result in self.list_engine_results)

    @property
    def search_captcha_failures_total(self) -> int:
        return sum(result.search_captcha_failures_total for result in self.list_engine_results)


SyncDictionaryStep = Callable[[SyncDictionaryCommand], SyncDictionaryResult]
CreateCrawlRunStep = Callable[[CreateCrawlRunCommand], CrawlRun]
PlanRunV2Step = Callable[[PlanRunV2Command], PlanRunResult]
RunListEngineV2Step = Callable[[RunListEngineV2Command], RunListEngineV2Result]
ReportRunCoverageStep = Callable[[ReportRunCoverageCommand], RunCoverageReport]
RequeueFailedPartitionsStep = Callable[[UUID], Sequence[object]]
SelectDetailCandidatesStep = Callable[
    [SelectDetailCandidatesCommand],
    SelectDetailCandidatesResult,
]
FetchVacancyDetailStep = Callable[[FetchVacancyDetailCommand], FetchVacancyDetailResult]
ReconcileRunStep = Callable[[ReconcileRunCommand], ReconcileRunResult]
FinalizeCrawlRunStep = Callable[[FinalizeCrawlRunCommand], FinalizeCrawlRunResult]


class DetailRepairBacklogMetricsRecorder(Protocol):
    def set_detail_repair_backlog(
        self,
        *,
        run_id: str,
        run_type: str,
        backlog_size: int,
    ) -> None:
        """Persist the current detail repair backlog size for a crawl_run."""


def run_collection_once_v2(
    command: RunCollectionOnceV2Command,
    *,
    sync_dictionary_step: SyncDictionaryStep,
    create_crawl_run_step: CreateCrawlRunStep,
    plan_run_v2_step: PlanRunV2Step,
    run_list_engine_v2_step: RunListEngineV2Step,
    report_run_coverage_step: ReportRunCoverageStep,
    select_detail_candidates_step: SelectDetailCandidatesStep,
    fetch_vacancy_detail_step: FetchVacancyDetailStep,
    reconcile_run_step: ReconcileRunStep,
    finalize_crawl_run_step: FinalizeCrawlRunStep,
    metrics_recorder: DetailRepairBacklogMetricsRecorder | None = None,
    requeue_failed_partitions_step: RequeueFailedPartitionsStep | None = None,
) -> RunCollectionOnceV2Result:
    started_at = log_operation_started(
        LOGGER,
        operation="run_collection_once_v2",
        sync_dictionaries=command.sync_dictionaries,
        detail_limit=command.detail_limit,
        detail_refresh_ttl_days=command.detail_refresh_ttl_days,
        run_type=command.run_type,
        triggered_by=command.triggered_by,
    )
    current_run_id: UUID | None = None
    dictionary_results: tuple[SyncDictionaryResult, ...] = ()
    planned_partition_ids: tuple[UUID, ...] = ()
    list_engine_results: tuple[RunListEngineV2Result, ...] = ()
    final_coverage_report: RunCoverageReport | None = None
    detail_selection_result: SelectDetailCandidatesResult | None = None
    detail_results: tuple[FetchVacancyDetailResult, ...] = ()
    list_stage_status = LIST_STAGE_STATUS_FAILED
    detail_stage_status = DETAIL_STAGE_STATUS_SKIPPED
    reconciliation_result: ReconcileRunResult | None = None
    run_finalized = False
    completed_steps: list[str] = []

    try:
        dictionary_results = tuple(_sync_requested_dictionaries(command, sync_dictionary_step))
        if command.sync_dictionaries:
            completed_steps.append("sync_dictionaries")

        try:
            crawl_run = create_crawl_run_step(
                CreateCrawlRunCommand(
                    run_type=command.run_type,
                    triggered_by=command.triggered_by,
                )
            )
        except Exception as error:
            raise RunCollectionOnceV2StepError(
                step="create_crawl_run",
                message=str(error),
                run_id=current_run_id,
            ) from error
        current_run_id = crawl_run.id
        completed_steps.append("create_crawl_run")

        try:
            plan_result = plan_run_v2_step(PlanRunV2Command(crawl_run_id=crawl_run.id))
        except Exception as error:
            raise RunCollectionOnceV2StepError(
                step="plan_sweep_v2",
                message=str(error),
                run_id=current_run_id,
            ) from error
        planned_partition_ids = tuple(partition.id for partition in plan_result.partitions)
        completed_steps.append("plan_sweep_v2")

        (
            list_engine_results,
            final_coverage_report,
            list_stage_status,
            list_stage_error_message,
        ) = _run_list_stage(
            crawl_run_id=crawl_run.id,
            run_list_engine_v2_step=run_list_engine_v2_step,
            report_run_coverage_step=report_run_coverage_step,
            requeue_failed_partitions_step=requeue_failed_partitions_step,
            search_transport_consecutive_failure_limit=(
                command.search_transport_consecutive_failure_limit
            ),
            search_transport_total_failure_limit=command.search_transport_total_failure_limit,
        )
        completed_steps.append("run_list_engine_v2")

        if list_stage_status == LIST_STAGE_STATUS_FAILED:
            raise RunCollectionOnceV2StepError(
                step="run_list_engine_v2",
                message=list_stage_error_message or "list stage failed",
                run_id=crawl_run.id,
                final_coverage_report=final_coverage_report,
                list_engine_results=list_engine_results,
                list_stage_status=list_stage_status,
            )
        if list_stage_status == LIST_STAGE_STATUS_COMPLETED_WITH_UNRESOLVED:
            _finalize_run(
                crawl_run_id=crawl_run.id,
                final_status=RUN_COLLECTION_ONCE_V2_STATUS_COMPLETED_WITH_UNRESOLVED,
                notes=list_stage_error_message,
                finalize_crawl_run_step=finalize_crawl_run_step,
            )
            run_finalized = True
            completed_steps.append("finalize_crawl_run")
            final_coverage_report = _report_run_coverage(
                crawl_run.id,
                report_run_coverage_step,
            )
            result = _build_terminal_result(
                status=RUN_COLLECTION_ONCE_V2_STATUS_COMPLETED_WITH_UNRESOLVED,
                command=command,
                run_id=crawl_run.id,
                dictionary_results=dictionary_results,
                planned_partition_ids=planned_partition_ids,
                list_engine_results=list_engine_results,
                final_coverage_report=final_coverage_report,
                list_stage_status=list_stage_status,
                detail_selection_result=None,
                detail_results=(),
                detail_stage_status=DETAIL_STAGE_STATUS_SKIPPED,
                reconciliation_result=None,
                failed_step=None,
                error_message=list_stage_error_message,
                completed_steps=tuple(completed_steps),
            )
            record_operation_failed(
                LOGGER,
                operation="run_collection_once_v2",
                started_at=started_at,
                error_type="RunCollectionOnceV2CoverageUnresolved",
                error_message=result.error_message or "run coverage is unresolved",
                run_id=result.run_id,
                run_type=result.run_type,
                triggered_by=result.triggered_by,
                list_stage_status=result.list_stage_status,
                detail_stage_status=result.detail_stage_status,
                search_transport_failures_total=result.search_transport_failures_total,
                search_captcha_failures_total=result.search_captcha_failures_total,
                coverage_ratio=result.coverage_ratio,
                total_partitions=result.total_partitions,
                covered_terminal_partitions=result.covered_terminal_partitions,
                pending_terminal_partitions=result.pending_terminal_partitions,
                split_partitions=result.split_partitions,
                unresolved_partitions=result.unresolved_partitions,
                failed_partitions=result.failed_partitions,
                reconciliation_status=result.reconciliation_status,
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
                raise RunCollectionOnceV2StepError(
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
                raise RunCollectionOnceV2StepError(
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
                        RUN_COLLECTION_ONCE_V2_STATUS_COMPLETED_WITH_DETAIL_ERRORS
                        if detail_stage_status == DETAIL_STAGE_STATUS_COMPLETED_WITH_FAILURES
                        else RUN_COLLECTION_ONCE_V2_STATUS_SUCCEEDED
                    ),
                    notes=(
                        f"{failed_detail_fetch_count} "
                        "detail fetch(es) failed"
                        if detail_stage_status == DETAIL_STAGE_STATUS_COMPLETED_WITH_FAILURES
                        else None
                    ),
                )
            )
        except Exception as error:
            raise RunCollectionOnceV2StepError(
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
        final_coverage_report = _report_run_coverage(
            crawl_run.id,
            report_run_coverage_step,
        )
    except RunCollectionOnceV2StepError as error:
        finalized_run_id = error.run_id or current_run_id
        if finalized_run_id is not None and not run_finalized:
            finalize_result = _finalize_run_safely(
                crawl_run_id=finalized_run_id,
                error_message=str(error),
                finalize_crawl_run_step=finalize_crawl_run_step,
            )
            if finalize_result is not None:
                run_finalized = True
                completed_steps.append("finalize_crawl_run")
                final_coverage_report = _report_run_coverage_safely(
                    finalized_run_id,
                    report_run_coverage_step,
                    fallback_report=error.final_coverage_report or final_coverage_report,
                )
        result = _build_terminal_result(
            status=RUN_COLLECTION_ONCE_V2_STATUS_FAILED,
            command=command,
            run_id=finalized_run_id,
            dictionary_results=dictionary_results,
            planned_partition_ids=planned_partition_ids,
            list_engine_results=error.list_engine_results or list_engine_results,
            final_coverage_report=error.final_coverage_report or final_coverage_report,
            list_stage_status=error.list_stage_status,
            detail_selection_result=error.detail_selection_result or detail_selection_result,
            detail_results=error.detail_results or detail_results,
            detail_stage_status=error.detail_stage_status,
            reconciliation_result=None,
            failed_step=error.step,
            error_message=str(error),
            completed_steps=tuple(completed_steps),
        )
        record_operation_failed(
            LOGGER,
            operation="run_collection_once_v2",
            started_at=started_at,
            error_type="RunCollectionOnceV2StepError",
            error_message=result.error_message or str(error),
            run_id=result.run_id,
            run_type=result.run_type,
            triggered_by=result.triggered_by,
            failed_step=result.failed_step,
            list_stage_status=result.list_stage_status,
            detail_stage_status=result.detail_stage_status,
            search_transport_failures_total=result.search_transport_failures_total,
            search_captcha_failures_total=result.search_captcha_failures_total,
            coverage_ratio=result.coverage_ratio,
            total_partitions=result.total_partitions,
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
            operation="run_collection_once_v2",
            started_at=started_at,
            error_type=error.__class__.__name__,
            error_message=str(error),
            run_id=current_run_id,
            run_type=command.run_type,
            triggered_by=command.triggered_by,
        )
        raise

    final_status = (
        RUN_COLLECTION_ONCE_V2_STATUS_COMPLETED_WITH_DETAIL_ERRORS
        if detail_stage_status == DETAIL_STAGE_STATUS_COMPLETED_WITH_FAILURES
        else RUN_COLLECTION_ONCE_V2_STATUS_SUCCEEDED
    )
    error_message = None
    failed_step = None
    if final_status == RUN_COLLECTION_ONCE_V2_STATUS_COMPLETED_WITH_DETAIL_ERRORS:
        failed_detail_fetches = sum(
            1 for result in detail_results if result.error_message is not None
        )
        error_message = f"{failed_detail_fetches} detail fetch(es) failed"

    result = _build_terminal_result(
        status=final_status,
        command=command,
        run_id=crawl_run.id,
        dictionary_results=dictionary_results,
        planned_partition_ids=planned_partition_ids,
        list_engine_results=list_engine_results,
        final_coverage_report=final_coverage_report,
        list_stage_status=list_stage_status,
        detail_selection_result=detail_selection_result,
        detail_results=detail_results,
        detail_stage_status=detail_stage_status,
        reconciliation_result=reconciliation_result,
        failed_step=failed_step,
        error_message=error_message,
        completed_steps=tuple(completed_steps),
    )
    if metrics_recorder is not None:
        metrics_recorder.set_detail_repair_backlog(
            run_id=str(result.run_id),
            run_type=result.run_type,
            backlog_size=result.detail_fetch_failed,
        )
    if result.status in (
        RUN_COLLECTION_ONCE_V2_STATUS_SUCCEEDED,
        RUN_COLLECTION_ONCE_V2_STATUS_COMPLETED_WITH_DETAIL_ERRORS,
    ):
        record_operation_succeeded(
            LOGGER,
            operation="run_collection_once_v2",
            started_at=started_at,
            run_id=result.run_id,
            run_type=result.run_type,
            triggered_by=result.triggered_by,
            dictionaries_synced=len(result.dictionary_results),
            partitions_planned=result.partitions_planned,
            list_engine_iterations=result.list_engine_iterations,
            total_partitions=result.total_partitions,
            covered_terminal_partitions=result.covered_terminal_partitions,
            pending_terminal_partitions=result.pending_terminal_partitions,
            split_partitions=result.split_partitions,
            unresolved_partitions=result.unresolved_partitions,
            coverage_ratio=result.coverage_ratio,
            list_stage_status=result.list_stage_status,
            final_status=result.status,
            search_transport_failures_total=result.search_transport_failures_total,
            search_captcha_failures_total=result.search_captcha_failures_total,
            detail_stage_status=result.detail_stage_status,
            detail_fetch_attempted=result.detail_fetch_attempted,
            detail_fetch_failed=result.detail_fetch_failed,
            reconciliation_status=result.reconciliation_status,
            error_message=result.error_message,
        )
    else:
        record_operation_failed(
            LOGGER,
            operation="run_collection_once_v2",
            started_at=started_at,
            error_type="RunCollectionOnceV2DetailFailures",
            error_message=result.error_message or "detail stage completed with failures",
            run_id=result.run_id,
            run_type=result.run_type,
            triggered_by=result.triggered_by,
            failed_step=result.failed_step,
            list_stage_status=result.list_stage_status,
            detail_stage_status=result.detail_stage_status,
            search_transport_failures_total=result.search_transport_failures_total,
            search_captcha_failures_total=result.search_captcha_failures_total,
            coverage_ratio=result.coverage_ratio,
            total_partitions=result.total_partitions,
            covered_terminal_partitions=result.covered_terminal_partitions,
            pending_terminal_partitions=result.pending_terminal_partitions,
            split_partitions=result.split_partitions,
            unresolved_partitions=result.unresolved_partitions,
            failed_partitions=result.failed_partitions,
            detail_fetch_attempted=result.detail_fetch_attempted,
            detail_fetch_failed=result.detail_fetch_failed,
            reconciliation_status=result.reconciliation_status,
        )
    return result


def _sync_requested_dictionaries(
    command: RunCollectionOnceV2Command,
    sync_dictionary_step: SyncDictionaryStep,
) -> list[SyncDictionaryResult]:
    if not command.sync_dictionaries:
        return []

    results: list[SyncDictionaryResult] = []
    for dictionary_name in command.dictionary_names:
        result = sync_dictionary_step(SyncDictionaryCommand(dictionary_name=dictionary_name))
        results.append(result)
        if (
            result.status != DictionarySyncStatus.SUCCEEDED.value
            or result.error_message is not None
        ):
            raise RunCollectionOnceV2StepError(
                step="sync_dictionaries",
                message=(
                    f"dictionary sync failed for {dictionary_name}: "
                    f"{result.error_message or result.status}"
                ),
            )
    return results


def _run_list_stage(
    *,
    crawl_run_id: UUID,
    run_list_engine_v2_step: RunListEngineV2Step,
    report_run_coverage_step: ReportRunCoverageStep,
    requeue_failed_partitions_step: RequeueFailedPartitionsStep | None,
    search_transport_consecutive_failure_limit: int,
    search_transport_total_failure_limit: int,
) -> tuple[
    tuple[RunListEngineV2Result, ...],
    RunCoverageReport,
    str,
    str | None,
]:
    coverage_report = _report_run_coverage(crawl_run_id, report_run_coverage_step)
    list_engine_results: list[RunListEngineV2Result] = []
    search_transport_failures_total = 0
    search_transport_failures_consecutive = 0

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
            raise RunCollectionOnceV2StepError(
                step="run_list_engine_v2",
                message=str(error),
                run_id=crawl_run_id,
                final_coverage_report=coverage_report,
                list_engine_results=tuple(list_engine_results),
            ) from error
        list_engine_results.append(_compact_run_list_engine_result(run_result))
        coverage_report = _report_run_coverage(crawl_run_id, report_run_coverage_step)

        if run_result.status == LIST_STAGE_STATUS_FAILED:
            if run_result.search_transport_failures_total > 0:
                search_transport_failures_total += run_result.search_transport_failures_total
                search_transport_failures_consecutive += (
                    run_result.search_transport_failures_total
                )
                failure_message = _build_run_list_engine_failure_message(
                    run_result,
                    coverage_report=coverage_report,
                )
                if not _search_transport_budget_allows_requeue(
                    consecutive_failures=search_transport_failures_consecutive,
                    total_failures=search_transport_failures_total,
                    consecutive_failure_limit=search_transport_consecutive_failure_limit,
                    total_failure_limit=search_transport_total_failure_limit,
                ):
                    return (
                        tuple(list_engine_results),
                        coverage_report,
                        LIST_STAGE_STATUS_FAILED,
                        _build_search_transport_budget_exhausted_message(
                            consecutive_failures=search_transport_failures_consecutive,
                            total_failures=search_transport_failures_total,
                            consecutive_failure_limit=(
                                search_transport_consecutive_failure_limit
                            ),
                            total_failure_limit=search_transport_total_failure_limit,
                            failure_message=failure_message,
                        ),
                    )
                if requeue_failed_partitions_step is not None:
                    requeued_partitions = requeue_failed_partitions_step(crawl_run_id)
                    if requeued_partitions:
                        LOGGER.warning(
                            "run_collection_once_v2.search_transport_failure_requeued",
                            extra={
                                "run_id": str(crawl_run_id),
                                "requeued_failed_partitions": len(requeued_partitions),
                                "search_transport_failures_consecutive": (
                                    search_transport_failures_consecutive
                                ),
                                "search_transport_failures_total": (
                                    search_transport_failures_total
                                ),
                                "search_transport_consecutive_failure_limit": (
                                    search_transport_consecutive_failure_limit
                                ),
                                "search_transport_total_failure_limit": (
                                    search_transport_total_failure_limit
                                ),
                            },
                        )
                        coverage_report = _report_run_coverage(
                            crawl_run_id,
                            report_run_coverage_step,
                        )
                        continue
                    return (
                        tuple(list_engine_results),
                        coverage_report,
                        LIST_STAGE_STATUS_FAILED,
                        f"{failure_message}; failed partition requeue made no progress",
                    )
            return (
                tuple(list_engine_results),
                coverage_report,
                LIST_STAGE_STATUS_FAILED,
                _build_run_list_engine_failure_message(
                    run_result,
                    coverage_report=coverage_report,
                ),
            )

        search_transport_failures_consecutive = 0

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


def _build_run_list_engine_failure_message(
    run_result: RunListEngineV2Result,
    *,
    coverage_report: RunCoverageReport,
) -> str:
    failure_message = run_result.first_failure_error_message
    if run_result.search_captcha_failures_total > 0:
        return failure_message or "search request blocked by captcha"
    if run_result.search_transport_failures_total > 0:
        prefix = "search transport request failed after bounded retries"
        return f"{prefix}: {failure_message}" if failure_message else prefix
    if failure_message:
        return failure_message
    return (
        f"tree coverage failed with {coverage_report.summary.failed_partitions} "
        "failed partition(s)"
    )


def _search_transport_budget_allows_requeue(
    *,
    consecutive_failures: int,
    total_failures: int,
    consecutive_failure_limit: int,
    total_failure_limit: int,
) -> bool:
    return (
        consecutive_failures < consecutive_failure_limit
        and total_failures < total_failure_limit
    )


def _build_search_transport_budget_exhausted_message(
    *,
    consecutive_failures: int,
    total_failures: int,
    consecutive_failure_limit: int,
    total_failure_limit: int,
    failure_message: str,
) -> str:
    return (
        "search transport budget exhausted "
        f"({consecutive_failures}/{consecutive_failure_limit} consecutive, "
        f"{total_failures}/{total_failure_limit} total): {failure_message}"
    )


def _report_run_coverage(
    crawl_run_id: UUID,
    report_run_coverage_step: ReportRunCoverageStep,
) -> RunCoverageReport:
    try:
        return report_run_coverage_step(ReportRunCoverageCommand(crawl_run_id=crawl_run_id))
    except Exception as error:
        raise RunCollectionOnceV2StepError(
            step="report_run_coverage",
            message=str(error),
            run_id=crawl_run_id,
        ) from error


def _report_run_coverage_safely(
    crawl_run_id: UUID,
    report_run_coverage_step: ReportRunCoverageStep,
    *,
    fallback_report: RunCoverageReport | None,
) -> RunCoverageReport | None:
    try:
        return _report_run_coverage(crawl_run_id, report_run_coverage_step)
    except RunCollectionOnceV2StepError:
        return fallback_report


def _finalize_run(
    *,
    crawl_run_id: UUID,
    final_status: str,
    notes: str | None,
    finalize_crawl_run_step: FinalizeCrawlRunStep,
) -> FinalizeCrawlRunResult:
    return finalize_crawl_run_step(
        FinalizeCrawlRunCommand(
            crawl_run_id=crawl_run_id,
            final_status=final_status,
            notes=notes,
        )
    )


def _finalize_run_safely(
    *,
    crawl_run_id: UUID,
    error_message: str,
    finalize_crawl_run_step: FinalizeCrawlRunStep,
) -> FinalizeCrawlRunResult | None:
    try:
        return _finalize_run(
            crawl_run_id=crawl_run_id,
            final_status=RUN_COLLECTION_ONCE_V2_STATUS_FAILED,
            notes=error_message,
            finalize_crawl_run_step=finalize_crawl_run_step,
        )
    except Exception:
        return None


def _build_terminal_result(
    *,
    status: str,
    command: RunCollectionOnceV2Command,
    run_id: UUID | None,
    dictionary_results: tuple[SyncDictionaryResult, ...],
    planned_partition_ids: tuple[UUID, ...],
    list_engine_results: tuple[RunListEngineV2Result, ...],
    final_coverage_report: RunCoverageReport | None,
    list_stage_status: str,
    detail_selection_result: SelectDetailCandidatesResult | None,
    detail_results: tuple[FetchVacancyDetailResult, ...],
    detail_stage_status: str,
    reconciliation_result: ReconcileRunResult | None,
    failed_step: str | None,
    error_message: str | None,
    completed_steps: tuple[str, ...],
) -> RunCollectionOnceV2Result:
    return RunCollectionOnceV2Result(
        status=status,
        run_id=run_id,
        run_type=command.run_type,
        triggered_by=command.triggered_by,
        dictionary_results=dictionary_results,
        planned_partition_ids=planned_partition_ids,
        list_engine_results=list_engine_results,
        final_coverage_report=final_coverage_report,
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


def _expected_steps(command: RunCollectionOnceV2Command) -> tuple[str, ...]:
    steps: list[str] = []
    if command.sync_dictionaries:
        steps.append("sync_dictionaries")
    steps.extend(("create_crawl_run", "plan_sweep_v2", "run_list_engine_v2"))
    if command.detail_limit > 0:
        steps.append("fetch_vacancy_detail")
    steps.append("reconcile_run")
    return tuple(steps)
