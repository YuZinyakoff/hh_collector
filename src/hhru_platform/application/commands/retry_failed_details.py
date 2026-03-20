from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol
from uuid import UUID

from hhru_platform.application.commands.fetch_vacancy_detail import (
    FetchVacancyDetailCommand,
    FetchVacancyDetailResult,
)
from hhru_platform.domain.entities.crawl_run import CrawlRun
from hhru_platform.domain.entities.detail_fetch_attempt import DetailFetchAttempt
from hhru_platform.domain.value_objects.enums import CrawlRunStatus
from hhru_platform.infrastructure.observability.lifecycle import (
    RunTerminalStatusMetricsRecorder,
    publish_run_terminal_status,
)
from hhru_platform.infrastructure.observability.operations import (
    log_operation_started,
    record_operation_failed,
    record_operation_succeeded,
)

LOGGER = logging.getLogger(__name__)
DETAIL_REPAIR_REASON = "repair_backlog"
RETRY_FAILED_DETAILS_STATUS_SUCCEEDED = CrawlRunStatus.SUCCEEDED.value
RETRY_FAILED_DETAILS_STATUS_COMPLETED_WITH_DETAIL_ERRORS = (
    CrawlRunStatus.COMPLETED_WITH_DETAIL_ERRORS.value
)
REPAIRABLE_RUN_STATUSES = {
    CrawlRunStatus.COMPLETED_WITH_DETAIL_ERRORS.value,
    CrawlRunStatus.SUCCEEDED.value,
}


class RetryFailedDetailsNotAllowedError(ValueError):
    def __init__(self, crawl_run_id: UUID, message: str) -> None:
        super().__init__(f"crawl_run {crawl_run_id} cannot repair details: {message}")
        self.crawl_run_id = crawl_run_id


@dataclass(slots=True, frozen=True)
class RetryFailedDetailsCommand:
    crawl_run_id: UUID
    triggered_by: str = "retry-failed-details"

    def __post_init__(self) -> None:
        normalized_triggered_by = self.triggered_by.strip()
        if not normalized_triggered_by:
            raise ValueError("triggered_by must not be empty")

        object.__setattr__(self, "triggered_by", normalized_triggered_by)


@dataclass(slots=True, frozen=True)
class RetryFailedDetailsResult:
    status: str
    run_id: UUID
    run_type: str
    triggered_by: str
    run_status_before: str
    run_status_after: str
    backlog_before: tuple[DetailFetchAttempt, ...]
    backlog_after: tuple[DetailFetchAttempt, ...]
    detail_results: tuple[FetchVacancyDetailResult, ...]
    error_message: str | None = None

    @property
    def backlog_size(self) -> int:
        return len(self.backlog_before)

    @property
    def retried_count(self) -> int:
        return len(self.detail_results)

    @property
    def remaining_backlog_count(self) -> int:
        return len(self.backlog_after)

    @property
    def repaired_count(self) -> int:
        remaining_vacancy_ids = {item.vacancy_id for item in self.backlog_after}
        return sum(
            1 for item in self.backlog_before if item.vacancy_id not in remaining_vacancy_ids
        )

    @property
    def still_failing_count(self) -> int:
        return self.remaining_backlog_count


class CrawlRunRepository(Protocol):
    def get(self, run_id: UUID) -> CrawlRun | None:
        """Return a crawl run by id."""

    def complete(
        self,
        *,
        run_id: UUID,
        status: str,
        finished_at: datetime,
        partitions_done: int,
        partitions_failed: int,
        notes: str | None = None,
    ) -> CrawlRun:
        """Update the crawl run status and terminal metadata."""


class DetailFetchAttemptRepository(Protocol):
    def list_repair_backlog_by_run_id(self, crawl_run_id: UUID) -> list[DetailFetchAttempt]:
        """Return unresolved detail repair backlog items for a crawl run."""


class FetchVacancyDetailStep(Protocol):
    def __call__(self, command: FetchVacancyDetailCommand) -> FetchVacancyDetailResult:
        """Retry one vacancy detail fetch."""


class RetryFailedDetailsMetricsRecorder(RunTerminalStatusMetricsRecorder, Protocol):
    def set_detail_repair_backlog(
        self,
        *,
        run_id: str,
        run_type: str,
        backlog_size: int,
    ) -> None:
        """Persist the current detail repair backlog size for a crawl_run."""

    def record_detail_repair_attempt(
        self,
        *,
        run_type: str,
        outcome: str,
        retried_count: int,
        repaired_count: int,
        still_failing_count: int,
    ) -> None:
        """Persist one detail repair outcome."""


def retry_failed_details(
    command: RetryFailedDetailsCommand,
    *,
    crawl_run_repository: CrawlRunRepository,
    detail_fetch_attempt_repository: DetailFetchAttemptRepository,
    fetch_vacancy_detail_step: FetchVacancyDetailStep,
    metrics_recorder: RetryFailedDetailsMetricsRecorder | None = None,
) -> RetryFailedDetailsResult:
    started_at = log_operation_started(
        LOGGER,
        operation="retry_failed_details",
        run_id=command.crawl_run_id,
        triggered_by=command.triggered_by,
    )
    crawl_run = crawl_run_repository.get(command.crawl_run_id)
    if crawl_run is None:
        if metrics_recorder is not None:
            metrics_recorder.record_detail_repair_attempt(
                run_type="unknown",
                outcome="not_found",
                retried_count=0,
                repaired_count=0,
                still_failing_count=0,
            )
        record_operation_failed(
            LOGGER,
            operation="retry_failed_details",
            started_at=started_at,
            error_type="LookupError",
            error_message=f"crawl_run not found: {command.crawl_run_id}",
            run_id=command.crawl_run_id,
        )
        raise LookupError(f"crawl_run not found: {command.crawl_run_id}")
    if crawl_run.status not in REPAIRABLE_RUN_STATUSES:
        message = f"status={crawl_run.status}"
        if metrics_recorder is not None:
            metrics_recorder.record_detail_repair_attempt(
                run_type=crawl_run.run_type,
                outcome="not_allowed",
                retried_count=0,
                repaired_count=0,
                still_failing_count=0,
            )
        record_operation_failed(
            LOGGER,
            operation="retry_failed_details",
            started_at=started_at,
            error_type="RetryFailedDetailsNotAllowedError",
            error_message=message,
            run_id=crawl_run.id,
        )
        raise RetryFailedDetailsNotAllowedError(crawl_run.id, message)

    run_status_before = crawl_run.status
    run_finished_at_before = crawl_run.finished_at
    backlog_before = tuple(
        detail_fetch_attempt_repository.list_repair_backlog_by_run_id(crawl_run.id)
    )
    if metrics_recorder is not None:
        metrics_recorder.set_detail_repair_backlog(
            run_id=str(crawl_run.id),
            run_type=crawl_run.run_type,
            backlog_size=len(backlog_before),
        )
    try:
        detail_results = tuple(
            fetch_vacancy_detail_step(
                FetchVacancyDetailCommand(
                    vacancy_id=item.vacancy_id,
                    reason=DETAIL_REPAIR_REASON,
                    attempt=item.attempt + 1,
                    crawl_run_id=crawl_run.id,
                )
            )
            for item in backlog_before
        )
        backlog_after = tuple(
            detail_fetch_attempt_repository.list_repair_backlog_by_run_id(crawl_run.id)
        )
        run_status_after = (
            RETRY_FAILED_DETAILS_STATUS_COMPLETED_WITH_DETAIL_ERRORS
            if backlog_after
            else RETRY_FAILED_DETAILS_STATUS_SUCCEEDED
        )
        updated_run = crawl_run
        if backlog_before or crawl_run.status != run_status_after:
            updated_run = crawl_run_repository.complete(
                run_id=crawl_run.id,
                status=run_status_after,
                finished_at=datetime.now(UTC),
                partitions_done=crawl_run.partitions_done,
                partitions_failed=crawl_run.partitions_failed,
                notes=_build_run_note(
                    backlog_before_count=len(backlog_before),
                    backlog_after_count=len(backlog_after),
                ),
            )
    except Exception as error:
        if metrics_recorder is not None:
            metrics_recorder.record_detail_repair_attempt(
                run_type=crawl_run.run_type,
                outcome="failed",
                retried_count=len(backlog_before),
                repaired_count=0,
                still_failing_count=len(backlog_before),
            )
        record_operation_failed(
            LOGGER,
            operation="retry_failed_details",
            started_at=started_at,
            error_type=error.__class__.__name__,
            error_message=str(error),
            run_id=crawl_run.id,
            run_type=crawl_run.run_type,
            triggered_by=command.triggered_by,
            backlog_size=len(backlog_before),
        )
        raise

    result = RetryFailedDetailsResult(
        status=run_status_after,
        run_id=updated_run.id,
        run_type=updated_run.run_type,
        triggered_by=command.triggered_by,
        run_status_before=run_status_before,
        run_status_after=updated_run.status,
        backlog_before=backlog_before,
        backlog_after=backlog_after,
        detail_results=detail_results,
        error_message=(
            f"{len(backlog_after)} detail repair backlog item(s) still failing"
            if backlog_after
            else None
        ),
    )
    if metrics_recorder is not None:
        metrics_recorder.set_detail_repair_backlog(
            run_id=str(result.run_id),
            run_type=result.run_type,
            backlog_size=result.remaining_backlog_count,
        )
        metrics_recorder.record_detail_repair_attempt(
            run_type=result.run_type,
            outcome=result.status,
            retried_count=result.retried_count,
            repaired_count=result.repaired_count,
            still_failing_count=result.still_failing_count,
        )
        publish_run_terminal_status(
            metrics_recorder,
            run_type=result.run_type,
            previous_status=run_status_before,
            previous_finished_at=run_finished_at_before,
            current_status=result.run_status_after,
            recorded_at=updated_run.finished_at,
        )
    if backlog_after:
        record_operation_failed(
            LOGGER,
            operation="retry_failed_details",
            started_at=started_at,
            error_type="DetailRepairBacklogRemaining",
            error_message=result.error_message or "detail repair backlog remains",
            run_id=result.run_id,
            run_type=result.run_type,
            triggered_by=result.triggered_by,
            run_status_before=result.run_status_before,
            run_status_after=result.run_status_after,
            backlog_size=result.backlog_size,
            retried_count=result.retried_count,
            repaired_count=result.repaired_count,
            still_failing_count=result.still_failing_count,
            remaining_backlog_count=result.remaining_backlog_count,
        )
        return result

    record_operation_succeeded(
        LOGGER,
        operation="retry_failed_details",
        started_at=started_at,
        run_id=result.run_id,
        run_type=result.run_type,
        triggered_by=result.triggered_by,
        run_status_before=result.run_status_before,
        run_status_after=result.run_status_after,
        backlog_size=result.backlog_size,
        retried_count=result.retried_count,
        repaired_count=result.repaired_count,
        still_failing_count=result.still_failing_count,
        remaining_backlog_count=result.remaining_backlog_count,
    )
    return result


def _build_run_note(*, backlog_before_count: int, backlog_after_count: int) -> str:
    if backlog_before_count == 0:
        return "detail repair backlog already empty"
    if backlog_after_count == 0:
        return (
            f"detail repair backlog cleared after retry-failed-details; "
            f"repaired={backlog_before_count}"
        )
    return (
        "detail repair backlog remains after retry-failed-details; "
        f"repaired={backlog_before_count - backlog_after_count}; "
        f"remaining={backlog_after_count}"
    )
