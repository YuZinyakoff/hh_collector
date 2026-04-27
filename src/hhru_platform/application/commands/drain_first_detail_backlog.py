from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Protocol
from uuid import UUID

from hhru_platform.application.commands.fetch_vacancy_detail import (
    FetchVacancyDetailCommand,
    FetchVacancyDetailResult,
)
from hhru_platform.domain.entities.vacancy_current_state import VacancyCurrentState
from hhru_platform.domain.value_objects.enums import DetailFetchStatus
from hhru_platform.infrastructure.observability.operations import (
    log_operation_started,
    record_operation_failed,
    record_operation_succeeded,
)

LOGGER = logging.getLogger(__name__)

FIRST_DETAIL_BACKLOG_REASON = "first_detail_backlog"
DRAIN_FIRST_DETAIL_BACKLOG_STATUS_SUCCEEDED = "succeeded"
DRAIN_FIRST_DETAIL_BACKLOG_STATUS_COMPLETED_WITH_FAILURES = "completed_with_failures"


@dataclass(slots=True, frozen=True)
class DrainFirstDetailBacklogCommand:
    limit: int = 100
    triggered_by: str = "drain-first-detail-backlog"
    include_inactive: bool = False

    def __post_init__(self) -> None:
        normalized_triggered_by = self.triggered_by.strip()
        if self.limit < 0:
            raise ValueError("limit must be greater than or equal to zero")
        if not normalized_triggered_by:
            raise ValueError("triggered_by must not be empty")

        object.__setattr__(self, "triggered_by", normalized_triggered_by)


@dataclass(slots=True, frozen=True)
class FirstDetailBacklogItemResult:
    vacancy_id: UUID
    detail_fetch_status: str
    detail_fetch_attempt_id: int | None
    request_log_id: int | None
    raw_payload_id: int | None
    snapshot_id: int | None
    error_message: str | None
    exception_type: str | None = None

    @property
    def succeeded(self) -> bool:
        return (
            self.detail_fetch_status == DetailFetchStatus.SUCCEEDED.value
            and self.error_message is None
            and self.exception_type is None
        )

    @property
    def terminal(self) -> bool:
        return (
            self.detail_fetch_status == DetailFetchStatus.TERMINAL_404.value
            and self.exception_type is None
        )

    @property
    def resolved(self) -> bool:
        return self.succeeded or self.terminal


@dataclass(slots=True, frozen=True)
class DrainFirstDetailBacklogResult:
    status: str
    triggered_by: str
    include_inactive: bool
    limit: int
    backlog_size_before: int
    backlog_size_after: int
    item_results: tuple[FirstDetailBacklogItemResult, ...]

    @property
    def selected_count(self) -> int:
        return len(self.item_results)

    @property
    def detail_fetch_attempted(self) -> int:
        return len(self.item_results)

    @property
    def detail_fetch_succeeded(self) -> int:
        return sum(1 for item in self.item_results if item.succeeded)

    @property
    def detail_fetch_terminal(self) -> int:
        return sum(1 for item in self.item_results if item.terminal)

    @property
    def detail_fetch_failed(self) -> int:
        return (
            self.detail_fetch_attempted
            - self.detail_fetch_succeeded
            - self.detail_fetch_terminal
        )


class VacancyCurrentStateRepository(Protocol):
    def count_first_detail_backlog(self, *, include_inactive: bool) -> int:
        """Return current vacancies that still have no successful detail payload."""

    def list_first_detail_backlog(
        self,
        *,
        limit: int,
        include_inactive: bool,
    ) -> list[VacancyCurrentState]:
        """Return a bounded deterministic first-detail backlog batch."""


class DetailFetchAttemptRepository(Protocol):
    def latest_attempt_numbers_by_vacancy_ids(
        self,
        vacancy_ids: list[UUID],
    ) -> dict[UUID, int]:
        """Return latest recorded attempt number per vacancy."""


class FetchVacancyDetailStep(Protocol):
    def __call__(self, command: FetchVacancyDetailCommand) -> FetchVacancyDetailResult:
        """Fetch one vacancy detail payload."""


class FirstDetailBacklogMetricsRecorder(Protocol):
    def set_first_detail_backlog(
        self,
        *,
        include_inactive: bool,
        backlog_size: int,
    ) -> None:
        """Persist current global first-detail backlog size."""

    def record_first_detail_drain_attempt(
        self,
        *,
        include_inactive: bool,
        outcome: str,
        selected_count: int,
        succeeded_count: int,
        terminal_count: int,
        failed_count: int,
    ) -> None:
        """Persist one first-detail backlog drain attempt."""


def drain_first_detail_backlog(
    command: DrainFirstDetailBacklogCommand,
    *,
    vacancy_current_state_repository: VacancyCurrentStateRepository,
    detail_fetch_attempt_repository: DetailFetchAttemptRepository,
    fetch_vacancy_detail_step: FetchVacancyDetailStep,
    metrics_recorder: FirstDetailBacklogMetricsRecorder | None = None,
) -> DrainFirstDetailBacklogResult:
    started_at = log_operation_started(
        LOGGER,
        operation="drain_first_detail_backlog",
        limit=command.limit,
        include_inactive=command.include_inactive,
        triggered_by=command.triggered_by,
    )
    try:
        backlog_size_before = vacancy_current_state_repository.count_first_detail_backlog(
            include_inactive=command.include_inactive
        )
        candidate_states = vacancy_current_state_repository.list_first_detail_backlog(
            limit=command.limit,
            include_inactive=command.include_inactive,
        )
        latest_attempt_numbers = (
            detail_fetch_attempt_repository.latest_attempt_numbers_by_vacancy_ids(
                [state.vacancy_id for state in candidate_states]
            )
            if candidate_states
            else {}
        )

        item_results = tuple(
            _fetch_one_backlog_item(
                state,
                latest_attempt_number=latest_attempt_numbers.get(state.vacancy_id, 0),
                fetch_vacancy_detail_step=fetch_vacancy_detail_step,
            )
            for state in candidate_states
        )
        backlog_size_after = vacancy_current_state_repository.count_first_detail_backlog(
            include_inactive=command.include_inactive
        )
    except Exception as error:
        record_operation_failed(
            LOGGER,
            operation="drain_first_detail_backlog",
            started_at=started_at,
            error_type=error.__class__.__name__,
            error_message=str(error),
            limit=command.limit,
            include_inactive=command.include_inactive,
            triggered_by=command.triggered_by,
        )
        raise

    status = (
        DRAIN_FIRST_DETAIL_BACKLOG_STATUS_COMPLETED_WITH_FAILURES
        if any(not item.resolved for item in item_results)
        else DRAIN_FIRST_DETAIL_BACKLOG_STATUS_SUCCEEDED
    )
    result = DrainFirstDetailBacklogResult(
        status=status,
        triggered_by=command.triggered_by,
        include_inactive=command.include_inactive,
        limit=command.limit,
        backlog_size_before=backlog_size_before,
        backlog_size_after=backlog_size_after,
        item_results=item_results,
    )
    if metrics_recorder is not None:
        metrics_recorder.set_first_detail_backlog(
            include_inactive=result.include_inactive,
            backlog_size=result.backlog_size_after,
        )
        metrics_recorder.record_first_detail_drain_attempt(
            include_inactive=result.include_inactive,
            outcome=result.status,
            selected_count=result.selected_count,
            succeeded_count=result.detail_fetch_succeeded,
            terminal_count=result.detail_fetch_terminal,
            failed_count=result.detail_fetch_failed,
        )

    if result.detail_fetch_failed > 0:
        record_operation_failed(
            LOGGER,
            operation="drain_first_detail_backlog",
            started_at=started_at,
            error_type="FirstDetailBacklogItemFailures",
            error_message=f"{result.detail_fetch_failed} detail fetch(es) failed",
            level=logging.WARNING,
            limit=result.limit,
            include_inactive=result.include_inactive,
            triggered_by=result.triggered_by,
            backlog_size_before=result.backlog_size_before,
            backlog_size_after=result.backlog_size_after,
            selected_count=result.selected_count,
            detail_fetch_succeeded=result.detail_fetch_succeeded,
            detail_fetch_terminal=result.detail_fetch_terminal,
            detail_fetch_failed=result.detail_fetch_failed,
        )
        return result

    record_operation_succeeded(
        LOGGER,
        operation="drain_first_detail_backlog",
        started_at=started_at,
        records_written={"vacancy_detail": result.detail_fetch_succeeded},
        limit=result.limit,
        include_inactive=result.include_inactive,
        triggered_by=result.triggered_by,
        backlog_size_before=result.backlog_size_before,
        backlog_size_after=result.backlog_size_after,
        selected_count=result.selected_count,
        detail_fetch_succeeded=result.detail_fetch_succeeded,
        detail_fetch_terminal=result.detail_fetch_terminal,
        detail_fetch_failed=result.detail_fetch_failed,
    )
    return result


def _fetch_one_backlog_item(
    state: VacancyCurrentState,
    *,
    latest_attempt_number: int,
    fetch_vacancy_detail_step: FetchVacancyDetailStep,
) -> FirstDetailBacklogItemResult:
    try:
        detail_result = fetch_vacancy_detail_step(
            FetchVacancyDetailCommand(
                vacancy_id=state.vacancy_id,
                reason=FIRST_DETAIL_BACKLOG_REASON,
                attempt=latest_attempt_number + 1,
                crawl_run_id=None,
            )
        )
    except Exception as error:
        LOGGER.warning(
            "first detail backlog item failed: vacancy_id=%s error=%s",
            state.vacancy_id,
            error,
        )
        return FirstDetailBacklogItemResult(
            vacancy_id=state.vacancy_id,
            detail_fetch_status=DetailFetchStatus.FAILED.value,
            detail_fetch_attempt_id=None,
            request_log_id=None,
            raw_payload_id=None,
            snapshot_id=None,
            error_message=str(error),
            exception_type=error.__class__.__name__,
        )

    return FirstDetailBacklogItemResult(
        vacancy_id=detail_result.vacancy_id,
        detail_fetch_status=detail_result.detail_fetch_status,
        detail_fetch_attempt_id=detail_result.detail_fetch_attempt_id,
        request_log_id=detail_result.request_log_id,
        raw_payload_id=detail_result.raw_payload_id,
        snapshot_id=detail_result.snapshot_id,
        error_message=detail_result.error_message,
    )
