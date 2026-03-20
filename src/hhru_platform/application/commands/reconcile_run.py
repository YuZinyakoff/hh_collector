from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol
from uuid import UUID

from hhru_platform.domain.entities.crawl_partition import CrawlPartition
from hhru_platform.domain.entities.crawl_run import CrawlRun
from hhru_platform.domain.entities.vacancy_current_state import (
    VacancyCurrentState,
    VacancyCurrentStateReconciliationUpdate,
)
from hhru_platform.domain.value_objects.enums import CrawlPartitionStatus, CrawlRunStatus
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


class CrawlRunNotFoundError(LookupError):
    def __init__(self, crawl_run_id: UUID) -> None:
        super().__init__(f"crawl_run not found: {crawl_run_id}")
        self.crawl_run_id = crawl_run_id


@dataclass(slots=True, frozen=True)
class ReconcileRunCommand:
    crawl_run_id: UUID
    final_run_status: str = CrawlRunStatus.COMPLETED.value
    notes: str | None = None

    def __post_init__(self) -> None:
        normalized_final_run_status = self.final_run_status.strip()
        if not normalized_final_run_status:
            raise ValueError("final_run_status must not be empty")

        object.__setattr__(self, "final_run_status", normalized_final_run_status)


@dataclass(slots=True, frozen=True)
class ReconcileRunResult:
    crawl_run_id: UUID
    observed_in_run_count: int
    missing_updated_count: int
    marked_inactive_count: int
    run_status: str


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
        """Mark a crawl run as completed and persist final counters."""


class CrawlPartitionRepository(Protocol):
    def list_by_run_id(self, run_id: UUID) -> list[CrawlPartition]:
        """Return all partitions for a crawl run."""


class VacancySeenEventRepository(Protocol):
    def list_distinct_vacancy_ids_by_run(self, crawl_run_id: UUID) -> list[UUID]:
        """Return distinct vacancy identifiers observed in the run."""


class VacancyCurrentStateRepository(Protocol):
    def list_all(self) -> list[VacancyCurrentState]:
        """Return all current vacancy states."""

    def apply_reconciliation_updates(
        self,
        *,
        updated_at: datetime,
        updates: list[VacancyCurrentStateReconciliationUpdate],
    ) -> int:
        """Persist reconciliation changes for current vacancy states."""


class ReconciliationPolicy(Protocol):
    def decide(
        self,
        *,
        vacancy_state: VacancyCurrentState,
        seen_in_run: bool,
        crawl_run_id: UUID,
    ) -> VacancyCurrentStateReconciliationUpdate:
        """Return the next current-state values for reconciliation."""


def reconcile_run(
    command: ReconcileRunCommand,
    crawl_run_repository: CrawlRunRepository,
    crawl_partition_repository: CrawlPartitionRepository,
    vacancy_seen_event_repository: VacancySeenEventRepository,
    vacancy_current_state_repository: VacancyCurrentStateRepository,
    reconciliation_policy: ReconciliationPolicy,
    metrics_recorder: RunTerminalStatusMetricsRecorder | None = None,
) -> ReconcileRunResult:
    started_at = log_operation_started(
        LOGGER,
        operation="reconcile_run",
        run_id=command.crawl_run_id,
    )
    try:
        crawl_run = crawl_run_repository.get(command.crawl_run_id)
        if crawl_run is None:
            raise CrawlRunNotFoundError(command.crawl_run_id)

        observed_vacancy_ids = set(
            vacancy_seen_event_repository.list_distinct_vacancy_ids_by_run(command.crawl_run_id)
        )
        current_states = vacancy_current_state_repository.list_all()
        reconciled_at = datetime.now(UTC)
        previous_run_status = crawl_run.status
        previous_finished_at = crawl_run.finished_at

        updates: list[VacancyCurrentStateReconciliationUpdate] = []
        missing_updated_count = 0
        marked_inactive_count = 0

        for current_state in current_states:
            seen_in_run = current_state.vacancy_id in observed_vacancy_ids
            update = reconciliation_policy.decide(
                vacancy_state=current_state,
                seen_in_run=seen_in_run,
                crawl_run_id=command.crawl_run_id,
            )
            updates.append(update)

            if not seen_in_run:
                missing_updated_count += 1
            if not current_state.is_probably_inactive and update.is_probably_inactive:
                marked_inactive_count += 1

        vacancy_current_state_repository.apply_reconciliation_updates(
            updated_at=reconciled_at,
            updates=updates,
        )

        partitions = crawl_partition_repository.list_by_run_id(command.crawl_run_id)
        completed_run = crawl_run_repository.complete(
            run_id=command.crawl_run_id,
            status=command.final_run_status,
            finished_at=reconciled_at,
            partitions_done=sum(
                1
                for partition in partitions
                if partition.status
                in (
                    CrawlPartitionStatus.DONE.value,
                    CrawlPartitionStatus.SPLIT_DONE.value,
                )
            ),
            partitions_failed=sum(
                1
                for partition in partitions
                if partition.status == CrawlPartitionStatus.FAILED.value
                or partition.status == CrawlPartitionStatus.UNRESOLVED.value
            ),
            notes=command.notes,
        )
    except Exception as error:
        record_operation_failed(
            LOGGER,
            operation="reconcile_run",
            started_at=started_at,
            error_type=error.__class__.__name__,
            error_message=str(error),
            level=logging.WARNING if isinstance(error, CrawlRunNotFoundError) else logging.ERROR,
            run_id=command.crawl_run_id,
        )
        raise

    result = ReconcileRunResult(
        crawl_run_id=completed_run.id,
        observed_in_run_count=len(observed_vacancy_ids),
        missing_updated_count=missing_updated_count,
        marked_inactive_count=marked_inactive_count,
        run_status=completed_run.status,
    )
    publish_run_terminal_status(
        metrics_recorder,
        run_type=completed_run.run_type,
        previous_status=previous_run_status,
        previous_finished_at=previous_finished_at,
        current_status=completed_run.status,
        recorded_at=completed_run.finished_at,
    )
    record_operation_succeeded(
        LOGGER,
        operation="reconcile_run",
        started_at=started_at,
        records_written={"vacancy_current_state": len(updates)},
        run_id=result.crawl_run_id,
        observed_in_run_count=result.observed_in_run_count,
        missing_updated_count=result.missing_updated_count,
        marked_inactive_count=result.marked_inactive_count,
        run_status=result.run_status,
    )
    return result
