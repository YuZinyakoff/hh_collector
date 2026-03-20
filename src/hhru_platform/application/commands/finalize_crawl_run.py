from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol
from uuid import UUID

from hhru_platform.domain.entities.crawl_partition import CrawlPartition
from hhru_platform.domain.entities.crawl_run import CrawlRun
from hhru_platform.domain.value_objects.enums import CrawlPartitionStatus
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
class FinalizeCrawlRunCommand:
    crawl_run_id: UUID
    final_status: str
    notes: str | None = None

    def __post_init__(self) -> None:
        normalized_final_status = self.final_status.strip()
        if not normalized_final_status:
            raise ValueError("final_status must not be empty")

        object.__setattr__(self, "final_status", normalized_final_status)


@dataclass(slots=True, frozen=True)
class FinalizeCrawlRunResult:
    crawl_run_id: UUID
    run_status: str
    partitions_done: int
    partitions_failed: int


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


def finalize_crawl_run(
    command: FinalizeCrawlRunCommand,
    crawl_run_repository: CrawlRunRepository,
    crawl_partition_repository: CrawlPartitionRepository,
    metrics_recorder: RunTerminalStatusMetricsRecorder | None = None,
) -> FinalizeCrawlRunResult:
    started_at = log_operation_started(
        LOGGER,
        operation="finalize_crawl_run",
        run_id=command.crawl_run_id,
        final_status=command.final_status,
    )
    try:
        crawl_run = crawl_run_repository.get(command.crawl_run_id)
        if crawl_run is None:
            raise CrawlRunNotFoundError(command.crawl_run_id)

        partitions = crawl_partition_repository.list_by_run_id(command.crawl_run_id)
        previous_run_status = crawl_run.status
        previous_finished_at = crawl_run.finished_at
        completed_run = crawl_run_repository.complete(
            run_id=command.crawl_run_id,
            status=command.final_status,
            finished_at=datetime.now(UTC),
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
                if partition.status
                in (
                    CrawlPartitionStatus.FAILED.value,
                    CrawlPartitionStatus.UNRESOLVED.value,
                )
            ),
            notes=command.notes,
        )
    except Exception as error:
        record_operation_failed(
            LOGGER,
            operation="finalize_crawl_run",
            started_at=started_at,
            error_type=error.__class__.__name__,
            error_message=str(error),
            run_id=command.crawl_run_id,
            final_status=command.final_status,
        )
        raise

    result = FinalizeCrawlRunResult(
        crawl_run_id=completed_run.id,
        run_status=completed_run.status,
        partitions_done=completed_run.partitions_done,
        partitions_failed=completed_run.partitions_failed,
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
        operation="finalize_crawl_run",
        started_at=started_at,
        run_id=result.crawl_run_id,
        run_status=result.run_status,
        partitions_done=result.partitions_done,
        partitions_failed=result.partitions_failed,
    )
    return result
