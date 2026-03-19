from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Protocol
from uuid import UUID

from hhru_platform.application.commands.process_partition_v2 import (
    ProcessPartitionV2Command,
    ProcessPartitionV2Result,
)
from hhru_platform.domain.entities.crawl_partition import CrawlPartition
from hhru_platform.domain.entities.crawl_run import CrawlRun
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
class RunListEngineV2Command:
    crawl_run_id: UUID
    partition_limit: int | None = None

    def __post_init__(self) -> None:
        if self.partition_limit is not None and self.partition_limit < 1:
            raise ValueError("partition_limit must be greater than or equal to one")


@dataclass(slots=True, frozen=True)
class RunListEngineV2Result:
    status: str
    crawl_run_id: UUID
    partition_results: tuple[ProcessPartitionV2Result, ...]
    remaining_pending_terminal_partitions: tuple[CrawlPartition, ...]

    @property
    def partitions_attempted(self) -> int:
        return len(self.partition_results)

    @property
    def partitions_completed(self) -> int:
        return sum(1 for result in self.partition_results if result.status == "succeeded")

    @property
    def partitions_failed(self) -> int:
        return self.partitions_attempted - self.partitions_completed

    @property
    def pages_attempted(self) -> int:
        return sum(result.pages_attempted for result in self.partition_results)

    @property
    def pages_processed(self) -> int:
        return sum(result.pages_processed for result in self.partition_results)

    @property
    def vacancies_found(self) -> int:
        return sum(result.vacancies_found for result in self.partition_results)

    @property
    def vacancies_created(self) -> int:
        return sum(result.vacancies_created for result in self.partition_results)

    @property
    def seen_events_created(self) -> int:
        return sum(result.seen_events_created for result in self.partition_results)

    @property
    def saturated_partitions(self) -> int:
        return sum(1 for result in self.partition_results if result.saturated)

    @property
    def children_created_total(self) -> int:
        return sum(result.children_created_count for result in self.partition_results)

    @property
    def remaining_pending_terminal_count(self) -> int:
        return len(self.remaining_pending_terminal_partitions)


class CrawlRunRepository(Protocol):
    def get(self, run_id: UUID) -> CrawlRun | None:
        """Return a crawl run by id."""


class CrawlPartitionRepository(Protocol):
    def list_pending_terminal_by_run_id(
        self,
        run_id: UUID,
        *,
        limit: int | None = None,
    ) -> list[CrawlPartition]:
        """Return pending terminal partitions ordered for execution."""


class ProcessPartitionV2Step(Protocol):
    def __call__(self, command: ProcessPartitionV2Command) -> ProcessPartitionV2Result:
        """Process one planner v2 partition."""


def run_list_engine_v2(
    command: RunListEngineV2Command,
    crawl_run_repository: CrawlRunRepository,
    crawl_partition_repository: CrawlPartitionRepository,
    process_partition_v2_step: ProcessPartitionV2Step,
) -> RunListEngineV2Result:
    started_at = log_operation_started(
        LOGGER,
        operation="run_list_engine_v2",
        run_id=command.crawl_run_id,
        partition_limit=command.partition_limit,
    )
    try:
        crawl_run = crawl_run_repository.get(command.crawl_run_id)
        if crawl_run is None:
            raise CrawlRunNotFoundError(command.crawl_run_id)

        partition_results: list[ProcessPartitionV2Result] = []
        while command.partition_limit is None or len(partition_results) < command.partition_limit:
            pending_partitions = crawl_partition_repository.list_pending_terminal_by_run_id(
                command.crawl_run_id,
                limit=1,
            )
            if not pending_partitions:
                break

            partition = pending_partitions[0]
            partition_results.append(
                process_partition_v2_step(ProcessPartitionV2Command(partition_id=partition.id))
            )

        remaining_pending_terminal_partitions = tuple(
            crawl_partition_repository.list_pending_terminal_by_run_id(command.crawl_run_id)
        )
    except Exception as error:
        record_operation_failed(
            LOGGER,
            operation="run_list_engine_v2",
            started_at=started_at,
            error_type=error.__class__.__name__,
            error_message=str(error),
            run_id=command.crawl_run_id,
        )
        raise

    result = RunListEngineV2Result(
        status=(
            "failed"
            if any(item.status == "failed" for item in partition_results)
            else "succeeded"
        ),
        crawl_run_id=crawl_run.id,
        partition_results=tuple(partition_results),
        remaining_pending_terminal_partitions=remaining_pending_terminal_partitions,
    )
    if result.status == "failed":
        record_operation_failed(
            LOGGER,
            operation="run_list_engine_v2",
            started_at=started_at,
            error_type="RunListEngineV2PartitionFailures",
            error_message=(
                f"{result.partitions_failed} partition(s) failed during "
                "list engine execution"
            ),
            run_id=result.crawl_run_id,
            partitions_attempted=result.partitions_attempted,
            partitions_completed=result.partitions_completed,
            partitions_failed=result.partitions_failed,
            pages_attempted=result.pages_attempted,
            pages_processed=result.pages_processed,
            vacancies_found=result.vacancies_found,
            saturated_partitions=result.saturated_partitions,
            children_created_total=result.children_created_total,
            remaining_pending_terminal_count=result.remaining_pending_terminal_count,
        )
        return result

    record_operation_succeeded(
        LOGGER,
        operation="run_list_engine_v2",
        started_at=started_at,
        records_written={
            "crawl_partition": result.children_created_total,
            "vacancy": result.vacancies_found,
            "vacancy_seen_event": result.seen_events_created,
        },
        run_id=result.crawl_run_id,
        partitions_attempted=result.partitions_attempted,
        partitions_completed=result.partitions_completed,
        partitions_failed=result.partitions_failed,
        pages_attempted=result.pages_attempted,
        pages_processed=result.pages_processed,
        vacancies_found=result.vacancies_found,
        vacancies_created=result.vacancies_created,
        seen_events_created=result.seen_events_created,
        saturated_partitions=result.saturated_partitions,
        children_created_total=result.children_created_total,
        remaining_pending_terminal_count=result.remaining_pending_terminal_count,
    )
    return result
