from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Protocol
from uuid import UUID

from hhru_platform.application.policies.planner import PartitionPlanDefinition
from hhru_platform.domain.entities.crawl_partition import CrawlPartition
from hhru_platform.domain.entities.crawl_run import CrawlRun
from hhru_platform.domain.value_objects.enums import CrawlPartitionStatus
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
class PlanRunCommand:
    crawl_run_id: UUID


@dataclass(slots=True, frozen=True)
class PlanRunResult:
    crawl_run_id: UUID
    created_partitions: list[CrawlPartition]
    partitions: list[CrawlPartition]


class CrawlRunRepository(Protocol):
    def get(self, run_id: UUID) -> CrawlRun | None:
        """Return a crawl run by id."""

    def set_partitions_total(self, run_id: UUID, partitions_total: int) -> CrawlRun:
        """Persist the total number of partitions for a crawl run."""


class CrawlPartitionRepository(Protocol):
    def add(
        self,
        *,
        crawl_run_id: UUID,
        partition_key: str,
        status: str,
        params_json: dict[str, object],
    ) -> CrawlPartition:
        """Persist and return a newly created crawl partition."""

    def list_by_run_id(self, run_id: UUID) -> list[CrawlPartition]:
        """Return all partitions for a crawl run."""


class PlannerPolicy(Protocol):
    def build(self, crawl_run: CrawlRun) -> list[PartitionPlanDefinition]:
        """Return the desired partition plan for the given crawl run."""


def plan_sweep(
    command: PlanRunCommand,
    crawl_run_repository: CrawlRunRepository,
    crawl_partition_repository: CrawlPartitionRepository,
    planner_policy: PlannerPolicy,
) -> PlanRunResult:
    started_at = log_operation_started(
        LOGGER,
        operation="plan_sweep",
        run_id=command.crawl_run_id,
    )
    try:
        crawl_run = crawl_run_repository.get(command.crawl_run_id)
        if crawl_run is None:
            raise CrawlRunNotFoundError(command.crawl_run_id)

        existing_partitions = {
            partition.partition_key: partition
            for partition in crawl_partition_repository.list_by_run_id(command.crawl_run_id)
        }
        created_partitions: list[CrawlPartition] = []
        planned_partitions: list[CrawlPartition] = []

        for definition in planner_policy.build(crawl_run):
            partition = existing_partitions.get(definition.partition_key)
            if partition is None:
                partition = crawl_partition_repository.add(
                    crawl_run_id=command.crawl_run_id,
                    partition_key=definition.partition_key,
                    status=CrawlPartitionStatus.PENDING.value,
                    params_json=dict(definition.params_json),
                )
                created_partitions.append(partition)
                existing_partitions[partition.partition_key] = partition

            planned_partitions.append(partition)

        crawl_run_repository.set_partitions_total(
            command.crawl_run_id,
            partitions_total=len(existing_partitions),
        )
    except Exception as error:
        record_operation_failed(
            LOGGER,
            operation="plan_sweep",
            started_at=started_at,
            error_type=error.__class__.__name__,
            error_message=str(error),
            level=logging.WARNING
            if isinstance(error, CrawlRunNotFoundError)
            else logging.ERROR,
            run_id=command.crawl_run_id,
        )
        raise

    result = PlanRunResult(
        crawl_run_id=command.crawl_run_id,
        created_partitions=created_partitions,
        partitions=planned_partitions,
    )
    record_operation_succeeded(
        LOGGER,
        operation="plan_sweep",
        started_at=started_at,
        records_written={"crawl_partition": len(created_partitions)},
        run_id=result.crawl_run_id,
        partitions_created=len(result.created_partitions),
        partitions_total=len(result.partitions),
    )
    return result
