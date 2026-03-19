from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Protocol
from uuid import UUID

from hhru_platform.application.policies.planner import (
    PLANNER_POLICY_VERSION_V2,
    build_area_partition_definition,
)
from hhru_platform.domain.entities.area import Area
from hhru_platform.domain.entities.crawl_partition import CrawlPartition
from hhru_platform.domain.entities.crawl_run import CrawlRun
from hhru_platform.infrastructure.observability.operations import (
    log_operation_started,
    record_operation_failed,
    record_operation_succeeded,
)

LOGGER = logging.getLogger(__name__)


class CrawlPartitionNotFoundError(LookupError):
    def __init__(self, partition_id: UUID) -> None:
        super().__init__(f"crawl_partition not found: {partition_id}")
        self.partition_id = partition_id


class UnsupportedPartitionSplitError(ValueError):
    def __init__(self, partition_id: UUID, planner_policy_version: str) -> None:
        super().__init__(
            f"crawl_partition {partition_id} cannot be split by planner v2 semantics; "
            f"planner_policy_version={planner_policy_version}"
        )
        self.partition_id = partition_id
        self.planner_policy_version = planner_policy_version


class PartitionScopeConflictError(RuntimeError):
    def __init__(self, partition_key: str) -> None:
        super().__init__(f"crawl_partition scope conflict for partition_key={partition_key}")
        self.partition_key = partition_key


@dataclass(slots=True, frozen=True)
class SplitPartitionCommand:
    partition_id: UUID


@dataclass(slots=True, frozen=True)
class SplitPartitionResult:
    parent_partition: CrawlPartition
    created_children: tuple[CrawlPartition, ...]
    children: tuple[CrawlPartition, ...]
    resolution_message: str | None


class CrawlPartitionRepository(Protocol):
    def get(self, partition_id: UUID) -> CrawlPartition | None:
        """Return one crawl partition."""

    def add(
        self,
        *,
        crawl_run_id: UUID,
        partition_key: str,
        status: str,
        params_json: dict[str, object],
        parent_partition_id: UUID | None = None,
        depth: int = 0,
        split_dimension: str | None = None,
        split_value: str | None = None,
        scope_key: str | None = None,
        planner_policy_version: str = "v1",
        is_terminal: bool = True,
        is_saturated: bool = False,
        coverage_status: str = "unassessed",
    ) -> CrawlPartition:
        """Persist and return a crawl partition."""

    def list_by_run_id(self, run_id: UUID) -> list[CrawlPartition]:
        """Return all partitions for a crawl run."""

    def list_children(self, parent_partition_id: UUID) -> list[CrawlPartition]:
        """Return all child partitions for the parent."""

    def mark_split_required(self, partition_id: UUID) -> CrawlPartition:
        """Mark a partition as needing a split."""

    def mark_split_done(self, partition_id: UUID) -> CrawlPartition:
        """Mark a partition as successfully split into child scopes."""

    def mark_unresolved(self, *, partition_id: UUID, error_message: str) -> CrawlPartition:
        """Mark a partition as unresolved for coverage purposes."""


class CrawlRunRepository(Protocol):
    def get(self, run_id: UUID) -> CrawlRun | None:
        """Return a crawl run by id."""

    def set_partitions_total(self, run_id: UUID, partitions_total: int) -> CrawlRun:
        """Persist the total number of partitions for a crawl run."""


class AreaRepository(Protocol):
    def list_active_children_by_hh_area_id(self, parent_hh_area_id: str) -> list[Area]:
        """Return active child areas for a given hh parent area."""


def split_partition(
    command: SplitPartitionCommand,
    crawl_partition_repository: CrawlPartitionRepository,
    crawl_run_repository: CrawlRunRepository,
    area_repository: AreaRepository,
) -> SplitPartitionResult:
    started_at = log_operation_started(
        LOGGER,
        operation="split_partition",
        partition_id=command.partition_id,
    )
    try:
        parent_partition = crawl_partition_repository.get(command.partition_id)
        if parent_partition is None:
            raise CrawlPartitionNotFoundError(command.partition_id)
        if parent_partition.planner_policy_version != PLANNER_POLICY_VERSION_V2:
            raise UnsupportedPartitionSplitError(
                parent_partition.id,
                parent_partition.planner_policy_version,
            )

        existing_children = crawl_partition_repository.list_children(parent_partition.id)
        if parent_partition.status == "split_done" and existing_children:
            result = SplitPartitionResult(
                parent_partition=parent_partition,
                created_children=(),
                children=tuple(existing_children),
                resolution_message=None,
            )
            record_operation_succeeded(
                LOGGER,
                operation="split_partition",
                started_at=started_at,
                records_written={"crawl_partition": 0},
                partition_id=parent_partition.id,
                run_id=parent_partition.crawl_run_id,
                parent_status=parent_partition.status,
                children_created=0,
                children_total=len(result.children),
            )
            return result

        crawl_run = crawl_run_repository.get(parent_partition.crawl_run_id)
        if crawl_run is None:
            raise LookupError(f"crawl_run not found: {parent_partition.crawl_run_id}")

        parent_area_hh_id = _extract_partition_area_hh_id(parent_partition)
        if parent_area_hh_id is None:
            raise UnsupportedPartitionSplitError(
                parent_partition.id,
                parent_partition.planner_policy_version,
            )

        crawl_partition_repository.mark_split_required(parent_partition.id)
        child_areas = area_repository.list_active_children_by_hh_area_id(parent_area_hh_id)
        if not child_areas:
            unresolved_message = (
                f"active child areas not found for hh_area_id={parent_area_hh_id}; "
                "area-based split cannot refine this saturated partition"
            )
            unresolved_parent = crawl_partition_repository.mark_unresolved(
                partition_id=parent_partition.id,
                error_message=unresolved_message,
            )
            result = SplitPartitionResult(
                parent_partition=unresolved_parent,
                created_children=(),
                children=(),
                resolution_message=unresolved_message,
            )
            record_operation_succeeded(
                LOGGER,
                operation="split_partition",
                started_at=started_at,
                records_written={"crawl_partition": 0},
                partition_id=result.parent_partition.id,
                run_id=result.parent_partition.crawl_run_id,
                parent_status=result.parent_partition.status,
                children_created=0,
                children_total=0,
                resolution_message=result.resolution_message,
            )
            return result

        partitions_by_key = {
            partition.partition_key: partition
            for partition in crawl_partition_repository.list_by_run_id(
                parent_partition.crawl_run_id
            )
        }
        created_children: list[CrawlPartition] = []

        for child_area in child_areas:
            definition = build_area_partition_definition(
                area=child_area,
                crawl_run=crawl_run,
                parent_partition_id=parent_partition.id,
                depth=parent_partition.depth + 1,
            )
            existing_partition = partitions_by_key.get(definition.partition_key)
            if existing_partition is not None:
                if existing_partition.parent_partition_id != parent_partition.id:
                    raise PartitionScopeConflictError(definition.partition_key)
                continue

            created_child = crawl_partition_repository.add(
                crawl_run_id=parent_partition.crawl_run_id,
                partition_key=definition.partition_key,
                status="pending",
                params_json=dict(definition.params_json),
                parent_partition_id=definition.parent_partition_id,
                depth=definition.depth,
                split_dimension=definition.split_dimension,
                split_value=definition.split_value,
                scope_key=definition.scope_key,
                planner_policy_version=definition.planner_policy_version,
                is_terminal=definition.is_terminal,
                is_saturated=definition.is_saturated,
                coverage_status=definition.coverage_status,
            )
            created_children.append(created_child)
            partitions_by_key[created_child.partition_key] = created_child

        crawl_run_repository.set_partitions_total(
            parent_partition.crawl_run_id,
            partitions_total=len(partitions_by_key),
        )
        updated_parent = crawl_partition_repository.mark_split_done(parent_partition.id)
        children = tuple(crawl_partition_repository.list_children(parent_partition.id))
    except Exception as error:
        record_operation_failed(
            LOGGER,
            operation="split_partition",
            started_at=started_at,
            error_type=error.__class__.__name__,
            error_message=str(error),
            partition_id=command.partition_id,
        )
        raise

    result = SplitPartitionResult(
        parent_partition=updated_parent,
        created_children=tuple(created_children),
        children=children,
        resolution_message=None,
    )
    record_operation_succeeded(
        LOGGER,
        operation="split_partition",
        started_at=started_at,
        records_written={"crawl_partition": len(result.created_children)},
        partition_id=result.parent_partition.id,
        run_id=result.parent_partition.crawl_run_id,
        parent_status=result.parent_partition.status,
        children_created=len(result.created_children),
        children_total=len(result.children),
    )
    return result


def _extract_partition_area_hh_id(partition: CrawlPartition) -> str | None:
    if partition.split_value:
        return partition.split_value

    params = partition.params_json.get("params")
    if isinstance(params, dict):
        area_value = params.get("area")
        if isinstance(area_value, str) and area_value.strip():
            return area_value.strip()

    scope = partition.params_json.get("scope")
    if isinstance(scope, dict):
        area_value = scope.get("hh_area_id")
        if isinstance(area_value, str) and area_value.strip():
            return area_value.strip()

    return None
