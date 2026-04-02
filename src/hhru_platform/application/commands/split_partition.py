from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol
from uuid import UUID

from hhru_platform.application.policies.planner import (
    AREA_SPLIT_DIMENSION,
    PLANNER_POLICY_VERSION_V2,
    TIME_WINDOW_FALLBACK_START,
    TIME_WINDOW_SPLIT_DIMENSION,
    PartitionPlanDefinition,
    build_area_partition_definition,
    build_time_window_partition_definition,
    normalize_split_datetime,
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
MIN_TIME_WINDOW_SPLIT_SECONDS = 2


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

        crawl_partition_repository.mark_split_required(parent_partition.id)
        child_definitions, unresolved_message = _build_child_partition_definitions(
            parent_partition=parent_partition,
            crawl_run=crawl_run,
            area_repository=area_repository,
        )
        if not child_definitions:
            if unresolved_message is None:
                unresolved_message = (
                    "split_partition could not build child scopes "
                    "for a saturated planner v2 partition"
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

        for definition in child_definitions:
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


def _build_child_partition_definitions(
    *,
    parent_partition: CrawlPartition,
    crawl_run: CrawlRun,
    area_repository: AreaRepository,
) -> tuple[list[PartitionPlanDefinition], str | None]:
    partition_dimension = _extract_partition_split_dimension(parent_partition)
    if partition_dimension == AREA_SPLIT_DIMENSION:
        return _build_area_child_definitions(
            parent_partition=parent_partition,
            crawl_run=crawl_run,
            area_repository=area_repository,
        )
    if partition_dimension == TIME_WINDOW_SPLIT_DIMENSION:
        return _build_time_window_child_definitions(
            parent_partition=parent_partition,
            crawl_run=crawl_run,
        )
    raise UnsupportedPartitionSplitError(
        parent_partition.id,
        parent_partition.planner_policy_version,
    )


def _build_area_child_definitions(
    *,
    parent_partition: CrawlPartition,
    crawl_run: CrawlRun,
    area_repository: AreaRepository,
) -> tuple[list[PartitionPlanDefinition], str | None]:
    parent_area_hh_id = _extract_partition_area_hh_id(parent_partition)
    if parent_area_hh_id is None:
        raise UnsupportedPartitionSplitError(
            parent_partition.id,
            parent_partition.planner_policy_version,
        )

    child_areas = area_repository.list_active_children_by_hh_area_id(parent_area_hh_id)
    if child_areas:
        return (
            [
                build_area_partition_definition(
                    area=child_area,
                    crawl_run=crawl_run,
                    parent_partition_id=parent_partition.id,
                    depth=parent_partition.depth + 1,
                )
                for child_area in child_areas
            ],
            None,
        )

    return _build_time_window_child_definitions(
        parent_partition=parent_partition,
        crawl_run=crawl_run,
    )


def _build_time_window_child_definitions(
    *,
    parent_partition: CrawlPartition,
    crawl_run: CrawlRun,
) -> tuple[list[PartitionPlanDefinition], str | None]:
    area_hh_id = _extract_partition_area_hh_id(parent_partition)
    if area_hh_id is None:
        raise UnsupportedPartitionSplitError(
            parent_partition.id,
            parent_partition.planner_policy_version,
        )

    time_window = _extract_partition_time_window(parent_partition)
    if time_window is None:
        window_start = TIME_WINDOW_FALLBACK_START
        anchor_end = crawl_run.started_at or parent_partition.created_at or datetime.now(UTC)
        window_end = normalize_split_datetime(anchor_end)
    else:
        window_start, window_end = time_window

    midpoint = _compute_time_window_midpoint(window_start=window_start, window_end=window_end)
    if midpoint is None:
        unresolved_message = (
            "time-window split cannot refine this saturated partition further; "
            f"hh_area_id={area_hh_id} interval={window_start.isoformat()}..{window_end.isoformat()}"
        )
        return ([], unresolved_message)

    area_name = _extract_partition_scope_text(parent_partition, "area_name")
    path_text = _extract_partition_scope_text(parent_partition, "path_text")
    definitions = [
        build_time_window_partition_definition(
            area_hh_id=area_hh_id,
            date_from=window_start,
            date_to=midpoint,
            crawl_run=crawl_run,
            parent_partition_id=parent_partition.id,
            depth=parent_partition.depth + 1,
            area_name=area_name,
            path_text=path_text,
        ),
        build_time_window_partition_definition(
            area_hh_id=area_hh_id,
            date_from=midpoint,
            date_to=window_end,
            crawl_run=crawl_run,
            parent_partition_id=parent_partition.id,
            depth=parent_partition.depth + 1,
            area_name=area_name,
            path_text=path_text,
        ),
    ]
    return (definitions, None)


def _extract_partition_split_dimension(partition: CrawlPartition) -> str | None:
    if partition.split_dimension:
        return partition.split_dimension

    scope = partition.params_json.get("scope")
    if isinstance(scope, dict):
        dimension = scope.get("dimension")
        if isinstance(dimension, str) and dimension.strip():
            return dimension.strip()

    return None


def _extract_partition_area_hh_id(partition: CrawlPartition) -> str | None:
    if partition.split_dimension == AREA_SPLIT_DIMENSION and partition.split_value:
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


def _extract_partition_time_window(partition: CrawlPartition) -> tuple[datetime, datetime] | None:
    params = partition.params_json.get("params")
    if isinstance(params, dict):
        time_window = _parse_time_window(
            date_from_value=params.get("date_from"),
            date_to_value=params.get("date_to"),
        )
        if time_window is not None:
            return time_window

    scope = partition.params_json.get("scope")
    if isinstance(scope, dict):
        return _parse_time_window(
            date_from_value=scope.get("date_from"),
            date_to_value=scope.get("date_to"),
        )

    return None


def _parse_time_window(
    *,
    date_from_value: object,
    date_to_value: object,
) -> tuple[datetime, datetime] | None:
    if not isinstance(date_from_value, str) or not isinstance(date_to_value, str):
        return None
    date_from = _parse_split_datetime(date_from_value)
    date_to = _parse_split_datetime(date_to_value)
    if date_from is None or date_to is None:
        return None
    if date_to <= date_from:
        return None
    return (date_from, date_to)


def _parse_split_datetime(value: str) -> datetime | None:
    normalized_value = value.strip()
    if not normalized_value:
        return None
    try:
        parsed = datetime.fromisoformat(normalized_value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return normalize_split_datetime(parsed)


def _compute_time_window_midpoint(
    *,
    window_start: datetime,
    window_end: datetime,
) -> datetime | None:
    normalized_start = normalize_split_datetime(window_start)
    normalized_end = normalize_split_datetime(window_end)
    if normalized_end <= normalized_start:
        return None

    total_seconds = int((normalized_end - normalized_start).total_seconds())
    if total_seconds < MIN_TIME_WINDOW_SPLIT_SECONDS:
        return None

    midpoint_offset_seconds = total_seconds // 2
    midpoint = normalized_start + timedelta(seconds=midpoint_offset_seconds)
    if midpoint <= normalized_start or midpoint >= normalized_end:
        return None
    return midpoint


def _extract_partition_scope_text(partition: CrawlPartition, field_name: str) -> str | None:
    scope = partition.params_json.get("scope")
    if not isinstance(scope, dict):
        return None
    value = scope.get(field_name)
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None
