from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest

from hhru_platform.application.commands.plan_sweep_v2 import (
    PlannerV2AreasNotReadyError,
    PlanRunV2Command,
    plan_sweep_v2,
)
from hhru_platform.application.commands.split_partition import (
    SplitPartitionCommand,
    split_partition,
)
from hhru_platform.application.policies.planner import (
    TIME_WINDOW_FALLBACK_START,
    build_time_window_partition_definition,
    serialize_split_datetime,
)
from hhru_platform.domain.entities.area import Area
from hhru_platform.domain.entities.crawl_partition import CrawlPartition
from hhru_platform.domain.entities.crawl_run import CrawlRun
from hhru_platform.domain.value_objects.enums import (
    CrawlPartitionCoverageStatus,
    CrawlPartitionStatus,
)


class InMemoryCrawlRunRepository:
    def __init__(self, crawl_run: CrawlRun | None) -> None:
        self._crawl_run = crawl_run

    def get(self, run_id: UUID) -> CrawlRun | None:
        if self._crawl_run is None or self._crawl_run.id != run_id:
            return None
        return self._crawl_run

    def set_partitions_total(self, run_id: UUID, partitions_total: int) -> CrawlRun:
        assert self._crawl_run is not None
        assert self._crawl_run.id == run_id
        self._crawl_run.partitions_total = partitions_total
        return self._crawl_run


class InMemoryAreaRepository:
    def __init__(
        self,
        *,
        root_areas: list[Area],
        children_by_hh_parent_id: dict[str, list[Area]] | None = None,
    ) -> None:
        self._root_areas = list(root_areas)
        self._children_by_hh_parent_id = children_by_hh_parent_id or {}

    def list_active_root_areas(self) -> list[Area]:
        return list(self._root_areas)

    def list_active_children_by_hh_area_id(self, parent_hh_area_id: str) -> list[Area]:
        return list(self._children_by_hh_parent_id.get(parent_hh_area_id, ()))


class InMemoryCrawlPartitionRepository:
    def __init__(self) -> None:
        self._partitions: dict[UUID, CrawlPartition] = {}

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
        partition = CrawlPartition(
            id=uuid4(),
            crawl_run_id=crawl_run_id,
            partition_key=partition_key,
            params_json=dict(params_json),
            status=status,
            pages_total_expected=None,
            pages_processed=0,
            items_seen=0,
            retry_count=0,
            started_at=None,
            finished_at=None,
            last_error_message=None,
            created_at=datetime(2026, 3, 19, 12, 0, tzinfo=UTC),
            parent_partition_id=parent_partition_id,
            depth=depth,
            split_dimension=split_dimension,
            split_value=split_value,
            scope_key=scope_key or partition_key,
            planner_policy_version=planner_policy_version,
            is_terminal=is_terminal,
            is_saturated=is_saturated,
            coverage_status=coverage_status,
        )
        self._partitions[partition.id] = partition
        return partition

    def get(self, partition_id: UUID) -> CrawlPartition | None:
        return self._partitions.get(partition_id)

    def list_by_run_id(self, run_id: UUID) -> list[CrawlPartition]:
        return sorted(
            (
                partition
                for partition in self._partitions.values()
                if partition.crawl_run_id == run_id
            ),
            key=lambda partition: partition.partition_key,
        )

    def list_children(self, parent_partition_id: UUID) -> list[CrawlPartition]:
        return sorted(
            (
                partition
                for partition in self._partitions.values()
                if partition.parent_partition_id == parent_partition_id
            ),
            key=lambda partition: partition.partition_key,
        )

    def mark_split_required(self, partition_id: UUID) -> CrawlPartition:
        partition = self._partitions[partition_id]
        partition.status = CrawlPartitionStatus.SPLIT_REQUIRED.value
        partition.is_saturated = True
        partition.is_terminal = False
        partition.coverage_status = CrawlPartitionCoverageStatus.SATURATED.value
        partition.finished_at = None
        partition.last_error_message = None
        return partition

    def mark_split_done(self, partition_id: UUID) -> CrawlPartition:
        partition = self._partitions[partition_id]
        partition.status = CrawlPartitionStatus.SPLIT_DONE.value
        partition.is_saturated = True
        partition.is_terminal = False
        partition.coverage_status = CrawlPartitionCoverageStatus.SPLIT.value
        partition.finished_at = datetime(2026, 3, 19, 12, 10, tzinfo=UTC)
        partition.last_error_message = None
        return partition

    def mark_unresolved(self, *, partition_id: UUID, error_message: str) -> CrawlPartition:
        partition = self._partitions[partition_id]
        partition.status = CrawlPartitionStatus.UNRESOLVED.value
        partition.is_saturated = True
        partition.is_terminal = True
        partition.coverage_status = CrawlPartitionCoverageStatus.UNRESOLVED.value
        partition.finished_at = datetime(2026, 3, 19, 12, 11, tzinfo=UTC)
        partition.last_error_message = error_message
        return partition


def _build_crawl_run() -> CrawlRun:
    return CrawlRun(
        id=uuid4(),
        run_type="weekly_sweep",
        status="created",
        started_at=datetime(2026, 3, 19, 12, 0, tzinfo=UTC),
        finished_at=None,
        triggered_by="pytest",
        config_snapshot_json={},
        partitions_total=0,
        partitions_done=0,
        partitions_failed=0,
        notes=None,
    )


def _build_area(
    *,
    hh_area_id: str,
    name: str,
    parent_area_id: UUID | None = None,
    level: int | None = None,
    path_text: str | None = None,
) -> Area:
    return Area(
        id=uuid4(),
        hh_area_id=hh_area_id,
        name=name,
        parent_area_id=parent_area_id,
        level=level,
        path_text=path_text,
        is_active=True,
    )


def test_plan_sweep_v2_creates_disjoint_root_area_partitions() -> None:
    crawl_run = _build_crawl_run()
    root_area_one = _build_area(hh_area_id="113", name="Russia", level=0, path_text="Russia")
    root_area_two = _build_area(hh_area_id="40", name="Kazakhstan", level=0, path_text="Kazakhstan")

    result = plan_sweep_v2(
        PlanRunV2Command(crawl_run_id=crawl_run.id),
        crawl_run_repository=InMemoryCrawlRunRepository(crawl_run),
        crawl_partition_repository=InMemoryCrawlPartitionRepository(),
        area_repository=InMemoryAreaRepository(root_areas=[root_area_one, root_area_two]),
    )

    assert len(result.created_partitions) == 2
    assert [partition.partition_key for partition in result.partitions] == ["area:40", "area:113"]
    assert all(partition.parent_partition_id is None for partition in result.partitions)
    assert all(partition.depth == 0 for partition in result.partitions)
    assert all(partition.scope_key == partition.partition_key for partition in result.partitions)
    assert all(partition.planner_policy_version == "v2" for partition in result.partitions)
    assert all(
        partition.params_json["params"]["area"] in {"113", "40"} for partition in result.partitions
    )
    assert crawl_run.partitions_total == 2


def test_plan_sweep_v2_raises_when_active_root_areas_are_missing() -> None:
    crawl_run = _build_crawl_run()

    with pytest.raises(PlannerV2AreasNotReadyError):
        plan_sweep_v2(
            PlanRunV2Command(crawl_run_id=crawl_run.id),
            crawl_run_repository=InMemoryCrawlRunRepository(crawl_run),
            crawl_partition_repository=InMemoryCrawlPartitionRepository(),
            area_repository=InMemoryAreaRepository(root_areas=[]),
        )


def test_split_partition_creates_child_area_partitions_and_is_idempotent() -> None:
    crawl_run = _build_crawl_run()
    root_area = _build_area(hh_area_id="113", name="Russia", level=0, path_text="Russia")
    child_area_one = _build_area(
        hh_area_id="1",
        name="Moscow",
        parent_area_id=root_area.id,
        level=1,
        path_text="Russia / Moscow",
    )
    child_area_two = _build_area(
        hh_area_id="2",
        name="Saint Petersburg",
        parent_area_id=root_area.id,
        level=1,
        path_text="Russia / Saint Petersburg",
    )

    run_repository = InMemoryCrawlRunRepository(crawl_run)
    partition_repository = InMemoryCrawlPartitionRepository()
    area_repository = InMemoryAreaRepository(
        root_areas=[root_area],
        children_by_hh_parent_id={"113": [child_area_one, child_area_two]},
    )
    plan_result = plan_sweep_v2(
        PlanRunV2Command(crawl_run_id=crawl_run.id),
        crawl_run_repository=run_repository,
        crawl_partition_repository=partition_repository,
        area_repository=area_repository,
    )
    parent_partition = plan_result.partitions[0]

    first_split_result = split_partition(
        SplitPartitionCommand(partition_id=parent_partition.id),
        crawl_partition_repository=partition_repository,
        crawl_run_repository=run_repository,
        area_repository=area_repository,
    )
    second_split_result = split_partition(
        SplitPartitionCommand(partition_id=parent_partition.id),
        crawl_partition_repository=partition_repository,
        crawl_run_repository=run_repository,
        area_repository=area_repository,
    )

    assert first_split_result.parent_partition.status == CrawlPartitionStatus.SPLIT_DONE.value
    assert first_split_result.parent_partition.is_terminal is False
    assert first_split_result.parent_partition.is_saturated is True
    assert first_split_result.parent_partition.coverage_status == "split"
    assert len(first_split_result.created_children) == 2
    assert len(first_split_result.children) == 2
    assert all(
        child.parent_partition_id == parent_partition.id
        for child in first_split_result.children
    )
    assert all(child.depth == 1 for child in first_split_result.children)
    assert {child.partition_key for child in first_split_result.children} == {"area:1", "area:2"}
    assert all(
        child.status == CrawlPartitionStatus.PENDING.value
        for child in first_split_result.children
    )
    assert len(second_split_result.created_children) == 0
    assert len(second_split_result.children) == 2


def test_split_partition_falls_back_to_time_window_when_area_split_has_no_children() -> None:
    crawl_run = _build_crawl_run()
    root_area = _build_area(hh_area_id="1", name="Moscow", level=1, path_text="Russia / Moscow")
    run_repository = InMemoryCrawlRunRepository(crawl_run)
    partition_repository = InMemoryCrawlPartitionRepository()
    area_repository = InMemoryAreaRepository(root_areas=[root_area], children_by_hh_parent_id={})
    plan_result = plan_sweep_v2(
        PlanRunV2Command(crawl_run_id=crawl_run.id),
        crawl_run_repository=run_repository,
        crawl_partition_repository=partition_repository,
        area_repository=area_repository,
    )
    parent_partition = plan_result.partitions[0]

    result = split_partition(
        SplitPartitionCommand(partition_id=parent_partition.id),
        crawl_partition_repository=partition_repository,
        crawl_run_repository=run_repository,
        area_repository=area_repository,
    )

    assert result.parent_partition.status == CrawlPartitionStatus.SPLIT_DONE.value
    assert result.parent_partition.coverage_status == CrawlPartitionCoverageStatus.SPLIT.value
    assert result.parent_partition.is_terminal is False
    assert result.parent_partition.is_saturated is True
    assert result.resolution_message is None
    assert len(result.created_children) == 2
    assert len(result.children) == 2
    assert all(child.parent_partition_id == parent_partition.id for child in result.children)
    assert all(child.depth == 1 for child in result.children)
    assert all(child.split_dimension == "time_window" for child in result.children)
    assert all(child.partition_key.startswith("time_window:1:") for child in result.children)
    assert {
        child.params_json["params"]["date_from"]  # type: ignore[index]
        for child in result.children
    } == {
        serialize_split_datetime(TIME_WINDOW_FALLBACK_START),
        serialize_split_datetime(
            TIME_WINDOW_FALLBACK_START
            + (crawl_run.started_at - TIME_WINDOW_FALLBACK_START) / 2
        ),
    }
    assert {
        child.params_json["params"]["date_to"]  # type: ignore[index]
        for child in result.children
    } == {
        serialize_split_datetime(
            TIME_WINDOW_FALLBACK_START
            + (crawl_run.started_at - TIME_WINDOW_FALLBACK_START) / 2
        ),
        serialize_split_datetime(crawl_run.started_at),
    }


def test_split_partition_recursively_bisects_existing_time_window_partition() -> None:
    crawl_run = _build_crawl_run()
    run_repository = InMemoryCrawlRunRepository(crawl_run)
    partition_repository = InMemoryCrawlPartitionRepository()
    area_repository = InMemoryAreaRepository(root_areas=[], children_by_hh_parent_id={})
    parent_definition = build_time_window_partition_definition(
        area_hh_id="1",
        date_from=datetime(2026, 3, 1, 0, 0, tzinfo=UTC),
        date_to=datetime(2026, 3, 2, 0, 0, tzinfo=UTC),
        crawl_run=crawl_run,
        area_name="Moscow",
        path_text="Russia / Moscow",
    )
    parent_partition = partition_repository.add(
        crawl_run_id=crawl_run.id,
        partition_key=parent_definition.partition_key,
        status=CrawlPartitionStatus.PENDING.value,
        params_json=dict(parent_definition.params_json),
        parent_partition_id=parent_definition.parent_partition_id,
        depth=parent_definition.depth,
        split_dimension=parent_definition.split_dimension,
        split_value=parent_definition.split_value,
        scope_key=parent_definition.scope_key,
        planner_policy_version=parent_definition.planner_policy_version,
        is_terminal=parent_definition.is_terminal,
        is_saturated=True,
        coverage_status=parent_definition.coverage_status,
    )

    result = split_partition(
        SplitPartitionCommand(partition_id=parent_partition.id),
        crawl_partition_repository=partition_repository,
        crawl_run_repository=run_repository,
        area_repository=area_repository,
    )

    assert result.parent_partition.status == CrawlPartitionStatus.SPLIT_DONE.value
    assert len(result.created_children) == 2
    assert all(child.split_dimension == "time_window" for child in result.children)
    assert all(child.parent_partition_id == parent_partition.id for child in result.children)
    assert all(child.depth == 1 for child in result.children)
    child_ranges = sorted(
        (
            child.params_json["params"]["date_from"],  # type: ignore[index]
            child.params_json["params"]["date_to"],  # type: ignore[index]
        )
        for child in result.children
    )
    assert child_ranges == [
        ("2026-03-01T00:00:00+00:00", "2026-03-01T12:00:00+00:00"),
        ("2026-03-01T12:00:00+00:00", "2026-03-02T00:00:00+00:00"),
    ]


def test_split_partition_marks_parent_unresolved_when_time_window_cannot_be_refined() -> None:
    crawl_run = _build_crawl_run()
    run_repository = InMemoryCrawlRunRepository(crawl_run)
    partition_repository = InMemoryCrawlPartitionRepository()
    area_repository = InMemoryAreaRepository(root_areas=[], children_by_hh_parent_id={})
    parent_definition = build_time_window_partition_definition(
        area_hh_id="1",
        date_from=datetime(2026, 3, 1, 0, 0, 0, tzinfo=UTC),
        date_to=datetime(2026, 3, 1, 0, 0, 1, tzinfo=UTC),
        crawl_run=crawl_run,
        area_name="Moscow",
        path_text="Russia / Moscow",
    )
    parent_partition = partition_repository.add(
        crawl_run_id=crawl_run.id,
        partition_key=parent_definition.partition_key,
        status=CrawlPartitionStatus.PENDING.value,
        params_json=dict(parent_definition.params_json),
        parent_partition_id=parent_definition.parent_partition_id,
        depth=parent_definition.depth,
        split_dimension=parent_definition.split_dimension,
        split_value=parent_definition.split_value,
        scope_key=parent_definition.scope_key,
        planner_policy_version=parent_definition.planner_policy_version,
        is_terminal=parent_definition.is_terminal,
        is_saturated=True,
        coverage_status=parent_definition.coverage_status,
    )

    result = split_partition(
        SplitPartitionCommand(partition_id=parent_partition.id),
        crawl_partition_repository=partition_repository,
        crawl_run_repository=run_repository,
        area_repository=area_repository,
    )

    assert result.parent_partition.status == CrawlPartitionStatus.UNRESOLVED.value
    assert result.parent_partition.coverage_status == CrawlPartitionCoverageStatus.UNRESOLVED.value
    assert result.parent_partition.is_terminal is True
    assert result.parent_partition.is_saturated is True
    assert result.children == ()
    assert result.resolution_message is not None
    assert "time-window split cannot refine" in result.resolution_message
