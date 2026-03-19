from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from hhru_platform.domain.entities.area import Area
from hhru_platform.domain.entities.crawl_run import CrawlRun
from hhru_platform.domain.value_objects.enums import CrawlPartitionCoverageStatus

PLANNER_POLICY_VERSION_V1 = "v1"
PLANNER_POLICY_VERSION_V2 = "v2"
AREA_SPLIT_DIMENSION = "area"
DEFAULT_LIST_PER_PAGE = 20


@dataclass(slots=True, frozen=True)
class PartitionPlanDefinition:
    partition_key: str
    params_json: dict[str, Any]
    parent_partition_id: UUID | None = None
    depth: int = 0
    split_dimension: str | None = None
    split_value: str | None = None
    scope_key: str | None = None
    planner_policy_version: str = PLANNER_POLICY_VERSION_V1
    is_terminal: bool = True
    is_saturated: bool = False
    coverage_status: str = CrawlPartitionCoverageStatus.UNASSESSED.value


class SinglePartitionPlannerPolicyV1:
    def build(self, crawl_run: CrawlRun) -> list[PartitionPlanDefinition]:
        return [
            PartitionPlanDefinition(
                partition_key="global-default",
                params_json={
                    "planner_policy": "single_partition_v1",
                    "scope": "global",
                    "run_type": crawl_run.run_type,
                },
                scope_key="global-default",
                planner_policy_version=PLANNER_POLICY_VERSION_V1,
            )
        ]


class AreaExhaustivePlannerPolicyV2:
    def __init__(
        self,
        root_areas: Sequence[Area],
        *,
        per_page: int = DEFAULT_LIST_PER_PAGE,
    ) -> None:
        self._root_areas = tuple(root_areas)
        self._per_page = per_page

    def build(self, crawl_run: CrawlRun) -> list[PartitionPlanDefinition]:
        return [
            build_area_partition_definition(
                area=area,
                crawl_run=crawl_run,
                per_page=self._per_page,
            )
            for area in _sorted_areas(self._root_areas)
        ]


def build_area_partition_definition(
    *,
    area: Area,
    crawl_run: CrawlRun,
    parent_partition_id: UUID | None = None,
    depth: int = 0,
    per_page: int = DEFAULT_LIST_PER_PAGE,
) -> PartitionPlanDefinition:
    scope_key = build_area_scope_key(area.hh_area_id)
    return PartitionPlanDefinition(
        partition_key=scope_key,
        scope_key=scope_key,
        parent_partition_id=parent_partition_id,
        depth=depth,
        split_dimension=AREA_SPLIT_DIMENSION,
        split_value=area.hh_area_id,
        planner_policy_version=PLANNER_POLICY_VERSION_V2,
        is_terminal=True,
        is_saturated=False,
        coverage_status=CrawlPartitionCoverageStatus.UNASSESSED.value,
        params_json={
            "planner_policy": "area_exhaustive_v2",
            "planner_policy_version": PLANNER_POLICY_VERSION_V2,
            "scope": {
                "dimension": AREA_SPLIT_DIMENSION,
                "scope_key": scope_key,
                "hh_area_id": area.hh_area_id,
                "area_name": area.name,
                "path_text": area.path_text,
                "depth": depth,
            },
            "params": {
                "area": area.hh_area_id,
                "page": 0,
                "per_page": per_page,
            },
            "run_type": crawl_run.run_type,
        },
    )


def build_area_scope_key(hh_area_id: str) -> str:
    normalized_hh_area_id = hh_area_id.strip()
    if not normalized_hh_area_id:
        raise ValueError("hh_area_id must not be empty")
    return f"{AREA_SPLIT_DIMENSION}:{normalized_hh_area_id}"


def _sorted_areas(areas: Sequence[Area]) -> list[Area]:
    return sorted(
        areas,
        key=lambda area: (
            area.level if area.level is not None else -1,
            area.path_text or "",
            area.name,
            area.hh_area_id,
        ),
    )
