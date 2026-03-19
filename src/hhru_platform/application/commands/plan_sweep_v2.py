from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol
from uuid import UUID

from hhru_platform.application.commands.plan_sweep import (
    PlanRunCommand,
    PlanRunResult,
    plan_sweep,
)
from hhru_platform.application.policies.planner import AreaExhaustivePlannerPolicyV2
from hhru_platform.domain.entities.area import Area
from hhru_platform.domain.entities.crawl_partition import CrawlPartition
from hhru_platform.domain.entities.crawl_run import CrawlRun


class PlannerV2AreasNotReadyError(LookupError):
    def __init__(self) -> None:
        super().__init__(
            "planner v2 requires synced active root areas; "
            "run sync-dictionaries --name areas"
        )


@dataclass(slots=True, frozen=True)
class PlanRunV2Command:
    crawl_run_id: UUID


class CrawlRunRepository(Protocol):
    def get(self, run_id: UUID) -> CrawlRun | None:
        """Return a crawl run by id."""

    def set_partitions_total(self, run_id: UUID, partitions_total: int) -> CrawlRun:
        """Persist the total number of partitions for a crawl run."""


class AreaRepository(Protocol):
    def list_active_root_areas(self) -> list[Area]:
        """Return active top-level areas used as exhaustive planner roots."""


class CrawlPartitionRepository(Protocol):
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


def plan_sweep_v2(
    command: PlanRunV2Command,
    crawl_run_repository: CrawlRunRepository,
    crawl_partition_repository: CrawlPartitionRepository,
    area_repository: AreaRepository,
) -> PlanRunResult:
    root_areas = area_repository.list_active_root_areas()
    if not root_areas:
        raise PlannerV2AreasNotReadyError()

    return plan_sweep(
        command=PlanRunCommand(crawl_run_id=command.crawl_run_id),
        crawl_run_repository=crawl_run_repository,
        crawl_partition_repository=crawl_partition_repository,
        planner_policy=AreaExhaustivePlannerPolicyV2(root_areas=root_areas),
    )
