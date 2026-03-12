from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest

from hhru_platform.application.commands.plan_sweep import (
    CrawlRunNotFoundError,
    PlanRunCommand,
    plan_sweep,
)
from hhru_platform.application.policies.planner import (
    PartitionPlanDefinition,
    SinglePartitionPlannerPolicyV1,
)
from hhru_platform.domain.entities.crawl_partition import CrawlPartition
from hhru_platform.domain.entities.crawl_run import CrawlRun


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


class InMemoryCrawlPartitionRepository:
    def __init__(self) -> None:
        self._partitions: list[CrawlPartition] = []

    def add(
        self,
        *,
        crawl_run_id: UUID,
        partition_key: str,
        status: str,
        params_json: dict[str, object],
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
            created_at=datetime(2026, 3, 12, 12, 0, tzinfo=UTC),
        )
        self._partitions.append(partition)
        return partition

    def list_by_run_id(self, run_id: UUID) -> list[CrawlPartition]:
        return [partition for partition in self._partitions if partition.crawl_run_id == run_id]


class StaticPlannerPolicy:
    def __init__(self, definitions: list[PartitionPlanDefinition]) -> None:
        self._definitions = definitions

    def build(self, crawl_run: CrawlRun) -> list[PartitionPlanDefinition]:
        return list(self._definitions)


def _build_crawl_run() -> CrawlRun:
    return CrawlRun(
        id=uuid4(),
        run_type="weekly_sweep",
        status="created",
        started_at=datetime(2026, 3, 12, 12, 0, tzinfo=UTC),
        finished_at=None,
        triggered_by="pytest",
        config_snapshot_json={},
        partitions_total=0,
        partitions_done=0,
        partitions_failed=0,
        notes=None,
    )


def test_plan_sweep_creates_pending_partition_and_updates_total() -> None:
    crawl_run = _build_crawl_run()
    run_repository = InMemoryCrawlRunRepository(crawl_run)
    partition_repository = InMemoryCrawlPartitionRepository()

    result = plan_sweep(
        PlanRunCommand(crawl_run_id=crawl_run.id),
        run_repository,
        partition_repository,
        SinglePartitionPlannerPolicyV1(),
    )

    assert len(result.created_partitions) == 1
    assert len(result.partitions) == 1
    assert result.partitions[0].partition_key == "global-default"
    assert result.partitions[0].status == "pending"
    assert result.partitions[0].params_json["planner_policy"] == "single_partition_v1"
    assert crawl_run.partitions_total == 1


def test_plan_sweep_is_idempotent_for_existing_partition_key() -> None:
    crawl_run = _build_crawl_run()
    run_repository = InMemoryCrawlRunRepository(crawl_run)
    partition_repository = InMemoryCrawlPartitionRepository()
    policy = StaticPlannerPolicy(
        [
            PartitionPlanDefinition(
                partition_key="global-default",
                params_json={"planner_policy": "static"},
            )
        ]
    )

    first_result = plan_sweep(
        PlanRunCommand(crawl_run_id=crawl_run.id),
        run_repository,
        partition_repository,
        policy,
    )
    second_result = plan_sweep(
        PlanRunCommand(crawl_run_id=crawl_run.id),
        run_repository,
        partition_repository,
        policy,
    )

    assert len(first_result.created_partitions) == 1
    assert len(second_result.created_partitions) == 0
    assert second_result.partitions[0].id == first_result.partitions[0].id


def test_plan_sweep_raises_for_missing_crawl_run() -> None:
    run_repository = InMemoryCrawlRunRepository(None)
    partition_repository = InMemoryCrawlPartitionRepository()

    with pytest.raises(CrawlRunNotFoundError):
        plan_sweep(
            PlanRunCommand(crawl_run_id=uuid4()),
            run_repository,
            partition_repository,
            SinglePartitionPlannerPolicyV1(),
        )
