from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID, uuid4

from hhru_platform.application.commands.process_list_page import ProcessListPageResult
from hhru_platform.application.commands.process_partition_v2 import (
    ProcessPartitionV2Command,
    ProcessPartitionV2Result,
    process_partition_v2,
)
from hhru_platform.application.commands.run_list_engine_v2 import (
    RunListEngineV2Command,
    run_list_engine_v2,
)
from hhru_platform.application.commands.split_partition import SplitPartitionResult
from hhru_platform.application.dto import StoredVacancyReference
from hhru_platform.application.policies.list_engine import (
    PartitionSaturationPolicyV1,
)
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


class InMemoryCrawlPartitionRepository:
    def __init__(self, partitions: list[CrawlPartition]) -> None:
        self._partitions = {partition.id: partition for partition in partitions}
        self.pending_marks = 0
        self.covered_marks = 0

    def get(self, partition_id: UUID) -> CrawlPartition | None:
        return self._partitions.get(partition_id)

    def list_pending_terminal_by_run_id(
        self,
        run_id: UUID,
        *,
        limit: int | None = None,
    ) -> list[CrawlPartition]:
        partitions = sorted(
            (
                partition
                for partition in self._partitions.values()
                if partition.crawl_run_id == run_id
                and partition.is_terminal
                and partition.status == CrawlPartitionStatus.PENDING.value
            ),
            key=lambda partition: (partition.depth, partition.partition_key),
        )
        if limit is None:
            return partitions
        return partitions[:limit]

    def mark_pending(self, partition_id: UUID) -> CrawlPartition:
        partition = self._partitions[partition_id]
        partition.status = CrawlPartitionStatus.PENDING.value
        partition.finished_at = None
        partition.last_error_message = None
        self.pending_marks += 1
        return partition

    def mark_covered(self, partition_id: UUID) -> CrawlPartition:
        partition = self._partitions[partition_id]
        partition.status = CrawlPartitionStatus.DONE.value
        partition.coverage_status = CrawlPartitionCoverageStatus.COVERED.value
        partition.is_terminal = True
        partition.is_saturated = False
        partition.finished_at = datetime(2026, 3, 20, 10, 0, tzinfo=UTC)
        partition.last_error_message = None
        self.covered_marks += 1
        return partition

    def add(self, partition: CrawlPartition) -> None:
        self._partitions[partition.id] = partition


def _build_crawl_run() -> CrawlRun:
    return CrawlRun(
        id=uuid4(),
        run_type="weekly_sweep",
        status="created",
        started_at=datetime(2026, 3, 20, 9, 0, tzinfo=UTC),
        finished_at=None,
        triggered_by="pytest",
        config_snapshot_json={},
        partitions_total=1,
        partitions_done=0,
        partitions_failed=0,
        notes=None,
    )


def _build_partition(
    *,
    crawl_run_id: UUID,
    partition_key: str = "area:113",
    status: str = "pending",
    pages_total_expected: int | None = None,
    pages_processed: int = 0,
    coverage_status: str = "unassessed",
    depth: int = 0,
    parent_partition_id: UUID | None = None,
) -> CrawlPartition:
    return CrawlPartition(
        id=uuid4(),
        crawl_run_id=crawl_run_id,
        partition_key=partition_key,
        params_json={"planner_policy": "area_exhaustive_v2", "params": {"area": partition_key}},
        status=status,
        pages_total_expected=pages_total_expected,
        pages_processed=pages_processed,
        items_seen=0,
        retry_count=0,
        started_at=None,
        finished_at=None,
        last_error_message=None,
        created_at=datetime(2026, 3, 20, 9, 0, tzinfo=UTC),
        parent_partition_id=parent_partition_id,
        depth=depth,
        split_dimension="area",
        split_value=partition_key.split(":")[-1],
        scope_key=partition_key,
        planner_policy_version="v2",
        is_terminal=True,
        is_saturated=False,
        coverage_status=coverage_status,
    )


def _page_result(
    *,
    partition_id: UUID,
    page: int,
    pages_total_expected: int,
    vacancy_suffix: str,
    partition_status: str = "done",
    error_message: str | None = None,
) -> ProcessListPageResult:
    return ProcessListPageResult(
        partition_id=partition_id,
        partition_status=partition_status,
        page=page,
        pages_total_expected=pages_total_expected,
        vacancies_processed=1 if error_message is None else 0,
        vacancies_created=1 if error_message is None else 0,
        seen_events_created=1 if error_message is None else 0,
        request_log_id=100 + page if error_message is None else None,
        raw_payload_id=200 + page if error_message is None else None,
        processed_vacancies=(
            [
                StoredVacancyReference(
                    id=uuid4(),
                    hh_vacancy_id=f"hh-{vacancy_suffix}",
                    name_current=f"Vacancy {vacancy_suffix}",
                )
            ]
            if error_message is None
            else []
        ),
        error_message=error_message,
    )


def test_partition_saturation_policy_v1_marks_threshold_as_saturated() -> None:
    policy = PartitionSaturationPolicyV1(pages_threshold=100)

    decision = policy.decide(pages_total_expected=100)

    assert decision.is_saturated is True
    assert decision.pages_total_expected == 100
    assert decision.reason is not None


def test_partition_saturation_policy_v1_keeps_smaller_scope_non_saturated() -> None:
    policy = PartitionSaturationPolicyV1(pages_threshold=100)

    decision = policy.decide(pages_total_expected=99)

    assert decision.is_saturated is False
    assert decision.pages_total_expected == 99
    assert decision.reason is None


def test_process_partition_v2_reads_all_pages_and_marks_leaf_covered() -> None:
    crawl_run = _build_crawl_run()
    partition = _build_partition(crawl_run_id=crawl_run.id)
    repository = InMemoryCrawlPartitionRepository([partition])
    observed_pages: list[int] = []

    def process_list_page_step(command):
        observed_pages.append(command.page or 0)
        partition.pages_processed += 1
        partition.pages_total_expected = 3
        partition.items_seen += 1
        return _page_result(
            partition_id=partition.id,
            page=command.page or 0,
            pages_total_expected=3,
            vacancy_suffix=str(command.page or 0),
        )

    def split_partition_step(command):
        raise AssertionError(f"unexpected split for {command.partition_id}")

    result = process_partition_v2(
        ProcessPartitionV2Command(partition_id=partition.id),
        crawl_partition_repository=repository,
        process_list_page_step=process_list_page_step,
        split_partition_step=split_partition_step,
        saturation_policy=PartitionSaturationPolicyV1(pages_threshold=100),
    )

    assert observed_pages == [0, 1, 2]
    assert result.status == "succeeded"
    assert result.final_partition_status == "done"
    assert result.final_coverage_status == "covered"
    assert result.saturated is False
    assert result.pages_attempted == 3
    assert result.pages_processed == 3
    assert result.vacancies_found == 3
    assert result.vacancies_created == 3
    assert result.seen_events_created == 3
    assert result.children_created_count == 0
    assert repository.pending_marks == 2
    assert repository.covered_marks == 1


def test_process_partition_v2_splits_saturated_partition_after_first_page() -> None:
    crawl_run = _build_crawl_run()
    partition = _build_partition(crawl_run_id=crawl_run.id)
    repository = InMemoryCrawlPartitionRepository([partition])
    child_partition = _build_partition(
        crawl_run_id=crawl_run.id,
        partition_key="area:1",
        depth=1,
        parent_partition_id=partition.id,
    )

    def process_list_page_step(command):
        partition.pages_processed += 1
        partition.pages_total_expected = 100
        partition.items_seen += 1
        return _page_result(
            partition_id=partition.id,
            page=command.page or 0,
            pages_total_expected=100,
            vacancy_suffix="root",
        )

    def split_partition_step(command):
        assert command.partition_id == partition.id
        partition.status = CrawlPartitionStatus.SPLIT_DONE.value
        partition.coverage_status = CrawlPartitionCoverageStatus.SPLIT.value
        partition.is_terminal = False
        partition.is_saturated = True
        repository.add(child_partition)
        return SplitPartitionResult(
            parent_partition=partition,
            created_children=(child_partition,),
            children=(child_partition,),
            resolution_message=None,
        )

    result = process_partition_v2(
        ProcessPartitionV2Command(partition_id=partition.id),
        crawl_partition_repository=repository,
        process_list_page_step=process_list_page_step,
        split_partition_step=split_partition_step,
        saturation_policy=PartitionSaturationPolicyV1(pages_threshold=100),
    )

    assert result.status == "succeeded"
    assert result.final_partition_status == "split_done"
    assert result.final_coverage_status == "split"
    assert result.saturated is True
    assert result.pages_attempted == 1
    assert result.pages_processed == 1
    assert result.children_created_count == 1
    assert result.children_total_count == 1
    assert result.saturation_reason is not None
    assert repository.covered_marks == 0


def test_run_list_engine_v2_processes_new_child_leaves_recursively() -> None:
    crawl_run = _build_crawl_run()
    root_partition = _build_partition(crawl_run_id=crawl_run.id, partition_key="area:113")
    repository = InMemoryCrawlPartitionRepository([root_partition])
    run_repository = InMemoryCrawlRunRepository(crawl_run)
    processed_order: list[UUID] = []
    child_one = _build_partition(
        crawl_run_id=crawl_run.id,
        partition_key="area:1",
        depth=1,
        parent_partition_id=root_partition.id,
    )
    child_two = _build_partition(
        crawl_run_id=crawl_run.id,
        partition_key="area:2",
        depth=1,
        parent_partition_id=root_partition.id,
    )

    def process_partition_v2_step(command: ProcessPartitionV2Command) -> ProcessPartitionV2Result:
        processed_order.append(command.partition_id)
        if command.partition_id == root_partition.id:
            root_partition.status = CrawlPartitionStatus.SPLIT_DONE.value
            root_partition.coverage_status = CrawlPartitionCoverageStatus.SPLIT.value
            root_partition.is_terminal = False
            root_partition.is_saturated = True
            repository.add(child_one)
            repository.add(child_two)
            run_repository.set_partitions_total(crawl_run.id, 3)
            return ProcessPartitionV2Result(
                partition_id=root_partition.id,
                crawl_run_id=crawl_run.id,
                final_partition_status=root_partition.status,
                final_coverage_status=root_partition.coverage_status,
                saturated=True,
                page_results=(
                    _page_result(
                        partition_id=root_partition.id,
                        page=0,
                        pages_total_expected=100,
                        vacancy_suffix="root",
                    ),
                ),
                split_result=SplitPartitionResult(
                    parent_partition=root_partition,
                    created_children=(child_one, child_two),
                    children=(child_one, child_two),
                    resolution_message=None,
                ),
                saturation_reason="threshold reached",
                error_message=None,
            )

        leaf_partition = repository.get(command.partition_id)
        assert leaf_partition is not None
        leaf_partition.status = CrawlPartitionStatus.DONE.value
        leaf_partition.coverage_status = CrawlPartitionCoverageStatus.COVERED.value
        return ProcessPartitionV2Result(
            partition_id=leaf_partition.id,
            crawl_run_id=crawl_run.id,
            final_partition_status=leaf_partition.status,
            final_coverage_status=leaf_partition.coverage_status,
            saturated=False,
            page_results=(
                _page_result(
                    partition_id=leaf_partition.id,
                    page=0,
                    pages_total_expected=1,
                    vacancy_suffix=leaf_partition.partition_key,
                ),
            ),
            split_result=None,
            saturation_reason=None,
            error_message=None,
        )

    result = run_list_engine_v2(
        RunListEngineV2Command(crawl_run_id=crawl_run.id),
        crawl_run_repository=run_repository,
        crawl_partition_repository=repository,
        process_partition_v2_step=process_partition_v2_step,
    )

    assert processed_order == [root_partition.id, child_one.id, child_two.id]
    assert result.status == "succeeded"
    assert result.partitions_attempted == 3
    assert result.partitions_completed == 3
    assert result.partitions_failed == 0
    assert result.saturated_partitions == 1
    assert result.children_created_total == 2
    assert result.remaining_pending_terminal_count == 0
