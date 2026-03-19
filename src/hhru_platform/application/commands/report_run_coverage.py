from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Protocol
from uuid import UUID

from hhru_platform.domain.entities.crawl_partition import CrawlPartition
from hhru_platform.domain.entities.crawl_run import CrawlRun
from hhru_platform.domain.value_objects.enums import (
    CrawlPartitionCoverageStatus,
    CrawlPartitionStatus,
)


class CrawlRunNotFoundError(LookupError):
    def __init__(self, crawl_run_id: UUID) -> None:
        super().__init__(f"crawl_run not found: {crawl_run_id}")
        self.crawl_run_id = crawl_run_id


@dataclass(slots=True, frozen=True)
class ReportRunCoverageCommand:
    crawl_run_id: UUID


@dataclass(slots=True, frozen=True)
class RunCoverageSummary:
    crawl_run_id: UUID
    run_type: str
    run_status: str
    total_partitions: int
    root_partitions: int
    terminal_partitions: int
    covered_terminal_partitions: int
    pending_partitions: int
    pending_terminal_partitions: int
    running_partitions: int
    split_partitions: int
    unresolved_partitions: int
    failed_partitions: int
    coverage_ratio: float

    @property
    def is_fully_covered(self) -> bool:
        if self.terminal_partitions == 0:
            return False
        return (
            self.covered_terminal_partitions == self.terminal_partitions
            and self.pending_terminal_partitions == 0
            and self.running_partitions == 0
            and self.unresolved_partitions == 0
            and self.failed_partitions == 0
        )


@dataclass(slots=True, frozen=True)
class RunCoverageTreeRow:
    partition_id: UUID
    parent_partition_id: UUID | None
    depth: int
    scope_key: str
    status: str
    coverage_status: str
    is_terminal: bool
    is_saturated: bool


@dataclass(slots=True, frozen=True)
class RunCoverageReport:
    crawl_run: CrawlRun
    summary: RunCoverageSummary
    tree_rows: tuple[RunCoverageTreeRow, ...]


class CrawlRunRepository(Protocol):
    def get(self, run_id: UUID) -> CrawlRun | None:
        """Return a crawl run by id."""


class CrawlPartitionRepository(Protocol):
    def list_by_run_id(self, run_id: UUID) -> list[CrawlPartition]:
        """Return all partitions for a crawl run."""


class RunCoverageMetricsRecorder(Protocol):
    def set_run_tree_coverage(
        self,
        *,
        run_id: str,
        run_type: str,
        coverage_ratio: float,
        covered_terminal_partitions: int,
        pending_terminal_partitions: int,
        split_partitions: int,
        unresolved_partitions: int,
    ) -> None:
        """Persist the latest run tree coverage gauges."""


def report_run_coverage(
    command: ReportRunCoverageCommand,
    crawl_run_repository: CrawlRunRepository,
    crawl_partition_repository: CrawlPartitionRepository,
    metrics_recorder: RunCoverageMetricsRecorder | None = None,
) -> RunCoverageReport:
    crawl_run = crawl_run_repository.get(command.crawl_run_id)
    if crawl_run is None:
        raise CrawlRunNotFoundError(command.crawl_run_id)

    partitions = crawl_partition_repository.list_by_run_id(command.crawl_run_id)
    summary = _build_summary(crawl_run=crawl_run, partitions=partitions)
    if metrics_recorder is not None:
        metrics_recorder.set_run_tree_coverage(
            run_id=str(crawl_run.id),
            run_type=crawl_run.run_type,
            coverage_ratio=summary.coverage_ratio,
            covered_terminal_partitions=summary.covered_terminal_partitions,
            pending_terminal_partitions=summary.pending_terminal_partitions,
            split_partitions=summary.split_partitions,
            unresolved_partitions=summary.unresolved_partitions,
        )

    return RunCoverageReport(
        crawl_run=crawl_run,
        summary=summary,
        tree_rows=_build_tree_rows(partitions),
    )


def _build_summary(
    *,
    crawl_run: CrawlRun,
    partitions: list[CrawlPartition],
) -> RunCoverageSummary:
    partition_ids = {partition.id for partition in partitions}
    root_partitions = sum(
        1
        for partition in partitions
        if partition.parent_partition_id is None
        or partition.parent_partition_id not in partition_ids
    )
    terminal_partitions = sum(1 for partition in partitions if partition.is_terminal)
    covered_terminal_partitions = sum(
        1
        for partition in partitions
        if partition.is_terminal
        and partition.coverage_status == CrawlPartitionCoverageStatus.COVERED.value
    )
    pending_partitions = sum(
        1 for partition in partitions if partition.status == CrawlPartitionStatus.PENDING.value
    )
    pending_terminal_partitions = sum(
        1
        for partition in partitions
        if partition.is_terminal and partition.status == CrawlPartitionStatus.PENDING.value
    )
    running_partitions = sum(
        1 for partition in partitions if partition.status == CrawlPartitionStatus.RUNNING.value
    )
    split_partitions = sum(
        1
        for partition in partitions
        if partition.status
        in (
            CrawlPartitionStatus.SPLIT_REQUIRED.value,
            CrawlPartitionStatus.SPLIT_DONE.value,
        )
        or partition.coverage_status
        in (
            CrawlPartitionCoverageStatus.SATURATED.value,
            CrawlPartitionCoverageStatus.SPLIT.value,
        )
    )
    unresolved_partitions = sum(
        1
        for partition in partitions
        if partition.status == CrawlPartitionStatus.UNRESOLVED.value
        or partition.coverage_status == CrawlPartitionCoverageStatus.UNRESOLVED.value
    )
    failed_partitions = sum(
        1 for partition in partitions if partition.status == CrawlPartitionStatus.FAILED.value
    )
    coverage_ratio = (
        covered_terminal_partitions / terminal_partitions if terminal_partitions > 0 else 0.0
    )

    return RunCoverageSummary(
        crawl_run_id=crawl_run.id,
        run_type=crawl_run.run_type,
        run_status=crawl_run.status,
        total_partitions=len(partitions),
        root_partitions=root_partitions,
        terminal_partitions=terminal_partitions,
        covered_terminal_partitions=covered_terminal_partitions,
        pending_partitions=pending_partitions,
        pending_terminal_partitions=pending_terminal_partitions,
        running_partitions=running_partitions,
        split_partitions=split_partitions,
        unresolved_partitions=unresolved_partitions,
        failed_partitions=failed_partitions,
        coverage_ratio=coverage_ratio,
    )


def _build_tree_rows(partitions: list[CrawlPartition]) -> tuple[RunCoverageTreeRow, ...]:
    if not partitions:
        return ()

    partitions_by_id = {partition.id: partition for partition in partitions}
    children_by_parent_id: dict[UUID, list[CrawlPartition]] = defaultdict(list)
    root_partitions: list[CrawlPartition] = []

    for partition in partitions:
        if (
            partition.parent_partition_id is None
            or partition.parent_partition_id not in partitions_by_id
        ):
            root_partitions.append(partition)
            continue
        children_by_parent_id[partition.parent_partition_id].append(partition)

    rows: list[RunCoverageTreeRow] = []

    def visit(partition: CrawlPartition) -> None:
        rows.append(
            RunCoverageTreeRow(
                partition_id=partition.id,
                parent_partition_id=partition.parent_partition_id,
                depth=partition.depth,
                scope_key=partition.scope_key or partition.partition_key,
                status=partition.status,
                coverage_status=partition.coverage_status,
                is_terminal=partition.is_terminal,
                is_saturated=partition.is_saturated,
            )
        )
        for child in sorted(children_by_parent_id.get(partition.id, []), key=_partition_sort_key):
            visit(child)

    for root_partition in sorted(root_partitions, key=_partition_sort_key):
        visit(root_partition)

    return tuple(rows)


def _partition_sort_key(partition: CrawlPartition) -> tuple[int, str, str, str]:
    return (
        partition.depth,
        partition.scope_key or partition.partition_key,
        partition.partition_key,
        str(partition.id),
    )
