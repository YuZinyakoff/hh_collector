from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID, uuid4

from hhru_platform.application.commands.report_run_coverage import (
    ReportRunCoverageCommand,
    report_run_coverage,
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


class InMemoryCrawlPartitionRepository:
    def __init__(self, partitions: list[CrawlPartition]) -> None:
        self._partitions = list(partitions)

    def list_by_run_id(self, run_id: UUID) -> list[CrawlPartition]:
        return [partition for partition in self._partitions if partition.crawl_run_id == run_id]


class RecordingMetricsRecorder:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def set_run_tree_coverage(self, **kwargs) -> None:
        self.calls.append(kwargs)


def _build_crawl_run() -> CrawlRun:
    return CrawlRun(
        id=uuid4(),
        run_type="weekly_sweep",
        status="created",
        started_at=datetime(2026, 3, 21, 10, 0, tzinfo=UTC),
        finished_at=None,
        triggered_by="pytest",
        config_snapshot_json={},
        partitions_total=0,
        partitions_done=0,
        partitions_failed=0,
        notes=None,
    )


def _build_partition(
    *,
    crawl_run_id: UUID,
    partition_key: str,
    status: str,
    coverage_status: str,
    is_terminal: bool,
    is_saturated: bool = False,
    depth: int = 0,
    parent_partition_id: UUID | None = None,
) -> CrawlPartition:
    return CrawlPartition(
        id=uuid4(),
        crawl_run_id=crawl_run_id,
        partition_key=partition_key,
        params_json={"params": {"area": partition_key}},
        status=status,
        pages_total_expected=None,
        pages_processed=0,
        items_seen=0,
        retry_count=0,
        started_at=None,
        finished_at=None,
        last_error_message=None,
        created_at=datetime(2026, 3, 21, 10, 0, tzinfo=UTC),
        parent_partition_id=parent_partition_id,
        depth=depth,
        split_dimension="area",
        split_value=partition_key.split(":")[-1],
        scope_key=partition_key,
        planner_policy_version="v2",
        is_terminal=is_terminal,
        is_saturated=is_saturated,
        coverage_status=coverage_status,
    )


def test_report_run_coverage_aggregates_tree_semantics_and_records_metrics() -> None:
    crawl_run = _build_crawl_run()
    split_root = _build_partition(
        crawl_run_id=crawl_run.id,
        partition_key="area:113",
        status="split_done",
        coverage_status="split",
        is_terminal=False,
        is_saturated=True,
    )
    covered_child = _build_partition(
        crawl_run_id=crawl_run.id,
        partition_key="area:1",
        status="done",
        coverage_status="covered",
        is_terminal=True,
        depth=1,
        parent_partition_id=split_root.id,
    )
    pending_child = _build_partition(
        crawl_run_id=crawl_run.id,
        partition_key="area:2",
        status="pending",
        coverage_status="unassessed",
        is_terminal=True,
        depth=1,
        parent_partition_id=split_root.id,
    )
    unresolved_root = _build_partition(
        crawl_run_id=crawl_run.id,
        partition_key="area:120",
        status="unresolved",
        coverage_status="unresolved",
        is_terminal=True,
        is_saturated=True,
    )
    running_root = _build_partition(
        crawl_run_id=crawl_run.id,
        partition_key="area:130",
        status="running",
        coverage_status="unassessed",
        is_terminal=True,
    )
    failed_root = _build_partition(
        crawl_run_id=crawl_run.id,
        partition_key="area:140",
        status="failed",
        coverage_status="unassessed",
        is_terminal=True,
    )
    metrics_recorder = RecordingMetricsRecorder()

    report = report_run_coverage(
        ReportRunCoverageCommand(crawl_run_id=crawl_run.id),
        crawl_run_repository=InMemoryCrawlRunRepository(crawl_run),
        crawl_partition_repository=InMemoryCrawlPartitionRepository(
            [
                unresolved_root,
                covered_child,
                split_root,
                failed_root,
                pending_child,
                running_root,
            ]
        ),
        metrics_recorder=metrics_recorder,
    )

    assert report.summary.total_partitions == 6
    assert report.summary.root_partitions == 4
    assert report.summary.terminal_partitions == 5
    assert report.summary.covered_terminal_partitions == 1
    assert report.summary.pending_partitions == 1
    assert report.summary.pending_terminal_partitions == 1
    assert report.summary.running_partitions == 1
    assert report.summary.split_partitions == 1
    assert report.summary.unresolved_partitions == 1
    assert report.summary.failed_partitions == 1
    assert report.summary.coverage_ratio == 0.2
    assert report.summary.is_fully_covered is False

    assert [row.scope_key for row in report.tree_rows] == [
        "area:113",
        "area:1",
        "area:2",
        "area:120",
        "area:130",
        "area:140",
    ]
    assert metrics_recorder.calls == [
        {
            "run_id": str(crawl_run.id),
            "run_type": "weekly_sweep",
            "coverage_ratio": 0.2,
            "total_partitions": 6,
            "covered_terminal_partitions": 1,
            "pending_terminal_partitions": 1,
            "split_partitions": 1,
            "unresolved_partitions": 1,
            "failed_partitions": 1,
        }
    ]


def test_report_run_coverage_marks_fully_covered_when_all_terminal_leaves_are_covered() -> None:
    crawl_run = _build_crawl_run()
    root_partition = _build_partition(
        crawl_run_id=crawl_run.id,
        partition_key="area:113",
        status="done",
        coverage_status="covered",
        is_terminal=True,
    )

    report = report_run_coverage(
        ReportRunCoverageCommand(crawl_run_id=crawl_run.id),
        crawl_run_repository=InMemoryCrawlRunRepository(crawl_run),
        crawl_partition_repository=InMemoryCrawlPartitionRepository([root_partition]),
    )

    assert report.summary.coverage_ratio == 1.0
    assert report.summary.is_fully_covered is True
