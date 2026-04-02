from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from hhru_platform.domain.entities.crawl_partition import CrawlPartition
from hhru_platform.domain.value_objects.enums import (
    CrawlPartitionCoverageStatus,
    CrawlPartitionStatus,
)
from hhru_platform.infrastructure.db.models.crawl_partition import (
    CrawlPartition as CrawlPartitionModel,
)


class SqlAlchemyCrawlPartitionRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def add(
        self,
        *,
        crawl_run_id: UUID,
        partition_key: str,
        status: str,
        params_json: dict[str, Any],
        parent_partition_id: UUID | None = None,
        depth: int = 0,
        split_dimension: str | None = None,
        split_value: str | None = None,
        scope_key: str | None = None,
        planner_policy_version: str = "v1",
        is_terminal: bool = True,
        is_saturated: bool = False,
        coverage_status: str = CrawlPartitionCoverageStatus.UNASSESSED.value,
    ) -> CrawlPartition:
        crawl_partition = CrawlPartitionModel(
            crawl_run_id=crawl_run_id,
            parent_partition_id=parent_partition_id,
            partition_key=partition_key,
            scope_key=scope_key or partition_key,
            status=status,
            params_json=dict(params_json),
            depth=depth,
            split_dimension=split_dimension,
            split_value=split_value,
            planner_policy_version=planner_policy_version,
            is_terminal=is_terminal,
            is_saturated=is_saturated,
            coverage_status=coverage_status,
        )
        self._session.add(crawl_partition)
        self._session.flush()
        self._session.refresh(crawl_partition)
        return self._to_entity(crawl_partition)

    def list_by_run_id(self, run_id: UUID) -> list[CrawlPartition]:
        statement = (
            select(CrawlPartitionModel)
            .where(CrawlPartitionModel.crawl_run_id == run_id)
            .order_by(CrawlPartitionModel.partition_key)
        )
        return [self._to_entity(model) for model in self._session.scalars(statement)]

    def list_pending_terminal_by_run_id(
        self,
        run_id: UUID,
        *,
        limit: int | None = None,
    ) -> list[CrawlPartition]:
        statement = (
            select(CrawlPartitionModel)
            .where(
                CrawlPartitionModel.crawl_run_id == run_id,
                CrawlPartitionModel.is_terminal.is_(True),
                CrawlPartitionModel.status == CrawlPartitionStatus.PENDING.value,
            )
            .order_by(CrawlPartitionModel.depth, CrawlPartitionModel.partition_key)
        )
        if limit is not None:
            statement = statement.limit(limit)
        return [self._to_entity(model) for model in self._session.scalars(statement)]

    def count_pending_terminal_by_run_id(self, run_id: UUID) -> int:
        statement = select(func.count()).select_from(CrawlPartitionModel).where(
            CrawlPartitionModel.crawl_run_id == run_id,
            CrawlPartitionModel.is_terminal.is_(True),
            CrawlPartitionModel.status == CrawlPartitionStatus.PENDING.value,
        )
        return int(self._session.scalar(statement) or 0)

    def list_children(self, parent_partition_id: UUID) -> list[CrawlPartition]:
        statement = (
            select(CrawlPartitionModel)
            .where(CrawlPartitionModel.parent_partition_id == parent_partition_id)
            .order_by(CrawlPartitionModel.depth, CrawlPartitionModel.partition_key)
        )
        return [self._to_entity(model) for model in self._session.scalars(statement)]

    def get(self, partition_id: UUID) -> CrawlPartition | None:
        crawl_partition = self._session.get(CrawlPartitionModel, partition_id)
        if crawl_partition is None:
            return None

        return self._to_entity(crawl_partition)

    def mark_running(self, partition_id: UUID) -> CrawlPartition:
        crawl_partition = self._get_model(partition_id)
        if crawl_partition.started_at is None:
            crawl_partition.started_at = datetime.now(UTC)
        crawl_partition.status = "running"
        crawl_partition.finished_at = None
        crawl_partition.last_error_message = None
        self._session.add(crawl_partition)
        self._session.flush()
        self._session.refresh(crawl_partition)
        return self._to_entity(crawl_partition)

    def mark_pending(self, partition_id: UUID) -> CrawlPartition:
        crawl_partition = self._get_model(partition_id)
        crawl_partition.status = CrawlPartitionStatus.PENDING.value
        crawl_partition.finished_at = None
        crawl_partition.last_error_message = None
        self._session.add(crawl_partition)
        self._session.flush()
        self._session.refresh(crawl_partition)
        return self._to_entity(crawl_partition)

    def record_page_processed(
        self,
        *,
        partition_id: UUID,
        pages_total_expected: int | None,
        items_seen_delta: int,
        status: str,
    ) -> CrawlPartition:
        crawl_partition = self._get_model(partition_id)
        if crawl_partition.started_at is None:
            crawl_partition.started_at = datetime.now(UTC)
        crawl_partition.pages_total_expected = pages_total_expected
        crawl_partition.pages_processed += 1
        crawl_partition.items_seen += items_seen_delta
        crawl_partition.status = status
        crawl_partition.finished_at = datetime.now(UTC)
        crawl_partition.last_error_message = None
        self._session.add(crawl_partition)
        self._session.flush()
        self._session.refresh(crawl_partition)
        return self._to_entity(crawl_partition)

    def mark_covered(self, partition_id: UUID) -> CrawlPartition:
        crawl_partition = self._get_model(partition_id)
        crawl_partition.status = CrawlPartitionStatus.DONE.value
        crawl_partition.is_terminal = True
        crawl_partition.is_saturated = False
        crawl_partition.coverage_status = CrawlPartitionCoverageStatus.COVERED.value
        crawl_partition.finished_at = datetime.now(UTC)
        crawl_partition.last_error_message = None
        self._session.add(crawl_partition)
        self._session.flush()
        self._session.refresh(crawl_partition)
        return self._to_entity(crawl_partition)

    def mark_split_required(self, partition_id: UUID) -> CrawlPartition:
        crawl_partition = self._get_model(partition_id)
        crawl_partition.status = CrawlPartitionStatus.SPLIT_REQUIRED.value
        crawl_partition.is_saturated = True
        crawl_partition.is_terminal = False
        crawl_partition.coverage_status = CrawlPartitionCoverageStatus.SATURATED.value
        crawl_partition.finished_at = None
        crawl_partition.last_error_message = None
        self._session.add(crawl_partition)
        self._session.flush()
        self._session.refresh(crawl_partition)
        return self._to_entity(crawl_partition)

    def mark_split_done(self, partition_id: UUID) -> CrawlPartition:
        crawl_partition = self._get_model(partition_id)
        crawl_partition.status = CrawlPartitionStatus.SPLIT_DONE.value
        crawl_partition.is_saturated = True
        crawl_partition.is_terminal = False
        crawl_partition.coverage_status = CrawlPartitionCoverageStatus.SPLIT.value
        crawl_partition.finished_at = datetime.now(UTC)
        crawl_partition.last_error_message = None
        self._session.add(crawl_partition)
        self._session.flush()
        self._session.refresh(crawl_partition)
        return self._to_entity(crawl_partition)

    def mark_unresolved(self, *, partition_id: UUID, error_message: str) -> CrawlPartition:
        crawl_partition = self._get_model(partition_id)
        crawl_partition.status = CrawlPartitionStatus.UNRESOLVED.value
        crawl_partition.is_saturated = True
        crawl_partition.is_terminal = True
        crawl_partition.coverage_status = CrawlPartitionCoverageStatus.UNRESOLVED.value
        crawl_partition.finished_at = datetime.now(UTC)
        crawl_partition.last_error_message = error_message
        self._session.add(crawl_partition)
        self._session.flush()
        self._session.refresh(crawl_partition)
        return self._to_entity(crawl_partition)

    def requeue_unresolved_by_run_id(self, run_id: UUID) -> list[CrawlPartition]:
        statement = (
            select(CrawlPartitionModel)
            .where(
                CrawlPartitionModel.crawl_run_id == run_id,
                CrawlPartitionModel.is_terminal.is_(True),
                CrawlPartitionModel.status == CrawlPartitionStatus.UNRESOLVED.value,
            )
            .order_by(CrawlPartitionModel.depth, CrawlPartitionModel.partition_key)
        )
        requeued: list[CrawlPartition] = []
        for crawl_partition in self._session.scalars(statement):
            crawl_partition.status = CrawlPartitionStatus.PENDING.value
            crawl_partition.coverage_status = CrawlPartitionCoverageStatus.UNASSESSED.value
            crawl_partition.finished_at = None
            crawl_partition.last_error_message = None
            crawl_partition.retry_count += 1
            self._session.add(crawl_partition)
            self._session.flush()
            self._session.refresh(crawl_partition)
            requeued.append(self._to_entity(crawl_partition))
        return requeued

    def mark_failed(self, *, partition_id: UUID, error_message: str) -> CrawlPartition:
        crawl_partition = self._get_model(partition_id)
        if crawl_partition.started_at is None:
            crawl_partition.started_at = datetime.now(UTC)
        crawl_partition.status = "failed"
        crawl_partition.finished_at = datetime.now(UTC)
        crawl_partition.last_error_message = error_message
        self._session.add(crawl_partition)
        self._session.flush()
        self._session.refresh(crawl_partition)
        return self._to_entity(crawl_partition)

    @staticmethod
    def _to_entity(model: CrawlPartitionModel) -> CrawlPartition:
        return CrawlPartition(
            id=model.id,
            crawl_run_id=model.crawl_run_id,
            partition_key=model.partition_key,
            params_json=dict(model.params_json or {}),
            status=model.status,
            pages_total_expected=model.pages_total_expected,
            pages_processed=model.pages_processed,
            items_seen=model.items_seen,
            retry_count=model.retry_count,
            started_at=model.started_at,
            finished_at=model.finished_at,
            last_error_message=model.last_error_message,
            created_at=model.created_at,
            parent_partition_id=model.parent_partition_id,
            depth=model.depth,
            split_dimension=model.split_dimension,
            split_value=model.split_value,
            scope_key=model.scope_key,
            planner_policy_version=model.planner_policy_version,
            is_terminal=model.is_terminal,
            is_saturated=model.is_saturated,
            coverage_status=model.coverage_status,
        )

    def _get_model(self, partition_id: UUID) -> CrawlPartitionModel:
        crawl_partition = self._session.get(CrawlPartitionModel, partition_id)
        if crawl_partition is None:
            raise LookupError(f"crawl_partition not found: {partition_id}")
        return crawl_partition
