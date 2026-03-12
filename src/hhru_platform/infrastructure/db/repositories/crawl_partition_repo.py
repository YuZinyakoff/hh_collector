from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session

from hhru_platform.domain.entities.crawl_partition import CrawlPartition
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
    ) -> CrawlPartition:
        crawl_partition = CrawlPartitionModel(
            crawl_run_id=crawl_run_id,
            partition_key=partition_key,
            status=status,
            params_json=dict(params_json),
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
        )

    def _get_model(self, partition_id: UUID) -> CrawlPartitionModel:
        crawl_partition = self._session.get(CrawlPartitionModel, partition_id)
        if crawl_partition is None:
            raise LookupError(f"crawl_partition not found: {partition_id}")
        return crawl_partition
