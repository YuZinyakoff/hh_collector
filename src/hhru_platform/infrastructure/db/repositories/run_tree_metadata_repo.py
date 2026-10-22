from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import datetime
from typing import Any, cast
from uuid import UUID

from sqlalchemy import DateTime, Table, select, update
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import Session

from hhru_platform.infrastructure.db.models.crawl_partition import CrawlPartition
from hhru_platform.infrastructure.db.models.crawl_run import CrawlRun


class SqlAlchemyRunTreeMetadataRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def iter_records(self, run_id: UUID) -> Iterable[Mapping[str, Any]]:
        root = (
            self._session.execute(select(CrawlRun.__table__).where(CrawlRun.id == run_id))
            .mappings()
            .one_or_none()
        )
        if root is None:
            raise LookupError("run-tree metadata root not found")
        if root["status"] == "created" or root["finished_at"] is None:
            raise ValueError("cannot archive an unfinished run")
        yield {"table": "crawl_run", "row": dict(root)}
        rows = self._session.execute(
            select(CrawlPartition.__table__)
            .where(CrawlPartition.crawl_run_id == run_id)
            .order_by(CrawlPartition.id)
            .execution_options(yield_per=1000, stream_results=True)
        ).mappings()
        for row in rows:
            yield {"table": "crawl_partition", "row": dict(row)}

    def restore_into_empty_tables(self, records: Iterable[Mapping[str, Any]]) -> None:
        """Restore metadata only into an isolated empty target; caller owns transaction."""
        for model in (CrawlRun, CrawlPartition):
            if self._session.scalar(select(model.id).limit(1)) is not None:
                raise ValueError("metadata restore requires empty run/partition tables")
        parents: list[tuple[UUID, UUID]] = []
        for record in records:
            table = cast(
                Table,
                {"crawl_run": CrawlRun.__table__, "crawl_partition": CrawlPartition.__table__}[
                    record["table"]
                ],
            )
            row = dict(record["row"])
            if set(row) != set(table.columns.keys()):
                raise ValueError("metadata columns do not match target schema")
            for column in table.columns:
                value = row[column.name]
                if value is not None:
                    if isinstance(column.type, PGUUID):
                        row[column.name] = UUID(value)
                    elif isinstance(column.type, DateTime):
                        row[column.name] = datetime.fromisoformat(value)
            if table.name == "crawl_partition" and row["parent_partition_id"] is not None:
                parents.append((row["id"], row["parent_partition_id"]))
                row["parent_partition_id"] = None
            self._session.execute(table.insert().values(**row))
        # Parent ids sort independently of children; restore links only after all inserts.
        for partition_id, parent_id in parents:
            self._session.execute(
                update(CrawlPartition)
                .where(CrawlPartition.id == partition_id)
                .values(parent_partition_id=parent_id)
            )
