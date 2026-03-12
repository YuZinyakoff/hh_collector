from __future__ import annotations

from uuid import UUID

from sqlalchemy.orm import Session

from hhru_platform.domain.entities.crawl_run import CrawlRun
from hhru_platform.infrastructure.db.models.crawl_run import CrawlRun as CrawlRunModel


class SqlAlchemyCrawlRunRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def add(self, *, run_type: str, status: str, triggered_by: str) -> CrawlRun:
        crawl_run = CrawlRunModel(
            run_type=run_type,
            status=status,
            triggered_by=triggered_by,
        )
        self._session.add(crawl_run)
        self._session.flush()
        self._session.refresh(crawl_run)
        return self._to_entity(crawl_run)

    def get(self, run_id: UUID) -> CrawlRun | None:
        crawl_run = self._session.get(CrawlRunModel, run_id)
        if crawl_run is None:
            return None

        return self._to_entity(crawl_run)

    def set_partitions_total(self, run_id: UUID, partitions_total: int) -> CrawlRun:
        crawl_run = self._session.get(CrawlRunModel, run_id)
        if crawl_run is None:
            raise LookupError(f"crawl_run not found: {run_id}")

        crawl_run.partitions_total = partitions_total
        self._session.add(crawl_run)
        self._session.flush()
        self._session.refresh(crawl_run)
        return self._to_entity(crawl_run)

    @staticmethod
    def _to_entity(model: CrawlRunModel) -> CrawlRun:
        config_snapshot_json = model.config_snapshot_json or {}
        return CrawlRun(
            id=model.id,
            run_type=model.run_type,
            status=model.status,
            started_at=model.started_at,
            finished_at=model.finished_at,
            triggered_by=model.triggered_by,
            config_snapshot_json=dict(config_snapshot_json),
            partitions_total=model.partitions_total,
            partitions_done=model.partitions_done,
            partitions_failed=model.partitions_failed,
            notes=model.notes,
        )
