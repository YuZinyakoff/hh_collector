from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy.orm import Session

from hhru_platform.domain.entities.dictionary_sync_run import DictionarySyncRun
from hhru_platform.infrastructure.db.models.dictionary_sync_run import (
    DictionarySyncRun as DictionarySyncRunModel,
)


class SqlAlchemyDictionarySyncRunRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def start(self, *, dictionary_name: str, status: str) -> DictionarySyncRun:
        sync_run = DictionarySyncRunModel(
            dictionary_name=dictionary_name,
            status=status,
        )
        self._session.add(sync_run)
        self._session.flush()
        self._session.refresh(sync_run)
        return self._to_entity(sync_run)

    def finish(
        self,
        *,
        run_id: UUID,
        status: str,
        etag: str | None,
        source_status_code: int | None,
        notes: str | None,
    ) -> DictionarySyncRun:
        sync_run = self._session.get(DictionarySyncRunModel, run_id)
        if sync_run is None:
            raise LookupError(f"dictionary_sync_run not found: {run_id}")

        sync_run.status = status
        sync_run.etag = etag
        sync_run.source_status_code = source_status_code
        sync_run.notes = notes
        sync_run.finished_at = datetime.now(UTC)
        self._session.add(sync_run)
        self._session.flush()
        self._session.refresh(sync_run)
        return self._to_entity(sync_run)

    @staticmethod
    def _to_entity(model: DictionarySyncRunModel) -> DictionarySyncRun:
        return DictionarySyncRun(
            id=model.id,
            dictionary_name=model.dictionary_name,
            status=model.status,
            etag=model.etag,
            source_status_code=model.source_status_code,
            notes=model.notes,
            started_at=model.started_at,
            finished_at=model.finished_at,
        )
