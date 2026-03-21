from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy.orm import Session

from hhru_platform.infrastructure.db.models.vacancy_snapshot import VacancySnapshot


class SqlAlchemyVacancySnapshotRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def add(
        self,
        *,
        vacancy_id: UUID,
        crawl_run_id: UUID | None,
        snapshot_type: str,
        captured_at: datetime,
        short_hash: str | None,
        detail_hash: str | None,
        short_payload_ref_id: int | None,
        detail_payload_ref_id: int | None,
        normalized_json: dict[str, Any] | None,
        change_reason: str | None,
    ) -> int:
        snapshot = VacancySnapshot(
            vacancy_id=vacancy_id,
            snapshot_type=snapshot_type,
            captured_at=captured_at,
            crawl_run_id=crawl_run_id,
            short_hash=short_hash,
            detail_hash=detail_hash,
            short_payload_ref_id=short_payload_ref_id,
            detail_payload_ref_id=detail_payload_ref_id,
            normalized_json=normalized_json,
            change_reason=change_reason,
        )
        self._session.add(snapshot)
        self._session.flush()
        return snapshot.id
