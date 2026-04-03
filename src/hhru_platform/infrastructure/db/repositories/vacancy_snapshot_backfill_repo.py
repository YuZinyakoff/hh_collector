from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import cast
from uuid import UUID

from sqlalchemy import case, exists, func, or_, select
from sqlalchemy.orm import Session
from sqlalchemy.sql.elements import ColumnElement

from hhru_platform.application.commands.backfill_vacancy_snapshots import (
    LegacyDetailSnapshotCandidate,
    ShortSnapshotBackfillCandidate,
)
from hhru_platform.domain.value_objects.enums import VacancySnapshotType
from hhru_platform.infrastructure.db.models.raw_api_payload import RawApiPayload
from hhru_platform.infrastructure.db.models.vacancy import Vacancy as VacancyModel
from hhru_platform.infrastructure.db.models.vacancy_current_state import (
    VacancyCurrentState as VacancyCurrentStateModel,
)
from hhru_platform.infrastructure.db.models.vacancy_seen_event import (
    VacancySeenEvent as VacancySeenEventModel,
)
from hhru_platform.infrastructure.db.models.vacancy_snapshot import (
    VacancySnapshot as VacancySnapshotModel,
)


class SqlAlchemyVacancySnapshotBackfillRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def list_legacy_detail_snapshot_candidates(
        self,
        *,
        limit: int,
    ) -> list[LegacyDetailSnapshotCandidate]:
        statement = (
            select(
                VacancySnapshotModel.id,
                VacancySnapshotModel.vacancy_id,
                VacancySnapshotModel.detail_payload_ref_id,
            )
            .where(
                VacancySnapshotModel.snapshot_type == VacancySnapshotType.DETAIL.value,
                VacancySnapshotModel.detail_payload_ref_id.is_not(None),
                _legacy_snapshot_filter(),
            )
            .order_by(VacancySnapshotModel.id)
            .limit(limit)
        )
        rows = self._session.execute(statement)
        return [
            LegacyDetailSnapshotCandidate(
                snapshot_id=snapshot_id,
                vacancy_id=vacancy_id,
                detail_payload_ref_id=cast(int, detail_payload_ref_id),
            )
            for snapshot_id, vacancy_id, detail_payload_ref_id in rows
        ]

    def update_detail_snapshot(
        self,
        *,
        snapshot_id: int,
        detail_hash: str,
        snapshot_json: dict[str, object],
    ) -> None:
        model = self._session.get(VacancySnapshotModel, snapshot_id)
        if model is None:
            raise LookupError(f"vacancy_snapshot not found: {snapshot_id}")
        model.detail_hash = detail_hash
        model.normalized_json = snapshot_json
        self._session.add(model)
        self._session.flush()

    def list_short_snapshot_backfill_candidates(
        self,
        *,
        limit: int,
    ) -> list[ShortSnapshotBackfillCandidate]:
        ranked_seen_events = (
            select(
                VacancySeenEventModel.id.label("seen_event_id"),
                VacancySeenEventModel.vacancy_id.label("vacancy_id"),
                VacancyModel.hh_vacancy_id.label("hh_vacancy_id"),
                VacancySeenEventModel.crawl_run_id.label("crawl_run_id"),
                VacancySeenEventModel.crawl_partition_id.label("crawl_partition_id"),
                VacancySeenEventModel.seen_at.label("seen_at"),
                VacancySeenEventModel.list_position.label("list_position"),
                VacancySeenEventModel.short_hash.label("short_hash"),
                VacancySeenEventModel.short_payload_ref_id.label("short_payload_ref_id"),
                func.lag(VacancySeenEventModel.short_hash)
                .over(
                    partition_by=VacancySeenEventModel.vacancy_id,
                    order_by=(
                        VacancySeenEventModel.seen_at,
                        VacancySeenEventModel.id,
                    ),
                )
                .label("previous_short_hash"),
            )
            .join(VacancyModel, VacancyModel.id == VacancySeenEventModel.vacancy_id)
            .subquery()
        )
        existing_short_snapshot = (
            select(VacancySnapshotModel.id)
            .where(
                VacancySnapshotModel.vacancy_id == ranked_seen_events.c.vacancy_id,
                VacancySnapshotModel.snapshot_type == VacancySnapshotType.SHORT.value,
                VacancySnapshotModel.captured_at == ranked_seen_events.c.seen_at,
                VacancySnapshotModel.short_hash == ranked_seen_events.c.short_hash,
            )
            .correlate(ranked_seen_events)
        )
        statement = (
            select(
                ranked_seen_events.c.vacancy_id,
                ranked_seen_events.c.hh_vacancy_id,
                ranked_seen_events.c.crawl_run_id,
                ranked_seen_events.c.crawl_partition_id,
                ranked_seen_events.c.seen_at,
                ranked_seen_events.c.list_position,
                ranked_seen_events.c.short_hash,
                ranked_seen_events.c.short_payload_ref_id,
                case(
                    (
                        ranked_seen_events.c.previous_short_hash.is_(None),
                        "first_seen",
                    ),
                    else_="short_hash_changed",
                ).label("change_reason"),
            )
            .where(
                ranked_seen_events.c.short_payload_ref_id.is_not(None),
                or_(
                    ranked_seen_events.c.previous_short_hash.is_(None),
                    ranked_seen_events.c.previous_short_hash != ranked_seen_events.c.short_hash,
                ),
                ~exists(existing_short_snapshot),
            )
            .order_by(ranked_seen_events.c.seen_at, ranked_seen_events.c.seen_event_id)
            .limit(limit)
        )
        rows = self._session.execute(statement)
        return [
            ShortSnapshotBackfillCandidate(
                vacancy_id=vacancy_id,
                hh_vacancy_id=hh_vacancy_id,
                crawl_run_id=crawl_run_id,
                crawl_partition_id=crawl_partition_id,
                seen_at=seen_at,
                list_position=list_position,
                short_hash=short_hash,
                short_payload_ref_id=cast(int, short_payload_ref_id),
                change_reason=str(change_reason),
            )
            for (
                vacancy_id,
                hh_vacancy_id,
                crawl_run_id,
                crawl_partition_id,
                seen_at,
                list_position,
                short_hash,
                short_payload_ref_id,
                change_reason,
            ) in rows
        ]

    def load_raw_payload_json(self, payload_id: int) -> object | None:
        model = self._session.get(RawApiPayload, payload_id)
        if model is None:
            return None
        return cast(object, model.payload_json)

    def add_short_snapshot(
        self,
        *,
        vacancy_id: UUID,
        crawl_run_id: UUID,
        captured_at: datetime,
        short_hash: str,
        short_payload_ref_id: int,
        snapshot_json: dict[str, object],
        change_reason: str,
    ) -> int:
        model = VacancySnapshotModel(
            vacancy_id=vacancy_id,
            crawl_run_id=crawl_run_id,
            snapshot_type=VacancySnapshotType.SHORT.value,
            captured_at=captured_at,
            short_hash=short_hash,
            detail_hash=None,
            short_payload_ref_id=short_payload_ref_id,
            detail_payload_ref_id=None,
            normalized_json=snapshot_json,
            change_reason=change_reason,
        )
        self._session.add(model)
        self._session.flush()
        return model.id

    def sync_current_state_detail_hashes(self, *, vacancy_ids: Sequence[UUID]) -> int:
        updated_count = 0
        for vacancy_id in dict.fromkeys(vacancy_ids):
            current_state = self._session.get(VacancyCurrentStateModel, vacancy_id)
            if current_state is None:
                continue
            latest_snapshot = self._session.scalar(
                select(VacancySnapshotModel)
                .where(
                    VacancySnapshotModel.vacancy_id == vacancy_id,
                    VacancySnapshotModel.snapshot_type == VacancySnapshotType.DETAIL.value,
                    VacancySnapshotModel.detail_hash.is_not(None),
                )
                .order_by(
                    VacancySnapshotModel.captured_at.desc(),
                    VacancySnapshotModel.id.desc(),
                )
                .limit(1)
            )
            if latest_snapshot is None:
                continue
            current_state.last_detail_hash = latest_snapshot.detail_hash
            current_state.last_detail_fetched_at = latest_snapshot.captured_at
            self._session.add(current_state)
            updated_count += 1

        self._session.flush()
        return updated_count


def _legacy_snapshot_filter() -> ColumnElement[bool]:
    return or_(
        VacancySnapshotModel.normalized_json.is_(None),
        func.coalesce(
            func.jsonb_extract_path_text(
                VacancySnapshotModel.normalized_json,
                "schema_version",
            ),
            "",
        )
        != "2",
    )
