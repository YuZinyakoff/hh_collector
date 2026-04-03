from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import cast
from uuid import UUID

from sqlalchemy import delete, func, or_, select
from sqlalchemy.engine import CursorResult
from sqlalchemy.orm import Session
from sqlalchemy.sql import Select

from hhru_platform.domain.value_objects.enums import CrawlRunStatus
from hhru_platform.infrastructure.db.models.api_request_log import ApiRequestLog
from hhru_platform.infrastructure.db.models.crawl_partition import (
    CrawlPartition as CrawlPartitionModel,
)
from hhru_platform.infrastructure.db.models.crawl_run import CrawlRun as CrawlRunModel
from hhru_platform.infrastructure.db.models.detail_fetch_attempt import (
    DetailFetchAttempt as DetailFetchAttemptModel,
)
from hhru_platform.infrastructure.db.models.raw_api_payload import (
    RawApiPayload as RawApiPayloadModel,
)
from hhru_platform.infrastructure.db.models.vacancy_snapshot import (
    VacancySnapshot as VacancySnapshotModel,
)

ACTIVE_RUN_STATUS = CrawlRunStatus.CREATED.value


class SqlAlchemyHousekeepingRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def count_raw_api_payload_candidates(self, *, cutoff: datetime) -> int:
        statement = (
            select(func.count())
            .select_from(RawApiPayloadModel)
            .join(ApiRequestLog, RawApiPayloadModel.api_request_log_id == ApiRequestLog.id)
            .outerjoin(CrawlRunModel, ApiRequestLog.crawl_run_id == CrawlRunModel.id)
            .where(
                RawApiPayloadModel.received_at < cutoff,
                or_(
                    CrawlRunModel.id.is_(None),
                    CrawlRunModel.status != ACTIVE_RUN_STATUS,
                ),
                ~RawApiPayloadModel.id.in_(self._protected_raw_payload_ids_subquery()),
            )
        )
        return int(self._session.scalar(statement) or 0)

    def list_raw_api_payload_ids_for_retention(
        self,
        *,
        cutoff: datetime,
        limit: int,
    ) -> list[int]:
        statement = (
            select(RawApiPayloadModel.id)
            .join(ApiRequestLog, RawApiPayloadModel.api_request_log_id == ApiRequestLog.id)
            .outerjoin(CrawlRunModel, ApiRequestLog.crawl_run_id == CrawlRunModel.id)
            .where(
                RawApiPayloadModel.received_at < cutoff,
                or_(
                    CrawlRunModel.id.is_(None),
                    CrawlRunModel.status != ACTIVE_RUN_STATUS,
                ),
                ~RawApiPayloadModel.id.in_(self._protected_raw_payload_ids_subquery()),
            )
            .order_by(RawApiPayloadModel.received_at, RawApiPayloadModel.id)
            .limit(limit)
        )
        return list(self._session.scalars(statement))

    def delete_raw_api_payloads(self, payload_ids: Sequence[int]) -> int:
        if not payload_ids:
            return 0
        result = cast(
            CursorResult[object],
            self._session.execute(
            delete(RawApiPayloadModel).where(RawApiPayloadModel.id.in_(tuple(payload_ids)))
            ),
        )
        return int(result.rowcount or 0)

    def list_raw_api_payload_rows_for_archive(
        self,
        *,
        payload_ids: Sequence[int],
    ) -> list[dict[str, object]]:
        if not payload_ids:
            return []
        statement = (
            select(RawApiPayloadModel, ApiRequestLog.crawl_run_id.label("crawl_run_id"))
            .join(ApiRequestLog, RawApiPayloadModel.api_request_log_id == ApiRequestLog.id)
            .where(RawApiPayloadModel.id.in_(tuple(payload_ids)))
            .order_by(RawApiPayloadModel.received_at, RawApiPayloadModel.id)
        )
        rows: list[dict[str, object]] = []
        for raw_payload, crawl_run_id in self._session.execute(statement):
            rows.append(
                {
                    "id": raw_payload.id,
                    "api_request_log_id": raw_payload.api_request_log_id,
                    "crawl_run_id": crawl_run_id,
                    "endpoint_type": raw_payload.endpoint_type,
                    "entity_hh_id": raw_payload.entity_hh_id,
                    "payload_json": raw_payload.payload_json,
                    "payload_hash": raw_payload.payload_hash,
                    "received_at": raw_payload.received_at,
                }
            )
        return rows

    def count_vacancy_snapshot_candidates(self, *, cutoff: datetime) -> int:
        statement = (
            select(func.count())
            .select_from(VacancySnapshotModel)
            .outerjoin(CrawlRunModel, VacancySnapshotModel.crawl_run_id == CrawlRunModel.id)
            .where(
                VacancySnapshotModel.captured_at < cutoff,
                or_(
                    CrawlRunModel.id.is_(None),
                    CrawlRunModel.status != ACTIVE_RUN_STATUS,
                ),
                ~VacancySnapshotModel.id.in_(self._latest_snapshot_ids_subquery()),
            )
        )
        return int(self._session.scalar(statement) or 0)

    def list_vacancy_snapshot_ids_for_retention(
        self,
        *,
        cutoff: datetime,
        limit: int,
    ) -> list[int]:
        statement = (
            select(VacancySnapshotModel.id)
            .outerjoin(CrawlRunModel, VacancySnapshotModel.crawl_run_id == CrawlRunModel.id)
            .where(
                VacancySnapshotModel.captured_at < cutoff,
                or_(
                    CrawlRunModel.id.is_(None),
                    CrawlRunModel.status != ACTIVE_RUN_STATUS,
                ),
                ~VacancySnapshotModel.id.in_(self._latest_snapshot_ids_subquery()),
            )
            .order_by(VacancySnapshotModel.captured_at, VacancySnapshotModel.id)
            .limit(limit)
        )
        return list(self._session.scalars(statement))

    def delete_vacancy_snapshots(self, snapshot_ids: Sequence[int]) -> int:
        if not snapshot_ids:
            return 0
        result = cast(
            CursorResult[object],
            self._session.execute(
            delete(VacancySnapshotModel).where(VacancySnapshotModel.id.in_(tuple(snapshot_ids)))
            ),
        )
        return int(result.rowcount or 0)

    def list_vacancy_snapshot_rows_for_archive(
        self,
        *,
        snapshot_ids: Sequence[int],
    ) -> list[dict[str, object]]:
        if not snapshot_ids:
            return []
        statement = (
            select(VacancySnapshotModel)
            .where(VacancySnapshotModel.id.in_(tuple(snapshot_ids)))
            .order_by(VacancySnapshotModel.captured_at, VacancySnapshotModel.id)
        )
        return [
            {
                "id": snapshot.id,
                "vacancy_id": snapshot.vacancy_id,
                "snapshot_type": snapshot.snapshot_type,
                "captured_at": snapshot.captured_at,
                "crawl_run_id": snapshot.crawl_run_id,
                "short_hash": snapshot.short_hash,
                "detail_hash": snapshot.detail_hash,
                "short_payload_ref_id": snapshot.short_payload_ref_id,
                "detail_payload_ref_id": snapshot.detail_payload_ref_id,
                "normalized_json": snapshot.normalized_json,
                "change_reason": snapshot.change_reason,
            }
            for snapshot in self._session.scalars(statement)
        ]

    def count_detail_fetch_attempt_candidates(self, *, cutoff: datetime) -> int:
        statement = (
            select(func.count())
            .select_from(DetailFetchAttemptModel)
            .outerjoin(CrawlRunModel, DetailFetchAttemptModel.crawl_run_id == CrawlRunModel.id)
            .where(
                DetailFetchAttemptModel.requested_at < cutoff,
                or_(
                    CrawlRunModel.id.is_(None),
                    CrawlRunModel.status != ACTIVE_RUN_STATUS,
                ),
                ~DetailFetchAttemptModel.id.in_(self._latest_detail_attempt_ids_subquery()),
            )
        )
        return int(self._session.scalar(statement) or 0)

    def list_detail_fetch_attempt_ids_for_retention(
        self,
        *,
        cutoff: datetime,
        limit: int,
    ) -> list[int]:
        statement = (
            select(DetailFetchAttemptModel.id)
            .outerjoin(CrawlRunModel, DetailFetchAttemptModel.crawl_run_id == CrawlRunModel.id)
            .where(
                DetailFetchAttemptModel.requested_at < cutoff,
                or_(
                    CrawlRunModel.id.is_(None),
                    CrawlRunModel.status != ACTIVE_RUN_STATUS,
                ),
                ~DetailFetchAttemptModel.id.in_(self._latest_detail_attempt_ids_subquery()),
            )
            .order_by(
                DetailFetchAttemptModel.requested_at,
                DetailFetchAttemptModel.id,
            )
            .limit(limit)
        )
        return list(self._session.scalars(statement))

    def delete_detail_fetch_attempts(self, attempt_ids: Sequence[int]) -> int:
        if not attempt_ids:
            return 0
        result = cast(
            CursorResult[object],
            self._session.execute(
            delete(DetailFetchAttemptModel).where(
                DetailFetchAttemptModel.id.in_(tuple(attempt_ids))
            )
            ),
        )
        return int(result.rowcount or 0)

    def count_finished_crawl_run_candidates(self, *, cutoff: datetime) -> int:
        statement = select(func.count()).select_from(CrawlRunModel).where(
            CrawlRunModel.status != ACTIVE_RUN_STATUS,
            CrawlRunModel.finished_at.is_not(None),
            CrawlRunModel.finished_at < cutoff,
        )
        return int(self._session.scalar(statement) or 0)

    def list_finished_crawl_run_ids_for_retention(
        self,
        *,
        cutoff: datetime,
        limit: int,
    ) -> list[UUID]:
        statement = (
            select(CrawlRunModel.id)
            .where(
                CrawlRunModel.status != ACTIVE_RUN_STATUS,
                CrawlRunModel.finished_at.is_not(None),
                CrawlRunModel.finished_at < cutoff,
            )
            .order_by(CrawlRunModel.finished_at, CrawlRunModel.id)
            .limit(limit)
        )
        return list(self._session.scalars(statement))

    def delete_finished_crawl_runs(self, run_ids: Sequence[UUID]) -> int:
        if not run_ids:
            return 0
        result = cast(
            CursorResult[object],
            self._session.execute(
            delete(CrawlRunModel).where(CrawlRunModel.id.in_(tuple(run_ids)))
            ),
        )
        return int(result.rowcount or 0)

    def count_crawl_partition_candidates_for_finished_runs(self, *, cutoff: datetime) -> int:
        statement = (
            select(func.count())
            .select_from(CrawlPartitionModel)
            .join(CrawlRunModel, CrawlPartitionModel.crawl_run_id == CrawlRunModel.id)
            .where(
                CrawlRunModel.status != ACTIVE_RUN_STATUS,
                CrawlRunModel.finished_at.is_not(None),
                CrawlRunModel.finished_at < cutoff,
            )
        )
        return int(self._session.scalar(statement) or 0)

    def count_crawl_partitions_for_run_ids(self, run_ids: Sequence[UUID]) -> int:
        if not run_ids:
            return 0
        statement = select(func.count()).select_from(CrawlPartitionModel).where(
            CrawlPartitionModel.crawl_run_id.in_(tuple(run_ids))
        )
        return int(self._session.scalar(statement) or 0)

    @staticmethod
    def _latest_snapshot_ids_subquery() -> Select[tuple[int]]:
        ranked_snapshots = (
            select(
                VacancySnapshotModel.id.label("snapshot_id"),
                func.row_number()
                .over(
                    partition_by=(
                        VacancySnapshotModel.vacancy_id,
                        VacancySnapshotModel.snapshot_type,
                    ),
                    order_by=(
                        VacancySnapshotModel.captured_at.desc(),
                        VacancySnapshotModel.id.desc(),
                    ),
                )
                .label("snapshot_rank"),
            )
            .subquery()
        )
        return cast(
            Select[tuple[int]],
            select(ranked_snapshots.c.snapshot_id).where(ranked_snapshots.c.snapshot_rank == 1),
        )

    @staticmethod
    def _latest_detail_attempt_ids_subquery() -> Select[tuple[int]]:
        return select(func.max(DetailFetchAttemptModel.id)).group_by(
            DetailFetchAttemptModel.vacancy_id,
            DetailFetchAttemptModel.crawl_run_id,
        )

    @staticmethod
    def _protected_raw_payload_ids_subquery() -> Select[tuple[int]]:
        legacy_snapshot_filter = or_(
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
        protected_short_payload_ids = (
            select(VacancySnapshotModel.short_payload_ref_id.label("payload_id"))
            .where(
                VacancySnapshotModel.short_payload_ref_id.is_not(None),
                legacy_snapshot_filter,
            )
        )
        protected_detail_payload_ids = (
            select(VacancySnapshotModel.detail_payload_ref_id.label("payload_id"))
            .where(
                VacancySnapshotModel.detail_payload_ref_id.is_not(None),
                legacy_snapshot_filter,
            )
        )
        protected_payload_ids = protected_short_payload_ids.union(
            protected_detail_payload_ids
        ).subquery()
        return cast(Select[tuple[int]], select(protected_payload_ids.c.payload_id))
