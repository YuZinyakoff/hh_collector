from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol
from uuid import UUID

from hhru_platform.infrastructure.normalization.vacancy_snapshot_document import (
    build_detail_snapshot_document,
    build_payload_hash,
    build_short_snapshot_document,
    extract_search_item_payload,
)
from hhru_platform.infrastructure.observability.operations import (
    log_operation_started,
    record_operation_failed,
    record_operation_succeeded,
)

LOGGER = logging.getLogger(__name__)


@dataclass(slots=True, frozen=True)
class LegacyDetailSnapshotCandidate:
    snapshot_id: int
    vacancy_id: UUID
    detail_payload_ref_id: int


@dataclass(slots=True, frozen=True)
class ShortSnapshotBackfillCandidate:
    vacancy_id: UUID
    hh_vacancy_id: str
    crawl_run_id: UUID
    crawl_partition_id: UUID
    seen_at: datetime
    list_position: int | None
    short_hash: str
    short_payload_ref_id: int


@dataclass(slots=True, frozen=True)
class BackfillVacancySnapshotsCommand:
    batch_size: int = 500
    max_batches: int | None = None
    triggered_by: str = "backfill-vacancy-snapshots"

    def __post_init__(self) -> None:
        if self.batch_size < 1:
            raise ValueError("batch_size must be greater than or equal to one")
        if self.max_batches is not None and self.max_batches < 1:
            raise ValueError("max_batches must be greater than or equal to one")
        normalized_triggered_by = self.triggered_by.strip()
        if not normalized_triggered_by:
            raise ValueError("triggered_by must not be empty")
        object.__setattr__(self, "triggered_by", normalized_triggered_by)


@dataclass(slots=True, frozen=True)
class BackfillVacancySnapshotsResult:
    status: str
    triggered_by: str
    detail_candidates_seen: int
    detail_snapshots_updated: int
    short_candidates_seen: int
    short_snapshots_created: int
    skipped_missing_raw_payload: int
    skipped_missing_search_item: int
    batches_processed: int
    finished_at: datetime


class VacancySnapshotBackfillRepository(Protocol):
    def list_legacy_detail_snapshot_candidates(
        self,
        *,
        limit: int,
    ) -> list[LegacyDetailSnapshotCandidate]:
        """Return legacy detail snapshots that still depend on raw payloads."""

    def update_detail_snapshot(
        self,
        *,
        snapshot_id: int,
        detail_hash: str,
        snapshot_json: dict[str, object],
    ) -> None:
        """Replace legacy detail snapshot contents with a full snapshot document."""

    def list_short_snapshot_backfill_candidates(
        self,
        *,
        limit: int,
    ) -> list[ShortSnapshotBackfillCandidate]:
        """Return search observations that still need short snapshots."""

    def load_raw_payload_json(self, payload_id: int) -> object | None:
        """Load raw_api_payload.payload_json by id."""

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
        """Persist one short vacancy snapshot."""

    def sync_current_state_detail_hashes(self, *, vacancy_ids: list[UUID]) -> int:
        """Refresh vacancy_current_state detail hash fields from latest detail snapshots."""


def backfill_vacancy_snapshots(
    command: BackfillVacancySnapshotsCommand,
    *,
    repository: VacancySnapshotBackfillRepository,
) -> BackfillVacancySnapshotsResult:
    started_at = log_operation_started(
        LOGGER,
        operation="backfill_vacancy_snapshots",
        batch_size=command.batch_size,
        max_batches=command.max_batches,
        triggered_by=command.triggered_by,
    )
    finished_at = datetime.now(UTC)

    try:
        (
            detail_candidates_seen,
            detail_snapshots_updated,
            detail_skipped_missing_raw_payload,
            detail_batches,
        ) = _backfill_detail_snapshots(command=command, repository=repository)
        (
            short_candidates_seen,
            short_snapshots_created,
            short_skipped_missing_raw_payload,
            skipped_missing_search_item,
            short_batches,
        ) = _backfill_short_snapshots(
            command=command,
            repository=repository,
        )
        finished_at = datetime.now(UTC)
    except Exception as error:
        record_operation_failed(
            LOGGER,
            operation="backfill_vacancy_snapshots",
            started_at=started_at,
            error_type=error.__class__.__name__,
            error_message=str(error),
            batch_size=command.batch_size,
            max_batches=command.max_batches,
            triggered_by=command.triggered_by,
        )
        raise

    result = BackfillVacancySnapshotsResult(
        status="succeeded",
        triggered_by=command.triggered_by,
        detail_candidates_seen=detail_candidates_seen,
        detail_snapshots_updated=detail_snapshots_updated,
        short_candidates_seen=short_candidates_seen,
        short_snapshots_created=short_snapshots_created,
        skipped_missing_raw_payload=(
            detail_skipped_missing_raw_payload + short_skipped_missing_raw_payload
        ),
        skipped_missing_search_item=skipped_missing_search_item,
        batches_processed=detail_batches + short_batches,
        finished_at=finished_at,
    )
    record_operation_succeeded(
        LOGGER,
        operation="backfill_vacancy_snapshots",
        started_at=started_at,
        records_written={
            "vacancy_snapshot_detail_updated": result.detail_snapshots_updated,
            "vacancy_snapshot_short_created": result.short_snapshots_created,
        },
        triggered_by=result.triggered_by,
        detail_candidates_seen=result.detail_candidates_seen,
        detail_snapshots_updated=result.detail_snapshots_updated,
        short_candidates_seen=result.short_candidates_seen,
        short_snapshots_created=result.short_snapshots_created,
        skipped_missing_raw_payload=result.skipped_missing_raw_payload,
        skipped_missing_search_item=result.skipped_missing_search_item,
        batches_processed=result.batches_processed,
    )
    return result


def _backfill_detail_snapshots(
    *,
    command: BackfillVacancySnapshotsCommand,
    repository: VacancySnapshotBackfillRepository,
) -> tuple[int, int, int, int]:
    candidates_seen = 0
    snapshots_updated = 0
    skipped_missing_raw_payload = 0
    batches_processed = 0

    while _should_continue_batch_loop(
        max_batches=command.max_batches,
        batches_processed=batches_processed,
    ):
        candidates = repository.list_legacy_detail_snapshot_candidates(limit=command.batch_size)
        if not candidates:
            break

        batches_processed += 1
        candidates_seen += len(candidates)
        touched_vacancy_ids: list[UUID] = []

        for candidate in candidates:
            payload_json = repository.load_raw_payload_json(candidate.detail_payload_ref_id)
            if payload_json is None:
                skipped_missing_raw_payload += 1
                continue

            snapshot_json = build_detail_snapshot_document(payload_json)
            repository.update_detail_snapshot(
                snapshot_id=candidate.snapshot_id,
                detail_hash=build_payload_hash(payload_json),
                snapshot_json=snapshot_json,
            )
            snapshots_updated += 1
            touched_vacancy_ids.append(candidate.vacancy_id)

        if touched_vacancy_ids:
            repository.sync_current_state_detail_hashes(vacancy_ids=touched_vacancy_ids)

    return candidates_seen, snapshots_updated, skipped_missing_raw_payload, batches_processed


def _backfill_short_snapshots(
    *,
    command: BackfillVacancySnapshotsCommand,
    repository: VacancySnapshotBackfillRepository,
) -> tuple[int, int, int, int, int]:
    candidates_seen = 0
    snapshots_created = 0
    skipped_missing_raw_payload = 0
    skipped_missing_search_item = 0
    batches_processed = 0

    while _should_continue_batch_loop(
        max_batches=command.max_batches,
        batches_processed=batches_processed,
    ):
        candidates = repository.list_short_snapshot_backfill_candidates(limit=command.batch_size)
        if not candidates:
            break

        batches_processed += 1
        candidates_seen += len(candidates)

        for candidate in candidates:
            page_payload_json = repository.load_raw_payload_json(candidate.short_payload_ref_id)
            if page_payload_json is None:
                skipped_missing_raw_payload += 1
                continue

            item_payload = extract_search_item_payload(
                page_payload_json,
                hh_vacancy_id=candidate.hh_vacancy_id,
            )
            if item_payload is None:
                skipped_missing_search_item += 1
                continue

            repository.add_short_snapshot(
                vacancy_id=candidate.vacancy_id,
                crawl_run_id=candidate.crawl_run_id,
                captured_at=candidate.seen_at,
                short_hash=candidate.short_hash,
                short_payload_ref_id=candidate.short_payload_ref_id,
                snapshot_json=build_short_snapshot_document(
                    item_payload,
                    seen_at=candidate.seen_at,
                    crawl_partition_id=candidate.crawl_partition_id,
                    list_position=candidate.list_position or 0,
                    page=None,
                    per_page=None,
                    found=None,
                    pages=None,
                    search_params={},
                ),
                change_reason="backfill_search_observation",
            )
            snapshots_created += 1

    return (
        candidates_seen,
        snapshots_created,
        skipped_missing_raw_payload,
        skipped_missing_search_item,
        batches_processed,
    )


def _should_continue_batch_loop(*, max_batches: int | None, batches_processed: int) -> bool:
    if max_batches is None:
        return True
    return batches_processed < max_batches
