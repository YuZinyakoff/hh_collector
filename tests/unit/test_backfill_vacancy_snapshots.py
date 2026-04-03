from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from hhru_platform.application.commands.backfill_vacancy_snapshots import (
    BackfillVacancySnapshotsCommand,
    LegacyDetailSnapshotCandidate,
    ShortSnapshotBackfillCandidate,
    backfill_vacancy_snapshots,
)


class FakeVacancySnapshotBackfillRepository:
    def __init__(self) -> None:
        self.detail_candidates = [
            LegacyDetailSnapshotCandidate(
                snapshot_id=11,
                vacancy_id=uuid4(),
                detail_payload_ref_id=101,
            )
        ]
        self.short_candidates = [
            ShortSnapshotBackfillCandidate(
                vacancy_id=uuid4(),
                hh_vacancy_id="pytest-short-vacancy",
                crawl_run_id=uuid4(),
                crawl_partition_id=uuid4(),
                seen_at=datetime(2026, 3, 21, 12, 0, tzinfo=UTC),
                list_position=3,
                short_hash="pytest-short-hash",
                short_payload_ref_id=201,
                change_reason="first_seen",
            )
        ]
        self.raw_payloads: dict[int, object] = {
            101: {
                "id": "pytest-detail-vacancy",
                "name": "Senior Python Engineer",
                "description": "Detailed vacancy description",
            },
            201: {
                "items": [
                    {
                        "id": "pytest-short-vacancy",
                        "name": "Python Engineer",
                        "alternate_url": "https://hh.ru/vacancy/pytest-short-vacancy",
                    }
                ]
            },
        }
        self.updated_detail_snapshots: list[dict[str, object]] = []
        self.created_short_snapshots: list[dict[str, object]] = []
        self.synced_vacancy_ids: list[tuple[object, ...]] = []

    def list_legacy_detail_snapshot_candidates(self, *, limit: int):
        candidates = self.detail_candidates[:limit]
        self.detail_candidates = self.detail_candidates[limit:]
        return candidates

    def update_detail_snapshot(
        self,
        *,
        snapshot_id: int,
        detail_hash: str,
        snapshot_json: dict[str, object],
    ) -> None:
        self.updated_detail_snapshots.append(
            {
                "snapshot_id": snapshot_id,
                "detail_hash": detail_hash,
                "snapshot_json": snapshot_json,
            }
        )

    def list_short_snapshot_backfill_candidates(self, *, limit: int):
        candidates = self.short_candidates[:limit]
        self.short_candidates = self.short_candidates[limit:]
        return candidates

    def load_raw_payload_json(self, payload_id: int) -> object | None:
        return self.raw_payloads.get(payload_id)

    def add_short_snapshot(self, **kwargs: object) -> int:
        self.created_short_snapshots.append(dict(kwargs))
        return len(self.created_short_snapshots)

    def sync_current_state_detail_hashes(self, *, vacancy_ids: list) -> int:
        self.synced_vacancy_ids.append(tuple(vacancy_ids))
        return len(vacancy_ids)


def test_backfill_vacancy_snapshots_updates_legacy_detail_rows_and_creates_short_rows() -> None:
    repository = FakeVacancySnapshotBackfillRepository()

    result = backfill_vacancy_snapshots(
        BackfillVacancySnapshotsCommand(batch_size=10),
        repository=repository,
    )

    assert result.status == "succeeded"
    assert result.detail_candidates_seen == 1
    assert result.detail_snapshots_updated == 1
    assert result.short_candidates_seen == 1
    assert result.short_snapshots_created == 1
    assert result.skipped_missing_raw_payload == 0
    assert result.skipped_missing_search_item == 0
    assert repository.updated_detail_snapshots[0]["snapshot_json"]["payload"]["id"] == (
        "pytest-detail-vacancy"
    )
    assert repository.created_short_snapshots[0]["snapshot_json"]["payload"]["id"] == (
        "pytest-short-vacancy"
    )
    assert repository.created_short_snapshots[0]["change_reason"] == "first_seen"
    assert repository.synced_vacancy_ids


def test_backfill_vacancy_snapshots_reports_missing_raw_and_missing_search_item() -> None:
    repository = FakeVacancySnapshotBackfillRepository()
    repository.detail_candidates = [
        LegacyDetailSnapshotCandidate(
            snapshot_id=12,
            vacancy_id=uuid4(),
            detail_payload_ref_id=999,
        )
    ]
    repository.short_candidates = [
        ShortSnapshotBackfillCandidate(
            vacancy_id=uuid4(),
            hh_vacancy_id="missing-search-item",
            crawl_run_id=uuid4(),
            crawl_partition_id=uuid4(),
            seen_at=datetime(2026, 3, 21, 12, 30, tzinfo=UTC),
            list_position=0,
            short_hash="pytest-short-hash-missing",
            short_payload_ref_id=201,
            change_reason="short_hash_changed",
        )
    ]

    result = backfill_vacancy_snapshots(
        BackfillVacancySnapshotsCommand(batch_size=10),
        repository=repository,
    )

    assert result.detail_candidates_seen == 1
    assert result.detail_snapshots_updated == 0
    assert result.short_candidates_seen == 1
    assert result.short_snapshots_created == 0
    assert result.skipped_missing_raw_payload == 1
    assert result.skipped_missing_search_item == 1
