from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from uuid import UUID

import pytest

from hhru_platform.application.commands.apply_research_archive_housekeeping import (
    ApplyResearchArchiveHousekeepingCommand,
    apply_research_archive_housekeeping,
)
from hhru_platform.application.commands.preview_research_archive_housekeeping import (
    DATASET_DETAIL_FETCH_ATTEMPT,
    DATASET_RAW_API_PAYLOAD,
    DATASET_VACANCY_SNAPSHOT,
    PreviewResearchArchiveHousekeepingResult,
    ResearchArchiveHousekeepingPreviewSummary,
    ResearchArchiveHousekeepingRunTreePreviewSummary,
)
from hhru_platform.application.commands.run_housekeeping import (
    TARGET_DETAIL_FETCH_ATTEMPT,
    TARGET_RAW_API_PAYLOAD,
    TARGET_VACANCY_SNAPSHOT,
)

EVALUATED_AT = datetime(2026, 6, 1, 12, 0, tzinfo=UTC)
CUTOFF = datetime(2026, 5, 31, 12, 0, tzinfo=UTC)
RUN_ID = UUID("00000000-0000-0000-0000-000000000001")


class FakeApplyHousekeepingRepository:
    def __init__(
        self,
        *,
        lock_matches: bool = True,
        raw_deleted_count: int | None = None,
    ) -> None:
        self.calls: list[tuple[object, ...]] = []
        self.lock_matches = lock_matches
        self.raw_deleted_count = raw_deleted_count

    def list_raw_api_payload_ids_for_retention(
        self,
        *,
        cutoff: datetime,
        limit: int,
        max_source_id: int | None = None,
    ) -> list[int]:
        self.calls.append(("list_raw", cutoff, limit, max_source_id))
        return [3, 5]

    def list_vacancy_snapshot_ids_for_retention(
        self,
        *,
        cutoff: datetime,
        limit: int,
        max_source_id: int | None = None,
    ) -> list[int]:
        self.calls.append(("list_snapshot", cutoff, limit, max_source_id))
        return [11]

    def list_detail_fetch_attempt_ids_for_retention(
        self,
        *,
        cutoff: datetime,
        limit: int,
        max_source_id: int | None = None,
    ) -> list[int]:
        self.calls.append(("list_detail_attempt", cutoff, limit, max_source_id))
        return [21, 22]

    def list_finished_crawl_run_ids_for_retention_bounded_by_seen_event_coverage(
        self,
        *,
        cutoff: datetime,
        limit: int,
        max_seen_event_source_id: int,
    ) -> list[UUID]:
        self.calls.append(("list_safe_run", cutoff, limit, max_seen_event_source_id))
        return [RUN_ID]

    def lock_finished_crawl_run_ids_for_retention_bounded_by_seen_event_coverage(
        self,
        *,
        run_ids,
        cutoff: datetime,
        max_seen_event_source_id: int,
    ) -> list[UUID]:
        self.calls.append(("lock_safe_run", tuple(run_ids), cutoff, max_seen_event_source_id))
        return [RUN_ID] if self.lock_matches else []

    def count_crawl_partitions_for_run_ids(self, run_ids) -> int:
        self.calls.append(("count_partition", tuple(run_ids)))
        return 7

    def count_vacancy_seen_events_for_run_ids(self, run_ids) -> int:
        self.calls.append(("count_seen_event", tuple(run_ids)))
        return 11

    def delete_raw_api_payloads(self, payload_ids) -> int:
        self.calls.append(("delete_raw", tuple(payload_ids)))
        return self.raw_deleted_count if self.raw_deleted_count is not None else len(payload_ids)

    def delete_vacancy_snapshots(self, snapshot_ids) -> int:
        self.calls.append(("delete_snapshot", tuple(snapshot_ids)))
        return len(snapshot_ids)

    def delete_detail_fetch_attempts(self, attempt_ids) -> int:
        self.calls.append(("delete_detail_attempt", tuple(attempt_ids)))
        return len(attempt_ids)

    def delete_finished_crawl_runs(self, run_ids) -> int:
        self.calls.append(("delete_run", tuple(run_ids)))
        return len(run_ids)


def test_apply_research_archive_housekeeping_requires_explicit_confirmation() -> None:
    with pytest.raises(ValueError, match="--apply confirmation is required"):
        _command(confirmed_apply=False)


def test_apply_research_archive_housekeeping_rejects_non_production_archive() -> None:
    with pytest.raises(ValueError, match="archive_kind must be production"):
        _command(archive_kind="incremental_validation")


def test_apply_research_archive_housekeeping_rejects_non_canonical_offsite_root() -> None:
    with pytest.raises(ValueError, match="offsite_root must be /hhru-platform/research-archive"):
        _command(offsite_root="/hhru-platform/research-archive-smoke/test")


def test_apply_research_archive_housekeeping_deletes_nothing_if_coverage_is_incomplete(
    monkeypatch,
) -> None:
    repository = FakeApplyHousekeepingRepository()
    monkeypatch.setattr(
        "hhru_platform.application.commands.apply_research_archive_housekeeping."
        "preview_research_archive_housekeeping",
        lambda *args, **kwargs: _preview_result(status="blocked"),
    )

    with pytest.raises(RuntimeError, match="verified research archive coverage is incomplete"):
        apply_research_archive_housekeeping(
            _command(),
            housekeeping_repository=cast(Any, repository),
            checkpoint_store=cast(Any, object()),
            receipt_store=cast(Any, object()),
            checkpoint_receipt_store=cast(Any, object()),
        )

    assert repository.calls == []


def test_apply_research_archive_housekeeping_deletes_only_replanned_verified_ids(
    monkeypatch,
) -> None:
    repository = FakeApplyHousekeepingRepository()
    monkeypatch.setattr(
        "hhru_platform.application.commands.apply_research_archive_housekeeping."
        "preview_research_archive_housekeeping",
        lambda *args, **kwargs: _preview_result(),
    )

    result = apply_research_archive_housekeeping(
        _command(),
        housekeeping_repository=cast(Any, repository),
        checkpoint_store=cast(Any, object()),
        receipt_store=cast(Any, object()),
        checkpoint_receipt_store=cast(Any, object()),
    )

    assert result.direct_deleted_count == 6
    assert result.total_deleted_count == 24
    assert result.run_tree_summary.cascade_partition_count == 7
    assert result.run_tree_summary.cascade_vacancy_seen_event_count == 11
    assert repository.calls == [
        ("list_raw", CUTOFF, 10, 81),
        ("list_snapshot", CUTOFF, 10, 1240),
        ("list_detail_attempt", CUTOFF, 10, 403),
        ("list_safe_run", CUTOFF, 10, 1240),
        ("lock_safe_run", (RUN_ID,), CUTOFF, 1240),
        ("count_partition", (RUN_ID,)),
        ("count_seen_event", (RUN_ID,)),
        ("delete_raw", (3, 5)),
        ("delete_snapshot", (11,)),
        ("delete_detail_attempt", (21, 22)),
        ("delete_run", (RUN_ID,)),
    ]


def test_apply_research_archive_housekeeping_deletes_nothing_if_run_lock_recheck_fails(
    monkeypatch,
) -> None:
    repository = FakeApplyHousekeepingRepository(lock_matches=False)
    monkeypatch.setattr(
        "hhru_platform.application.commands.apply_research_archive_housekeeping."
        "preview_research_archive_housekeeping",
        lambda *args, **kwargs: _preview_result(),
    )

    with pytest.raises(RuntimeError, match="selection changed before lock acquisition"):
        apply_research_archive_housekeeping(
            _command(),
            housekeeping_repository=cast(Any, repository),
            checkpoint_store=cast(Any, object()),
            receipt_store=cast(Any, object()),
            checkpoint_receipt_store=cast(Any, object()),
        )

    assert not [call for call in repository.calls if str(call[0]).startswith("delete_")]


def test_apply_research_archive_housekeeping_raises_on_delete_count_mismatch(
    monkeypatch,
) -> None:
    repository = FakeApplyHousekeepingRepository(raw_deleted_count=1)
    monkeypatch.setattr(
        "hhru_platform.application.commands.apply_research_archive_housekeeping."
        "preview_research_archive_housekeeping",
        lambda *args, **kwargs: _preview_result(),
    )

    with pytest.raises(RuntimeError, match="raw_api_payload delete count mismatch"):
        apply_research_archive_housekeeping(
            _command(),
            housekeeping_repository=cast(Any, repository),
            checkpoint_store=cast(Any, object()),
            receipt_store=cast(Any, object()),
            checkpoint_receipt_store=cast(Any, object()),
        )


def _command(
    *,
    archive_kind: str = "production",
    offsite_root: str = "/hhru-platform/research-archive",
    confirmed_apply: bool = True,
) -> ApplyResearchArchiveHousekeepingCommand:
    return ApplyResearchArchiveHousekeepingCommand(
        archive_dir=Path(".state/archive/research"),
        archive_kind=archive_kind,
        offsite_url="https://s3.example.test/bucket",
        offsite_root=offsite_root,
        raw_api_payload_retention_days=1,
        vacancy_snapshot_retention_days=1,
        detail_fetch_attempt_retention_days=1,
        finished_crawl_run_retention_days=1,
        delete_limit_per_target=10,
        confirmed_apply=confirmed_apply,
        triggered_by="unit-test",
        evaluated_at=EVALUATED_AT,
    )


def _preview_result(*, status: str = "ready") -> PreviewResearchArchiveHousekeepingResult:
    summaries = (
        _summary(TARGET_RAW_API_PAYLOAD, DATASET_RAW_API_PAYLOAD, 81),
        _summary(TARGET_VACANCY_SNAPSHOT, DATASET_VACANCY_SNAPSHOT, 1240),
        _summary(TARGET_DETAIL_FETCH_ATTEMPT, DATASET_DETAIL_FETCH_ATTEMPT, 403),
    )
    return PreviewResearchArchiveHousekeepingResult(
        status=status,
        archive_dir=Path(".state/archive/research"),
        archive_kind="production",
        triggered_by="unit-test",
        evaluated_at=EVALUATED_AT,
        coverage=cast(Any, SimpleNamespace(status="complete")),
        summaries=summaries,
        run_tree_summary=ResearchArchiveHousekeepingRunTreePreviewSummary(
            retention_days=1,
            cutoff=CUTOFF,
            seen_event_source_id_covered=1240,
            candidate_count=1,
            coverage_safe_candidate_count=1,
            coverage_blocked_candidate_count=0,
            action_count=1,
            selected_partition_count=7,
            selected_vacancy_seen_event_count=11,
            enabled=True,
        ),
    )


def _summary(
    target: str,
    dataset: str,
    source_id_covered: int,
) -> ResearchArchiveHousekeepingPreviewSummary:
    return ResearchArchiveHousekeepingPreviewSummary(
        target=target,
        dataset=dataset,
        retention_days=1,
        cutoff=CUTOFF,
        source_id_covered=source_id_covered,
        candidate_count=2,
        action_count=2,
        selected_min_id=1,
        selected_max_id=2,
        enabled=True,
    )
