from __future__ import annotations

import os
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest

from hhru_platform.application.commands.run_housekeeping import (
    HOUSEKEEPING_MODE_DRY_RUN,
    HOUSEKEEPING_MODE_EXECUTE,
    HousekeepingRetentionPolicy,
    RunHousekeepingCommand,
    run_housekeeping,
)
from hhru_platform.infrastructure.housekeeping import LocalReportArtifactStore


class RecordingHousekeepingMetricsRecorder:
    def __init__(self) -> None:
        self.runs: list[dict[str, object]] = []
        self.actions: list[dict[str, object]] = []
        self.deleted: list[dict[str, object]] = []

    def record_housekeeping_run(
        self,
        *,
        mode: str,
        status: str,
        recorded_at: datetime,
    ) -> None:
        self.runs.append(
            {
                "mode": mode,
                "status": status,
                "recorded_at": recorded_at,
            }
        )

    def set_housekeeping_last_action_count(
        self,
        *,
        target: str,
        mode: str,
        count: int,
    ) -> None:
        self.actions.append(
            {
                "target": target,
                "mode": mode,
                "count": count,
            }
        )

    def record_housekeeping_deleted(
        self,
        *,
        target: str,
        count: int,
    ) -> None:
        self.deleted.append(
            {
                "target": target,
                "count": count,
            }
        )


class FakeHousekeepingRepository:
    def __init__(self) -> None:
        self.raw_ids = [1, 2]
        self.snapshot_ids = [10]
        self.detail_attempt_ids = [20, 21]
        self.run_ids = [uuid4()]
        self.partition_count_for_selected_runs = 4
        self.delete_calls: list[tuple[str, tuple[object, ...]]] = []

    def count_raw_api_payload_candidates(self, *, cutoff: datetime) -> int:
        return 3

    def list_raw_api_payload_ids_for_retention(
        self,
        *,
        cutoff: datetime,
        limit: int,
    ) -> list[int]:
        return self.raw_ids[:limit]

    def delete_raw_api_payloads(self, payload_ids) -> int:
        self.delete_calls.append(("raw_api_payload", tuple(payload_ids)))
        return len(tuple(payload_ids))

    def list_raw_api_payload_rows_for_archive(
        self,
        *,
        payload_ids,
    ) -> list[dict[str, object]]:
        return [
            {
                "id": payload_id,
                "payload_hash": f"raw-{payload_id}",
                "received_at": datetime(2026, 1, 1, 10, 0, tzinfo=UTC),
            }
            for payload_id in payload_ids
        ]

    def count_vacancy_snapshot_candidates(self, *, cutoff: datetime) -> int:
        return 1

    def list_vacancy_snapshot_ids_for_retention(
        self,
        *,
        cutoff: datetime,
        limit: int,
    ) -> list[int]:
        return self.snapshot_ids[:limit]

    def delete_vacancy_snapshots(self, snapshot_ids) -> int:
        self.delete_calls.append(("vacancy_snapshot", tuple(snapshot_ids)))
        return len(tuple(snapshot_ids))

    def list_vacancy_snapshot_rows_for_archive(
        self,
        *,
        snapshot_ids,
    ) -> list[dict[str, object]]:
        return [
            {
                "id": snapshot_id,
                "snapshot_type": "short",
                "captured_at": datetime(2025, 1, 1, 10, 0, tzinfo=UTC),
            }
            for snapshot_id in snapshot_ids
        ]

    def count_detail_fetch_attempt_candidates(self, *, cutoff: datetime) -> int:
        return 4

    def list_detail_fetch_attempt_ids_for_retention(
        self,
        *,
        cutoff: datetime,
        limit: int,
    ) -> list[int]:
        return self.detail_attempt_ids[:limit]

    def delete_detail_fetch_attempts(self, attempt_ids) -> int:
        self.delete_calls.append(("detail_fetch_attempt", tuple(attempt_ids)))
        return len(tuple(attempt_ids))

    def count_finished_crawl_run_candidates(self, *, cutoff: datetime) -> int:
        return 2

    def list_finished_crawl_run_ids_for_retention(
        self,
        *,
        cutoff: datetime,
        limit: int,
    ) -> list:
        return self.run_ids[:limit]

    def delete_finished_crawl_runs(self, run_ids) -> int:
        self.delete_calls.append(("crawl_run", tuple(run_ids)))
        return len(tuple(run_ids))

    def count_crawl_partition_candidates_for_finished_runs(self, *, cutoff: datetime) -> int:
        return 7

    def count_crawl_partitions_for_run_ids(self, run_ids) -> int:
        assert tuple(run_ids) == tuple(self.run_ids)
        return self.partition_count_for_selected_runs


class FakeReportArtifactStore:
    def __init__(self, paths: list[Path]) -> None:
        self.paths = paths
        self.delete_calls: list[tuple[Path, ...]] = []

    def count_candidates(self, *, root_dir: Path, cutoff: datetime) -> int:
        return len(self.paths) + 1

    def list_candidates(
        self,
        *,
        root_dir: Path,
        cutoff: datetime,
        limit: int | None,
    ) -> list[Path]:
        if limit is None:
            return list(self.paths)
        return self.paths[:limit]

    def delete_candidates(self, paths: list[Path]) -> int:
        self.delete_calls.append(tuple(paths))
        return len(paths)


class FakeRetentionArchiveStore:
    def __init__(self) -> None:
        self.write_calls: list[dict[str, object]] = []

    def write_records(
        self,
        *,
        archive_dir: Path,
        target: str,
        evaluated_at: datetime,
        records,
        metadata,
    ):
        self.write_calls.append(
            {
                "archive_dir": archive_dir,
                "target": target,
                "evaluated_at": evaluated_at,
                "records": tuple(records),
                "metadata": dict(metadata),
            }
        )
        return type(
            "ArchiveSummary",
            (),
            {
                "archive_file": archive_dir / target / "chunk.jsonl.gz",
                "manifest_file": archive_dir / target / "chunk.manifest.json",
                "archive_size_bytes": 256,
                "archive_sha256": f"sha-{target}",
                "record_count": len(tuple(records)),
            },
        )()


def test_run_housekeeping_dry_run_plans_candidates_without_deleting() -> None:
    repository = FakeHousekeepingRepository()
    artifact_store = FakeReportArtifactStore([Path("old-report-a"), Path("old-report-b")])
    metrics_recorder = RecordingHousekeepingMetricsRecorder()
    evaluated_at = datetime(2026, 3, 21, 12, 0, tzinfo=UTC)

    result = run_housekeeping(
        RunHousekeepingCommand(
            retention_policy=HousekeepingRetentionPolicy(
                raw_api_payload_retention_days=90,
                vacancy_snapshot_retention_days=365,
                finished_crawl_run_retention_days=30,
                detail_fetch_attempt_retention_days=180,
                report_artifact_retention_days=14,
                delete_limit_per_target=2,
            ),
            execute=False,
            triggered_by="pytest-housekeeping-dry-run",
            evaluated_at=evaluated_at,
        ),
        housekeeping_repository=repository,
        report_artifact_store=artifact_store,
        metrics_recorder=metrics_recorder,
    )

    assert result.status == "succeeded"
    assert result.mode == HOUSEKEEPING_MODE_DRY_RUN
    assert result.total_candidates == 20
    assert result.total_action_count == 12
    assert result.total_deleted == 0
    assert result.total_archived == 0
    assert repository.delete_calls == []
    assert artifact_store.delete_calls == []
    assert metrics_recorder.runs == [
        {
            "mode": "dry_run",
            "status": "succeeded",
            "recorded_at": evaluated_at,
        }
    ]
    assert metrics_recorder.deleted == []
    assert metrics_recorder.actions == [
        {"target": "raw_api_payload", "mode": "dry_run", "count": 2},
        {"target": "vacancy_snapshot", "mode": "dry_run", "count": 1},
        {"target": "detail_fetch_attempt", "mode": "dry_run", "count": 2},
        {"target": "crawl_run", "mode": "dry_run", "count": 1},
        {"target": "crawl_partition", "mode": "dry_run", "count": 4},
        {"target": "detail_payload_study_artifact", "mode": "dry_run", "count": 2},
    ]
    partition_summary = next(
        summary for summary in result.summaries if summary.target == "crawl_partition"
    )
    assert partition_summary.candidate_count == 7
    assert partition_summary.action_count == 4
    assert partition_summary.deleted_count == 0
    assert partition_summary.limited is True


def test_run_housekeeping_execute_deletes_selected_rows_and_files() -> None:
    repository = FakeHousekeepingRepository()
    artifact_store = FakeReportArtifactStore([Path("old-report-a")])
    metrics_recorder = RecordingHousekeepingMetricsRecorder()
    evaluated_at = datetime(2026, 3, 21, 13, 0, tzinfo=UTC)

    result = run_housekeeping(
        RunHousekeepingCommand(
            retention_policy=HousekeepingRetentionPolicy(
                raw_api_payload_retention_days=90,
                vacancy_snapshot_retention_days=365,
                finished_crawl_run_retention_days=30,
                detail_fetch_attempt_retention_days=180,
                report_artifact_retention_days=14,
                delete_limit_per_target=2,
            ),
            execute=True,
            triggered_by="pytest-housekeeping-execute",
            evaluated_at=evaluated_at,
        ),
        housekeeping_repository=repository,
        report_artifact_store=artifact_store,
        metrics_recorder=metrics_recorder,
    )

    assert result.mode == HOUSEKEEPING_MODE_EXECUTE
    assert result.total_deleted == 11
    assert result.total_archived == 0
    assert repository.delete_calls == [
        ("raw_api_payload", (1, 2)),
        ("vacancy_snapshot", (10,)),
        ("detail_fetch_attempt", (20, 21)),
        ("crawl_run", tuple(repository.run_ids)),
    ]
    assert artifact_store.delete_calls == [(Path("old-report-a"),)]
    assert metrics_recorder.runs == [
        {
            "mode": "execute",
            "status": "succeeded",
            "recorded_at": evaluated_at,
        }
    ]
    assert metrics_recorder.deleted == [
        {"target": "raw_api_payload", "count": 2},
        {"target": "vacancy_snapshot", "count": 1},
        {"target": "detail_fetch_attempt", "count": 2},
        {"target": "crawl_run", "count": 1},
        {"target": "crawl_partition", "count": 4},
        {"target": "detail_payload_study_artifact", "count": 1},
    ]
    partition_summary = next(
        summary for summary in result.summaries if summary.target == "crawl_partition"
    )
    assert partition_summary.deleted_count == 4


def test_run_housekeeping_execute_archives_raw_and_snapshot_before_delete() -> None:
    repository = FakeHousekeepingRepository()
    artifact_store = FakeReportArtifactStore([Path("old-report-a")])
    archive_store = FakeRetentionArchiveStore()
    evaluated_at = datetime(2026, 3, 21, 15, 0, tzinfo=UTC)

    result = run_housekeeping(
        RunHousekeepingCommand(
            retention_policy=HousekeepingRetentionPolicy(
                raw_api_payload_retention_days=90,
                vacancy_snapshot_retention_days=365,
                finished_crawl_run_retention_days=30,
                detail_fetch_attempt_retention_days=180,
                report_artifact_retention_days=14,
                delete_limit_per_target=2,
            ),
            execute=True,
            archive_before_delete=True,
            archive_dir=Path(".state/archive/retention"),
            triggered_by="pytest-housekeeping-archive-execute",
            evaluated_at=evaluated_at,
        ),
        housekeeping_repository=repository,
        report_artifact_store=artifact_store,
        retention_archive_store=archive_store,
    )

    assert result.total_deleted == 11
    assert result.total_archived == 3
    assert [call["target"] for call in archive_store.write_calls] == [
        "raw_api_payload",
        "vacancy_snapshot",
    ]
    assert archive_store.write_calls[0]["metadata"]["selected_ids"] == (1, 2)
    assert archive_store.write_calls[1]["metadata"]["selected_ids"] == (10,)
    raw_summary = next(
        summary for summary in result.summaries if summary.target == "raw_api_payload"
    )
    snapshot_summary = next(
        summary for summary in result.summaries if summary.target == "vacancy_snapshot"
    )
    assert raw_summary.archived_count == 2
    assert raw_summary.archive_sha256 == "sha-raw_api_payload"
    assert snapshot_summary.archived_count == 1
    assert snapshot_summary.archive_sha256 == "sha-vacancy_snapshot"


def test_local_report_artifact_store_lists_and_deletes_old_children_only(
    tmp_path: Path,
) -> None:
    root_dir = tmp_path / "reports"
    root_dir.mkdir()
    old_dir = root_dir / "20260301T000000Z"
    old_dir.mkdir()
    (old_dir / "summary.md").write_text("old", encoding="utf-8")
    new_dir = root_dir / "20260321T000000Z"
    new_dir.mkdir()
    old_file = root_dir / "orphan.txt"
    old_file.write_text("old-file", encoding="utf-8")
    store = LocalReportArtifactStore()
    cutoff = datetime(2026, 3, 15, tzinfo=UTC)
    old_timestamp = datetime(2026, 3, 1, tzinfo=UTC).timestamp()
    new_timestamp = datetime(2026, 3, 21, tzinfo=UTC).timestamp()

    old_dir.chmod(0o755)
    new_dir.chmod(0o755)
    old_file.chmod(0o644)
    os.utime(old_dir, (old_timestamp, old_timestamp))
    os.utime(old_file, (old_timestamp, old_timestamp))
    os.utime(new_dir, (new_timestamp, new_timestamp))

    candidates = store.list_candidates(root_dir=root_dir, cutoff=cutoff, limit=None)

    assert candidates == [old_dir, old_file]
    assert store.count_candidates(root_dir=root_dir, cutoff=cutoff) == 2
    assert store.delete_candidates(candidates) == 2
    assert not old_dir.exists()
    assert not old_file.exists()
    assert new_dir.exists()


def test_housekeeping_retention_policy_rejects_negative_windows() -> None:
    with pytest.raises(ValueError):
        HousekeepingRetentionPolicy(
            raw_api_payload_retention_days=-1,
            vacancy_snapshot_retention_days=365,
            finished_crawl_run_retention_days=30,
            detail_fetch_attempt_retention_days=180,
            report_artifact_retention_days=14,
        )
