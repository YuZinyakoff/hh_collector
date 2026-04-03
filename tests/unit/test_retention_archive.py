from __future__ import annotations

import gzip
import json
from datetime import UTC, datetime
from pathlib import Path

from hhru_platform.application.commands.export_retention_archive import (
    ExportRetentionArchiveCommand,
    export_retention_archive,
)
from hhru_platform.application.commands.run_housekeeping import (
    HousekeepingRetentionPolicy,
)
from hhru_platform.infrastructure.housekeeping import LocalRetentionArchiveStore


class FakeRetentionArchiveRepository:
    def count_raw_api_payload_candidates(self, *, cutoff: datetime) -> int:
        return 3

    def list_raw_api_payload_ids_for_retention(
        self,
        *,
        cutoff: datetime,
        limit: int,
    ) -> list[int]:
        return [101, 102][:limit]

    def list_raw_api_payload_rows_for_archive(
        self,
        *,
        payload_ids: tuple[int, ...],
    ) -> list[dict[str, object]]:
        assert payload_ids == (101, 102)
        return [
            {
                "id": 101,
                "api_request_log_id": 1,
                "crawl_run_id": "run-1",
                "endpoint_type": "vacancies.search",
                "entity_hh_id": "hh-1",
                "payload_json": {"items": [{"id": "hh-1"}]},
                "payload_hash": "hash-101",
                "received_at": datetime(2026, 3, 1, 10, 0, tzinfo=UTC),
            },
            {
                "id": 102,
                "api_request_log_id": 2,
                "crawl_run_id": "run-2",
                "endpoint_type": "vacancies.search",
                "entity_hh_id": "hh-2",
                "payload_json": {"items": [{"id": "hh-2"}]},
                "payload_hash": "hash-102",
                "received_at": datetime(2026, 3, 1, 11, 0, tzinfo=UTC),
            },
        ]

    def count_vacancy_snapshot_candidates(self, *, cutoff: datetime) -> int:
        return 1

    def list_vacancy_snapshot_ids_for_retention(
        self,
        *,
        cutoff: datetime,
        limit: int,
    ) -> list[int]:
        return [201][:limit]

    def list_vacancy_snapshot_rows_for_archive(
        self,
        *,
        snapshot_ids: tuple[int, ...],
    ) -> list[dict[str, object]]:
        assert snapshot_ids == (201,)
        return [
            {
                "id": 201,
                "vacancy_id": "vac-1",
                "snapshot_type": "short",
                "captured_at": datetime(2026, 3, 2, 12, 0, tzinfo=UTC),
                "crawl_run_id": "run-1",
                "short_hash": "short-1",
                "detail_hash": None,
                "short_payload_ref_id": 101,
                "detail_payload_ref_id": None,
                "normalized_json": {"schema_version": "2"},
                "change_reason": "first_seen",
            }
        ]


def test_export_retention_archive_writes_gzip_chunks_and_manifest(tmp_path: Path) -> None:
    result = export_retention_archive(
        ExportRetentionArchiveCommand(
            retention_policy=HousekeepingRetentionPolicy(
                raw_api_payload_retention_days=90,
                vacancy_snapshot_retention_days=365,
                finished_crawl_run_retention_days=60,
                detail_fetch_attempt_retention_days=180,
                report_artifact_retention_days=21,
                delete_limit_per_target=10,
            ),
            archive_dir=tmp_path / "archive",
            triggered_by="unit-test",
            evaluated_at=datetime(2026, 4, 3, 12, 0, tzinfo=UTC),
        ),
        retention_archive_repository=FakeRetentionArchiveRepository(),
        retention_archive_store=LocalRetentionArchiveStore(),
    )

    assert result.status == "succeeded"
    assert result.total_candidates == 4
    assert result.total_exported == 3

    raw_summary = next(
        summary for summary in result.summaries if summary.target == "raw_api_payload"
    )
    snapshot_summary = next(
        summary for summary in result.summaries if summary.target == "vacancy_snapshot"
    )

    assert raw_summary.exported_count == 2
    assert raw_summary.archive_file is not None
    assert raw_summary.manifest_file is not None
    assert raw_summary.archive_file.exists()
    assert raw_summary.manifest_file.exists()
    with gzip.open(raw_summary.archive_file, "rt", encoding="utf-8") as handle:
        raw_lines = [json.loads(line) for line in handle]
    assert [line["id"] for line in raw_lines] == [101, 102]

    manifest_payload = json.loads(raw_summary.manifest_file.read_text(encoding="utf-8"))
    assert manifest_payload["target"] == "raw_api_payload"
    assert manifest_payload["record_count"] == 2
    assert manifest_payload["metadata"]["triggered_by"] == "unit-test"
    assert manifest_payload["metadata"]["selected_ids"] == [101, 102]

    assert snapshot_summary.exported_count == 1
    assert snapshot_summary.archive_file is not None
    with gzip.open(snapshot_summary.archive_file, "rt", encoding="utf-8") as handle:
        snapshot_lines = [json.loads(line) for line in handle]
    assert snapshot_lines[0]["id"] == 201


def test_export_retention_archive_disables_targets_with_zero_retention(tmp_path: Path) -> None:
    result = export_retention_archive(
        ExportRetentionArchiveCommand(
            retention_policy=HousekeepingRetentionPolicy(
                raw_api_payload_retention_days=0,
                vacancy_snapshot_retention_days=0,
                finished_crawl_run_retention_days=60,
                detail_fetch_attempt_retention_days=180,
                report_artifact_retention_days=21,
                delete_limit_per_target=10,
            ),
            archive_dir=tmp_path / "archive",
            triggered_by="unit-test",
            evaluated_at=datetime(2026, 4, 3, 12, 0, tzinfo=UTC),
        ),
        retention_archive_repository=FakeRetentionArchiveRepository(),
        retention_archive_store=LocalRetentionArchiveStore(),
    )

    assert result.total_candidates == 0
    assert result.total_exported == 0
    assert all(summary.enabled is False for summary in result.summaries)
