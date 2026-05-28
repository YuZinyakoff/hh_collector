from __future__ import annotations

import gzip
import json
from collections.abc import Iterable, Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from hhru_platform.application.commands.export_research_archive import (
    ExportResearchArchiveCommand,
    export_research_archive,
)
from hhru_platform.application.commands.verify_research_archive import (
    VerifyResearchArchiveCommand,
    verify_research_archive,
)
from hhru_platform.infrastructure.research_archive import (
    LocalResearchArchiveStore,
    ResearchArchiveManifestVerifier,
)


class FakeResearchArchiveRepository:
    def __init__(self) -> None:
        self.seen_datasets: list[str] = []

    def iter_dataset_records(
        self,
        *,
        dataset: str,
        batch_size: int,
        limit: int | None,
    ) -> Iterable[Mapping[str, Any]]:
        assert batch_size == 100
        assert limit is None
        self.seen_datasets.append(dataset)
        if dataset == "bronze/raw_api_payload":
            yield {
                "raw_api_payload_id": 101,
                "api_request_log_id": 201,
                "crawl_run_id": "run-1",
                "crawl_partition_id": "partition-1",
                "request_type": "vacancy_detail",
                "endpoint_type": "vacancy_detail",
                "endpoint": "/vacancies/123",
                "method": "GET",
                "params_json": {"locale": "RU"},
                "status_code": 200,
                "latency_ms": 96,
                "requested_at": datetime(2026, 5, 26, 10, 0, tzinfo=UTC),
                "response_received_at": datetime(2026, 5, 26, 10, 0, 1, tzinfo=UTC),
                "entity_hh_id": "123",
                "payload_hash": "hash-101",
                "received_at": datetime(2026, 5, 26, 10, 0, 2, tzinfo=UTC),
                "payload_json": {"id": "123", "name": "Python developer"},
            }
        elif dataset == "silver/vacancy_current_state":
            yield {
                "vacancy_id": "vacancy-1",
                "hh_vacancy_id": "123",
                "first_seen_at": datetime(2026, 5, 25, 10, 0, tzinfo=UTC),
                "last_seen_at": datetime(2026, 5, 26, 10, 0, tzinfo=UTC),
                "seen_count": 2,
                "consecutive_missing_runs": 0,
                "is_probably_inactive": False,
                "last_seen_run_id": "run-2",
                "last_short_hash": "short-1",
                "last_detail_hash": "detail-1",
                "last_detail_fetched_at": datetime(2026, 5, 26, 10, 0, tzinfo=UTC),
                "detail_fetch_status": "succeeded",
                "updated_at": datetime(2026, 5, 26, 10, 1, tzinfo=UTC),
            }
        else:
            raise AssertionError(f"unexpected dataset: {dataset}")


def test_export_research_archive_writes_manifest_inventory_and_verifies(
    tmp_path: Path,
) -> None:
    repository = FakeResearchArchiveRepository()
    archive_dir = tmp_path / "research"
    created_at = datetime(2026, 5, 27, 12, 0, tzinfo=UTC)

    result = export_research_archive(
        ExportResearchArchiveCommand(
            archive_dir=archive_dir,
            datasets=("bronze/raw_api_payload", "silver/vacancy_current_state"),
            chunk_size=10,
            batch_size=100,
            archive_kind="tool_validation",
            triggered_by="unit-test",
            source_database="hhru_platform",
            source_git_revision="test-revision",
            source_command="pytest",
            created_at=created_at,
        ),
        research_archive_repository=repository,
        research_archive_store=LocalResearchArchiveStore(),
    )

    assert result.status == "succeeded"
    assert result.total_chunk_count == 2
    assert result.total_row_count == 2
    assert repository.seen_datasets == [
        "bronze/raw_api_payload",
        "silver/vacancy_current_state",
    ]

    raw_summary = result.summaries[0]
    raw_manifest = json.loads(raw_summary.manifest_files[0].read_text(encoding="utf-8"))
    assert raw_manifest["archive_schema_version"] == "research-archive-v1"
    assert raw_manifest["dataset"] == "raw_api_payload"
    assert raw_manifest["dataset_key"] == "bronze/raw_api_payload"
    assert raw_manifest["row_count"] == 1
    assert raw_manifest["source_database"] == "hhru_platform"
    assert raw_manifest["source_git_revision"] == "test-revision"

    with gzip.open(raw_summary.data_files[0], "rt", encoding="utf-8") as handle:
        raw_rows = [json.loads(line) for line in handle if line.strip()]
    assert raw_rows[0]["archive_schema_version"] == "research-archive-v1"
    assert raw_rows[0]["dataset"] == "raw_api_payload"
    assert raw_rows[0]["payload_json"]["name"] == "Python developer"

    current_state_summary = result.summaries[1]
    with gzip.open(current_state_summary.data_files[0], "rt", encoding="utf-8") as handle:
        current_state_rows = [json.loads(line) for line in handle if line.strip()]
    assert current_state_rows[0]["dataset"] == "vacancy_current_state"
    assert current_state_rows[0]["snapshot_date"] == "2026-05-27"

    inventory_file = archive_dir / "v1" / "inventory" / "archive-inventory.jsonl"
    inventory_rows = [
        json.loads(line) for line in inventory_file.read_text(encoding="utf-8").splitlines()
    ]
    assert [row["dataset_key"] for row in inventory_rows] == [
        "bronze/raw_api_payload",
        "silver/vacancy_current_state",
    ]
    assert all(row["status"] == "exported" for row in inventory_rows)

    verify_result = verify_research_archive(
        VerifyResearchArchiveCommand(
            archive_dir=archive_dir,
            triggered_by="unit-test",
        ),
        manifest_verifier=ResearchArchiveManifestVerifier(),
    )

    assert verify_result.status == "succeeded"
    assert verify_result.scanned_manifest_count == 2
    assert verify_result.verified_manifest_count == 2
    assert verify_result.total_row_count == 2


def test_verify_research_archive_detects_checksum_mismatch(tmp_path: Path) -> None:
    repository = FakeResearchArchiveRepository()
    archive_dir = tmp_path / "research"
    result = export_research_archive(
        ExportResearchArchiveCommand(
            archive_dir=archive_dir,
            datasets=("bronze/raw_api_payload",),
            chunk_size=10,
            batch_size=100,
            source_database="hhru_platform",
            source_git_revision="test-revision",
            source_command="pytest",
            created_at=datetime(2026, 5, 27, 12, 0, tzinfo=UTC),
        ),
        research_archive_repository=repository,
        research_archive_store=LocalResearchArchiveStore(),
    )

    result.summaries[0].data_files[0].write_bytes(b"corrupted")

    with pytest.raises(ValueError, match="archive sha256 mismatch"):
        verify_research_archive(
            VerifyResearchArchiveCommand(archive_dir=archive_dir),
            manifest_verifier=ResearchArchiveManifestVerifier(),
        )
