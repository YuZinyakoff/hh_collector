from __future__ import annotations

import gzip
import json
from collections.abc import Iterable, Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import pytest
from sqlalchemy.dialects import postgresql

from hhru_platform.application.commands.audit_research_archive_coverage import (
    AuditResearchArchiveCoverageCommand,
    audit_research_archive_coverage,
)
from hhru_platform.application.commands.export_research_archive import (
    INCREMENTAL_RESEARCH_ARCHIVE_DATASETS,
    ExportResearchArchiveCommand,
    export_research_archive,
)
from hhru_platform.application.commands.sync_research_archive_offsite import (
    SyncResearchArchiveOffsiteCommand,
    sync_research_archive_offsite,
)
from hhru_platform.application.commands.verify_research_archive import (
    VerifyResearchArchiveCommand,
    verify_research_archive,
)
from hhru_platform.application.commands.verify_research_archive_offsite import (
    VerifyResearchArchiveOffsiteCommand,
    verify_research_archive_offsite,
)
from hhru_platform.infrastructure.db.repositories.research_archive_repo import (
    SqlAlchemyResearchArchiveRepository,
)
from hhru_platform.infrastructure.research_archive import (
    LocalResearchArchiveCheckpointStore,
    LocalResearchArchiveCheckpointVerificationReceiptStore,
    LocalResearchArchiveCursorStore,
    LocalResearchArchiveOffsiteUploadReceiptStore,
    LocalResearchArchiveOffsiteVerificationReceiptStore,
    LocalResearchArchiveStore,
    ResearchArchiveManifestVerifier,
)
from hhru_platform.infrastructure.research_archive.checkpoint_store import (
    ResearchArchiveCheckpointDataset,
)


class FakeResearchArchiveRepository:
    def __init__(self) -> None:
        self.seen_datasets: list[str] = []
        self.seen_windows: list[tuple[str, int | None, datetime | None]] = []

    def iter_dataset_records(
        self,
        *,
        dataset: str,
        batch_size: int,
        limit: int | None,
        after_source_id: int | None,
        settled_before: datetime | None,
    ) -> Iterable[Mapping[str, Any]]:
        assert batch_size == 100
        assert limit is None
        self.seen_datasets.append(dataset)
        self.seen_windows.append((dataset, after_source_id, settled_before))
        if dataset == "bronze/raw_api_payload":
            record = _raw_payload_record(101)
            if after_source_id is not None and record["raw_api_payload_id"] <= after_source_id:
                return
            if settled_before is not None and record["received_at"] > settled_before:
                return
            yield record
        elif dataset == "silver/detail_fetch_attempt":
            record = _detail_fetch_attempt_record(401)
            if (
                after_source_id is not None
                and record["detail_fetch_attempt_id"] <= after_source_id
            ):
                return
            if settled_before is not None and record["requested_at"] > settled_before:
                return
            yield record
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


def _raw_payload_record(raw_api_payload_id: int) -> dict[str, Any]:
    return {
        "raw_api_payload_id": raw_api_payload_id,
        "api_request_log_id": raw_api_payload_id + 100,
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
        "payload_hash": f"hash-{raw_api_payload_id}",
        "received_at": datetime(2026, 5, 26, 10, 0, 2, tzinfo=UTC),
        "payload_json": {"id": "123", "name": "Python developer"},
    }


def _detail_fetch_attempt_record(detail_fetch_attempt_id: int) -> dict[str, Any]:
    return {
        "detail_fetch_attempt_id": detail_fetch_attempt_id,
        "vacancy_id": "vacancy-1",
        "hh_vacancy_id": "123",
        "crawl_run_id": "run-1",
        "reason": "first_seen",
        "attempt": 1,
        "status": "succeeded",
        "requested_at": datetime(2026, 5, 26, 10, 0, tzinfo=UTC),
        "finished_at": datetime(2026, 5, 26, 10, 0, 1, tzinfo=UTC),
        "error_message": None,
    }


class FakeResearchArchiveRemoteStore:
    def __init__(self) -> None:
        self.upload_calls: list[tuple[str, bytes]] = []
        self.download_calls: list[tuple[str, str]] = []
        self.objects_by_remote_path: dict[str, bytes] = {}

    def upload_file(self, *, local_file: Path, remote_path: str) -> None:
        payload = local_file.read_bytes()
        self.upload_calls.append((remote_path, payload))
        self.objects_by_remote_path[remote_path] = payload

    def get_file_size(self, *, remote_path: str) -> int:
        return len(self.objects_by_remote_path[remote_path])

    def download_file(self, *, local_file: Path, remote_path: str) -> None:
        self.download_calls.append((remote_path, str(local_file)))
        local_file.write_bytes(self.objects_by_remote_path[remote_path])


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


def test_incremental_export_uses_manifest_cursor_and_is_locally_idempotent(
    tmp_path: Path,
) -> None:
    archive_dir = tmp_path / "research"
    settled_before = datetime(2026, 5, 27, 0, 0, tzinfo=UTC)
    cursor_store = LocalResearchArchiveCursorStore()
    checkpoint_store = LocalResearchArchiveCheckpointStore()
    first_repository = FakeResearchArchiveRepository()

    first_result = export_research_archive(
        ExportResearchArchiveCommand(
            archive_dir=archive_dir,
            datasets=("bronze/raw_api_payload",),
            chunk_size=10,
            batch_size=100,
            archive_kind="production",
            source_database="hhru_platform",
            source_git_revision="test-revision",
            source_command="pytest",
            created_at=datetime(2026, 5, 27, 12, 0, tzinfo=UTC),
            incremental=True,
            settled_before=settled_before,
        ),
        research_archive_repository=first_repository,
        research_archive_store=LocalResearchArchiveStore(),
        research_archive_cursor_store=cursor_store,
        research_archive_checkpoint_store=checkpoint_store,
    )

    assert first_repository.seen_windows == [
        ("bronze/raw_api_payload", 0, settled_before),
    ]
    assert first_result.incremental is True
    assert first_result.total_row_count == 1
    assert first_result.summaries[0].source_id_before == 0
    assert first_result.summaries[0].source_id_after == 101

    second_repository = FakeResearchArchiveRepository()
    second_result = export_research_archive(
        ExportResearchArchiveCommand(
            archive_dir=archive_dir,
            datasets=("bronze/raw_api_payload",),
            chunk_size=10,
            batch_size=100,
            archive_kind="production",
            source_database="hhru_platform",
            source_git_revision="test-revision",
            source_command="pytest",
            created_at=datetime(2026, 5, 27, 13, 0, tzinfo=UTC),
            incremental=True,
            settled_before=settled_before,
        ),
        research_archive_repository=second_repository,
        research_archive_store=LocalResearchArchiveStore(),
        research_archive_cursor_store=cursor_store,
        research_archive_checkpoint_store=checkpoint_store,
    )

    assert second_repository.seen_windows == [
        ("bronze/raw_api_payload", 101, settled_before),
    ]
    assert second_result.total_chunk_count == 0
    assert second_result.total_row_count == 0
    assert second_result.summaries[0].source_id_before == 101
    assert second_result.summaries[0].source_id_after == 101
    checkpoints = checkpoint_store.load_checkpoints(
        archive_dir=archive_dir,
        archive_kind="production",
    )
    assert len(checkpoints) == 2
    assert checkpoints[0].datasets[0].source_id_before == 0
    assert checkpoints[0].datasets[0].source_id_after == 101
    assert checkpoints[1].datasets[0].source_id_before == 101
    assert checkpoints[1].datasets[0].source_id_after == 101


def test_research_archive_manifest_source_range_compares_numeric_ids(
    tmp_path: Path,
) -> None:
    archive_dir = tmp_path / "research"
    summaries = LocalResearchArchiveStore().write_dataset(
        archive_dir=archive_dir,
        schema_version="research-archive-v1",
        dataset="bronze/raw_api_payload",
        records=(_raw_payload_record(99), _raw_payload_record(100)),
        chunk_size=10,
        created_at=datetime(2026, 5, 27, 12, 0, tzinfo=UTC),
        archive_kind="production",
        source_database="hhru_platform",
        source_git_revision="test-revision",
        source_command="pytest",
        triggered_by="unit-test",
    )

    manifest = json.loads(summaries[0].manifest_file.read_text(encoding="utf-8"))
    assert manifest["source_min_id"] == "99"
    assert manifest["source_max_id"] == "100"


def test_detail_fetch_attempt_is_an_incremental_archive_dataset(tmp_path: Path) -> None:
    archive_dir = tmp_path / "research"
    created_at = datetime(2026, 5, 27, 12, 0, tzinfo=UTC)
    summary = LocalResearchArchiveStore().write_dataset(
        archive_dir=archive_dir,
        schema_version="research-archive-v1",
        dataset="silver/detail_fetch_attempt",
        records=(_detail_fetch_attempt_record(401),),
        chunk_size=10,
        created_at=created_at,
        archive_kind="production",
        source_database="hhru_platform",
        source_git_revision="test-revision",
        source_command="pytest",
        triggered_by="unit-test",
    )[0]

    assert "silver/detail_fetch_attempt/year=2026/month=05/day=26" in str(
        summary.data_file
    )
    manifest = json.loads(summary.manifest_file.read_text(encoding="utf-8"))
    assert manifest["dataset_key"] == "silver/detail_fetch_attempt"
    assert manifest["source_min_id"] == "401"
    assert manifest["source_max_id"] == "401"
    assert "silver/detail_fetch_attempt" in INCREMENTAL_RESEARCH_ARCHIVE_DATASETS


def test_research_archive_store_does_not_overwrite_existing_chunk(tmp_path: Path) -> None:
    archive_dir = tmp_path / "research"
    store = LocalResearchArchiveStore()
    parameters = {
        "archive_dir": archive_dir,
        "schema_version": "research-archive-v1",
        "dataset": "bronze/raw_api_payload",
        "chunk_size": 10,
        "created_at": datetime(2026, 5, 27, 12, 0, tzinfo=UTC),
        "archive_kind": "production",
        "source_database": "hhru_platform",
        "source_git_revision": "test-revision",
        "source_command": "pytest",
        "triggered_by": "unit-test",
    }
    store.write_dataset(records=(_raw_payload_record(99),), **parameters)

    with pytest.raises(FileExistsError, match="research archive chunk already exists"):
        store.write_dataset(records=(_raw_payload_record(100),), **parameters)


def test_research_archive_incremental_sql_stops_before_first_unsettled_source_id() -> None:
    repository = SqlAlchemyResearchArchiveRepository(cast(Any, None))
    settled_before = datetime(2026, 5, 27, 0, 0, tzinfo=UTC)

    statement = repository._build_statement(
        "bronze/raw_api_payload",
        after_source_id=40,
        settled_before=settled_before,
    )
    sql = str(
        statement.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )

    assert "raw_api_payload.id > 40" in sql
    assert "min(raw_api_payload.id)" in sql
    assert "raw_api_payload.received_at > '2026-05-27 00:00:00+00:00'" in sql
    assert "raw_api_payload.id < (SELECT min(raw_api_payload.id)" in sql


def test_research_archive_bounded_incremental_sql_selects_source_id_prefix() -> None:
    repository = SqlAlchemyResearchArchiveRepository(cast(Any, None))

    statement = repository._build_statement(
        "bronze/raw_api_payload",
        after_source_id=40,
        settled_before=datetime(2026, 5, 27, 0, 0, tzinfo=UTC),
        incremental_limit=10,
    )
    sql = str(
        statement.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )

    assert "raw_api_payload.id IN (SELECT raw_api_payload.id" in sql
    assert "ORDER BY raw_api_payload.id" in sql
    assert "LIMIT 10" in sql


def test_detail_fetch_attempt_incremental_sql_uses_requested_at_watermark() -> None:
    repository = SqlAlchemyResearchArchiveRepository(cast(Any, None))

    statement = repository._build_statement(
        "silver/detail_fetch_attempt",
        after_source_id=400,
        settled_before=datetime(2026, 5, 27, 0, 0, tzinfo=UTC),
        incremental_limit=10,
    )
    sql = str(
        statement.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )

    assert "detail_fetch_attempt.id > 400" in sql
    assert "detail_fetch_attempt.requested_at > '2026-05-27 00:00:00+00:00'" in sql
    assert "ORDER BY detail_fetch_attempt.id" in sql
    assert "LIMIT 10" in sql


def test_incremental_export_rejects_point_in_time_dataset() -> None:
    with pytest.raises(ValueError, match="supports only append-only datasets"):
        ExportResearchArchiveCommand(
            datasets=("silver/vacancy_current_state",),
            incremental=True,
            settled_before=datetime(2026, 5, 27, 0, 0, tzinfo=UTC),
        )


def test_sync_and_verify_research_archive_offsite(
    tmp_path: Path,
) -> None:
    repository = FakeResearchArchiveRepository()
    archive_dir = tmp_path / "research"
    export_result = export_research_archive(
        ExportResearchArchiveCommand(
            archive_dir=archive_dir,
            datasets=("bronze/raw_api_payload", "silver/vacancy_current_state"),
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
    remote_store = FakeResearchArchiveRemoteStore()
    receipt_store = LocalResearchArchiveOffsiteUploadReceiptStore()

    result = sync_research_archive_offsite(
        SyncResearchArchiveOffsiteCommand(
            archive_dir=archive_dir,
            offsite_url="https://s3.example.test/bucket-id",
            offsite_root="/hhru-platform/research-archive",
            triggered_by="unit-test",
            synced_at=datetime(2026, 5, 28, 10, 0, tzinfo=UTC),
        ),
        offsite_uploader=remote_store,
        receipt_store=receipt_store,
    )

    manifest_paths = {
        manifest.relative_to(archive_dir).as_posix()
        for summary in export_result.summaries
        for manifest in summary.manifest_files
    }
    data_paths = {
        data_file.relative_to(archive_dir).as_posix()
        for summary in export_result.summaries
        for data_file in summary.data_files
    }
    assert result.status == "succeeded"
    assert result.scanned_manifest_count == 2
    assert result.uploaded_manifest_count == 2
    assert result.skipped_manifest_count == 0
    assert result.inventory_uploaded is True
    assert {remote_path for remote_path, _ in remote_store.upload_calls} == {
        *manifest_paths,
        *data_paths,
        "v1/inventory/archive-inventory.jsonl",
    }
    assert all(
        Path(f"{summary.manifest_file}.offsite.json").exists() for summary in result.summaries
    )

    second_result = sync_research_archive_offsite(
        SyncResearchArchiveOffsiteCommand(
            archive_dir=archive_dir,
            offsite_url="https://s3.example.test/bucket-id",
            offsite_root="/hhru-platform/research-archive",
            triggered_by="unit-test",
            synced_at=datetime(2026, 5, 28, 11, 0, tzinfo=UTC),
        ),
        offsite_uploader=remote_store,
        receipt_store=receipt_store,
    )

    assert second_result.uploaded_manifest_count == 0
    assert second_result.skipped_manifest_count == 2
    assert second_result.inventory_uploaded is True

    verify_result = verify_research_archive_offsite(
        VerifyResearchArchiveOffsiteCommand(
            archive_dir=archive_dir,
            offsite_url="https://s3.example.test/bucket-id",
            offsite_root="/hhru-platform/research-archive",
            readback_limit=1,
            triggered_by="unit-test",
            verified_at=datetime(2026, 5, 28, 12, 0, tzinfo=UTC),
        ),
        remote_store=remote_store,
        receipt_store=LocalResearchArchiveOffsiteVerificationReceiptStore(),
        checkpoint_receipt_store=LocalResearchArchiveCheckpointVerificationReceiptStore(),
    )

    assert verify_result.status == "succeeded"
    assert verify_result.scanned_manifest_count == 2
    assert verify_result.verified_manifest_count == 2
    assert verify_result.verified_object_count == 5
    assert verify_result.verification_receipt_count == 2
    assert verify_result.readback_count == 1
    assert verify_result.readbacks[0].row_count == 1
    assert [summary.readback_verified for summary in verify_result.verified_manifests] == [
        True,
        False,
    ]
    verification_receipt_store = LocalResearchArchiveOffsiteVerificationReceiptStore()
    verification_receipts = [
        verification_receipt_store.load_receipt(manifest_file=summary.manifest_file)
        for summary in verify_result.verified_manifests
    ]
    assert all(receipt is not None for receipt in verification_receipts)
    assert {
        receipt.verified_at for receipt in verification_receipts if receipt is not None
    } == {datetime(2026, 5, 28, 12, 0, tzinfo=UTC)}
    assert [
        receipt.readback_verified for receipt in verification_receipts if receipt is not None
    ] == [True, False]


def test_audit_research_archive_coverage_requires_verified_checkpoint_chain(
    tmp_path: Path,
) -> None:
    archive_dir = tmp_path / "research"
    checkpoint_store = LocalResearchArchiveCheckpointStore()
    export_research_archive(
        ExportResearchArchiveCommand(
            archive_dir=archive_dir,
            datasets=("bronze/raw_api_payload",),
            chunk_size=10,
            batch_size=100,
            archive_kind="production",
            source_database="hhru_platform",
            source_git_revision="test-revision",
            source_command="pytest",
            created_at=datetime(2026, 5, 27, 12, 0, tzinfo=UTC),
            incremental=True,
            settled_before=datetime(2026, 5, 27, 0, 0, tzinfo=UTC),
        ),
        research_archive_repository=FakeResearchArchiveRepository(),
        research_archive_store=LocalResearchArchiveStore(),
        research_archive_cursor_store=LocalResearchArchiveCursorStore(),
        research_archive_checkpoint_store=checkpoint_store,
    )
    verification_receipt_store = LocalResearchArchiveOffsiteVerificationReceiptStore()
    checkpoint_receipt_store = LocalResearchArchiveCheckpointVerificationReceiptStore()

    incomplete_result = audit_research_archive_coverage(
        AuditResearchArchiveCoverageCommand(
            archive_dir=archive_dir,
            datasets=("bronze/raw_api_payload",),
            offsite_url="https://s3.example.test/bucket-id",
            triggered_by="unit-test",
        ),
        checkpoint_store=checkpoint_store,
        receipt_store=verification_receipt_store,
        checkpoint_receipt_store=checkpoint_receipt_store,
    )

    assert incomplete_result.status == "incomplete"
    assert incomplete_result.issue_count == 1
    assert "checkpoint offsite verification receipt not found" in (
        incomplete_result.summaries[0].issues[0].message
    )

    remote_store = FakeResearchArchiveRemoteStore()
    sync_result = sync_research_archive_offsite(
        SyncResearchArchiveOffsiteCommand(
            archive_dir=archive_dir,
            offsite_url="https://s3.example.test/bucket-id",
            offsite_root="/hhru-platform/research-archive",
            triggered_by="unit-test",
        ),
        offsite_uploader=remote_store,
        receipt_store=LocalResearchArchiveOffsiteUploadReceiptStore(),
    )
    verify_result = verify_research_archive_offsite(
        VerifyResearchArchiveOffsiteCommand(
            archive_dir=archive_dir,
            offsite_url="https://s3.example.test/bucket-id",
            offsite_root="/hhru-platform/research-archive",
            triggered_by="unit-test",
        ),
        remote_store=remote_store,
        receipt_store=verification_receipt_store,
        checkpoint_receipt_store=checkpoint_receipt_store,
    )
    assert sync_result.checkpoint_uploaded_count == 1
    assert verify_result.verified_checkpoint_count == 1

    complete_result = audit_research_archive_coverage(
        AuditResearchArchiveCoverageCommand(
            archive_dir=archive_dir,
            datasets=("bronze/raw_api_payload",),
            offsite_url="https://s3.example.test/bucket-id",
            triggered_by="unit-test",
        ),
        checkpoint_store=checkpoint_store,
        receipt_store=verification_receipt_store,
        checkpoint_receipt_store=checkpoint_receipt_store,
    )

    assert complete_result.status == "complete"
    assert complete_result.issue_count == 0
    assert complete_result.summaries[0].verified_checkpoint_count == 1
    assert complete_result.summaries[0].verified_manifest_count == 1
    assert complete_result.summaries[0].verified_row_count == 1
    assert complete_result.summaries[0].source_id_covered == 101

    checkpoint = checkpoint_store.load_checkpoints(
        archive_dir=archive_dir,
        archive_kind="production",
    )[0]
    checkpoint.checkpoint_file.write_text(
        checkpoint.checkpoint_file.read_text(encoding="utf-8").replace(
            '"triggered_by": "export-research-archive"',
            '"triggered_by": "tamper-research-archive"',
        ),
        encoding="utf-8",
    )
    tampered_result = audit_research_archive_coverage(
        AuditResearchArchiveCoverageCommand(
            archive_dir=archive_dir,
            datasets=("bronze/raw_api_payload",),
            offsite_url="https://s3.example.test/bucket-id",
            triggered_by="unit-test",
        ),
        checkpoint_store=checkpoint_store,
        receipt_store=verification_receipt_store,
        checkpoint_receipt_store=checkpoint_receipt_store,
    )

    assert tampered_result.status == "incomplete"
    assert "checkpoint offsite verification receipt sha256 mismatch" in (
        tampered_result.summaries[0].issues[0].message
    )


def test_audit_research_archive_coverage_rejects_checkpoint_chain_break(
    tmp_path: Path,
) -> None:
    archive_dir = tmp_path / "research"
    checkpoint_store = LocalResearchArchiveCheckpointStore()
    checkpoint_store.write_checkpoint(
        archive_dir=archive_dir,
        archive_kind="production",
        created_at=datetime(2026, 5, 27, 12, 0, tzinfo=UTC),
        settled_before=datetime(2026, 5, 27, 0, 0, tzinfo=UTC),
        triggered_by="unit-test",
        datasets=(
            ResearchArchiveCheckpointDataset(
                dataset="bronze/raw_api_payload",
                source_id_before=10,
                source_id_after=10,
                chunk_count=0,
                row_count=0,
                manifest_files=(),
            ),
        ),
    )

    result = audit_research_archive_coverage(
        AuditResearchArchiveCoverageCommand(
            archive_dir=archive_dir,
            datasets=("bronze/raw_api_payload",),
            offsite_url="https://s3.example.test/bucket-id",
            triggered_by="unit-test",
        ),
        checkpoint_store=checkpoint_store,
        receipt_store=LocalResearchArchiveOffsiteVerificationReceiptStore(),
        checkpoint_receipt_store=LocalResearchArchiveCheckpointVerificationReceiptStore(),
    )

    assert result.status == "incomplete"
    assert result.issue_count == 1
    assert result.summaries[0].source_id_covered == 0
    assert "checkpoint chain break" in result.summaries[0].issues[0].message


def test_limited_research_archive_offsite_sync_does_not_upload_inventory(
    tmp_path: Path,
) -> None:
    repository = FakeResearchArchiveRepository()
    archive_dir = tmp_path / "research"
    export_research_archive(
        ExportResearchArchiveCommand(
            archive_dir=archive_dir,
            datasets=("bronze/raw_api_payload", "silver/vacancy_current_state"),
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
    remote_store = FakeResearchArchiveRemoteStore()

    sync_result = sync_research_archive_offsite(
        SyncResearchArchiveOffsiteCommand(
            archive_dir=archive_dir,
            limit=1,
            offsite_url="https://s3.example.test/bucket-id",
            offsite_root="/hhru-platform/research-archive",
            triggered_by="unit-test",
        ),
        offsite_uploader=remote_store,
        receipt_store=LocalResearchArchiveOffsiteUploadReceiptStore(),
    )

    assert sync_result.scanned_manifest_count == 1
    assert sync_result.inventory_uploaded is False
    assert "v1/inventory/archive-inventory.jsonl" not in remote_store.objects_by_remote_path

    verify_result = verify_research_archive_offsite(
        VerifyResearchArchiveOffsiteCommand(
            archive_dir=archive_dir,
            limit=1,
            offsite_url="https://s3.example.test/bucket-id",
            offsite_root="/hhru-platform/research-archive",
            readback_limit=1,
            triggered_by="unit-test",
        ),
        remote_store=remote_store,
        receipt_store=LocalResearchArchiveOffsiteVerificationReceiptStore(),
        checkpoint_receipt_store=LocalResearchArchiveCheckpointVerificationReceiptStore(),
    )

    assert verify_result.scanned_manifest_count == 1
    assert verify_result.verified_object_count == 2
    assert verify_result.verification_receipt_count == 1
    assert verify_result.readback_count == 1


def test_limited_research_archive_offsite_sync_does_not_upload_checkpoint(
    tmp_path: Path,
) -> None:
    archive_dir = tmp_path / "research"
    export_research_archive(
        ExportResearchArchiveCommand(
            archive_dir=archive_dir,
            datasets=("bronze/raw_api_payload",),
            chunk_size=10,
            batch_size=100,
            archive_kind="production",
            source_database="hhru_platform",
            source_git_revision="test-revision",
            source_command="pytest",
            created_at=datetime(2026, 5, 27, 12, 0, tzinfo=UTC),
            incremental=True,
            settled_before=datetime(2026, 5, 27, 0, 0, tzinfo=UTC),
        ),
        research_archive_repository=FakeResearchArchiveRepository(),
        research_archive_store=LocalResearchArchiveStore(),
        research_archive_cursor_store=LocalResearchArchiveCursorStore(),
        research_archive_checkpoint_store=LocalResearchArchiveCheckpointStore(),
    )
    remote_store = FakeResearchArchiveRemoteStore()

    sync_result = sync_research_archive_offsite(
        SyncResearchArchiveOffsiteCommand(
            archive_dir=archive_dir,
            limit=1,
            offsite_url="https://s3.example.test/bucket-id",
            offsite_root="/hhru-platform/research-archive",
            triggered_by="unit-test",
        ),
        offsite_uploader=remote_store,
        receipt_store=LocalResearchArchiveOffsiteUploadReceiptStore(),
    )

    assert sync_result.inventory_uploaded is False
    assert sync_result.checkpoint_uploaded_count == 0
    assert all(
        not remote_path.endswith(".checkpoint.json")
        for remote_path in remote_store.objects_by_remote_path
    )

    verify_result = verify_research_archive_offsite(
        VerifyResearchArchiveOffsiteCommand(
            archive_dir=archive_dir,
            limit=1,
            offsite_url="https://s3.example.test/bucket-id",
            offsite_root="/hhru-platform/research-archive",
            triggered_by="unit-test",
        ),
        remote_store=remote_store,
        receipt_store=LocalResearchArchiveOffsiteVerificationReceiptStore(),
        checkpoint_receipt_store=LocalResearchArchiveCheckpointVerificationReceiptStore(),
    )

    assert verify_result.verified_checkpoint_count == 0


def test_verify_research_archive_offsite_does_not_receipt_size_mismatch(
    tmp_path: Path,
) -> None:
    repository = FakeResearchArchiveRepository()
    archive_dir = tmp_path / "research"
    export_result = export_research_archive(
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
    remote_store = FakeResearchArchiveRemoteStore()
    sync_research_archive_offsite(
        SyncResearchArchiveOffsiteCommand(
            archive_dir=archive_dir,
            offsite_url="https://s3.example.test/bucket-id",
            offsite_root="/hhru-platform/research-archive",
            triggered_by="unit-test",
        ),
        offsite_uploader=remote_store,
        receipt_store=LocalResearchArchiveOffsiteUploadReceiptStore(),
    )
    summary = export_result.summaries[0]
    remote_data_path = summary.data_files[0].relative_to(archive_dir).as_posix()
    remote_store.objects_by_remote_path[remote_data_path] = b"corrupted"
    verification_receipt_store = LocalResearchArchiveOffsiteVerificationReceiptStore()

    with pytest.raises(RuntimeError, match="remote research archive object size mismatch"):
        verify_research_archive_offsite(
            VerifyResearchArchiveOffsiteCommand(
                archive_dir=archive_dir,
                offsite_url="https://s3.example.test/bucket-id",
                offsite_root="/hhru-platform/research-archive",
                readback_limit=1,
                triggered_by="unit-test",
            ),
            remote_store=remote_store,
            receipt_store=verification_receipt_store,
            checkpoint_receipt_store=LocalResearchArchiveCheckpointVerificationReceiptStore(),
        )

    assert (
        verification_receipt_store.load_receipt(
            manifest_file=summary.manifest_files[0],
        )
        is None
    )


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
