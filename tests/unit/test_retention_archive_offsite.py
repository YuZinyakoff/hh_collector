from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from hhru_platform.application.commands.sync_retention_archive_offsite import (
    SyncRetentionArchiveOffsiteCommand,
    sync_retention_archive_offsite,
)
from hhru_platform.infrastructure.housekeeping import (
    LocalRetentionArchiveStore,
    LocalRetentionArchiveUploadReceiptStore,
    WebDavArchiveUploader,
)


class FakeOffsiteUploader:
    def __init__(self) -> None:
        self.upload_calls: list[tuple[Path, str]] = []

    def upload_file(self, *, local_file: Path, remote_path: str) -> None:
        self.upload_calls.append((local_file, remote_path))


class RecordingWebDavTransport:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def request(self, *, method: str, url: str, headers, body: bytes | None = None) -> int:
        self.calls.append(
            {
                "method": method,
                "url": url,
                "headers": dict(headers),
                "body": body,
            }
        )
        if method == "MKCOL":
            return 201
        if method == "PUT":
            return 201
        raise AssertionError(f"unexpected method: {method}")


def test_sync_retention_archive_offsite_uploads_archive_and_manifest_once(
    tmp_path: Path,
) -> None:
    archive_dir = tmp_path / "archive"
    store = LocalRetentionArchiveStore()
    summary = store.write_records(
        archive_dir=archive_dir,
        target="raw_api_payload",
        evaluated_at=datetime(2026, 4, 3, 12, 0, tzinfo=UTC),
        records=(
            {
                "id": 101,
                "payload_hash": "hash-101",
                "received_at": datetime(2026, 3, 1, 10, 0, tzinfo=UTC),
            },
        ),
        metadata={"triggered_by": "unit-test"},
    )
    uploader = FakeOffsiteUploader()
    receipt_store = LocalRetentionArchiveUploadReceiptStore()

    result = sync_retention_archive_offsite(
        SyncRetentionArchiveOffsiteCommand(
            archive_dir=archive_dir,
            offsite_url="https://webdav.example.test",
            offsite_root="/hhru-platform",
            username="user",
            password="secret",
            triggered_by="unit-test",
            synced_at=datetime(2026, 4, 3, 13, 0, tzinfo=UTC),
        ),
        offsite_uploader=uploader,
        receipt_store=receipt_store,
    )

    assert result.status == "succeeded"
    assert result.scanned_manifest_count == 1
    assert result.candidate_bundle_count == 1
    assert result.uploaded_bundle_count == 1
    assert result.skipped_bundle_count == 0
    assert uploader.upload_calls == [
        (
            summary.archive_file.resolve(),
            "raw_api_payload/2026/04/20260403T120000Z-raw_api_payload-1.jsonl.gz",
        ),
        (
            summary.manifest_file.resolve(),
            "raw_api_payload/2026/04/20260403T120000Z-raw_api_payload-1.manifest.json",
        ),
    ]
    receipt_file = Path(f"{summary.manifest_file.resolve()}.uploaded.json")
    assert receipt_file.exists()

    second_uploader = FakeOffsiteUploader()
    second_result = sync_retention_archive_offsite(
        SyncRetentionArchiveOffsiteCommand(
            archive_dir=archive_dir,
            offsite_url="https://webdav.example.test",
            offsite_root="/hhru-platform",
            username="user",
            password="secret",
            triggered_by="unit-test",
            synced_at=datetime(2026, 4, 3, 14, 0, tzinfo=UTC),
        ),
        offsite_uploader=second_uploader,
        receipt_store=receipt_store,
    )

    assert second_result.scanned_manifest_count == 1
    assert second_result.candidate_bundle_count == 0
    assert second_result.uploaded_bundle_count == 0
    assert second_result.skipped_bundle_count == 1
    assert second_uploader.upload_calls == []


def test_webdav_archive_uploader_creates_directories_and_puts_file(tmp_path: Path) -> None:
    payload_file = tmp_path / "chunk.jsonl.gz"
    payload_file.write_bytes(b"payload")
    transport = RecordingWebDavTransport()
    uploader = WebDavArchiveUploader.with_basic_auth(
        base_url="https://webdav.example.test",
        remote_root="/hhru-platform",
        username="user",
        password="secret",
        timeout_seconds=60.0,
        transport=transport,
    )

    uploader.upload_file(
        local_file=payload_file,
        remote_path="raw_api_payload/2026/04/chunk.jsonl.gz",
    )

    assert [call["method"] for call in transport.calls] == [
        "MKCOL",
        "MKCOL",
        "MKCOL",
        "MKCOL",
        "PUT",
    ]
    assert transport.calls[0]["url"] == "https://webdav.example.test/hhru-platform"
    assert (
        transport.calls[1]["url"]
        == "https://webdav.example.test/hhru-platform/raw_api_payload"
    )
    assert (
        transport.calls[2]["url"]
        == "https://webdav.example.test/hhru-platform/raw_api_payload/2026"
    )
    assert (
        transport.calls[3]["url"]
        == "https://webdav.example.test/hhru-platform/raw_api_payload/2026/04"
    )
    assert (
        transport.calls[4]["url"]
        == "https://webdav.example.test/hhru-platform/raw_api_payload/2026/04/chunk.jsonl.gz"
    )
    assert transport.calls[4]["headers"]["Authorization"].startswith("Basic ")
    assert transport.calls[4]["body"] == b"payload"
