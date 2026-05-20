from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path

from hhru_platform.application.commands.sync_backup_offsite import (
    SyncBackupOffsiteCommand,
    sync_backup_offsite,
)
from hhru_platform.infrastructure.backup import LocalBackupOffsiteUploadReceiptStore


class FakeOffsiteUploader:
    def __init__(self) -> None:
        self.upload_calls: list[tuple[str, bytes]] = []

    def upload_file(self, *, local_file: Path, remote_path: str) -> None:
        self.upload_calls.append((remote_path, local_file.read_bytes()))


def test_sync_backup_offsite_uploads_latest_backup_and_manifest_once(
    tmp_path: Path,
) -> None:
    backup_dir = tmp_path / "backups"
    backup_dir.mkdir()
    old_backup = backup_dir / "hhru-platform_hhru_platform_20260515T084422Z.dump"
    latest_backup = backup_dir / "hhru-platform_hhru_platform_20260516T084422Z.dump"
    old_backup.write_bytes(b"old-backup")
    latest_backup.write_bytes(b"latest-backup")
    os.utime(old_backup, (1_768_000_000, 1_768_000_000))
    os.utime(latest_backup, (1_768_100_000, 1_768_100_000))
    uploader = FakeOffsiteUploader()
    receipt_store = LocalBackupOffsiteUploadReceiptStore()

    result = sync_backup_offsite(
        SyncBackupOffsiteCommand(
            backup_dir=backup_dir,
            offsite_url="https://webdav.example.test",
            offsite_root="/hhru-platform/backups",
            username="user",
            password="secret",
            chunk_size_bytes=6,
            limit=1,
            triggered_by="unit-test",
            synced_at=datetime(2026, 5, 16, 10, 0, tzinfo=UTC),
        ),
        offsite_uploader=uploader,
        receipt_store=receipt_store,
    )

    latest_manifest = Path(f"{latest_backup.resolve()}.manifest.json")
    assert result.status == "succeeded"
    assert result.scanned_backup_count == 1
    assert result.candidate_backup_count == 1
    assert result.uploaded_backup_count == 1
    assert result.skipped_backup_count == 0
    assert uploader.upload_calls == [
        (
            "hhru-platform_hhru_platform_20260516T084422Z.dump.parts/000001.part",
            b"latest",
        ),
        (
            "hhru-platform_hhru_platform_20260516T084422Z.dump.parts/000002.part",
            b"-backu",
        ),
        (
            "hhru-platform_hhru_platform_20260516T084422Z.dump.parts/000003.part",
            b"p",
        ),
        (
            "hhru-platform_hhru_platform_20260516T084422Z.dump.manifest.json",
            latest_manifest.read_bytes(),
        ),
    ]
    assert latest_manifest.exists()
    manifest_payload = json.loads(latest_manifest.read_text(encoding="utf-8"))
    assert manifest_payload["upload_mode"] == "parts"
    assert manifest_payload["chunk_size_bytes"] == 6
    assert [part["size_bytes"] for part in manifest_payload["parts"]] == [6, 6, 1]
    assert result.summaries[0].remote_backup_path == (
        "/hhru-platform/backups/"
        "hhru-platform_hhru_platform_20260516T084422Z.dump.parts"
    )
    assert result.summaries[0].part_count == 3
    assert Path(f"{latest_backup.resolve()}.offsite.json").exists()

    second_uploader = FakeOffsiteUploader()
    second_result = sync_backup_offsite(
        SyncBackupOffsiteCommand(
            backup_dir=backup_dir,
            offsite_url="https://webdav.example.test",
            offsite_root="/hhru-platform/backups",
            username="user",
            password="secret",
            chunk_size_bytes=6,
            limit=1,
            triggered_by="unit-test",
            synced_at=datetime(2026, 5, 16, 11, 0, tzinfo=UTC),
        ),
        offsite_uploader=second_uploader,
        receipt_store=receipt_store,
    )

    assert second_result.scanned_backup_count == 1
    assert second_result.candidate_backup_count == 0
    assert second_result.uploaded_backup_count == 0
    assert second_result.skipped_backup_count == 1
    assert second_uploader.upload_calls == []
