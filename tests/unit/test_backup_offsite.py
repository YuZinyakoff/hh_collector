from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path

import pytest

from hhru_platform.application.commands.cleanup_backup_offsite import (
    CleanupBackupOffsiteCommand,
    cleanup_backup_offsite,
)
from hhru_platform.application.commands.sync_backup_offsite import (
    SyncBackupOffsiteCommand,
    sync_backup_offsite,
)
from hhru_platform.application.commands.verify_backup_offsite import (
    VerifyBackupOffsiteCommand,
    verify_backup_offsite,
)
from hhru_platform.infrastructure.backup import (
    BackupOffsiteUploadReceipt,
    BackupOffsiteVerificationReceipt,
    LocalBackupOffsiteUploadReceiptStore,
    LocalBackupOffsiteVerificationReceiptStore,
)
from hhru_platform.infrastructure.backup.s3_backup_offsite_uploader import (
    S3BackupOffsiteUploader,
)


class FakeOffsiteUploader:
    def __init__(self) -> None:
        self.upload_calls: list[tuple[str, bytes]] = []

    def upload_file(self, *, local_file: Path, remote_path: str) -> None:
        self.upload_calls.append((remote_path, local_file.read_bytes()))


class FakeS3Client:
    def __init__(self) -> None:
        self.upload_calls: list[tuple[str, str, str, bytes]] = []
        self.download_calls: list[tuple[str, str, str]] = []
        self.delete_calls: list[tuple[str, str]] = []
        self.objects_by_key: dict[str, bytes] = {}
        self.object_sizes_by_key: dict[str, int] = {}

    def upload_file(self, Filename: str, Bucket: str, Key: str) -> None:
        payload = Path(Filename).read_bytes()
        self.upload_calls.append((Filename, Bucket, Key, payload))
        self.objects_by_key[Key] = payload
        self.object_sizes_by_key[Key] = len(payload)

    def download_file(self, Bucket: str, Key: str, Filename: str) -> None:
        self.download_calls.append((Bucket, Key, Filename))
        Path(Filename).write_bytes(self.objects_by_key[Key])

    def head_object(self, *, Bucket: str, Key: str) -> dict[str, int]:
        return {"ContentLength": self.object_sizes_by_key[Key]}

    def delete_object(self, *, Bucket: str, Key: str) -> object:
        self.delete_calls.append((Bucket, Key))
        self.objects_by_key.pop(Key, None)
        self.object_sizes_by_key.pop(Key, None)
        return {}


def test_s3_backup_offsite_uploader_maps_remote_path_to_object_key(
    tmp_path: Path,
) -> None:
    local_file = tmp_path / "part.bin"
    local_file.write_bytes(b"payload")
    client = FakeS3Client()
    uploader = S3BackupOffsiteUploader(
        endpoint_url="https://s3.twcstorage.ru/",
        bucket="bucket-id",
        key_prefix="/hhru-platform/backups/",
        client=client,
    )

    uploader.upload_file(
        local_file=local_file,
        remote_path="/backup.dump.parts/000001.part",
    )
    downloaded_file = tmp_path / "downloaded" / "part.bin"
    uploader.download_file(
        local_file=downloaded_file,
        remote_path="/backup.dump.parts/000001.part",
    )
    size_bytes = uploader.get_file_size(remote_path="/backup.dump.parts/000001.part")
    uploader.delete_file(remote_path="/backup.dump.parts/000001.part")

    assert client.upload_calls == [
        (
            str(local_file),
            "bucket-id",
            "hhru-platform/backups/backup.dump.parts/000001.part",
            b"payload",
        )
    ]
    assert client.download_calls == [
        (
            "bucket-id",
            "hhru-platform/backups/backup.dump.parts/000001.part",
            str(downloaded_file),
        )
    ]
    assert downloaded_file.read_bytes() == b"payload"
    assert size_bytes == 7
    assert client.delete_calls == [
        ("bucket-id", "hhru-platform/backups/backup.dump.parts/000001.part")
    ]


def test_verify_backup_offsite_checks_manifest_and_part_sizes(
    tmp_path: Path,
) -> None:
    backup_dir = tmp_path / "backups"
    backup_dir.mkdir()
    backup = backup_dir / "hhru-platform_hhru_platform_20260516T084422Z.dump"
    backup.write_bytes(b"latest-backup")
    manifest = Path(f"{backup.resolve()}.manifest.json")
    manifest_payload = {
        "manifest_version": 2,
        "upload_mode": "parts",
        "backup_file": backup.name,
        "backup_size_bytes": 13,
        "backup_sha256": "abc123",
        "chunk_size_bytes": 6,
        "parts": [
            {
                "index": 1,
                "file": f"{backup.name}.parts/000001.part",
                "size_bytes": 6,
                "sha256": "part1",
            },
            {
                "index": 2,
                "file": f"{backup.name}.parts/000002.part",
                "size_bytes": 7,
                "sha256": "part2",
            },
        ],
    }
    manifest.write_text(json.dumps(manifest_payload), encoding="utf-8")
    remote_store = FakeRemoteStore(
        sizes_by_remote_path={
            manifest.name: manifest.stat().st_size,
            f"{backup.name}.parts/000001.part": 6,
            f"{backup.name}.parts/000002.part": 7,
        }
    )

    receipt_store = LocalBackupOffsiteVerificationReceiptStore()
    result = verify_backup_offsite(
        VerifyBackupOffsiteCommand(
            backup_file=backup,
            backup_dir=backup_dir,
            offsite_url="https://s3.example.test/bucket",
            offsite_root="/hhru-platform/backups",
            triggered_by="unit-test",
            verified_at=datetime(2026, 5, 16, 11, 0, tzinfo=UTC),
        ),
        remote_store=remote_store,
        receipt_store=receipt_store,
    )

    assert result.status == "succeeded"
    assert result.backup_file == backup.resolve()
    assert result.backup_size_bytes == 13
    assert result.backup_sha256 == "abc123"
    assert result.chunk_size_bytes == 6
    assert result.part_count == 2
    assert result.verified_object_count == 3
    assert result.receipt_file == Path(f"{backup.resolve()}.offsite.verified.json")
    receipt = receipt_store.load_receipt(backup_file=backup.resolve())
    assert receipt is not None
    assert receipt.verified_at == datetime(2026, 5, 16, 11, 0, tzinfo=UTC)
    assert receipt.verified_object_count == 3
    assert [verified.remote_path for verified in result.verified_objects] == [
        manifest.name,
        f"{backup.name}.parts/000001.part",
        f"{backup.name}.parts/000002.part",
    ]


class FakeRemoteStore:
    def __init__(self, *, sizes_by_remote_path: dict[str, int]) -> None:
        self.sizes_by_remote_path = sizes_by_remote_path

    def get_file_size(self, *, remote_path: str) -> int:
        return self.sizes_by_remote_path[remote_path]


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


class FailingOnceOffsiteUploader:
    def __init__(self) -> None:
        self.upload_calls: list[tuple[str, bytes]] = []
        self.failed_remote_paths: set[str] = set()

    def upload_file(self, *, local_file: Path, remote_path: str) -> None:
        self.upload_calls.append((remote_path, local_file.read_bytes()))
        if ".parts/" in remote_path and remote_path not in self.failed_remote_paths:
            self.failed_remote_paths.add(remote_path)
            raise RuntimeError(f"temporary upload failure for {remote_path}")


def test_sync_backup_offsite_retries_part_uploads(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "hhru_platform.application.commands.sync_backup_offsite."
        "BACKUP_OFFSITE_PART_UPLOAD_RETRY_BACKOFF_SECONDS",
        0.0,
    )
    backup_dir = tmp_path / "backups"
    backup_dir.mkdir()
    backup = backup_dir / "hhru-platform_hhru_platform_20260516T084422Z.dump"
    backup.write_bytes(b"latest")
    uploader = FailingOnceOffsiteUploader()

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
        receipt_store=LocalBackupOffsiteUploadReceiptStore(),
    )

    assert result.status == "succeeded"
    assert [remote_path for remote_path, _ in uploader.upload_calls] == [
        "hhru-platform_hhru_platform_20260516T084422Z.dump.parts/000001.part",
        "hhru-platform_hhru_platform_20260516T084422Z.dump.parts/000001.part",
        "hhru-platform_hhru_platform_20260516T084422Z.dump.manifest.json",
    ]


class FailingPartOffsiteUploader:
    def __init__(self, *, failing_remote_path: str) -> None:
        self.failing_remote_path = failing_remote_path
        self.upload_calls: list[tuple[str, bytes]] = []

    def upload_file(self, *, local_file: Path, remote_path: str) -> None:
        self.upload_calls.append((remote_path, local_file.read_bytes()))
        if remote_path == self.failing_remote_path:
            raise RuntimeError(f"persistent upload failure for {remote_path}")


def test_sync_backup_offsite_resumes_uploaded_parts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "hhru_platform.application.commands.sync_backup_offsite."
        "BACKUP_OFFSITE_PART_UPLOAD_RETRY_BACKOFF_SECONDS",
        0.0,
    )
    backup_dir = tmp_path / "backups"
    backup_dir.mkdir()
    backup = backup_dir / "hhru-platform_hhru_platform_20260516T084422Z.dump"
    backup.write_bytes(b"latest-backup")
    first_uploader = FailingPartOffsiteUploader(
        failing_remote_path=(
            "hhru-platform_hhru_platform_20260516T084422Z.dump.parts/000002.part"
        )
    )
    receipt_store = LocalBackupOffsiteUploadReceiptStore()
    command = SyncBackupOffsiteCommand(
        backup_dir=backup_dir,
        offsite_url="https://webdav.example.test",
        offsite_root="/hhru-platform/backups",
        username="user",
        password="secret",
        chunk_size_bytes=6,
        limit=1,
        triggered_by="unit-test",
        synced_at=datetime(2026, 5, 16, 10, 0, tzinfo=UTC),
    )

    with pytest.raises(RuntimeError):
        sync_backup_offsite(
            command,
            offsite_uploader=first_uploader,
            receipt_store=receipt_store,
        )

    assert [remote_path for remote_path, _ in first_uploader.upload_calls] == [
        "hhru-platform_hhru_platform_20260516T084422Z.dump.parts/000001.part",
        "hhru-platform_hhru_platform_20260516T084422Z.dump.parts/000002.part",
        "hhru-platform_hhru_platform_20260516T084422Z.dump.parts/000002.part",
        "hhru-platform_hhru_platform_20260516T084422Z.dump.parts/000002.part",
    ]
    parts_receipt = Path(f"{backup.resolve()}.offsite.parts.json")
    assert parts_receipt.exists()
    parts_receipt_payload = json.loads(parts_receipt.read_text(encoding="utf-8"))
    assert [part["index"] for part in parts_receipt_payload["uploaded_parts"]] == [1]

    second_uploader = FakeOffsiteUploader()
    result = sync_backup_offsite(
        command,
        offsite_uploader=second_uploader,
        receipt_store=receipt_store,
    )

    assert result.status == "succeeded"
    assert [remote_path for remote_path, _ in second_uploader.upload_calls] == [
        "hhru-platform_hhru_platform_20260516T084422Z.dump.parts/000002.part",
        "hhru-platform_hhru_platform_20260516T084422Z.dump.parts/000003.part",
        "hhru-platform_hhru_platform_20260516T084422Z.dump.manifest.json",
    ]


class FakeCleanupRemoteStore:
    def __init__(self) -> None:
        self.delete_calls: list[str] = []

    def delete_file(self, *, remote_path: str) -> None:
        self.delete_calls.append(remote_path)


class FailingCleanupRemoteStore(FakeCleanupRemoteStore):
    def __init__(self, *, failing_remote_path: str) -> None:
        super().__init__()
        self.failing_remote_path = failing_remote_path

    def delete_file(self, *, remote_path: str) -> None:
        super().delete_file(remote_path=remote_path)
        if remote_path == self.failing_remote_path:
            raise RuntimeError(f"persistent delete failure for {remote_path}")


def test_cleanup_backup_offsite_dry_run_preserves_remote_objects(
    tmp_path: Path,
) -> None:
    backup_dir = tmp_path / "backups"
    backup_dir.mkdir()
    latest_backup = _write_cleanup_generation(
        backup_dir=backup_dir,
        timestamp="20260531T120000Z",
    )
    old_backup = _write_cleanup_generation(
        backup_dir=backup_dir,
        timestamp="20260501T120000Z",
    )
    unverified_backup = _write_cleanup_generation(
        backup_dir=backup_dir,
        timestamp="20260401T120000Z",
        verified=False,
    )
    remote_store = FakeCleanupRemoteStore()

    result = cleanup_backup_offsite(
        CleanupBackupOffsiteCommand(
            backup_dir=backup_dir,
            offsite_url="https://s3.example.test/bucket",
            offsite_root="/hhru-platform/backups",
            keep_latest=1,
            keep_weekly=0,
            triggered_by="unit-test",
            evaluated_at=datetime(2026, 5, 31, 13, 0, tzinfo=UTC),
        ),
        remote_store=remote_store,
        upload_receipt_store=LocalBackupOffsiteUploadReceiptStore(),
        verification_receipt_store=LocalBackupOffsiteVerificationReceiptStore(),
    )

    assert result.status == "succeeded"
    assert result.apply is False
    assert result.scanned_receipt_count == 3
    assert result.retained_generation_count == 1
    assert result.delete_candidate_count == 1
    assert result.deleted_generation_count == 0
    assert result.skipped_generation_count == 1
    assert remote_store.delete_calls == []
    assert _summary_for(result, latest_backup).action == "retained"
    assert _summary_for(result, old_backup).action == "delete_candidate"
    assert _summary_for(result, unverified_backup).action == "skipped_unverified"


def test_cleanup_backup_offsite_applies_remote_delete_then_local_sidecar_cleanup(
    tmp_path: Path,
) -> None:
    backup_dir = tmp_path / "backups"
    backup_dir.mkdir()
    _write_cleanup_generation(
        backup_dir=backup_dir,
        timestamp="20260531T120000Z",
    )
    old_backup = _write_cleanup_generation(
        backup_dir=backup_dir,
        timestamp="20260501T120000Z",
    )
    remote_store = FakeCleanupRemoteStore()

    result = cleanup_backup_offsite(
        CleanupBackupOffsiteCommand(
            backup_dir=backup_dir,
            offsite_url="https://s3.example.test/bucket",
            offsite_root="/hhru-platform/backups",
            keep_latest=1,
            keep_weekly=0,
            apply=True,
            triggered_by="unit-test",
        ),
        remote_store=remote_store,
        upload_receipt_store=LocalBackupOffsiteUploadReceiptStore(),
        verification_receipt_store=LocalBackupOffsiteVerificationReceiptStore(),
    )

    assert result.deleted_generation_count == 1
    assert result.remote_deleted_object_count == 3
    assert result.local_deleted_sidecar_count == 4
    assert remote_store.delete_calls == [
        f"{old_backup.name}.parts/000001.part",
        f"{old_backup.name}.parts/000002.part",
        f"{old_backup.name}.manifest.json",
    ]
    assert old_backup.exists()
    assert not Path(f"{old_backup}.manifest.json").exists()
    assert not Path(f"{old_backup}.offsite.json").exists()
    assert not Path(f"{old_backup}.offsite.parts.json").exists()
    assert not Path(f"{old_backup}.offsite.verified.json").exists()


def test_cleanup_backup_offsite_keeps_local_sidecars_after_remote_delete_failure(
    tmp_path: Path,
) -> None:
    backup_dir = tmp_path / "backups"
    backup_dir.mkdir()
    _write_cleanup_generation(
        backup_dir=backup_dir,
        timestamp="20260531T120000Z",
    )
    old_backup = _write_cleanup_generation(
        backup_dir=backup_dir,
        timestamp="20260501T120000Z",
    )
    remote_store = FailingCleanupRemoteStore(
        failing_remote_path=f"{old_backup.name}.parts/000002.part"
    )

    with pytest.raises(RuntimeError):
        cleanup_backup_offsite(
            CleanupBackupOffsiteCommand(
                backup_dir=backup_dir,
                offsite_url="https://s3.example.test/bucket",
                offsite_root="/hhru-platform/backups",
                keep_latest=1,
                keep_weekly=0,
                apply=True,
                triggered_by="unit-test",
            ),
            remote_store=remote_store,
            upload_receipt_store=LocalBackupOffsiteUploadReceiptStore(),
            verification_receipt_store=LocalBackupOffsiteVerificationReceiptStore(),
        )

    assert remote_store.delete_calls == [
        f"{old_backup.name}.parts/000001.part",
        f"{old_backup.name}.parts/000002.part",
    ]
    assert Path(f"{old_backup}.manifest.json").exists()
    assert Path(f"{old_backup}.offsite.json").exists()
    assert Path(f"{old_backup}.offsite.parts.json").exists()
    assert Path(f"{old_backup}.offsite.verified.json").exists()


def test_cleanup_backup_offsite_retains_weekly_and_protected_milestones(
    tmp_path: Path,
) -> None:
    backup_dir = tmp_path / "backups"
    backup_dir.mkdir()
    latest_backup = _write_cleanup_generation(
        backup_dir=backup_dir,
        timestamp="20260531T120000Z",
    )
    weekly_backup = _write_cleanup_generation(
        backup_dir=backup_dir,
        timestamp="20260520T120000Z",
    )
    protected_backup = _write_cleanup_generation(
        backup_dir=backup_dir,
        timestamp="20260501T120000Z",
        protected=True,
    )

    result = cleanup_backup_offsite(
        CleanupBackupOffsiteCommand(
            backup_dir=backup_dir,
            offsite_url="https://s3.example.test/bucket",
            offsite_root="/hhru-platform/backups",
            keep_latest=1,
            keep_weekly=2,
            triggered_by="unit-test",
        ),
        remote_store=FakeCleanupRemoteStore(),
        upload_receipt_store=LocalBackupOffsiteUploadReceiptStore(),
        verification_receipt_store=LocalBackupOffsiteVerificationReceiptStore(),
    )

    assert _summary_for(result, latest_backup).action == "retained"
    assert _summary_for(result, weekly_backup).action == "retained"
    protected_summary = _summary_for(result, protected_backup)
    assert protected_summary.action == "retained"
    assert protected_summary.reason == "protected milestone backup"


def _write_cleanup_generation(
    *,
    backup_dir: Path,
    timestamp: str,
    verified: bool = True,
    protected: bool = False,
) -> Path:
    backup_file = backup_dir / f"hhru-platform_hhru_platform_{timestamp}.dump"
    backup_file.write_bytes(b"backup")
    Path(f"{backup_file}.manifest.json").write_text("manifest\n", encoding="utf-8")
    Path(f"{backup_file}.offsite.parts.json").write_text("parts\n", encoding="utf-8")
    uploaded_at = datetime.strptime(timestamp, "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC)
    upload_receipt = BackupOffsiteUploadReceipt(
        uploaded_at=uploaded_at,
        offsite_url="https://s3.example.test/bucket",
        offsite_root="/hhru-platform/backups",
        backup_size_bytes=6,
        backup_sha256=f"backup-{timestamp}",
        manifest_sha256=f"manifest-{timestamp}",
        chunk_size_bytes=3,
        part_count=2,
        remote_backup_path=f"/hhru-platform/backups/{backup_file.name}.parts",
        remote_manifest_path=f"/hhru-platform/backups/{backup_file.name}.manifest.json",
    )
    LocalBackupOffsiteUploadReceiptStore().write_receipt(
        backup_file=backup_file,
        receipt=upload_receipt,
    )
    if verified:
        LocalBackupOffsiteVerificationReceiptStore().write_receipt(
            backup_file=backup_file,
            receipt=BackupOffsiteVerificationReceipt(
                verified_at=uploaded_at,
                offsite_url=upload_receipt.offsite_url,
                offsite_root=upload_receipt.offsite_root,
                backup_size_bytes=upload_receipt.backup_size_bytes,
                backup_sha256=upload_receipt.backup_sha256,
                manifest_sha256=upload_receipt.manifest_sha256,
                chunk_size_bytes=upload_receipt.chunk_size_bytes,
                part_count=upload_receipt.part_count,
                remote_backup_path=upload_receipt.remote_backup_path,
                remote_manifest_path=upload_receipt.remote_manifest_path,
                verified_object_count=3,
            ),
        )
    if protected:
        Path(f"{backup_file}.offsite.keep").touch()
    return backup_file


def _summary_for(result, backup_file: Path):
    return next(summary for summary in result.summaries if summary.backup_file == backup_file)
