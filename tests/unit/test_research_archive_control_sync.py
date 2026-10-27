import hashlib
import logging
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pytest

from hhru_platform.application.commands.sync_research_archive_offsite import (
    SyncResearchArchiveOffsiteCommand,
    sync_research_archive_offsite,
)
from hhru_platform.infrastructure.research_archive import (
    LocalResearchArchiveCheckpointVerificationReceiptStore,
    LocalResearchArchiveOffsiteUploadReceiptStore,
    ResearchArchiveCheckpointVerificationReceipt,
)
from hhru_platform.infrastructure.research_archive.control_upload_receipt_store import (
    LocalControlUploadReceiptStore,
)


class Remote:
    def __init__(self) -> None:
        self.calls: list[str] = []
        self.fail_on: str | None = None

    def upload_file(self, *, local_file: Path, remote_path: str) -> None:
        if self.fail_on == local_file.name:
            raise OSError("simulated upload failure")
        self.calls.append(remote_path)


def setup_archive(tmp_path):
    checkpoints = tmp_path / "v1/checkpoints/archive_kind=production"
    checkpoints.mkdir(parents=True)
    first = checkpoints / "a.checkpoint.json"
    second = checkpoints / "b.checkpoint.json"
    first.write_text("first")
    second.write_text("second")
    inventory = tmp_path / "v1/inventory/archive-inventory.jsonl"
    inventory.parent.mkdir()
    inventory.write_text("inventory")
    command = SyncResearchArchiveOffsiteCommand(
        archive_dir=tmp_path,
        offsite_url="https://s3.example.test/bucket",
    )
    return command, first, second, inventory


def sync(command, remote):
    return sync_research_archive_offsite(
        command,
        offsite_uploader=remote,
        receipt_store=LocalResearchArchiveOffsiteUploadReceiptStore(),
    )


def test_second_sync_skips_unchanged_checkpoints_and_inventory(tmp_path, caplog):
    command, _, _, _ = setup_archive(tmp_path)
    remote = Remote()
    caplog.set_level(logging.INFO)
    first = sync(command, remote)
    assert first.checkpoint_uploaded_count == 2
    assert first.checkpoint_scanned_count == 2
    assert first.checkpoint_skipped_count == 0
    assert first.inventory_uploaded
    second = sync(command, remote)
    assert second.checkpoint_uploaded_count == 0
    assert second.checkpoint_scanned_count == second.checkpoint_skipped_count == 2
    assert not second.inventory_uploaded
    assert second.checkpoint_files == second.remote_checkpoint_paths == ()
    assert len(remote.calls) == 3
    progress = [
        record
        for record in caplog.records
        if getattr(record, "event", "") == "sync_research_archive_offsite.progress"
    ]
    assert {record.phase for record in progress} == {"manifests", "inventory", "checkpoints"}
    assert progress[-1].completed == progress[-1].total == progress[-1].skipped == 2


def test_same_size_changed_checkpoint_and_inventory_are_reuploaded(tmp_path):
    command, first, _, inventory = setup_archive(tmp_path)
    remote = Remote()
    sync(command, remote)
    remote.calls.clear()
    first.write_text("FIRST")
    inventory.write_text("INVENTORY")
    result = sync(command, remote)
    assert result.inventory_uploaded
    assert result.checkpoint_uploaded_count == result.checkpoint_skipped_count == 1
    assert result.checkpoint_files == (first,)
    assert len(remote.calls) == 2


@pytest.mark.parametrize(
    "field,value",
    [
        ("offsite_url", "https://s3.example.test/another-bucket"),
        ("offsite_root", "/another/root"),
    ],
)
def test_destination_change_never_reuses_upload_cache(tmp_path, field, value):
    command, _, _, _ = setup_archive(tmp_path)
    remote = Remote()
    sync(command, remote)
    result = sync(replace(command, **{field: value}), remote)
    assert result.inventory_uploaded and result.checkpoint_uploaded_count == 2
    assert len(remote.calls) == 6


def test_retry_after_partial_failure_skips_completed_uploads(tmp_path):
    command, first, second, _ = setup_archive(tmp_path)
    remote = Remote()
    remote.fail_on = second.name
    with pytest.raises(OSError, match="simulated upload failure"):
        sync(command, remote)
    receipts = LocalControlUploadReceiptStore()
    assert receipts.receipt_path(first).exists()
    assert not receipts.receipt_path(second).exists()
    assert not Path(f"{first}.offsite.verified.json").exists()
    remote.fail_on = None
    result = sync(command, remote)
    assert not result.inventory_uploaded
    assert result.checkpoint_files == (second,)
    assert result.checkpoint_skipped_count == 1
    assert len(remote.calls) == 3


def test_existing_verified_checkpoint_is_not_reuploaded(tmp_path):
    command, first, _, _ = setup_archive(tmp_path)
    verifications = LocalResearchArchiveCheckpointVerificationReceiptStore()
    verifications.write_receipt(
        checkpoint_file=first,
        receipt=ResearchArchiveCheckpointVerificationReceipt(
            verified_at=datetime.now(UTC),
            offsite_url=command.offsite_url,
            offsite_root=command.offsite_root,
            checkpoint_size_bytes=first.stat().st_size,
            checkpoint_sha256=hashlib.sha256(first.read_bytes()).hexdigest(),
            remote_checkpoint_path=f"{command.offsite_root}/{first.relative_to(tmp_path).as_posix()}",
        ),
    )
    remote = Remote()
    result = sync(command, remote)
    assert result.checkpoint_uploaded_count == result.checkpoint_skipped_count == 1
    assert first not in result.checkpoint_files
    first.write_text("FIRST")
    result = sync(command, remote)
    assert result.checkpoint_files == (first,)


def test_malformed_receipts_are_not_trusted(tmp_path):
    command, first, second, _ = setup_archive(tmp_path)
    LocalControlUploadReceiptStore.receipt_path(first).write_text("not-json")
    Path(f"{second}.offsite.verified.json").write_text("{}")
    result = sync(command, Remote())
    assert result.checkpoint_uploaded_count == 2


def test_partial_sync_never_uploads_inventory_or_checkpoints(tmp_path):
    command, _, _, _ = setup_archive(tmp_path)
    remote = Remote()
    result = sync(replace(command, limit=1), remote)
    assert not remote.calls
    assert result.checkpoint_scanned_count == result.checkpoint_uploaded_count == 0
    assert not result.inventory_uploaded


def test_progress_reports_cached_checkpoint_batches(tmp_path, caplog):
    command, first, second, inventory = setup_archive(tmp_path)
    second.unlink()
    inventory.unlink()
    for index in range(200):
        first.with_name(f"{index:04d}.checkpoint.json").write_text("checkpoint")
    remote = Remote()
    sync(command, remote)
    caplog.clear()
    caplog.set_level(logging.INFO)
    sync(command, remote)
    progress = [
        record
        for record in caplog.records
        if getattr(record, "event", "") == "sync_research_archive_offsite.progress"
        and record.phase == "checkpoints"
    ]
    assert [record.completed for record in progress] == [0, 100, 200, 201]
    assert progress[-1].skipped == 201


def test_upload_cache_is_atomic_and_does_not_replace_old_proof_on_write_failure(
    tmp_path, monkeypatch
):
    source = tmp_path / "checkpoint"
    source.write_text("data")
    receipts = LocalControlUploadReceiptStore()
    fingerprint = {"sha256": "first", "offsite_url": "test"}
    receipts.write_receipt(local_file=source, fingerprint=fingerprint)
    receipt_file = receipts.receipt_path(source)
    original = receipt_file.read_bytes()

    def fail_replace(self, target):
        raise OSError("simulated replace failure")

    monkeypatch.setattr(Path, "replace", fail_replace)
    with pytest.raises(OSError, match="simulated replace failure"):
        receipts.write_receipt(local_file=source, fingerprint={**fingerprint, "sha256": "changed"})
    assert receipt_file.read_bytes() == original
    assert not list(tmp_path.glob(f".{receipt_file.name}.*"))


def test_control_file_changed_during_upload_does_not_get_cached(tmp_path):
    command, _, _, inventory = setup_archive(tmp_path)

    class MutatingRemote(Remote):
        def upload_file(self, *, local_file, remote_path):
            super().upload_file(local_file=local_file, remote_path=remote_path)
            local_file.write_text("changed during upload")

    with pytest.raises(RuntimeError, match="control file changed during upload"):
        sync(command, MutatingRemote())
    assert not LocalControlUploadReceiptStore.receipt_path(inventory).exists()


@pytest.mark.parametrize("mismatch", ["url", "root", "path", "size", "sha256"])
def test_legacy_verification_must_match_all_checkpoint_properties(tmp_path, mismatch):
    command, first, _, _ = setup_archive(tmp_path)
    receipt = ResearchArchiveCheckpointVerificationReceipt(
        verified_at=datetime.now(UTC),
        offsite_url=command.offsite_url,
        offsite_root=command.offsite_root,
        checkpoint_size_bytes=first.stat().st_size,
        checkpoint_sha256=hashlib.sha256(first.read_bytes()).hexdigest(),
        remote_checkpoint_path=f"{command.offsite_root}/{first.relative_to(tmp_path).as_posix()}",
    )
    changes = {
        "url": {"offsite_url": "https://other.example.test"},
        "root": {"offsite_root": "/other"},
        "path": {"remote_checkpoint_path": "/other/checkpoint"},
        "size": {"checkpoint_size_bytes": 99},
        "sha256": {"checkpoint_sha256": "wrong"},
    }
    LocalResearchArchiveCheckpointVerificationReceiptStore().write_receipt(
        checkpoint_file=first,
        receipt=replace(receipt, **changes[mismatch]),
    )
    result = sync(command, Remote())
    assert result.checkpoint_uploaded_count == 2
