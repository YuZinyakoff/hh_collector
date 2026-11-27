from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

import pytest

from hhru_platform.application.commands.run_backup_offsite_restore_drill import (
    RunBackupOffsiteIntegrityDrillCommand,
    run_backup_offsite_integrity_drill,
)
from hhru_platform.application.commands.stream_backup_offsite import (
    StreamBackupOffsiteCommand,
    stream_backup_offsite,
)
from hhru_platform.config.settings import Settings
from hhru_platform.infrastructure.backup.backup_offsite_receipt_store import (
    LocalBackupOffsiteUploadReceiptStore,
)
from hhru_platform.infrastructure.backup.backup_service import BackupService, BackupToolError
from hhru_platform.interfaces.cli.commands.backup import _build_stream_backup_pilot
from hhru_platform.interfaces.cli.main import build_parser

ARCHIVE = b"PGDMP0123456789"
REPO_ROOT = Path(__file__).resolve().parents[2]
PILOT_ROOT = "/hhru-platform/backups-streaming-pilot"


class FakeStore:
    def __init__(self, *, fail_index: str = "", failures: int = 0) -> None:
        self.objects: dict[str, bytes] = {}
        self.calls: list[str] = []
        self.fail_index = fail_index
        self.failures = failures

    def upload_file(self, *, local_file: Path, remote_path: str) -> None:
        if remote_path.endswith(".part"):
            assert list(local_file.parent.iterdir()) == [local_file]
            assert local_file.stat().st_size <= 8
        self.calls.append(remote_path)
        if self.fail_index and remote_path.endswith(self.fail_index) and self.failures:
            self.failures -= 1
            raise OSError("S3 unavailable")
        self.objects[remote_path] = local_file.read_bytes()

    def download_file(self, *, local_file: Path, remote_path: str) -> None:
        local_file.write_bytes(self.objects[remote_path])


@pytest.fixture
def backup_service(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> BackupService:
    bindir = tmp_path / "bin"
    bindir.mkdir()
    tools = {
        "pg_dump": """#!/usr/bin/env bash
set -eu
[[ "$1" == "--host=fixture" && "$PGPASSWORD" == "fixture-password" ]]
for arg in "$@"; do
  [[ "$arg" != --file=* ]] || exit 2
done
printf %s "$FAKE_DUMP_PAYLOAD"
if [[ "${FAKE_DUMP_FAIL:-0}" = 1 ]]; then
  echo 'database connection interrupted' >&2
  exit 1
fi
""",
        "pg_restore": """#!/usr/bin/env bash
set -eu
[[ "$#" = 1 && "$1" = --list ]]
read -r -n 5 header || true
[[ "$header" = PGDMP ]]
printf '1; TABLE public crawl_run\n'
""",
    }
    for name, script in tools.items():
        path = bindir / name
        path.write_text(script, encoding="ascii")
        path.chmod(0o700)
    monkeypatch.setenv("PATH", f"{bindir}:{os.environ['PATH']}")
    monkeypatch.setenv("FAKE_DUMP_PAYLOAD", ARCHIVE.decode("ascii"))
    monkeypatch.setenv("FAKE_DUMP_FAIL", "0")
    monkeypatch.setattr(
        "hhru_platform.application.commands.stream_backup_offsite"
        ".BACKUP_OFFSITE_PART_UPLOAD_RETRY_BACKOFF_SECONDS",
        0,
    )
    return BackupService(
        Settings(db_host="fixture", db_password="fixture-password"),
        repo_root=REPO_ROOT,
    )


def _command(tmp_path: Path, *, limit: int = len(ARCHIVE)) -> StreamBackupOffsiteCommand:
    return StreamBackupOffsiteCommand(
        backup_dir=tmp_path / "pilot",
        backup_name_prefix="hhru-platform_fixture",
        offsite_url="https://s3.example.test/bucket",
        offsite_root=PILOT_ROOT,
        chunk_size_bytes=8,
        max_backup_bytes=limit,
        triggered_by="test",
    )


def test_stream_backup_retries_part_and_readback_passes(
    tmp_path: Path, backup_service: BackupService
) -> None:
    command = _command(tmp_path)
    store = FakeStore(fail_index="000001.part", failures=1)
    receipts = LocalBackupOffsiteUploadReceiptStore()

    result = stream_backup_offsite(
        command, backup_service=backup_service, offsite_uploader=store, receipt_store=receipts
    )

    assert not result.backup_file.exists()
    assert set(command.backup_dir.iterdir()) == {result.manifest_file, result.receipt_file}
    manifest = json.loads(result.manifest_file.read_bytes())
    parts = manifest["parts"]
    assert [part["size_bytes"] for part in parts] == [8, len(ARCHIVE) - 8]
    assert b"".join(store.objects[part["file"]] for part in parts) == ARCHIVE
    assert store.calls == [
        parts[0]["file"],
        parts[0]["file"],
        parts[1]["file"],
        result.manifest_file.name,
    ]
    receipt = receipts.load_receipt(backup_file=result.backup_file)
    assert receipt is not None
    assert receipt.backup_sha256 == hashlib.sha256(ARCHIVE).hexdigest()
    assert receipt.manifest_sha256 == hashlib.sha256(result.manifest_file.read_bytes()).hexdigest()
    assert receipt.remote_backup_path == f"{PILOT_ROOT}/{result.backup_file.name}.parts"

    readback = run_backup_offsite_integrity_drill(
        RunBackupOffsiteIntegrityDrillCommand(
            backup_file=result.backup_file,
            backup_dir=command.backup_dir,
            offsite_url=command.offsite_url,
            offsite_root=PILOT_ROOT,
        ),
        remote_downloader=store,
        backup_service=backup_service,
    )
    assert readback.backup_sha256 == receipt.backup_sha256
    assert readback.downloaded_part_count == 2
    assert readback.archive_entry_count == 1


@pytest.mark.parametrize("failure", ["pg_dump", "upload", "byte_limit"])
def test_stream_backup_does_not_publish_failed_generation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    backup_service: BackupService,
    failure: str,
) -> None:
    store = FakeStore(fail_index="000002.part", failures=3 if failure == "upload" else 0)
    command = _command(tmp_path, limit=8 if failure == "byte_limit" else len(ARCHIVE))
    if failure == "pg_dump":
        monkeypatch.setenv("FAKE_DUMP_FAIL", "1")
        expected = (BackupToolError, "pg_dump failed: database connection interrupted")
    elif failure == "upload":
        expected = (OSError, "S3 unavailable")
    else:
        expected = (RuntimeError, "exceeds byte limit")

    with pytest.raises(expected[0], match=expected[1]):
        stream_backup_offsite(
            command,
            backup_service=backup_service,
            offsite_uploader=store,
            receipt_store=LocalBackupOffsiteUploadReceiptStore(),
        )

    assert store.objects
    assert all(key.endswith(".part") for key in store.calls)
    assert not list(command.backup_dir.iterdir())
    if failure == "byte_limit":
        assert sum(map(len, store.objects.values())) == 8


def test_cli_pilot_uses_separate_paths(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        "hhru_platform.infrastructure.backup.s3_backup_offsite_uploader._create_s3_client",
        lambda **_: object(),
    )
    settings = Settings(
        backup_dir=".state/backups",
        backup_offsite_backend="s3",
        backup_offsite_root="/hhru-platform/backups/",
        backup_offsite_s3_endpoint_url="https://s3.example.test",
        backup_offsite_s3_bucket="bucket",
        backup_offsite_s3_access_key_id="fixture",
        backup_offsite_s3_secret_access_key="fixture",
    )
    args = build_parser().parse_args(["stream-backup-offsite", "--max-backup-bytes", "8192"])
    command, _ = _build_stream_backup_pilot(args=args, settings=settings)
    assert command.backup_dir == tmp_path / ".state/reports/streaming-backup-pilot"
    assert command.offsite_root == PILOT_ROOT

    with pytest.raises(ValueError, match="must not overlap"):
        _build_stream_backup_pilot(
            args=args, settings=settings.model_copy(update={"backup_dir": ".state"})
        )
