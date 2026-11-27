from __future__ import annotations

import os
import shlex
import shutil
import subprocess
from pathlib import Path

import pytest

from hhru_platform.application.commands.run_backup_offsite_restore_drill import (
    RunBackupOffsiteRestoreDrillCommand,
    run_backup_offsite_restore_drill,
)
from hhru_platform.application.commands.stream_backup_offsite import (
    StreamBackupOffsiteCommand,
    stream_backup_offsite,
)
from hhru_platform.config.settings import Settings
from hhru_platform.infrastructure.backup.backup_offsite_receipt_store import (
    LocalBackupOffsiteUploadReceiptStore,
)
from hhru_platform.infrastructure.backup.backup_service import (
    BACKUP_DRILL_REQUIRED_TABLES,
    BackupService,
)

PG_BINARIES = ("initdb", "pg_ctl", "psql", "createdb", "pg_dump", "pg_restore")
pytestmark = pytest.mark.skipif(
    os.geteuid() == 0 or not all(shutil.which(binary) for binary in PG_BINARIES),
    reason="An unprivileged user and PostgreSQL server/client binaries are required.",
)


class FixtureObjectStore:
    def __init__(self, root: Path) -> None:
        self.root = root

    def upload_file(self, *, local_file: Path, remote_path: str) -> None:
        destination = self.root / remote_path
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(local_file, destination)

    def download_file(self, *, local_file: Path, remote_path: str) -> None:
        shutil.copyfile(self.root / remote_path, local_file)


def _run(*args: str) -> str:
    return subprocess.run(
        args, check=True, capture_output=True, text=True, timeout=30
    ).stdout.strip()


def test_streamed_pg_dump_restores_fixture_rows(tmp_path: Path) -> None:
    socket_dir = tmp_path / "socket"
    socket_dir.mkdir()
    data_dir = str(tmp_path / "pgdata")
    port = "55487"
    host = str(socket_dir)
    user = "stream_pilot"
    source_db, restored_db = "pilot_source", "pilot_restored"
    _run("initdb", "-D", data_dir, "-U", user, "-A", "trust", "--no-locale", "--no-sync")
    _run(
        "pg_ctl",
        "-D",
        data_dir,
        "-l",
        str(tmp_path / "postgres.log"),
        "-w",
        "-o",
        f"-F -h '' -k {shlex.quote(host)} -p {port}",
        "start",
    )
    connect = ("-h", host, "-p", port, "-U", user)
    try:
        _run("createdb", *connect, source_db)
        ddl = "; ".join(
            f"CREATE TABLE public.{table} (id integer PRIMARY KEY, value text)"
            for table in BACKUP_DRILL_REQUIRED_TABLES
        )
        _run(
            "psql",
            "-X",
            *connect,
            "-d",
            source_db,
            "-v",
            "ON_ERROR_STOP=1",
            "-c",
            ddl + "; INSERT INTO vacancy_current_state VALUES (42, 'restore proof')",
        )
        settings = Settings(
            db_host=host,
            db_port=int(port),
            db_user=user,
            db_name=source_db,
            db_password="unused-with-private-test-socket",
        )
        backup_service = BackupService(settings)
        store = FixtureObjectStore(tmp_path / "object-store")
        command = StreamBackupOffsiteCommand(
            backup_dir=tmp_path / "pilot",
            backup_name_prefix=source_db,
            offsite_url="https://s3.example.test/bucket",
            offsite_root="/backups-streaming-pilot",
            chunk_size_bytes=1024,
            max_backup_bytes=1024**2,
            triggered_by="pytest-stream-restore",
        )
        backup = stream_backup_offsite(
            command,
            backup_service=backup_service,
            offsite_uploader=store,
            receipt_store=LocalBackupOffsiteUploadReceiptStore(),
        )
        assert not backup.backup_file.exists()
        assert backup.part_count > 1

        restore = run_backup_offsite_restore_drill(
            RunBackupOffsiteRestoreDrillCommand(
                backup_file=backup.backup_file,
                target_db=restored_db,
                backup_dir=command.backup_dir,
                offsite_url=command.offsite_url,
                offsite_root=command.offsite_root,
                drop_target_db=False,
            ),
            remote_downloader=store,
            backup_service=backup_service,
        )
        assert restore.schema_verified
        assert restore.backup_sha256 == backup.backup_sha256
        assert (
            _run(
                "psql",
                "-XAt",
                *connect,
                "-d",
                restored_db,
                "-c",
                "SELECT value FROM vacancy_current_state WHERE id = 42",
            )
            == "restore proof"
        )
    finally:
        _run("pg_ctl", "-D", data_dir, "-m", "immediate", "-w", "stop")
