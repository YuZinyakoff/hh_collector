from __future__ import annotations

import hashlib
import json
import subprocess
from datetime import UTC, datetime
from pathlib import Path

from hhru_platform.application.commands.run_backup import RunBackupCommand, run_backup
from hhru_platform.application.commands.run_backup_offsite_restore_drill import (
    RunBackupOffsiteIntegrityDrillCommand,
    RunBackupOffsiteRestoreDrillCommand,
    run_backup_offsite_integrity_drill,
    run_backup_offsite_restore_drill,
)
from hhru_platform.application.commands.run_restore_drill import (
    RunRestoreDrillCommand,
    run_restore_drill,
)
from hhru_platform.config.settings import Settings
from hhru_platform.infrastructure.backup.backup_service import (
    BackupArchiveSummary,
    BackupService,
    RestoreDrillSummary,
)
from hhru_platform.infrastructure.observability.metrics import FileBackedMetricsRegistry

REPO_ROOT = Path(__file__).resolve().parents[2]


def test_backup_service_inspects_backup_archive_with_hash_and_entry_count(tmp_path) -> None:
    backup_file = tmp_path / "sample.dump"
    backup_file.write_bytes(b"backup-payload")

    def fake_runner(args, **kwargs) -> subprocess.CompletedProcess[str]:
        assert args == ["pg_restore", "--list", str(backup_file)]
        return subprocess.CompletedProcess(
            args=args,
            returncode=0,
            stdout=";\n1; 2615 2200 TABLE public crawl_run\n2; 0 0 COMMENT -\n",
            stderr="",
        )

    service = BackupService(
        Settings(
            db_host="postgres",
            db_port=5432,
            db_name="hhru_platform",
            db_user="hhru",
            db_password="secret",
        ),
        runner=fake_runner,
        repo_root=tmp_path,
    )

    summary = service.inspect_backup_file(backup_file)

    assert summary.backup_file == backup_file
    assert summary.size_bytes == len(b"backup-payload")
    assert summary.sha256 == hashlib.sha256(b"backup-payload").hexdigest()
    assert summary.archive_entry_count == 2


def test_backup_service_restore_to_target_db_verifies_required_tables(tmp_path) -> None:
    backup_file = tmp_path / "sample.dump"
    backup_file.write_bytes(b"restore-payload")
    scripts_dir = tmp_path / "scripts" / "backup"
    scripts_dir.mkdir(parents=True)
    (scripts_dir / "restore_postgres.sh").write_text("#!/usr/bin/env bash\n", encoding="utf-8")

    def fake_runner(args, **kwargs) -> subprocess.CompletedProcess[str]:
        if args[:2] == ["pg_restore", "--list"]:
            return subprocess.CompletedProcess(
                args=args,
                returncode=0,
                stdout="1; 2615 2200 TABLE public crawl_run\n",
                stderr="",
            )
        if args[0] == "bash":
            assert kwargs["env"]["PGPASSWORD"] == "secret"
            return subprocess.CompletedProcess(
                args=args,
                returncode=0,
                stdout=(
                    "restore_mode=target_db\n"
                    f"restored_from={backup_file}\n"
                    "restored_to_db=hhru_platform_restore_drill\n"
                ),
                stderr="",
            )
        if args[0] == "psql":
            assert kwargs["env"]["PGPASSWORD"] == "secret"
            return subprocess.CompletedProcess(
                args=args,
                returncode=0,
                stdout="5\n",
                stderr="",
            )
        raise AssertionError(f"unexpected command: {args}")

    service = BackupService(
        Settings(
            db_host="postgres",
            db_port=5432,
            db_name="hhru_platform",
            db_user="hhru",
            db_password="secret",
        ),
        runner=fake_runner,
        repo_root=tmp_path,
    )

    summary = service.restore_to_target_db(
        backup_file=backup_file,
        target_db="hhru_platform_restore_drill",
    )

    assert summary.backup_file == backup_file
    assert summary.target_db == "hhru_platform_restore_drill"
    assert summary.archive_entry_count == 1
    assert summary.schema_verified is True
    assert summary.present_table_count == 5


def test_run_backup_records_backup_lifecycle_metrics(tmp_path) -> None:
    metrics = FileBackedMetricsRegistry(tmp_path / "metrics.json")

    class StubBackupService:
        def create_backup(self) -> BackupArchiveSummary:
            return BackupArchiveSummary(
                backup_file=tmp_path / "sample.dump",
                size_bytes=128,
                sha256="abc123",
                archive_entry_count=7,
            )

    result = run_backup(
        RunBackupCommand(
            triggered_by="unit-test",
            recorded_at=datetime(2026, 3, 21, 10, 0, tzinfo=UTC),
        ),
        backup_service=StubBackupService(),  # type: ignore[arg-type]
        metrics_recorder=metrics,
    )

    rendered = metrics.render_prometheus()

    assert result.status == "succeeded"
    assert 'hhru_backup_run_total{status="succeeded"} 1' in rendered
    assert "hhru_backup_last_success_timestamp_seconds" in rendered


def test_run_restore_drill_records_restore_lifecycle_metrics(tmp_path) -> None:
    metrics = FileBackedMetricsRegistry(tmp_path / "metrics.json")

    class StubBackupService:
        def restore_to_target_db(
            self,
            *,
            backup_file: Path,
            target_db: str,
            drop_existing: bool = True,
        ) -> RestoreDrillSummary:
            assert backup_file == tmp_path / "sample.dump"
            assert target_db == "hhru_platform_restore_drill"
            assert drop_existing is True
            return RestoreDrillSummary(
                backup_file=backup_file,
                target_db=target_db,
                archive_entry_count=9,
                required_tables=(
                    "crawl_run",
                    "crawl_partition",
                    "raw_api_payload",
                    "vacancy_snapshot",
                    "vacancy_current_state",
                ),
                present_table_count=5,
                schema_verified=True,
            )

    result = run_restore_drill(
        RunRestoreDrillCommand(
            backup_file=tmp_path / "sample.dump",
            target_db="hhru_platform_restore_drill",
            drop_target_db=True,
            triggered_by="unit-test",
            recorded_at=datetime(2026, 3, 21, 10, 30, tzinfo=UTC),
        ),
        backup_service=StubBackupService(),  # type: ignore[arg-type]
        metrics_recorder=metrics,
    )

    rendered = metrics.render_prometheus()

    assert result.status == "succeeded"
    assert 'hhru_restore_drill_run_total{status="succeeded"} 1' in rendered
    assert "hhru_restore_drill_last_success_timestamp_seconds" in rendered


def test_run_backup_offsite_restore_drill_downloads_parts_and_restores(tmp_path) -> None:
    backup_dir = tmp_path / "backups"
    backup_dir.mkdir()
    backup_file = backup_dir / "sample.dump"
    parts = (b"first-part", b"second-part")
    backup_payload = b"".join(parts)
    backup_sha256 = hashlib.sha256(backup_payload).hexdigest()
    manifest_file = Path(f"{backup_file.resolve()}.manifest.json")
    manifest_payload = {
        "manifest_version": 2,
        "upload_mode": "parts",
        "backup_file": backup_file.name,
        "backup_size_bytes": len(backup_payload),
        "backup_sha256": backup_sha256,
        "chunk_size_bytes": 10,
        "parts": [
            {
                "index": 1,
                "file": f"{backup_file.name}.parts/000001.part",
                "size_bytes": len(parts[0]),
                "sha256": hashlib.sha256(parts[0]).hexdigest(),
            },
            {
                "index": 2,
                "file": f"{backup_file.name}.parts/000002.part",
                "size_bytes": len(parts[1]),
                "sha256": hashlib.sha256(parts[1]).hexdigest(),
            },
        ],
    }
    manifest_file.write_text(json.dumps(manifest_payload), encoding="utf-8")

    class FakeRemoteDownloader:
        def __init__(self) -> None:
            self.download_calls: list[str] = []
            self.payloads_by_remote_path = {
                manifest_file.name: manifest_file.read_bytes(),
                f"{backup_file.name}.parts/000001.part": parts[0],
                f"{backup_file.name}.parts/000002.part": parts[1],
            }

        def download_file(self, *, local_file: Path, remote_path: str) -> None:
            self.download_calls.append(remote_path)
            local_file.write_bytes(self.payloads_by_remote_path[remote_path])

    class StubBackupService:
        def restore_to_target_db(
            self,
            *,
            backup_file: Path,
            target_db: str,
            drop_existing: bool = True,
        ) -> RestoreDrillSummary:
            assert backup_file.read_bytes() == backup_payload
            assert target_db == "hhru_platform_restore_drill"
            assert drop_existing is True
            return RestoreDrillSummary(
                backup_file=backup_file,
                target_db=target_db,
                archive_entry_count=11,
                required_tables=(
                    "crawl_run",
                    "crawl_partition",
                    "raw_api_payload",
                    "vacancy_snapshot",
                    "vacancy_current_state",
                ),
                present_table_count=5,
                schema_verified=True,
            )

    remote_downloader = FakeRemoteDownloader()
    metrics = FileBackedMetricsRegistry(tmp_path / "metrics.json")

    result = run_backup_offsite_restore_drill(
        RunBackupOffsiteRestoreDrillCommand(
            backup_file=backup_file,
            backup_dir=backup_dir,
            offsite_url="https://s3.example.test/bucket",
            offsite_root="/hhru-platform/backups",
            target_db="hhru_platform_restore_drill",
            drop_target_db=True,
            triggered_by="unit-test",
            recorded_at=datetime(2026, 5, 23, 12, 0, tzinfo=UTC),
        ),
        remote_downloader=remote_downloader,
        backup_service=StubBackupService(),  # type: ignore[arg-type]
        metrics_recorder=metrics,
    )

    assert result.status == "succeeded"
    assert result.backup_file == backup_file.resolve()
    assert result.manifest_file == manifest_file
    assert result.backup_size_bytes == len(backup_payload)
    assert result.backup_sha256 == backup_sha256
    assert result.chunk_size_bytes == 10
    assert result.part_count == 2
    assert result.downloaded_part_count == 2
    assert result.archive_entry_count == 11
    assert result.schema_verified is True
    assert remote_downloader.download_calls == [
        manifest_file.name,
        f"{backup_file.name}.parts/000001.part",
        f"{backup_file.name}.parts/000002.part",
    ]
    assert 'hhru_restore_drill_run_total{status="succeeded"} 1' in (
        metrics.render_prometheus()
    )


def test_run_backup_offsite_integrity_drill_downloads_parts_without_restore(
    tmp_path,
) -> None:
    backup_dir = tmp_path / "backups"
    backup_dir.mkdir()
    backup_file = backup_dir / "sample.dump"
    parts = (b"first-part", b"second-part")
    backup_payload = b"".join(parts)
    backup_sha256 = hashlib.sha256(backup_payload).hexdigest()
    manifest_file = Path(f"{backup_file.resolve()}.manifest.json")
    manifest_payload = {
        "manifest_version": 2,
        "upload_mode": "parts",
        "backup_file": backup_file.name,
        "backup_size_bytes": len(backup_payload),
        "backup_sha256": backup_sha256,
        "chunk_size_bytes": 10,
        "parts": [
            {
                "index": 1,
                "file": f"{backup_file.name}.parts/000001.part",
                "size_bytes": len(parts[0]),
                "sha256": hashlib.sha256(parts[0]).hexdigest(),
            },
            {
                "index": 2,
                "file": f"{backup_file.name}.parts/000002.part",
                "size_bytes": len(parts[1]),
                "sha256": hashlib.sha256(parts[1]).hexdigest(),
            },
        ],
    }
    manifest_file.write_text(json.dumps(manifest_payload), encoding="utf-8")

    class FakeRemoteDownloader:
        def __init__(self) -> None:
            self.download_calls: list[str] = []
            self.payloads_by_remote_path = {
                manifest_file.name: manifest_file.read_bytes(),
                f"{backup_file.name}.parts/000001.part": parts[0],
                f"{backup_file.name}.parts/000002.part": parts[1],
            }

        def download_file(self, *, local_file: Path, remote_path: str) -> None:
            self.download_calls.append(remote_path)
            local_file.write_bytes(self.payloads_by_remote_path[remote_path])

    class StubBackupService:
        def __init__(self) -> None:
            self.inspected_backup_payload: bytes | None = None

        def inspect_backup_file(self, backup_file: Path) -> BackupArchiveSummary:
            self.inspected_backup_payload = backup_file.read_bytes()
            return BackupArchiveSummary(
                backup_file=backup_file,
                size_bytes=backup_file.stat().st_size,
                sha256=hashlib.sha256(self.inspected_backup_payload).hexdigest(),
                archive_entry_count=11,
            )

        def restore_to_target_db(self, **kwargs) -> RestoreDrillSummary:  # pragma: no cover
            raise AssertionError("integrity drill must not restore a database")

    remote_downloader = FakeRemoteDownloader()
    backup_service = StubBackupService()

    result = run_backup_offsite_integrity_drill(
        RunBackupOffsiteIntegrityDrillCommand(
            backup_file=backup_file,
            backup_dir=backup_dir,
            offsite_url="https://s3.example.test/bucket",
            offsite_root="/hhru-platform/backups",
            triggered_by="unit-test",
            recorded_at=datetime(2026, 6, 28, 12, 0, tzinfo=UTC),
        ),
        remote_downloader=remote_downloader,
        backup_service=backup_service,  # type: ignore[arg-type]
    )

    assert result.status == "succeeded"
    assert result.backup_file == backup_file.resolve()
    assert result.manifest_file == manifest_file
    assert result.backup_size_bytes == len(backup_payload)
    assert result.backup_sha256 == backup_sha256
    assert result.chunk_size_bytes == 10
    assert result.part_count == 2
    assert result.downloaded_part_count == 2
    assert result.archive_entry_count == 11
    assert backup_service.inspected_backup_payload == backup_payload
    assert remote_downloader.download_calls == [
        manifest_file.name,
        f"{backup_file.name}.parts/000001.part",
        f"{backup_file.name}.parts/000002.part",
    ]


def test_backup_scripts_are_shell_syntax_valid() -> None:
    for script_path in (
        REPO_ROOT / "scripts" / "backup" / "backup_postgres.sh",
        REPO_ROOT / "scripts" / "backup" / "restore_postgres.sh",
    ):
        subprocess.run(["bash", "-n", str(script_path)], check=True)
