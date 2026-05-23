from __future__ import annotations

import hashlib
import json
import logging
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol

from hhru_platform.application.commands.run_restore_drill import (
    RestoreDrillMetricsRecorder,
    RunRestoreDrillCommand,
    run_restore_drill,
)
from hhru_platform.infrastructure.backup.backup_service import BackupService
from hhru_platform.infrastructure.observability.logging import log_event
from hhru_platform.infrastructure.observability.operations import (
    log_operation_started,
    record_operation_failed,
    record_operation_succeeded,
)

LOGGER = logging.getLogger(__name__)

BACKUP_OFFSITE_RESTORE_DRILL_STATUS_SUCCEEDED = "succeeded"


@dataclass(slots=True, frozen=True)
class RunBackupOffsiteRestoreDrillCommand:
    backup_file: Path
    target_db: str
    backup_dir: Path = Path(".state/backups")
    offsite_url: str = ""
    offsite_root: str = "/hhru-platform/backups"
    drop_target_db: bool = True
    triggered_by: str = "run-backup-offsite-restore-drill"
    recorded_at: datetime | None = None

    def __post_init__(self) -> None:
        normalized_target_db = self.target_db.strip()
        normalized_triggered_by = self.triggered_by.strip()
        normalized_offsite_url = self.offsite_url.strip().rstrip("/")
        normalized_offsite_root = _normalize_offsite_root(self.offsite_root)
        if not normalized_target_db:
            raise ValueError("target_db must not be empty")
        if not normalized_triggered_by:
            raise ValueError("triggered_by must not be empty")
        if not normalized_offsite_url:
            raise ValueError("offsite_url must not be empty")
        object.__setattr__(self, "backup_file", Path(self.backup_file))
        object.__setattr__(self, "backup_dir", Path(self.backup_dir))
        object.__setattr__(self, "target_db", normalized_target_db)
        object.__setattr__(self, "triggered_by", normalized_triggered_by)
        object.__setattr__(self, "offsite_url", normalized_offsite_url)
        object.__setattr__(self, "offsite_root", normalized_offsite_root)


@dataclass(slots=True, frozen=True)
class BackupOffsiteDownloadedPart:
    remote_path: str
    size_bytes: int
    sha256: str


@dataclass(slots=True, frozen=True)
class RunBackupOffsiteRestoreDrillResult:
    status: str
    triggered_by: str
    recorded_at: datetime
    backup_file: Path
    manifest_file: Path
    offsite_url: str
    offsite_root: str
    target_db: str
    backup_size_bytes: int
    backup_sha256: str
    chunk_size_bytes: int
    downloaded_parts: tuple[BackupOffsiteDownloadedPart, ...]
    archive_entry_count: int
    checked_tables: tuple[str, ...]
    verified_tables_count: int
    schema_verified: bool

    @property
    def part_count(self) -> int:
        return len(self.downloaded_parts)

    @property
    def downloaded_part_count(self) -> int:
        return len(self.downloaded_parts)


class BackupOffsiteRemoteDownloader(Protocol):
    def download_file(self, *, local_file: Path, remote_path: str) -> None:
        """Download one off-host backup artifact into a local file."""


def run_backup_offsite_restore_drill(
    command: RunBackupOffsiteRestoreDrillCommand,
    *,
    remote_downloader: BackupOffsiteRemoteDownloader,
    backup_service: BackupService,
    metrics_recorder: RestoreDrillMetricsRecorder | None = None,
) -> RunBackupOffsiteRestoreDrillResult:
    started_at = log_operation_started(
        LOGGER,
        operation="run_backup_offsite_restore_drill",
        backup_file=str(command.backup_file),
        target_db=command.target_db,
        drop_target_db=command.drop_target_db,
        offsite_url=command.offsite_url,
        offsite_root=command.offsite_root,
        triggered_by=command.triggered_by,
    )
    try:
        result = _run_backup_offsite_restore_drill(
            command=command,
            remote_downloader=remote_downloader,
            backup_service=backup_service,
            metrics_recorder=metrics_recorder,
        )
    except Exception as error:
        record_operation_failed(
            LOGGER,
            operation="run_backup_offsite_restore_drill",
            started_at=started_at,
            error_type=error.__class__.__name__,
            error_message=str(error),
            backup_file=str(command.backup_file),
            target_db=command.target_db,
            offsite_url=command.offsite_url,
            offsite_root=command.offsite_root,
            triggered_by=command.triggered_by,
        )
        raise

    record_operation_succeeded(
        LOGGER,
        operation="run_backup_offsite_restore_drill",
        started_at=started_at,
        backup_file=str(result.backup_file),
        target_db=result.target_db,
        offsite_url=result.offsite_url,
        offsite_root=result.offsite_root,
        backup_size_bytes=result.backup_size_bytes,
        chunk_size_bytes=result.chunk_size_bytes,
        part_count=result.part_count,
        downloaded_part_count=result.downloaded_part_count,
        schema_verified=result.schema_verified,
        verified_tables_count=result.verified_tables_count,
        triggered_by=result.triggered_by,
    )
    return result


def _run_backup_offsite_restore_drill(
    *,
    command: RunBackupOffsiteRestoreDrillCommand,
    remote_downloader: BackupOffsiteRemoteDownloader,
    backup_service: BackupService,
    metrics_recorder: RestoreDrillMetricsRecorder | None,
) -> RunBackupOffsiteRestoreDrillResult:
    backup_root = command.backup_dir.resolve()
    backup_file = command.backup_file.resolve()
    manifest_file = Path(f"{backup_file}.manifest.json")
    if not manifest_file.exists():
        raise FileNotFoundError(f"backup manifest not found: {manifest_file}")

    manifest_payload = json.loads(manifest_file.read_text(encoding="utf-8"))
    backup_relative_path = backup_file.relative_to(backup_root).as_posix()
    manifest_relative_path = manifest_file.relative_to(backup_root).as_posix()
    if str(manifest_payload.get("backup_file", "")) != backup_relative_path:
        raise RuntimeError(
            "backup manifest does not match selected backup file: "
            f"manifest_backup_file={manifest_payload.get('backup_file')!r} "
            f"backup_file={backup_relative_path!r}"
        )

    recorded_at = command.recorded_at or datetime.now(UTC)
    parts = _manifest_parts(manifest_payload)
    with tempfile.TemporaryDirectory(prefix="hhru-backup-offsite-restore-") as temp_dir:
        temp_root = Path(temp_dir)
        _download_and_verify_remote_manifest(
            remote_downloader=remote_downloader,
            local_manifest_file=manifest_file,
            remote_manifest_relative_path=manifest_relative_path,
            temp_root=temp_root,
        )
        assembled_backup = temp_root / backup_file.name
        downloaded_parts = _download_and_assemble_parts(
            remote_downloader=remote_downloader,
            parts=parts,
            assembled_backup=assembled_backup,
        )
        assembled_size = assembled_backup.stat().st_size
        expected_size = _payload_int(manifest_payload, "backup_size_bytes")
        if assembled_size != expected_size:
            raise RuntimeError(
                f"assembled backup size mismatch: expected={expected_size} "
                f"actual={assembled_size}"
            )
        assembled_sha256 = _sha256_file(assembled_backup)
        expected_sha256 = str(manifest_payload["backup_sha256"])
        if assembled_sha256 != expected_sha256:
            raise RuntimeError(
                "assembled backup checksum mismatch: "
                f"expected={expected_sha256} actual={assembled_sha256}"
            )

        restore_result = run_restore_drill(
            RunRestoreDrillCommand(
                backup_file=assembled_backup,
                target_db=command.target_db,
                drop_target_db=command.drop_target_db,
                triggered_by=command.triggered_by,
                recorded_at=recorded_at,
            ),
            backup_service=backup_service,
            metrics_recorder=metrics_recorder,
        )

    return RunBackupOffsiteRestoreDrillResult(
        status=BACKUP_OFFSITE_RESTORE_DRILL_STATUS_SUCCEEDED,
        triggered_by=command.triggered_by,
        recorded_at=recorded_at,
        backup_file=backup_file,
        manifest_file=manifest_file,
        offsite_url=command.offsite_url,
        offsite_root=command.offsite_root,
        target_db=restore_result.target_db,
        backup_size_bytes=expected_size,
        backup_sha256=expected_sha256,
        chunk_size_bytes=_payload_int(manifest_payload, "chunk_size_bytes"),
        downloaded_parts=tuple(downloaded_parts),
        archive_entry_count=restore_result.archive_entry_count,
        checked_tables=restore_result.checked_tables,
        verified_tables_count=restore_result.verified_tables_count,
        schema_verified=restore_result.schema_verified,
    )


def _download_and_verify_remote_manifest(
    *,
    remote_downloader: BackupOffsiteRemoteDownloader,
    local_manifest_file: Path,
    remote_manifest_relative_path: str,
    temp_root: Path,
) -> None:
    remote_manifest_file = temp_root / local_manifest_file.name
    remote_downloader.download_file(
        local_file=remote_manifest_file,
        remote_path=remote_manifest_relative_path,
    )
    if remote_manifest_file.read_bytes() != local_manifest_file.read_bytes():
        raise RuntimeError(
            f"remote manifest does not match local manifest: {remote_manifest_relative_path}"
        )


def _download_and_assemble_parts(
    *,
    remote_downloader: BackupOffsiteRemoteDownloader,
    parts: list[dict[str, object]],
    assembled_backup: Path,
) -> list[BackupOffsiteDownloadedPart]:
    downloaded_parts: list[BackupOffsiteDownloadedPart] = []
    with assembled_backup.open("wb") as assembled_handle:
        for expected_index, part in enumerate(parts, start=1):
            part_index = _payload_int(part, "index")
            if part_index != expected_index:
                raise RuntimeError(
                    f"backup manifest part index is not sequential: "
                    f"expected={expected_index} actual={part_index}"
                )
            remote_path = str(part["file"])
            expected_size = _payload_int(part, "size_bytes")
            expected_sha256 = str(part["sha256"])
            part_file = assembled_backup.parent / Path(remote_path).name
            log_event(
                LOGGER,
                logging.INFO,
                "run_backup_offsite_restore_drill.part_download.started",
                operation="run_backup_offsite_restore_drill",
                status="started",
                part_index=part_index,
                part_count=len(parts),
                part_size_bytes=expected_size,
                remote_path=remote_path,
            )
            remote_downloader.download_file(local_file=part_file, remote_path=remote_path)
            actual_size = part_file.stat().st_size
            if actual_size != expected_size:
                raise RuntimeError(
                    f"downloaded part size mismatch for {remote_path}: "
                    f"expected={expected_size} actual={actual_size}"
                )
            actual_sha256 = _sha256_file(part_file)
            if actual_sha256 != expected_sha256:
                raise RuntimeError(
                    f"downloaded part checksum mismatch for {remote_path}: "
                    f"expected={expected_sha256} actual={actual_sha256}"
                )
            with part_file.open("rb") as part_handle:
                for chunk in iter(lambda: part_handle.read(1024 * 1024), b""):
                    assembled_handle.write(chunk)
            part_file.unlink()
            downloaded_parts.append(
                BackupOffsiteDownloadedPart(
                    remote_path=remote_path,
                    size_bytes=actual_size,
                    sha256=actual_sha256,
                )
            )
            log_event(
                LOGGER,
                logging.INFO,
                "run_backup_offsite_restore_drill.part_download.succeeded",
                operation="run_backup_offsite_restore_drill",
                status="succeeded",
                part_index=part_index,
                part_count=len(parts),
                part_size_bytes=actual_size,
                remote_path=remote_path,
            )
    return downloaded_parts


def _manifest_parts(manifest_payload: dict[str, object]) -> list[dict[str, object]]:
    parts_payload = manifest_payload.get("parts", [])
    if not isinstance(parts_payload, list):
        raise RuntimeError("backup manifest parts must be a list")
    parts: list[dict[str, object]] = []
    for part_payload in parts_payload:
        if not isinstance(part_payload, dict):
            raise RuntimeError("backup manifest part must be an object")
        for key in ("index", "file", "size_bytes", "sha256"):
            if key not in part_payload:
                raise RuntimeError(f"backup manifest part must contain {key}")
        parts.append(part_payload)
    if not parts:
        raise RuntimeError("backup manifest must contain at least one part")
    return parts


def _payload_int(payload: dict[str, object], key: str) -> int:
    value = payload[key]
    if isinstance(value, bool):
        raise RuntimeError(f"backup manifest field must be an integer: {key}")
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        return int(value)
    raise RuntimeError(f"backup manifest field must be an integer: {key}")


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _normalize_offsite_root(offsite_root: str) -> str:
    parts = tuple(part for part in offsite_root.strip().split("/") if part)
    if not parts:
        return "/"
    return "/" + "/".join(parts)
