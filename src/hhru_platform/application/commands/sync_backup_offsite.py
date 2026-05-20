from __future__ import annotations

import hashlib
import json
import logging
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol

from hhru_platform.infrastructure.backup.backup_offsite_receipt_store import (
    BackupOffsiteUploadReceipt,
)
from hhru_platform.infrastructure.observability.logging import log_event
from hhru_platform.infrastructure.observability.operations import (
    log_operation_started,
    record_operation_failed,
    record_operation_succeeded,
)

LOGGER = logging.getLogger(__name__)

BACKUP_OFFSITE_STATUS_SUCCEEDED = "succeeded"
DEFAULT_BACKUP_OFFSITE_CHUNK_SIZE_BYTES = 64 * 1024 * 1024


@dataclass(slots=True, frozen=True)
class SyncBackupOffsiteCommand:
    backup_dir: Path = Path(".state/backups")
    offsite_url: str = ""
    offsite_root: str = "/hhru-platform/backups"
    username: str | None = None
    password: str | None = None
    bearer_token: str | None = None
    timeout_seconds: float = 60.0
    chunk_size_bytes: int = DEFAULT_BACKUP_OFFSITE_CHUNK_SIZE_BYTES
    limit: int | None = 1
    triggered_by: str = "sync-backup-offsite"
    synced_at: datetime | None = None

    def __post_init__(self) -> None:
        normalized_triggered_by = self.triggered_by.strip()
        normalized_offsite_url = self.offsite_url.strip().rstrip("/")
        normalized_offsite_root = _normalize_offsite_root(self.offsite_root)
        normalized_username = self.username.strip() if self.username is not None else None
        normalized_password = self.password.strip() if self.password is not None else None
        normalized_bearer_token = (
            self.bearer_token.strip() if self.bearer_token is not None else None
        )
        if not normalized_triggered_by:
            raise ValueError("triggered_by must not be empty")
        if not normalized_offsite_url:
            raise ValueError("offsite_url must not be empty")
        if self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be greater than zero")
        if self.chunk_size_bytes < 1:
            raise ValueError("chunk_size_bytes must be greater than or equal to one")
        if self.limit is not None and self.limit < 1:
            raise ValueError("limit must be greater than or equal to one")

        basic_auth = bool(normalized_username and normalized_password)
        bearer_auth = bool(normalized_bearer_token)
        if basic_auth == bearer_auth:
            raise ValueError(
                "configure either username/password basic auth or bearer_token auth"
            )

        object.__setattr__(self, "triggered_by", normalized_triggered_by)
        object.__setattr__(self, "offsite_url", normalized_offsite_url)
        object.__setattr__(self, "offsite_root", normalized_offsite_root)
        object.__setattr__(self, "username", normalized_username)
        object.__setattr__(self, "password", normalized_password)
        object.__setattr__(self, "bearer_token", normalized_bearer_token)
        object.__setattr__(self, "backup_dir", Path(self.backup_dir))

    @property
    def auth_mode(self) -> str:
        if self.bearer_token:
            return "bearer"
        return "basic"


@dataclass(slots=True, frozen=True)
class BackupOffsiteSummary:
    backup_file: Path
    manifest_file: Path
    remote_backup_path: str
    remote_manifest_path: str
    backup_size_bytes: int
    backup_sha256: str
    manifest_sha256: str
    chunk_size_bytes: int
    part_count: int
    uploaded: bool
    skipped: bool
    receipt_file: Path | None


@dataclass(slots=True, frozen=True)
class SyncBackupOffsiteResult:
    status: str
    triggered_by: str
    synced_at: datetime
    backup_dir: Path
    offsite_url: str
    offsite_root: str
    auth_mode: str
    limit: int | None
    summaries: tuple[BackupOffsiteSummary, ...]

    @property
    def scanned_backup_count(self) -> int:
        return len(self.summaries)

    @property
    def uploaded_backup_count(self) -> int:
        return sum(1 for summary in self.summaries if summary.uploaded)

    @property
    def skipped_backup_count(self) -> int:
        return sum(1 for summary in self.summaries if summary.skipped)

    @property
    def candidate_backup_count(self) -> int:
        return self.scanned_backup_count - self.skipped_backup_count


class BackupOffsiteUploader(Protocol):
    def upload_file(self, *, local_file: Path, remote_path: str) -> None:
        """Upload one local backup artifact to off-host storage."""


class BackupOffsiteUploadReceiptStore(Protocol):
    def load_receipt(self, *, backup_file: Path) -> BackupOffsiteUploadReceipt | None:
        """Load the local upload receipt for one backup dump if it exists."""

    def write_receipt(
        self,
        *,
        backup_file: Path,
        receipt: BackupOffsiteUploadReceipt,
    ) -> Path:
        """Persist the local upload receipt for one backup dump."""


def sync_backup_offsite(
    command: SyncBackupOffsiteCommand,
    *,
    offsite_uploader: BackupOffsiteUploader,
    receipt_store: BackupOffsiteUploadReceiptStore,
) -> SyncBackupOffsiteResult:
    started_at = log_operation_started(
        LOGGER,
        operation="sync_backup_offsite",
        backup_dir=str(command.backup_dir),
        offsite_url=command.offsite_url,
        offsite_root=command.offsite_root,
        auth_mode=command.auth_mode,
        chunk_size_bytes=command.chunk_size_bytes,
        limit=command.limit or 0,
        triggered_by=command.triggered_by,
    )
    synced_at = command.synced_at or datetime.now(UTC)

    try:
        backup_root = command.backup_dir.resolve()
        backup_files = _list_backup_files(backup_root=backup_root, limit=command.limit)
        summaries = tuple(
            _sync_one_backup(
                command=command,
                backup_root=backup_root,
                backup_file=backup_file,
                synced_at=synced_at,
                offsite_uploader=offsite_uploader,
                receipt_store=receipt_store,
            )
            for backup_file in backup_files
        )
    except Exception as error:
        record_operation_failed(
            LOGGER,
            operation="sync_backup_offsite",
            started_at=started_at,
            error_type=error.__class__.__name__,
            error_message=str(error),
            backup_dir=str(command.backup_dir),
            offsite_url=command.offsite_url,
            offsite_root=command.offsite_root,
            chunk_size_bytes=command.chunk_size_bytes,
            triggered_by=command.triggered_by,
        )
        raise

    result = SyncBackupOffsiteResult(
        status=BACKUP_OFFSITE_STATUS_SUCCEEDED,
        triggered_by=command.triggered_by,
        synced_at=synced_at,
        backup_dir=backup_root,
        offsite_url=command.offsite_url,
        offsite_root=command.offsite_root,
        auth_mode=command.auth_mode,
        limit=command.limit,
        summaries=summaries,
    )
    record_operation_succeeded(
        LOGGER,
        operation="sync_backup_offsite",
        started_at=started_at,
        backup_dir=str(result.backup_dir),
        offsite_url=result.offsite_url,
        offsite_root=result.offsite_root,
        auth_mode=result.auth_mode,
        chunk_size_bytes=command.chunk_size_bytes,
        triggered_by=result.triggered_by,
        scanned_backup_count=result.scanned_backup_count,
        candidate_backup_count=result.candidate_backup_count,
        uploaded_backup_count=result.uploaded_backup_count,
        skipped_backup_count=result.skipped_backup_count,
    )
    return result


@dataclass(slots=True, frozen=True)
class _BackupBundle:
    backup_file: Path
    manifest_file: Path
    backup_size_bytes: int
    backup_sha256: str
    manifest_sha256: str
    chunk_size_bytes: int
    parts: tuple[_BackupPart, ...]
    remote_manifest_relative_path: str
    remote_parts_path: str
    remote_manifest_path: str


@dataclass(slots=True, frozen=True)
class _BackupPart:
    index: int
    relative_path: str
    remote_relative_path: str
    remote_path: str
    size_bytes: int
    sha256: str


def _list_backup_files(*, backup_root: Path, limit: int | None) -> list[Path]:
    if not backup_root.exists():
        return []
    backup_files = sorted(
        (path.resolve() for path in backup_root.rglob("*.dump") if path.is_file()),
        key=lambda path: (path.stat().st_mtime, path.name),
        reverse=True,
    )
    if limit is not None:
        return backup_files[:limit]
    return backup_files


def _sync_one_backup(
    *,
    command: SyncBackupOffsiteCommand,
    backup_root: Path,
    backup_file: Path,
    synced_at: datetime,
    offsite_uploader: BackupOffsiteUploader,
    receipt_store: BackupOffsiteUploadReceiptStore,
) -> BackupOffsiteSummary:
    bundle = _load_backup_bundle(
        backup_root=backup_root,
        backup_file=backup_file,
        offsite_root=command.offsite_root,
        chunk_size_bytes=command.chunk_size_bytes,
    )
    existing_receipt = receipt_store.load_receipt(backup_file=bundle.backup_file)
    if _receipt_matches_bundle(existing_receipt, bundle=bundle, command=command):
        return BackupOffsiteSummary(
            backup_file=bundle.backup_file,
            manifest_file=bundle.manifest_file,
            remote_backup_path=bundle.remote_parts_path,
            remote_manifest_path=bundle.remote_manifest_path,
            backup_size_bytes=bundle.backup_size_bytes,
            backup_sha256=bundle.backup_sha256,
            manifest_sha256=bundle.manifest_sha256,
            chunk_size_bytes=bundle.chunk_size_bytes,
            part_count=len(bundle.parts),
            uploaded=False,
            skipped=True,
            receipt_file=Path(f"{bundle.backup_file}.offsite.json"),
        )

    _upload_backup_parts(bundle=bundle, offsite_uploader=offsite_uploader)
    offsite_uploader.upload_file(
        local_file=bundle.manifest_file,
        remote_path=bundle.remote_manifest_relative_path,
    )
    receipt_file = receipt_store.write_receipt(
        backup_file=bundle.backup_file,
        receipt=BackupOffsiteUploadReceipt(
            uploaded_at=synced_at,
            offsite_url=command.offsite_url,
            offsite_root=command.offsite_root,
            backup_size_bytes=bundle.backup_size_bytes,
            backup_sha256=bundle.backup_sha256,
            manifest_sha256=bundle.manifest_sha256,
            chunk_size_bytes=bundle.chunk_size_bytes,
            part_count=len(bundle.parts),
            remote_backup_path=bundle.remote_parts_path,
            remote_manifest_path=bundle.remote_manifest_path,
        ),
    )
    return BackupOffsiteSummary(
        backup_file=bundle.backup_file,
        manifest_file=bundle.manifest_file,
        remote_backup_path=bundle.remote_parts_path,
        remote_manifest_path=bundle.remote_manifest_path,
        backup_size_bytes=bundle.backup_size_bytes,
        backup_sha256=bundle.backup_sha256,
        manifest_sha256=bundle.manifest_sha256,
        chunk_size_bytes=bundle.chunk_size_bytes,
        part_count=len(bundle.parts),
        uploaded=True,
        skipped=False,
        receipt_file=receipt_file,
    )


def _load_backup_bundle(
    *,
    backup_root: Path,
    backup_file: Path,
    offsite_root: str,
    chunk_size_bytes: int,
) -> _BackupBundle:
    resolved_backup_file = backup_file.resolve()
    backup_relative_path = resolved_backup_file.relative_to(backup_root).as_posix()
    backup_size_bytes = resolved_backup_file.stat().st_size
    backup_sha256, parts = _build_backup_parts(
        backup_file=resolved_backup_file,
        backup_relative_path=backup_relative_path,
        offsite_root=offsite_root,
        chunk_size_bytes=chunk_size_bytes,
    )
    manifest_file = Path(f"{resolved_backup_file}.manifest.json")
    _write_manifest_if_changed(
        manifest_file=manifest_file,
        payload={
            "manifest_version": 2,
            "upload_mode": "parts",
            "backup_file": backup_relative_path,
            "backup_size_bytes": backup_size_bytes,
            "backup_sha256": backup_sha256,
            "chunk_size_bytes": chunk_size_bytes,
            "parts": [
                {
                    "index": part.index,
                    "file": part.relative_path,
                    "size_bytes": part.size_bytes,
                    "sha256": part.sha256,
                }
                for part in parts
            ],
        },
    )
    manifest_relative_path = manifest_file.relative_to(backup_root).as_posix()
    return _BackupBundle(
        backup_file=resolved_backup_file,
        manifest_file=manifest_file,
        backup_size_bytes=backup_size_bytes,
        backup_sha256=backup_sha256,
        chunk_size_bytes=chunk_size_bytes,
        parts=parts,
        manifest_sha256=_sha256_file(manifest_file),
        remote_manifest_relative_path=manifest_relative_path,
        remote_parts_path=_join_remote_path(offsite_root, f"{backup_relative_path}.parts"),
        remote_manifest_path=_join_remote_path(offsite_root, manifest_relative_path),
    )


def _build_backup_parts(
    *,
    backup_file: Path,
    backup_relative_path: str,
    offsite_root: str,
    chunk_size_bytes: int,
) -> tuple[str, tuple[_BackupPart, ...]]:
    backup_digest = hashlib.sha256()
    parts: list[_BackupPart] = []
    with backup_file.open("rb") as handle:
        index = 1
        while chunk := handle.read(chunk_size_bytes):
            backup_digest.update(chunk)
            part_relative_path = f"{backup_relative_path}.parts/{index:06d}.part"
            part_sha256 = hashlib.sha256(chunk).hexdigest()
            parts.append(
                _BackupPart(
                    index=index,
                    relative_path=part_relative_path,
                    remote_relative_path=part_relative_path,
                    remote_path=_join_remote_path(offsite_root, part_relative_path),
                    size_bytes=len(chunk),
                    sha256=part_sha256,
                )
            )
            index += 1
    return backup_digest.hexdigest(), tuple(parts)


def _upload_backup_parts(
    *,
    bundle: _BackupBundle,
    offsite_uploader: BackupOffsiteUploader,
) -> None:
    with tempfile.TemporaryDirectory(prefix="hhru-backup-offsite-") as temporary_dir:
        temporary_root = Path(temporary_dir)
        part_count = len(bundle.parts)
        with bundle.backup_file.open("rb") as backup_handle:
            for part in bundle.parts:
                log_event(
                    LOGGER,
                    logging.INFO,
                    "sync_backup_offsite.part_upload.started",
                    operation="sync_backup_offsite",
                    status="started",
                    backup_file=bundle.backup_file,
                    part_index=part.index,
                    part_count=part_count,
                    part_size_bytes=part.size_bytes,
                    remote_path=part.remote_path,
                )
                part_file = temporary_root / Path(part.relative_path).name
                with part_file.open("wb") as part_handle:
                    remaining_bytes = part.size_bytes
                    while remaining_bytes > 0:
                        chunk = backup_handle.read(min(1024 * 1024, remaining_bytes))
                        if not chunk:
                            raise RuntimeError(
                                f"backup file ended before writing part {part.relative_path}"
                            )
                        part_handle.write(chunk)
                        remaining_bytes -= len(chunk)
                part_sha256 = _sha256_file(part_file)
                if part_sha256 != part.sha256:
                    raise RuntimeError(
                        f"backup part checksum mismatch for {part.relative_path}: "
                        f"expected={part.sha256} actual={part_sha256}"
                    )
                offsite_uploader.upload_file(
                    local_file=part_file,
                    remote_path=part.remote_relative_path,
                )
                log_event(
                    LOGGER,
                    logging.INFO,
                    "sync_backup_offsite.part_upload.succeeded",
                    operation="sync_backup_offsite",
                    status="succeeded",
                    backup_file=bundle.backup_file,
                    part_index=part.index,
                    part_count=part_count,
                    part_size_bytes=part.size_bytes,
                    remote_path=part.remote_path,
                )
                part_file.unlink()


def _write_manifest_if_changed(*, manifest_file: Path, payload: dict[str, object]) -> None:
    manifest_text = (
        json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n"
    )
    if manifest_file.exists() and manifest_file.read_text(encoding="utf-8") == manifest_text:
        return
    manifest_file.write_text(manifest_text, encoding="utf-8")


def _receipt_matches_bundle(
    receipt: BackupOffsiteUploadReceipt | None,
    *,
    bundle: _BackupBundle,
    command: SyncBackupOffsiteCommand,
) -> bool:
    if receipt is None:
        return False
    return (
        receipt.offsite_url == command.offsite_url
        and receipt.offsite_root == command.offsite_root
        and receipt.backup_size_bytes == bundle.backup_size_bytes
        and receipt.backup_sha256 == bundle.backup_sha256
        and receipt.manifest_sha256 == bundle.manifest_sha256
        and receipt.chunk_size_bytes == bundle.chunk_size_bytes
        and receipt.part_count == len(bundle.parts)
        and receipt.remote_backup_path == bundle.remote_parts_path
        and receipt.remote_manifest_path == bundle.remote_manifest_path
    )


def _normalize_offsite_root(offsite_root: str) -> str:
    parts = tuple(part for part in offsite_root.strip().split("/") if part)
    if not parts:
        return "/"
    return "/" + "/".join(parts)


def _join_remote_path(offsite_root: str, relative_path: str) -> str:
    normalized_root = _normalize_offsite_root(offsite_root).strip("/")
    if not normalized_root:
        return "/" + relative_path.strip("/")
    return "/" + normalized_root + "/" + relative_path.strip("/")


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
