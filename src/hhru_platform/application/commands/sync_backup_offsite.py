from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol

from hhru_platform.infrastructure.backup.backup_offsite_receipt_store import (
    BackupOffsiteUploadReceipt,
)
from hhru_platform.infrastructure.observability.operations import (
    log_operation_started,
    record_operation_failed,
    record_operation_succeeded,
)

LOGGER = logging.getLogger(__name__)

BACKUP_OFFSITE_STATUS_SUCCEEDED = "succeeded"


@dataclass(slots=True, frozen=True)
class SyncBackupOffsiteCommand:
    backup_dir: Path = Path(".state/backups")
    offsite_url: str = ""
    offsite_root: str = "/hhru-platform/backups"
    username: str | None = None
    password: str | None = None
    bearer_token: str | None = None
    timeout_seconds: float = 60.0
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
    remote_backup_relative_path: str
    remote_manifest_relative_path: str
    remote_backup_path: str
    remote_manifest_path: str


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
    )
    existing_receipt = receipt_store.load_receipt(backup_file=bundle.backup_file)
    if _receipt_matches_bundle(existing_receipt, bundle=bundle, command=command):
        return BackupOffsiteSummary(
            backup_file=bundle.backup_file,
            manifest_file=bundle.manifest_file,
            remote_backup_path=bundle.remote_backup_path,
            remote_manifest_path=bundle.remote_manifest_path,
            backup_size_bytes=bundle.backup_size_bytes,
            backup_sha256=bundle.backup_sha256,
            manifest_sha256=bundle.manifest_sha256,
            uploaded=False,
            skipped=True,
            receipt_file=Path(f"{bundle.backup_file}.offsite.json"),
        )

    offsite_uploader.upload_file(
        local_file=bundle.backup_file,
        remote_path=bundle.remote_backup_relative_path,
    )
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
            remote_backup_path=bundle.remote_backup_path,
            remote_manifest_path=bundle.remote_manifest_path,
        ),
    )
    return BackupOffsiteSummary(
        backup_file=bundle.backup_file,
        manifest_file=bundle.manifest_file,
        remote_backup_path=bundle.remote_backup_path,
        remote_manifest_path=bundle.remote_manifest_path,
        backup_size_bytes=bundle.backup_size_bytes,
        backup_sha256=bundle.backup_sha256,
        manifest_sha256=bundle.manifest_sha256,
        uploaded=True,
        skipped=False,
        receipt_file=receipt_file,
    )


def _load_backup_bundle(
    *,
    backup_root: Path,
    backup_file: Path,
    offsite_root: str,
) -> _BackupBundle:
    resolved_backup_file = backup_file.resolve()
    backup_relative_path = resolved_backup_file.relative_to(backup_root).as_posix()
    backup_size_bytes = resolved_backup_file.stat().st_size
    backup_sha256 = _sha256_file(resolved_backup_file)
    manifest_file = Path(f"{resolved_backup_file}.manifest.json")
    _write_manifest_if_changed(
        manifest_file=manifest_file,
        payload={
            "manifest_version": 1,
            "backup_file": backup_relative_path,
            "backup_size_bytes": backup_size_bytes,
            "backup_sha256": backup_sha256,
        },
    )
    manifest_relative_path = manifest_file.relative_to(backup_root).as_posix()
    return _BackupBundle(
        backup_file=resolved_backup_file,
        manifest_file=manifest_file,
        backup_size_bytes=backup_size_bytes,
        backup_sha256=backup_sha256,
        manifest_sha256=_sha256_file(manifest_file),
        remote_backup_relative_path=backup_relative_path,
        remote_manifest_relative_path=manifest_relative_path,
        remote_backup_path=_join_remote_path(offsite_root, backup_relative_path),
        remote_manifest_path=_join_remote_path(offsite_root, manifest_relative_path),
    )


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
        and receipt.remote_backup_path == bundle.remote_backup_path
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
