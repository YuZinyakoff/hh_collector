from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol

from hhru_platform.infrastructure.housekeeping import RetentionArchiveUploadReceipt
from hhru_platform.infrastructure.observability.operations import (
    log_operation_started,
    record_operation_failed,
    record_operation_succeeded,
)

LOGGER = logging.getLogger(__name__)

RETENTION_ARCHIVE_OFFSITE_STATUS_SUCCEEDED = "succeeded"


@dataclass(slots=True, frozen=True)
class SyncRetentionArchiveOffsiteCommand:
    archive_dir: Path = Path(".state/archive/retention")
    offsite_url: str = ""
    offsite_root: str = "/hhru-platform"
    username: str | None = None
    password: str | None = None
    bearer_token: str | None = None
    timeout_seconds: float = 60.0
    limit: int | None = None
    triggered_by: str = "sync-retention-archive-offsite"
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
        object.__setattr__(self, "archive_dir", Path(self.archive_dir))

    @property
    def auth_mode(self) -> str:
        if self.bearer_token:
            return "bearer"
        return "basic"


@dataclass(slots=True, frozen=True)
class RetentionArchiveOffsiteSummary:
    manifest_file: Path
    archive_file: Path
    remote_manifest_path: str
    remote_archive_path: str
    manifest_sha256: str
    archive_sha256: str
    uploaded: bool
    skipped: bool
    receipt_file: Path | None


@dataclass(slots=True, frozen=True)
class SyncRetentionArchiveOffsiteResult:
    status: str
    triggered_by: str
    synced_at: datetime
    archive_dir: Path
    offsite_url: str
    offsite_root: str
    auth_mode: str
    limit: int | None
    summaries: tuple[RetentionArchiveOffsiteSummary, ...]

    @property
    def scanned_manifest_count(self) -> int:
        return len(self.summaries)

    @property
    def uploaded_bundle_count(self) -> int:
        return sum(1 for summary in self.summaries if summary.uploaded)

    @property
    def skipped_bundle_count(self) -> int:
        return sum(1 for summary in self.summaries if summary.skipped)

    @property
    def candidate_bundle_count(self) -> int:
        return self.scanned_manifest_count - self.skipped_bundle_count


class RetentionArchiveOffsiteUploader(Protocol):
    def upload_file(self, *, local_file: Path, remote_path: str) -> None:
        """Upload one file to off-host storage under the configured remote root."""


class RetentionArchiveUploadReceiptStore(Protocol):
    def load_receipt(self, *, manifest_file: Path) -> RetentionArchiveUploadReceipt | None:
        """Load the local upload receipt for one manifest if it exists."""

    def write_receipt(
        self,
        *,
        manifest_file: Path,
        receipt: RetentionArchiveUploadReceipt,
    ) -> Path:
        """Persist the local upload receipt for one manifest."""


def sync_retention_archive_offsite(
    command: SyncRetentionArchiveOffsiteCommand,
    *,
    offsite_uploader: RetentionArchiveOffsiteUploader,
    receipt_store: RetentionArchiveUploadReceiptStore,
) -> SyncRetentionArchiveOffsiteResult:
    started_at = log_operation_started(
        LOGGER,
        operation="sync_retention_archive_offsite",
        archive_dir=str(command.archive_dir),
        offsite_url=command.offsite_url,
        offsite_root=command.offsite_root,
        auth_mode=command.auth_mode,
        limit=command.limit or 0,
        triggered_by=command.triggered_by,
    )
    synced_at = command.synced_at or datetime.now(UTC)

    try:
        archive_root = command.archive_dir.resolve()
        manifest_files = sorted(archive_root.rglob("*.manifest.json"))
        if command.limit is not None:
            manifest_files = manifest_files[: command.limit]
        summaries = tuple(
            _sync_one_bundle(
                command=command,
                archive_root=archive_root,
                manifest_file=manifest_file,
                synced_at=synced_at,
                offsite_uploader=offsite_uploader,
                receipt_store=receipt_store,
            )
            for manifest_file in manifest_files
        )
    except Exception as error:
        record_operation_failed(
            LOGGER,
            operation="sync_retention_archive_offsite",
            started_at=started_at,
            error_type=error.__class__.__name__,
            error_message=str(error),
            archive_dir=str(command.archive_dir),
            offsite_url=command.offsite_url,
            offsite_root=command.offsite_root,
            triggered_by=command.triggered_by,
        )
        raise

    result = SyncRetentionArchiveOffsiteResult(
        status=RETENTION_ARCHIVE_OFFSITE_STATUS_SUCCEEDED,
        triggered_by=command.triggered_by,
        synced_at=synced_at,
        archive_dir=archive_root,
        offsite_url=command.offsite_url,
        offsite_root=command.offsite_root,
        auth_mode=command.auth_mode,
        limit=command.limit,
        summaries=summaries,
    )
    record_operation_succeeded(
        LOGGER,
        operation="sync_retention_archive_offsite",
        started_at=started_at,
        archive_dir=str(result.archive_dir),
        offsite_url=result.offsite_url,
        offsite_root=result.offsite_root,
        auth_mode=result.auth_mode,
        triggered_by=result.triggered_by,
        scanned_manifest_count=result.scanned_manifest_count,
        candidate_bundle_count=result.candidate_bundle_count,
        uploaded_bundle_count=result.uploaded_bundle_count,
        skipped_bundle_count=result.skipped_bundle_count,
    )
    return result


@dataclass(slots=True, frozen=True)
class _ArchiveBundle:
    manifest_file: Path
    archive_file: Path
    manifest_sha256: str
    archive_sha256: str
    remote_manifest_relative_path: str
    remote_archive_relative_path: str
    remote_manifest_path: str
    remote_archive_path: str


def _sync_one_bundle(
    *,
    command: SyncRetentionArchiveOffsiteCommand,
    archive_root: Path,
    manifest_file: Path,
    synced_at: datetime,
    offsite_uploader: RetentionArchiveOffsiteUploader,
    receipt_store: RetentionArchiveUploadReceiptStore,
) -> RetentionArchiveOffsiteSummary:
    bundle = _load_bundle(
        archive_root=archive_root,
        manifest_file=manifest_file,
        offsite_root=command.offsite_root,
    )
    existing_receipt = receipt_store.load_receipt(manifest_file=bundle.manifest_file)
    if _receipt_matches_bundle(existing_receipt, bundle=bundle, command=command):
        return RetentionArchiveOffsiteSummary(
            manifest_file=bundle.manifest_file,
            archive_file=bundle.archive_file,
            remote_manifest_path=bundle.remote_manifest_path,
            remote_archive_path=bundle.remote_archive_path,
            manifest_sha256=bundle.manifest_sha256,
            archive_sha256=bundle.archive_sha256,
            uploaded=False,
            skipped=True,
            receipt_file=Path(f"{bundle.manifest_file}.uploaded.json"),
        )

    offsite_uploader.upload_file(
        local_file=bundle.archive_file,
        remote_path=bundle.remote_archive_relative_path,
    )
    offsite_uploader.upload_file(
        local_file=bundle.manifest_file,
        remote_path=bundle.remote_manifest_relative_path,
    )
    receipt_file = receipt_store.write_receipt(
        manifest_file=bundle.manifest_file,
        receipt=RetentionArchiveUploadReceipt(
            uploaded_at=synced_at,
            offsite_url=command.offsite_url,
            offsite_root=command.offsite_root,
            manifest_sha256=bundle.manifest_sha256,
            archive_sha256=bundle.archive_sha256,
            remote_archive_path=bundle.remote_archive_path,
            remote_manifest_path=bundle.remote_manifest_path,
        ),
    )
    return RetentionArchiveOffsiteSummary(
        manifest_file=bundle.manifest_file,
        archive_file=bundle.archive_file,
        remote_manifest_path=bundle.remote_manifest_path,
        remote_archive_path=bundle.remote_archive_path,
        manifest_sha256=bundle.manifest_sha256,
        archive_sha256=bundle.archive_sha256,
        uploaded=True,
        skipped=False,
        receipt_file=receipt_file,
    )


def _load_bundle(
    *,
    archive_root: Path,
    manifest_file: Path,
    offsite_root: str,
) -> _ArchiveBundle:
    manifest_payload = json.loads(manifest_file.read_text(encoding="utf-8"))
    archive_file = _resolve_archive_file(
        manifest_file=manifest_file,
        archive_file_value=str(manifest_payload["archive_file"]),
    )
    if not archive_file.exists():
        raise FileNotFoundError(
            f"archive file not found for manifest {manifest_file}: {archive_file}"
        )
    archive_relative_path = archive_file.resolve().relative_to(archive_root).as_posix()
    manifest_relative_path = manifest_file.resolve().relative_to(archive_root).as_posix()
    archive_sha256 = str(
        manifest_payload.get("archive_sha256") or _sha256_file(archive_file)
    )
    return _ArchiveBundle(
        manifest_file=manifest_file.resolve(),
        archive_file=archive_file.resolve(),
        manifest_sha256=_sha256_file(manifest_file),
        archive_sha256=archive_sha256,
        remote_manifest_relative_path=manifest_relative_path,
        remote_archive_relative_path=archive_relative_path,
        remote_manifest_path=_join_remote_path(offsite_root, manifest_relative_path),
        remote_archive_path=_join_remote_path(offsite_root, archive_relative_path),
    )


def _resolve_archive_file(*, manifest_file: Path, archive_file_value: str) -> Path:
    archive_file = Path(archive_file_value)
    if archive_file.is_absolute():
        return archive_file
    if archive_file.exists():
        return archive_file.resolve()
    inferred_archive_file = Path(str(manifest_file).replace(".manifest.json", ".jsonl.gz"))
    if inferred_archive_file.exists():
        return inferred_archive_file.resolve()
    return (Path.cwd() / archive_file).resolve()


def _receipt_matches_bundle(
    receipt: RetentionArchiveUploadReceipt | None,
    *,
    bundle: _ArchiveBundle,
    command: SyncRetentionArchiveOffsiteCommand,
) -> bool:
    if receipt is None:
        return False
    return (
        receipt.offsite_url == command.offsite_url
        and receipt.offsite_root == command.offsite_root
        and receipt.manifest_sha256 == bundle.manifest_sha256
        and receipt.archive_sha256 == bundle.archive_sha256
        and receipt.remote_archive_path == bundle.remote_archive_path
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
