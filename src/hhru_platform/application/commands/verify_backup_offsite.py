from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol

from hhru_platform.infrastructure.backup.backup_offsite_verification_receipt_store import (
    BackupOffsiteVerificationReceipt,
)
from hhru_platform.infrastructure.observability.operations import (
    log_operation_started,
    record_operation_failed,
    record_operation_succeeded,
)

LOGGER = logging.getLogger(__name__)

BACKUP_OFFSITE_VERIFY_STATUS_SUCCEEDED = "succeeded"


@dataclass(slots=True, frozen=True)
class VerifyBackupOffsiteCommand:
    backup_file: Path
    backup_dir: Path = Path(".state/backups")
    offsite_url: str = ""
    offsite_root: str = "/hhru-platform/backups"
    triggered_by: str = "verify-backup-offsite"
    verified_at: datetime | None = None

    def __post_init__(self) -> None:
        normalized_triggered_by = self.triggered_by.strip()
        normalized_offsite_url = self.offsite_url.strip().rstrip("/")
        normalized_offsite_root = _normalize_offsite_root(self.offsite_root)
        if not normalized_triggered_by:
            raise ValueError("triggered_by must not be empty")
        if not normalized_offsite_url:
            raise ValueError("offsite_url must not be empty")
        object.__setattr__(self, "backup_file", Path(self.backup_file))
        object.__setattr__(self, "backup_dir", Path(self.backup_dir))
        object.__setattr__(self, "offsite_url", normalized_offsite_url)
        object.__setattr__(self, "offsite_root", normalized_offsite_root)
        object.__setattr__(self, "triggered_by", normalized_triggered_by)


@dataclass(slots=True, frozen=True)
class BackupOffsiteVerifiedObject:
    remote_path: str
    expected_size_bytes: int
    actual_size_bytes: int


@dataclass(slots=True, frozen=True)
class VerifyBackupOffsiteResult:
    status: str
    triggered_by: str
    backup_file: Path
    manifest_file: Path
    offsite_url: str
    offsite_root: str
    backup_size_bytes: int
    backup_sha256: str
    manifest_sha256: str
    chunk_size_bytes: int
    part_count: int
    verified_objects: tuple[BackupOffsiteVerifiedObject, ...]
    receipt_file: Path

    @property
    def verified_object_count(self) -> int:
        return len(self.verified_objects)


class BackupOffsiteRemoteStore(Protocol):
    def get_file_size(self, *, remote_path: str) -> int:
        """Return remote object size in bytes."""


class BackupOffsiteVerificationReceiptStore(Protocol):
    def write_receipt(
        self,
        *,
        backup_file: Path,
        receipt: BackupOffsiteVerificationReceipt,
    ) -> Path:
        """Persist proof that one offsite backup passed verification."""


def verify_backup_offsite(
    command: VerifyBackupOffsiteCommand,
    *,
    remote_store: BackupOffsiteRemoteStore,
    receipt_store: BackupOffsiteVerificationReceiptStore,
) -> VerifyBackupOffsiteResult:
    started_at = log_operation_started(
        LOGGER,
        operation="verify_backup_offsite",
        backup_file=str(command.backup_file),
        offsite_url=command.offsite_url,
        offsite_root=command.offsite_root,
        triggered_by=command.triggered_by,
    )
    try:
        result = _verify_backup_offsite(
            command=command,
            remote_store=remote_store,
            receipt_store=receipt_store,
        )
    except Exception as error:
        record_operation_failed(
            LOGGER,
            operation="verify_backup_offsite",
            started_at=started_at,
            error_type=error.__class__.__name__,
            error_message=str(error),
            backup_file=str(command.backup_file),
            offsite_url=command.offsite_url,
            offsite_root=command.offsite_root,
            triggered_by=command.triggered_by,
        )
        raise

    record_operation_succeeded(
        LOGGER,
        operation="verify_backup_offsite",
        started_at=started_at,
        backup_file=str(result.backup_file),
        offsite_url=result.offsite_url,
        offsite_root=result.offsite_root,
        backup_size_bytes=result.backup_size_bytes,
        chunk_size_bytes=result.chunk_size_bytes,
        part_count=result.part_count,
        verified_object_count=result.verified_object_count,
        triggered_by=result.triggered_by,
    )
    return result


def _verify_backup_offsite(
    *,
    command: VerifyBackupOffsiteCommand,
    remote_store: BackupOffsiteRemoteStore,
    receipt_store: BackupOffsiteVerificationReceiptStore,
) -> VerifyBackupOffsiteResult:
    backup_root = command.backup_dir.resolve()
    backup_file = command.backup_file.resolve()
    manifest_file = Path(f"{backup_file}.manifest.json")
    if not backup_file.exists():
        raise FileNotFoundError(f"backup file not found: {backup_file}")
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

    verified_objects = [
        _verify_remote_size(
            remote_store=remote_store,
            remote_relative_path=manifest_relative_path,
            expected_size_bytes=manifest_file.stat().st_size,
        )
    ]
    parts = _manifest_parts(manifest_payload)
    for part in parts:
        verified_objects.append(
            _verify_remote_size(
                remote_store=remote_store,
                remote_relative_path=str(part["file"]),
                expected_size_bytes=_payload_int(part, "size_bytes"),
            )
        )

    manifest_sha256 = _sha256_file(manifest_file)
    backup_size_bytes = _payload_int(manifest_payload, "backup_size_bytes")
    backup_sha256 = str(manifest_payload["backup_sha256"])
    chunk_size_bytes = _payload_int(manifest_payload, "chunk_size_bytes")
    remote_backup_path = _join_remote_path(
        command.offsite_root,
        f"{backup_relative_path}.parts",
    )
    remote_manifest_path = _join_remote_path(
        command.offsite_root,
        manifest_relative_path,
    )
    receipt_file = receipt_store.write_receipt(
        backup_file=backup_file,
        receipt=BackupOffsiteVerificationReceipt(
            verified_at=command.verified_at or datetime.now(UTC),
            offsite_url=command.offsite_url,
            offsite_root=command.offsite_root,
            backup_size_bytes=backup_size_bytes,
            backup_sha256=backup_sha256,
            manifest_sha256=manifest_sha256,
            chunk_size_bytes=chunk_size_bytes,
            part_count=len(parts),
            remote_backup_path=remote_backup_path,
            remote_manifest_path=remote_manifest_path,
            verified_object_count=len(verified_objects),
        ),
    )
    return VerifyBackupOffsiteResult(
        status=BACKUP_OFFSITE_VERIFY_STATUS_SUCCEEDED,
        triggered_by=command.triggered_by,
        backup_file=backup_file,
        manifest_file=manifest_file,
        offsite_url=command.offsite_url,
        offsite_root=command.offsite_root,
        backup_size_bytes=backup_size_bytes,
        backup_sha256=backup_sha256,
        manifest_sha256=manifest_sha256,
        chunk_size_bytes=chunk_size_bytes,
        part_count=len(parts),
        verified_objects=tuple(verified_objects),
        receipt_file=receipt_file,
    )


def _verify_remote_size(
    *,
    remote_store: BackupOffsiteRemoteStore,
    remote_relative_path: str,
    expected_size_bytes: int,
) -> BackupOffsiteVerifiedObject:
    actual_size_bytes = remote_store.get_file_size(remote_path=remote_relative_path)
    if actual_size_bytes != expected_size_bytes:
        raise RuntimeError(
            f"remote object size mismatch for {remote_relative_path}: "
            f"expected={expected_size_bytes} actual={actual_size_bytes}"
        )
    return BackupOffsiteVerifiedObject(
        remote_path=remote_relative_path,
        expected_size_bytes=expected_size_bytes,
        actual_size_bytes=actual_size_bytes,
    )


def _manifest_parts(manifest_payload: dict[str, object]) -> list[dict[str, object]]:
    parts_payload = manifest_payload.get("parts", [])
    if not isinstance(parts_payload, list):
        raise RuntimeError("backup manifest parts must be a list")
    parts: list[dict[str, object]] = []
    for part_payload in parts_payload:
        if not isinstance(part_payload, dict):
            raise RuntimeError("backup manifest part must be an object")
        if "file" not in part_payload or "size_bytes" not in part_payload:
            raise RuntimeError("backup manifest part must contain file and size_bytes")
        parts.append(part_payload)
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
