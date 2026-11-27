from __future__ import annotations

import hashlib
import json
import logging
import re
import tempfile
import time
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import IO, TypedDict
from uuid import uuid4

from hhru_platform.application.commands.sync_backup_offsite import (
    BACKUP_OFFSITE_PART_UPLOAD_ATTEMPTS,
    BACKUP_OFFSITE_PART_UPLOAD_RETRY_BACKOFF_SECONDS,
    BackupOffsiteSummary,
    BackupOffsiteUploader,
    BackupOffsiteUploadReceiptStore,
)
from hhru_platform.infrastructure.backup.backup_offsite_receipt_store import (
    BackupOffsiteUploadReceipt,
)
from hhru_platform.infrastructure.backup.backup_service import BackupService
from hhru_platform.infrastructure.observability.logging import log_event
from hhru_platform.infrastructure.observability.operations import (
    log_operation_started,
    record_operation_failed,
    record_operation_succeeded,
)

LOGGER = logging.getLogger(__name__)
OPERATION = "stream_backup_offsite"
MAX_STREAM_CHUNK_BYTES = 64 * 1024**2
MAX_STREAM_PARTS = 16_384


class _StreamLogFields(TypedDict):
    backup_file: str
    offsite_root: str
    triggered_by: str


@dataclass(slots=True, frozen=True)
class StreamBackupOffsiteCommand:
    backup_dir: Path
    backup_name_prefix: str
    offsite_url: str
    offsite_root: str
    chunk_size_bytes: int
    max_backup_bytes: int
    triggered_by: str

    def __post_init__(self) -> None:
        if not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.-]*", self.backup_name_prefix):
            raise ValueError("invalid streaming backup name prefix")
        if not 1 <= self.chunk_size_bytes <= MAX_STREAM_CHUNK_BYTES:
            raise ValueError("streaming chunk size must be between 1 byte and 64 MiB")
        if not 1 <= self.max_backup_bytes <= self.chunk_size_bytes * MAX_STREAM_PARTS:
            raise ValueError(f"streaming byte limit must fit in at most {MAX_STREAM_PARTS} parts")
        if not self.offsite_root.endswith("-streaming-pilot"):
            raise ValueError("streaming backups require a dedicated pilot prefix")


def stream_backup_offsite(
    command: StreamBackupOffsiteCommand,
    *,
    backup_service: BackupService,
    offsite_uploader: BackupOffsiteUploader,
    receipt_store: BackupOffsiteUploadReceiptStore,
) -> BackupOffsiteSummary:
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    backup_file = command.backup_dir.resolve() / (
        f"{command.backup_name_prefix}_stream_{uuid4().hex}_{stamp}.dump"
    )
    fields: _StreamLogFields = {
        "backup_file": str(backup_file),
        "offsite_root": command.offsite_root,
        "triggered_by": command.triggered_by,
    }
    started_at = log_operation_started(
        LOGGER, operation=OPERATION, max_backup_bytes=command.max_backup_bytes, **fields
    )
    try:
        result = _stream_one_backup(
            command=command,
            backup_file=backup_file,
            backup_service=backup_service,
            offsite_uploader=offsite_uploader,
            receipt_store=receipt_store,
        )
    except Exception as error:
        record_operation_failed(
            LOGGER,
            operation=OPERATION,
            started_at=started_at,
            error_type=type(error).__name__,
            error_message=str(error),
            **fields,
        )
        raise
    record_operation_succeeded(
        LOGGER,
        operation=OPERATION,
        started_at=started_at,
        backup_size_bytes=result.backup_size_bytes,
        part_count=result.part_count,
        **fields,
    )
    return result


def _stream_one_backup(
    *,
    command: StreamBackupOffsiteCommand,
    backup_file: Path,
    backup_service: BackupService,
    offsite_uploader: BackupOffsiteUploader,
    receipt_store: BackupOffsiteUploadReceiptStore,
) -> BackupOffsiteSummary:
    backup_file.parent.mkdir(parents=True, exist_ok=True)
    manifest_file = backup_file.with_suffix(".dump.manifest.json")
    parts_path = f"{backup_file.name}.parts"
    parts: list[dict[str, int | str]] = []

    with tempfile.TemporaryDirectory(dir=backup_file.parent, prefix=".stream-part-") as scratch_dir:
        scratch = Path(scratch_dir)
        with backup_service.open_backup_stream() as source:
            archive = backup_service.inspect_backup_stream(
                backup_file,
                _upload_parts(
                    command=command,
                    parts_path=parts_path,
                    source=source,
                    part_file=scratch / "upload.part",
                    offsite_uploader=offsite_uploader,
                    parts=parts,
                ),
            )

        # The manifest must not be published until both archive inspection and pg_dump succeed.
        if not parts or archive.size_bytes <= 0:
            raise RuntimeError("pg_dump returned an empty archive")
        manifest = {
            "manifest_version": 2,
            "upload_mode": "parts",
            "backup_file": backup_file.name,
            "backup_size_bytes": archive.size_bytes,
            "backup_sha256": archive.sha256,
            "chunk_size_bytes": command.chunk_size_bytes,
            "parts": parts,
        }
        payload = (json.dumps(manifest, ensure_ascii=True, indent=2, sort_keys=True) + "\n").encode(
            "utf-8"
        )
        pending_manifest = scratch / manifest_file.name
        pending_manifest.write_bytes(payload)
        _upload_with_retries(offsite_uploader, pending_manifest, manifest_file.name)
        pending_manifest.replace(manifest_file)

    receipt = BackupOffsiteUploadReceipt(
        uploaded_at=datetime.now(UTC),
        offsite_url=command.offsite_url,
        offsite_root=command.offsite_root,
        backup_size_bytes=archive.size_bytes,
        backup_sha256=archive.sha256,
        manifest_sha256=hashlib.sha256(payload).hexdigest(),
        chunk_size_bytes=command.chunk_size_bytes,
        part_count=len(parts),
        remote_backup_path=f"{command.offsite_root}/{parts_path}",
        remote_manifest_path=f"{command.offsite_root}/{manifest_file.name}",
    )
    receipt_file = receipt_store.write_receipt(backup_file=backup_file, receipt=receipt)
    return BackupOffsiteSummary(
        backup_file=backup_file,
        manifest_file=manifest_file,
        remote_backup_path=receipt.remote_backup_path,
        remote_manifest_path=receipt.remote_manifest_path,
        backup_size_bytes=receipt.backup_size_bytes,
        backup_sha256=receipt.backup_sha256,
        manifest_sha256=receipt.manifest_sha256,
        chunk_size_bytes=receipt.chunk_size_bytes,
        part_count=receipt.part_count,
        uploaded=True,
        skipped=False,
        receipt_file=receipt_file,
    )


def _upload_parts(
    *,
    command: StreamBackupOffsiteCommand,
    parts_path: str,
    source: IO[bytes],
    part_file: Path,
    offsite_uploader: BackupOffsiteUploader,
    parts: list[dict[str, int | str]],
) -> Iterator[bytes]:
    remaining = command.max_backup_bytes
    while chunk := source.read(min(command.chunk_size_bytes, remaining + 1)):
        if len(chunk) > remaining:
            raise RuntimeError(f"streaming backup exceeds byte limit: {command.max_backup_bytes}")
        index = len(parts) + 1
        remote_path = f"{parts_path}/{index:06d}.part"
        part_file.write_bytes(chunk)
        _upload_with_retries(offsite_uploader, part_file, remote_path)
        part_file.unlink()
        parts.append(
            {
                "index": index,
                "file": remote_path,
                "size_bytes": len(chunk),
                "sha256": hashlib.sha256(chunk).hexdigest(),
            }
        )
        remaining -= len(chunk)
        log_event(
            LOGGER,
            logging.INFO,
            f"{OPERATION}.part_upload.succeeded",
            operation=OPERATION,
            status="succeeded",
            part_index=index,
            part_size_bytes=len(chunk),
            remote_path=remote_path,
        )
        yield chunk


def _upload_with_retries(
    uploader: BackupOffsiteUploader, local_file: Path, remote_path: str
) -> None:
    for attempt in range(1, BACKUP_OFFSITE_PART_UPLOAD_ATTEMPTS + 1):
        try:
            uploader.upload_file(local_file=local_file, remote_path=remote_path)
            return
        except Exception as error:
            log_event(
                LOGGER,
                logging.WARNING,
                f"{OPERATION}.upload_attempt.failed",
                operation=OPERATION,
                status="failed",
                remote_path=remote_path,
                attempt=attempt,
                error_type=type(error).__name__,
                error_message=str(error),
            )
            if attempt == BACKUP_OFFSITE_PART_UPLOAD_ATTEMPTS:
                raise
            time.sleep(BACKUP_OFFSITE_PART_UPLOAD_RETRY_BACKOFF_SECONDS * attempt)
