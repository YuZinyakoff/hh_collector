from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from time import monotonic
from typing import Protocol

from hhru_platform.infrastructure.observability.logging import log_event
from hhru_platform.infrastructure.observability.operations import (
    log_operation_started,
    record_operation_failed,
    record_operation_succeeded,
)
from hhru_platform.infrastructure.research_archive import ResearchArchiveOffsiteUploadReceipt
from hhru_platform.infrastructure.research_archive.checkpoint_verification_receipt_store import (
    LocalResearchArchiveCheckpointVerificationReceiptStore,
)
from hhru_platform.infrastructure.research_archive.control_upload_receipt_store import (
    LocalControlUploadReceiptStore,
)

LOGGER = logging.getLogger(__name__)

RESEARCH_ARCHIVE_OFFSITE_SYNC_STATUS_SUCCEEDED = "succeeded"


@dataclass(slots=True, frozen=True)
class SyncResearchArchiveOffsiteCommand:
    archive_dir: Path = Path(".state/archive/research")
    manifest_files: tuple[Path, ...] = ()
    limit: int | None = None
    offsite_url: str = ""
    offsite_root: str = "/hhru-platform/research-archive"
    triggered_by: str = "sync-research-archive-offsite"
    synced_at: datetime | None = None

    def __post_init__(self) -> None:
        normalized_triggered_by = self.triggered_by.strip()
        normalized_offsite_url = self.offsite_url.strip().rstrip("/")
        normalized_offsite_root = _normalize_offsite_root(self.offsite_root)
        if not normalized_triggered_by:
            raise ValueError("triggered_by must not be empty")
        if not normalized_offsite_url:
            raise ValueError("offsite_url must not be empty")
        if self.limit is not None and self.limit < 1:
            raise ValueError("limit must be greater than or equal to one")
        object.__setattr__(self, "archive_dir", Path(self.archive_dir))
        object.__setattr__(
            self,
            "manifest_files",
            tuple(Path(path) for path in self.manifest_files),
        )
        object.__setattr__(self, "offsite_url", normalized_offsite_url)
        object.__setattr__(self, "offsite_root", normalized_offsite_root)
        object.__setattr__(self, "triggered_by", normalized_triggered_by)


@dataclass(slots=True, frozen=True)
class ResearchArchiveOffsiteSummary:
    dataset: str
    layer: str
    row_count: int
    data_file: Path
    manifest_file: Path
    remote_data_path: str
    remote_manifest_path: str
    data_size_bytes: int
    data_sha256: str
    manifest_sha256: str
    uploaded: bool
    skipped: bool
    receipt_file: Path | None


@dataclass(slots=True, frozen=True)
class SyncResearchArchiveOffsiteResult:
    status: str
    triggered_by: str
    synced_at: datetime
    archive_dir: Path
    offsite_url: str
    offsite_root: str
    limit: int | None
    inventory_file: Path | None
    remote_inventory_path: str | None
    inventory_uploaded: bool
    checkpoint_files: tuple[Path, ...]
    remote_checkpoint_paths: tuple[str, ...]
    summaries: tuple[ResearchArchiveOffsiteSummary, ...]
    checkpoint_scanned_count: int = 0
    checkpoint_skipped_count: int = 0

    @property
    def scanned_manifest_count(self) -> int:
        return len(self.summaries)

    @property
    def uploaded_manifest_count(self) -> int:
        return sum(1 for summary in self.summaries if summary.uploaded)

    @property
    def skipped_manifest_count(self) -> int:
        return sum(1 for summary in self.summaries if summary.skipped)

    @property
    def candidate_manifest_count(self) -> int:
        return self.scanned_manifest_count - self.skipped_manifest_count

    @property
    def checkpoint_uploaded_count(self) -> int:
        return len(self.checkpoint_files)


class ResearchArchiveOffsiteUploader(Protocol):
    def upload_file(self, *, local_file: Path, remote_path: str) -> None:
        """Upload one research archive object to offsite storage."""


class ResearchArchiveOffsiteReceiptStore(Protocol):
    def load_receipt(
        self,
        *,
        manifest_file: Path,
    ) -> ResearchArchiveOffsiteUploadReceipt | None:
        """Load a local upload receipt for one research archive manifest."""

    def write_receipt(
        self,
        *,
        manifest_file: Path,
        receipt: ResearchArchiveOffsiteUploadReceipt,
    ) -> Path:
        """Persist a local upload receipt for one research archive manifest."""


def sync_research_archive_offsite(
    command: SyncResearchArchiveOffsiteCommand,
    *,
    offsite_uploader: ResearchArchiveOffsiteUploader,
    receipt_store: ResearchArchiveOffsiteReceiptStore,
) -> SyncResearchArchiveOffsiteResult:
    started_at = log_operation_started(
        LOGGER,
        operation="sync_research_archive_offsite",
        archive_dir=str(command.archive_dir),
        offsite_url=command.offsite_url,
        offsite_root=command.offsite_root,
        limit=command.limit or 0,
        triggered_by=command.triggered_by,
    )
    synced_at = command.synced_at or datetime.now(UTC)
    try:
        archive_root = command.archive_dir.resolve()
        manifest_files = _select_manifest_files(
            archive_dir=archive_root,
            manifest_files=command.manifest_files,
            limit=command.limit,
        )
        _progress(phase="manifests", completed=0, total=len(manifest_files), uploaded=0, skipped=0)
        collected: list[ResearchArchiveOffsiteSummary] = []
        uploaded = skipped = 0
        last_progress = monotonic()
        for index, manifest_file in enumerate(manifest_files, 1):
            summary = _sync_one_manifest(
                command=command,
                archive_root=archive_root,
                manifest_file=manifest_file,
                synced_at=synced_at,
                offsite_uploader=offsite_uploader,
                receipt_store=receipt_store,
            )
            collected.append(summary)
            uploaded += int(summary.uploaded)
            skipped += int(summary.skipped)
            if index % 100 == 0 or monotonic() - last_progress >= 10:
                _progress(
                    phase="manifests",
                    completed=index,
                    total=len(manifest_files),
                    uploaded=uploaded,
                    skipped=skipped,
                )
                last_progress = monotonic()
        summaries = tuple(collected)
        _progress(
            phase="manifests",
            completed=len(summaries),
            total=len(manifest_files),
            uploaded=uploaded,
            skipped=skipped,
        )
        inventory_file, remote_inventory_path, inventory_uploaded = _sync_inventory(
            command=command,
            archive_root=archive_root,
            offsite_uploader=offsite_uploader,
        )
        checkpoint_files, remote_checkpoint_paths, checkpoint_scanned_count = _sync_checkpoints(
            command=command,
            archive_root=archive_root,
            offsite_uploader=offsite_uploader,
        )
    except Exception as error:
        record_operation_failed(
            LOGGER,
            operation="sync_research_archive_offsite",
            started_at=started_at,
            error_type=error.__class__.__name__,
            error_message=str(error),
            archive_dir=str(command.archive_dir),
            offsite_url=command.offsite_url,
            offsite_root=command.offsite_root,
            triggered_by=command.triggered_by,
        )
        raise

    result = SyncResearchArchiveOffsiteResult(
        status=RESEARCH_ARCHIVE_OFFSITE_SYNC_STATUS_SUCCEEDED,
        triggered_by=command.triggered_by,
        synced_at=synced_at,
        archive_dir=archive_root,
        offsite_url=command.offsite_url,
        offsite_root=command.offsite_root,
        limit=command.limit,
        inventory_file=inventory_file,
        remote_inventory_path=remote_inventory_path,
        inventory_uploaded=inventory_uploaded,
        checkpoint_files=checkpoint_files,
        remote_checkpoint_paths=remote_checkpoint_paths,
        summaries=summaries,
        checkpoint_scanned_count=checkpoint_scanned_count,
        checkpoint_skipped_count=checkpoint_scanned_count - len(checkpoint_files),
    )
    record_operation_succeeded(
        LOGGER,
        operation="sync_research_archive_offsite",
        started_at=started_at,
        archive_dir=str(result.archive_dir),
        offsite_url=result.offsite_url,
        offsite_root=result.offsite_root,
        triggered_by=result.triggered_by,
        scanned_manifest_count=result.scanned_manifest_count,
        candidate_manifest_count=result.candidate_manifest_count,
        uploaded_manifest_count=result.uploaded_manifest_count,
        skipped_manifest_count=result.skipped_manifest_count,
        inventory_uploaded=result.inventory_uploaded,
        checkpoint_uploaded_count=result.checkpoint_uploaded_count,
        checkpoint_scanned_count=result.checkpoint_scanned_count,
        checkpoint_skipped_count=result.checkpoint_skipped_count,
    )
    return result


@dataclass(slots=True, frozen=True)
class _ResearchArchiveBundle:
    dataset: str
    layer: str
    row_count: int
    data_file: Path
    manifest_file: Path
    remote_data_relative_path: str
    remote_manifest_relative_path: str
    remote_data_path: str
    remote_manifest_path: str
    data_size_bytes: int
    data_sha256: str
    manifest_sha256: str
    data_present: bool


def _sync_one_manifest(
    *,
    command: SyncResearchArchiveOffsiteCommand,
    archive_root: Path,
    manifest_file: Path,
    synced_at: datetime,
    offsite_uploader: ResearchArchiveOffsiteUploader,
    receipt_store: ResearchArchiveOffsiteReceiptStore,
) -> ResearchArchiveOffsiteSummary:
    bundle = _load_bundle(
        archive_root=archive_root,
        manifest_file=manifest_file,
        offsite_root=command.offsite_root,
    )
    existing_receipt = receipt_store.load_receipt(manifest_file=bundle.manifest_file)
    if _receipt_matches_bundle(existing_receipt, bundle=bundle, command=command):
        return ResearchArchiveOffsiteSummary(
            dataset=bundle.dataset,
            layer=bundle.layer,
            row_count=bundle.row_count,
            data_file=bundle.data_file,
            manifest_file=bundle.manifest_file,
            remote_data_path=bundle.remote_data_path,
            remote_manifest_path=bundle.remote_manifest_path,
            data_size_bytes=bundle.data_size_bytes,
            data_sha256=bundle.data_sha256,
            manifest_sha256=bundle.manifest_sha256,
            uploaded=False,
            skipped=True,
            receipt_file=Path(f"{bundle.manifest_file}.offsite.json"),
        )
    if not bundle.data_present:
        raise FileNotFoundError(
            "research archive data file is offsite-only but no matching upload "
            f"receipt exists: {bundle.data_file}"
        )

    log_event(
        LOGGER,
        logging.INFO,
        "sync_research_archive_offsite.chunk_upload.started",
        operation="sync_research_archive_offsite",
        status="started",
        dataset=bundle.dataset,
        layer=bundle.layer,
        row_count=bundle.row_count,
        data_file=bundle.data_file,
        manifest_file=bundle.manifest_file,
        remote_data_path=bundle.remote_data_path,
        remote_manifest_path=bundle.remote_manifest_path,
    )
    offsite_uploader.upload_file(
        local_file=bundle.data_file,
        remote_path=bundle.remote_data_relative_path,
    )
    offsite_uploader.upload_file(
        local_file=bundle.manifest_file,
        remote_path=bundle.remote_manifest_relative_path,
    )
    receipt_file = receipt_store.write_receipt(
        manifest_file=bundle.manifest_file,
        receipt=ResearchArchiveOffsiteUploadReceipt(
            uploaded_at=synced_at,
            offsite_url=command.offsite_url,
            offsite_root=command.offsite_root,
            data_size_bytes=bundle.data_size_bytes,
            data_sha256=bundle.data_sha256,
            manifest_sha256=bundle.manifest_sha256,
            remote_data_path=bundle.remote_data_path,
            remote_manifest_path=bundle.remote_manifest_path,
        ),
    )
    log_event(
        LOGGER,
        logging.INFO,
        "sync_research_archive_offsite.chunk_upload.succeeded",
        operation="sync_research_archive_offsite",
        status="succeeded",
        dataset=bundle.dataset,
        layer=bundle.layer,
        row_count=bundle.row_count,
        data_file=bundle.data_file,
        manifest_file=bundle.manifest_file,
        remote_data_path=bundle.remote_data_path,
        remote_manifest_path=bundle.remote_manifest_path,
    )
    return ResearchArchiveOffsiteSummary(
        dataset=bundle.dataset,
        layer=bundle.layer,
        row_count=bundle.row_count,
        data_file=bundle.data_file,
        manifest_file=bundle.manifest_file,
        remote_data_path=bundle.remote_data_path,
        remote_manifest_path=bundle.remote_manifest_path,
        data_size_bytes=bundle.data_size_bytes,
        data_sha256=bundle.data_sha256,
        manifest_sha256=bundle.manifest_sha256,
        uploaded=True,
        skipped=False,
        receipt_file=receipt_file,
    )


def _sync_inventory(
    *,
    command: SyncResearchArchiveOffsiteCommand,
    archive_root: Path,
    offsite_uploader: ResearchArchiveOffsiteUploader,
) -> tuple[Path | None, str | None, bool]:
    inventory_file = archive_root / "v1" / "inventory" / "archive-inventory.jsonl"
    if not inventory_file.is_file():
        return None, None, False
    remote_relative_path = inventory_file.relative_to(archive_root).as_posix()
    remote_path = _join_remote_path(command.offsite_root, remote_relative_path)
    is_partial_sync = bool(command.manifest_files) or command.limit is not None
    if is_partial_sync:
        return inventory_file, remote_path, False
    uploaded = _sync_control_file(
        command=command,
        local_file=inventory_file,
        remote_relative_path=remote_relative_path,
        offsite_uploader=offsite_uploader,
        kind="inventory",
    )
    _progress(
        phase="inventory", completed=1, total=1, uploaded=int(uploaded), skipped=int(not uploaded)
    )
    return inventory_file, remote_path, uploaded


def _sync_checkpoints(
    *,
    command: SyncResearchArchiveOffsiteCommand,
    archive_root: Path,
    offsite_uploader: ResearchArchiveOffsiteUploader,
) -> tuple[tuple[Path, ...], tuple[str, ...], int]:
    is_partial_sync = bool(command.manifest_files) or command.limit is not None
    if is_partial_sync:
        return (), (), 0
    checkpoint_root = archive_root / "v1" / "checkpoints"
    checkpoint_files = tuple(sorted(checkpoint_root.rglob("*.checkpoint.json")))
    remote_checkpoint_paths: list[str] = []
    uploaded_files: list[Path] = []
    skipped = 0
    last_progress = monotonic()
    _progress(phase="checkpoints", completed=0, total=len(checkpoint_files), uploaded=0, skipped=0)
    for index, checkpoint_file in enumerate(checkpoint_files, 1):
        remote_relative_path = checkpoint_file.relative_to(archive_root).as_posix()
        uploaded = _sync_control_file(
            command=command,
            local_file=checkpoint_file,
            remote_relative_path=remote_relative_path,
            offsite_uploader=offsite_uploader,
            kind="checkpoint",
        )
        if uploaded:
            uploaded_files.append(checkpoint_file)
            remote_checkpoint_paths.append(
                _join_remote_path(command.offsite_root, remote_relative_path)
            )
        else:
            skipped += 1
        if index % 100 == 0 or monotonic() - last_progress >= 10:
            _progress(
                phase="checkpoints",
                completed=index,
                total=len(checkpoint_files),
                uploaded=len(uploaded_files),
                skipped=skipped,
            )
            last_progress = monotonic()
    _progress(
        phase="checkpoints",
        completed=len(checkpoint_files),
        total=len(checkpoint_files),
        uploaded=len(uploaded_files),
        skipped=skipped,
    )
    return tuple(uploaded_files), tuple(remote_checkpoint_paths), len(checkpoint_files)


def _progress(*, phase: str, completed: int, total: int, uploaded: int, skipped: int) -> None:
    log_event(
        LOGGER,
        logging.INFO,
        "sync_research_archive_offsite.progress",
        operation="sync_research_archive_offsite",
        status="running",
        phase=phase,
        completed=completed,
        total=total,
        uploaded=uploaded,
        skipped=skipped,
    )


def _sync_control_file(
    *,
    command: SyncResearchArchiveOffsiteCommand,
    local_file: Path,
    remote_relative_path: str,
    offsite_uploader: ResearchArchiveOffsiteUploader,
    kind: str,
) -> bool:
    fingerprint: dict[str, object] = {
        "schema_version": "research-control-upload-v1",
        "offsite_url": command.offsite_url,
        "offsite_root": command.offsite_root,
        "remote_path": _join_remote_path(command.offsite_root, remote_relative_path),
        "size_bytes": local_file.stat().st_size,
        "sha256": _sha256_file(local_file),
        "kind": kind,
    }
    receipts = LocalControlUploadReceiptStore()
    if receipts.matches(local_file=local_file, fingerprint=fingerprint):
        return False
    if kind == "checkpoint" and _has_matching_checkpoint_verification(local_file, fingerprint):
        return False
    started = monotonic()
    log_event(
        LOGGER,
        logging.INFO,
        "sync_research_archive_offsite.control_upload.started",
        operation="sync_research_archive_offsite",
        status="started",
        kind=kind,
        local_file=local_file,
        remote_relative_path=remote_relative_path,
        size_bytes=fingerprint["size_bytes"],
    )
    offsite_uploader.upload_file(local_file=local_file, remote_path=remote_relative_path)
    if (
        local_file.stat().st_size != fingerprint["size_bytes"]
        or _sha256_file(local_file) != fingerprint["sha256"]
    ):
        raise RuntimeError("research archive control file changed during upload")
    # Persist only after upload success. This cache is NOT a verification receipt.
    receipts.write_receipt(local_file=local_file, fingerprint=fingerprint)
    log_event(
        LOGGER,
        logging.INFO,
        "sync_research_archive_offsite.control_upload.succeeded",
        operation="sync_research_archive_offsite",
        status="succeeded",
        kind=kind,
        local_file=local_file,
        duration_ms=int((monotonic() - started) * 1000),
    )
    return True


def _has_matching_checkpoint_verification(local_file: Path, fingerprint: dict[str, object]) -> bool:
    try:
        receipt = LocalResearchArchiveCheckpointVerificationReceiptStore().load_receipt(
            checkpoint_file=local_file
        )
    except (ValueError, KeyError, TypeError):
        LOGGER.warning("ignoring malformed checkpoint verification receipt: %s", local_file)
        return False
    return receipt is not None and (
        receipt.offsite_url == fingerprint["offsite_url"]
        and receipt.offsite_root == fingerprint["offsite_root"]
        and receipt.remote_checkpoint_path == fingerprint["remote_path"]
        and receipt.checkpoint_size_bytes == fingerprint["size_bytes"]
        and receipt.checkpoint_sha256 == fingerprint["sha256"]
    )


def _load_bundle(
    *,
    archive_root: Path,
    manifest_file: Path,
    offsite_root: str,
) -> _ResearchArchiveBundle:
    resolved_manifest_file = manifest_file.resolve()
    if not resolved_manifest_file.is_file():
        raise FileNotFoundError(f"research archive manifest not found: {resolved_manifest_file}")
    manifest_payload = json.loads(resolved_manifest_file.read_text(encoding="utf-8"))
    _require_manifest_fields(manifest_file=resolved_manifest_file, payload=manifest_payload)

    data_relative_path = str(manifest_payload["data_file"])
    data_file = Path(data_relative_path)
    if not data_file.is_absolute():
        data_file = archive_root / data_file
    data_file = data_file.resolve()
    expected_size_bytes = int(manifest_payload["data_size_bytes"])
    expected_sha256 = str(manifest_payload["data_sha256"])
    data_present = data_file.is_file()
    if data_present:
        actual_size_bytes = data_file.stat().st_size
        if actual_size_bytes != expected_size_bytes:
            raise RuntimeError(
                f"research archive data size mismatch for {data_file}: "
                f"expected={expected_size_bytes} actual={actual_size_bytes}"
            )
        actual_sha256 = _sha256_file(data_file)
        if actual_sha256 != expected_sha256:
            raise RuntimeError(
                f"research archive data sha256 mismatch for {data_file}: "
                f"expected={expected_sha256} actual={actual_sha256}"
            )

    manifest_relative_path = resolved_manifest_file.relative_to(archive_root).as_posix()
    manifest_sha256 = _sha256_file(resolved_manifest_file)
    return _ResearchArchiveBundle(
        dataset=str(manifest_payload["dataset_key"]),
        layer=str(manifest_payload["layer"]),
        row_count=int(manifest_payload["row_count"]),
        data_file=data_file,
        manifest_file=resolved_manifest_file,
        remote_data_relative_path=data_relative_path,
        remote_manifest_relative_path=manifest_relative_path,
        remote_data_path=_join_remote_path(offsite_root, data_relative_path),
        remote_manifest_path=_join_remote_path(offsite_root, manifest_relative_path),
        data_size_bytes=expected_size_bytes,
        data_sha256=expected_sha256,
        manifest_sha256=manifest_sha256,
        data_present=data_present,
    )


def _select_manifest_files(
    *,
    archive_dir: Path,
    manifest_files: tuple[Path, ...],
    limit: int | None,
) -> tuple[Path, ...]:
    if manifest_files:
        selected = tuple(
            path.resolve() if path.is_absolute() else (archive_dir / path).resolve()
            for path in manifest_files
        )
    elif archive_dir.exists():
        selected = tuple(sorted(path.resolve() for path in archive_dir.rglob("*.manifest.json")))
    else:
        selected = ()
    if limit is not None:
        selected = selected[:limit]
    return selected


def _receipt_matches_bundle(
    receipt: ResearchArchiveOffsiteUploadReceipt | None,
    *,
    bundle: _ResearchArchiveBundle,
    command: SyncResearchArchiveOffsiteCommand,
) -> bool:
    if receipt is None:
        return False
    return (
        receipt.offsite_url == command.offsite_url
        and receipt.offsite_root == command.offsite_root
        and receipt.data_size_bytes == bundle.data_size_bytes
        and receipt.data_sha256 == bundle.data_sha256
        and receipt.manifest_sha256 == bundle.manifest_sha256
        and receipt.remote_data_path == bundle.remote_data_path
        and receipt.remote_manifest_path == bundle.remote_manifest_path
    )


def _require_manifest_fields(*, manifest_file: Path, payload: dict[str, object]) -> None:
    required_fields = (
        "dataset_key",
        "layer",
        "row_count",
        "data_file",
        "data_size_bytes",
        "data_sha256",
    )
    missing_fields = [field for field in required_fields if field not in payload]
    if missing_fields:
        raise RuntimeError(
            f"research archive manifest {manifest_file} is missing fields: "
            f"{', '.join(missing_fields)}"
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
