from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Protocol

from hhru_platform.infrastructure.observability.operations import (
    log_operation_started,
    record_operation_failed,
    record_operation_succeeded,
)
from hhru_platform.infrastructure.research_archive import (
    ResearchArchiveOffsiteUploadReceipt,
    ResearchArchiveOffsiteVerificationReceipt,
)

LOGGER = logging.getLogger(__name__)


class ResearchArchiveOffsiteUploadReceiptLoader(Protocol):
    def load_receipt(
        self,
        *,
        manifest_file: Path,
    ) -> ResearchArchiveOffsiteUploadReceipt | None:
        """Load an upload receipt for one retained local manifest."""


class ResearchArchiveOffsiteVerificationReceiptLoader(Protocol):
    def load_receipt(
        self,
        *,
        manifest_file: Path,
    ) -> ResearchArchiveOffsiteVerificationReceipt | None:
        """Load an offsite verification receipt for one retained local manifest."""


@dataclass(slots=True, frozen=True)
class PruneResearchArchiveLocalCommand:
    archive_dir: Path
    offsite_url: str
    offsite_root: str
    min_age_hours: int = 24
    limit: int | None = None
    confirmed_apply: bool = False
    triggered_by: str = "prune-research-archive-local"
    evaluated_at: datetime | None = None

    def __post_init__(self) -> None:
        offsite_url = self.offsite_url.strip().rstrip("/")
        offsite_root = _normalize_offsite_root(self.offsite_root)
        triggered_by = self.triggered_by.strip()
        if not offsite_url:
            raise ValueError("offsite_url must not be empty")
        if self.min_age_hours < 0:
            raise ValueError("min_age_hours must be greater than or equal to zero")
        if self.limit is not None and self.limit < 1:
            raise ValueError("limit must be greater than or equal to one")
        if not triggered_by:
            raise ValueError("triggered_by must not be empty")
        object.__setattr__(self, "archive_dir", Path(self.archive_dir))
        object.__setattr__(self, "offsite_url", offsite_url)
        object.__setattr__(self, "offsite_root", offsite_root)
        object.__setattr__(self, "triggered_by", triggered_by)


@dataclass(slots=True, frozen=True)
class PruneResearchArchiveLocalResult:
    status: str
    archive_dir: Path
    triggered_by: str
    evaluated_at: datetime
    scanned_manifest_count: int
    candidate_count: int
    pruned_count: int
    already_offsite_only_count: int
    retained_count: int
    freed_bytes: int
    apply: bool


def prune_research_archive_local(
    command: PruneResearchArchiveLocalCommand,
    *,
    upload_receipt_store: ResearchArchiveOffsiteUploadReceiptLoader,
    verification_receipt_store: ResearchArchiveOffsiteVerificationReceiptLoader,
) -> PruneResearchArchiveLocalResult:
    started_at = log_operation_started(
        LOGGER,
        operation="prune_research_archive_local",
        archive_dir=str(command.archive_dir),
        offsite_url=command.offsite_url,
        offsite_root=command.offsite_root,
        apply=command.confirmed_apply,
        triggered_by=command.triggered_by,
    )
    evaluated_at = (command.evaluated_at or datetime.now(UTC)).astimezone(UTC)
    archive_root = command.archive_dir.resolve()
    cutoff = evaluated_at - timedelta(hours=command.min_age_hours)

    try:
        manifests = tuple(sorted(archive_root.rglob("*.manifest.json")))
        candidates: list[tuple[Path, int]] = []
        already_offsite_only_count = 0
        retained_count = 0
        for manifest_file in manifests:
            data_file, data_size_bytes = _validated_offsite_bundle(
                archive_root=archive_root,
                manifest_file=manifest_file,
                offsite_url=command.offsite_url,
                offsite_root=command.offsite_root,
                upload_receipt_store=upload_receipt_store,
                verification_receipt_store=verification_receipt_store,
            )
            if not data_file.exists():
                already_offsite_only_count += 1
                continue
            modified_at = datetime.fromtimestamp(data_file.stat().st_mtime, tz=UTC)
            if modified_at > cutoff:
                retained_count += 1
                continue
            candidates.append((data_file, data_size_bytes))

        selected = candidates[: command.limit] if command.limit is not None else candidates
        freed_bytes = 0
        if command.confirmed_apply:
            for data_file, data_size_bytes in selected:
                data_file.unlink()
                freed_bytes += data_size_bytes
        else:
            freed_bytes = sum(data_size_bytes for _, data_size_bytes in selected)
    except Exception as error:
        record_operation_failed(
            LOGGER,
            operation="prune_research_archive_local",
            started_at=started_at,
            error_type=error.__class__.__name__,
            error_message=str(error),
            archive_dir=str(command.archive_dir),
            triggered_by=command.triggered_by,
        )
        raise

    result = PruneResearchArchiveLocalResult(
        status="succeeded",
        archive_dir=archive_root,
        triggered_by=command.triggered_by,
        evaluated_at=evaluated_at,
        scanned_manifest_count=len(manifests),
        candidate_count=len(candidates),
        pruned_count=len(selected) if command.confirmed_apply else 0,
        already_offsite_only_count=already_offsite_only_count,
        retained_count=retained_count + len(candidates) - len(selected),
        freed_bytes=freed_bytes,
        apply=command.confirmed_apply,
    )
    record_operation_succeeded(
        LOGGER,
        operation="prune_research_archive_local",
        started_at=started_at,
        archive_dir=str(result.archive_dir),
        triggered_by=result.triggered_by,
        candidate_count=result.candidate_count,
        pruned_count=result.pruned_count,
        freed_bytes=result.freed_bytes,
        apply=result.apply,
    )
    return result


def _validated_offsite_bundle(
    *,
    archive_root: Path,
    manifest_file: Path,
    offsite_url: str,
    offsite_root: str,
    upload_receipt_store: ResearchArchiveOffsiteUploadReceiptLoader,
    verification_receipt_store: ResearchArchiveOffsiteVerificationReceiptLoader,
) -> tuple[Path, int]:
    payload = json.loads(manifest_file.read_text(encoding="utf-8"))
    required_fields = (
        "dataset_key",
        "layer",
        "row_count",
        "data_file",
        "data_size_bytes",
        "data_sha256",
    )
    missing = [field for field in required_fields if field not in payload]
    if missing:
        raise RuntimeError(
            f"research archive manifest {manifest_file} is missing fields: "
            f"{', '.join(missing)}"
        )

    relative_data_file = Path(str(payload["data_file"]))
    data_file = (
        relative_data_file.resolve()
        if relative_data_file.is_absolute()
        else (archive_root / relative_data_file).resolve()
    )
    if not data_file.is_relative_to(archive_root):
        raise RuntimeError(f"research archive data file escapes archive root: {data_file}")

    manifest_relative_path = manifest_file.resolve().relative_to(archive_root).as_posix()
    remote_data_path = _join_remote_path(offsite_root, relative_data_file.as_posix())
    remote_manifest_path = _join_remote_path(offsite_root, manifest_relative_path)
    manifest_sha256 = _sha256_file(manifest_file)
    data_size_bytes = int(payload["data_size_bytes"])
    data_sha256 = str(payload["data_sha256"])

    upload_receipt = upload_receipt_store.load_receipt(manifest_file=manifest_file)
    verification_receipt = verification_receipt_store.load_receipt(
        manifest_file=manifest_file
    )
    if not _upload_receipt_matches(
        upload_receipt,
        offsite_url=offsite_url,
        offsite_root=offsite_root,
        data_size_bytes=data_size_bytes,
        data_sha256=data_sha256,
        manifest_sha256=manifest_sha256,
        remote_data_path=remote_data_path,
        remote_manifest_path=remote_manifest_path,
    ):
        raise RuntimeError(f"matching offsite upload receipt is required: {manifest_file}")
    if not _verification_receipt_matches(
        verification_receipt,
        payload=payload,
        offsite_url=offsite_url,
        offsite_root=offsite_root,
        data_size_bytes=data_size_bytes,
        data_sha256=data_sha256,
        manifest_sha256=manifest_sha256,
        remote_data_path=remote_data_path,
        remote_manifest_path=remote_manifest_path,
    ):
        raise RuntimeError(
            f"matching offsite verification receipt is required: {manifest_file}"
        )
    if data_file.exists() and data_file.stat().st_size != data_size_bytes:
        raise RuntimeError(
            f"research archive data size mismatch for {data_file}: "
            f"expected={data_size_bytes} actual={data_file.stat().st_size}"
        )
    return data_file, data_size_bytes


def _upload_receipt_matches(
    receipt: ResearchArchiveOffsiteUploadReceipt | None,
    **expected: object,
) -> bool:
    if receipt is None:
        return False
    return all(getattr(receipt, key) == value for key, value in expected.items())


def _verification_receipt_matches(
    receipt: ResearchArchiveOffsiteVerificationReceipt | None,
    *,
    payload: dict[str, object],
    **expected: object,
) -> bool:
    if receipt is None or receipt.verified_object_count < 2:
        return False
    identity = {
        **expected,
        "dataset": str(payload["dataset_key"]),
        "layer": str(payload["layer"]),
        "row_count": int(str(payload["row_count"])),
    }
    return all(getattr(receipt, key) == value for key, value in identity.items())


def _normalize_offsite_root(offsite_root: str) -> str:
    parts = tuple(part for part in offsite_root.strip().split("/") if part)
    return "/" + "/".join(parts) if parts else "/"


def _join_remote_path(offsite_root: str, relative_path: str) -> str:
    root = _normalize_offsite_root(offsite_root).strip("/")
    suffix = relative_path.strip("/")
    return f"/{root}/{suffix}" if root else f"/{suffix}"


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
