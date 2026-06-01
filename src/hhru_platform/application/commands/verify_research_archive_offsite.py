from __future__ import annotations

import gzip
import hashlib
import json
import logging
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol

from hhru_platform.application.commands.sync_research_archive_offsite import (
    _join_remote_path,
    _load_bundle,
    _normalize_offsite_root,
    _select_manifest_files,
)
from hhru_platform.infrastructure.observability.operations import (
    log_operation_started,
    record_operation_failed,
    record_operation_succeeded,
)
from hhru_platform.infrastructure.research_archive import (
    ResearchArchiveOffsiteVerificationReceipt,
)

LOGGER = logging.getLogger(__name__)

RESEARCH_ARCHIVE_OFFSITE_VERIFY_STATUS_SUCCEEDED = "succeeded"


@dataclass(slots=True, frozen=True)
class VerifyResearchArchiveOffsiteCommand:
    archive_dir: Path = Path(".state/archive/research")
    manifest_files: tuple[Path, ...] = ()
    limit: int | None = None
    readback_limit: int = 1
    offsite_url: str = ""
    offsite_root: str = "/hhru-platform/research-archive"
    triggered_by: str = "verify-research-archive-offsite"
    verified_at: datetime | None = None

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
        if self.readback_limit < 0:
            raise ValueError("readback_limit must be greater than or equal to zero")
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
class ResearchArchiveOffsiteVerifiedObject:
    remote_path: str
    expected_size_bytes: int
    actual_size_bytes: int


@dataclass(slots=True, frozen=True)
class ResearchArchiveOffsiteReadbackSummary:
    remote_data_path: str
    row_count: int
    data_size_bytes: int
    data_sha256: str


@dataclass(slots=True, frozen=True)
class ResearchArchiveOffsiteVerifiedManifestSummary:
    manifest_file: Path
    receipt_file: Path
    dataset: str
    layer: str
    row_count: int
    data_size_bytes: int
    data_sha256: str
    manifest_sha256: str
    remote_data_path: str
    remote_manifest_path: str
    readback_verified: bool


@dataclass(slots=True, frozen=True)
class VerifyResearchArchiveOffsiteResult:
    status: str
    triggered_by: str
    archive_dir: Path
    offsite_url: str
    offsite_root: str
    scanned_manifest_count: int
    verified_manifest_count: int
    verified_objects: tuple[ResearchArchiveOffsiteVerifiedObject, ...]
    verified_manifests: tuple[ResearchArchiveOffsiteVerifiedManifestSummary, ...]
    readbacks: tuple[ResearchArchiveOffsiteReadbackSummary, ...]

    @property
    def verified_object_count(self) -> int:
        return len(self.verified_objects)

    @property
    def readback_count(self) -> int:
        return len(self.readbacks)

    @property
    def verification_receipt_count(self) -> int:
        return len(self.verified_manifests)


class ResearchArchiveOffsiteRemoteStore(Protocol):
    def get_file_size(self, *, remote_path: str) -> int:
        """Return remote object size in bytes."""

    def download_file(self, *, local_file: Path, remote_path: str) -> None:
        """Download one remote object into a local file."""


class ResearchArchiveOffsiteVerificationReceiptStore(Protocol):
    def write_receipt(
        self,
        *,
        manifest_file: Path,
        receipt: ResearchArchiveOffsiteVerificationReceipt,
    ) -> Path:
        """Persist proof that one research archive chunk passed offsite verification."""


def verify_research_archive_offsite(
    command: VerifyResearchArchiveOffsiteCommand,
    *,
    remote_store: ResearchArchiveOffsiteRemoteStore,
    receipt_store: ResearchArchiveOffsiteVerificationReceiptStore,
) -> VerifyResearchArchiveOffsiteResult:
    started_at = log_operation_started(
        LOGGER,
        operation="verify_research_archive_offsite",
        archive_dir=str(command.archive_dir),
        offsite_url=command.offsite_url,
        offsite_root=command.offsite_root,
        limit=command.limit or 0,
        readback_limit=command.readback_limit,
        triggered_by=command.triggered_by,
    )
    try:
        result = _verify_research_archive_offsite(
            command=command,
            remote_store=remote_store,
            receipt_store=receipt_store,
        )
    except Exception as error:
        record_operation_failed(
            LOGGER,
            operation="verify_research_archive_offsite",
            started_at=started_at,
            error_type=error.__class__.__name__,
            error_message=str(error),
            archive_dir=str(command.archive_dir),
            offsite_url=command.offsite_url,
            offsite_root=command.offsite_root,
            triggered_by=command.triggered_by,
        )
        raise

    record_operation_succeeded(
        LOGGER,
        operation="verify_research_archive_offsite",
        started_at=started_at,
        archive_dir=str(result.archive_dir),
        offsite_url=result.offsite_url,
        offsite_root=result.offsite_root,
        triggered_by=result.triggered_by,
        scanned_manifest_count=result.scanned_manifest_count,
        verified_manifest_count=result.verified_manifest_count,
        verified_object_count=result.verified_object_count,
        verification_receipt_count=result.verification_receipt_count,
        readback_count=result.readback_count,
    )
    return result


def _verify_research_archive_offsite(
    *,
    command: VerifyResearchArchiveOffsiteCommand,
    remote_store: ResearchArchiveOffsiteRemoteStore,
    receipt_store: ResearchArchiveOffsiteVerificationReceiptStore,
) -> VerifyResearchArchiveOffsiteResult:
    archive_root = command.archive_dir.resolve()
    manifest_files = _select_manifest_files(
        archive_dir=archive_root,
        manifest_files=command.manifest_files,
        limit=command.limit,
    )
    bundles = tuple(
        _load_bundle(
            archive_root=archive_root,
            manifest_file=manifest_file,
            offsite_root=command.offsite_root,
        )
        for manifest_file in manifest_files
    )

    verified_objects: list[ResearchArchiveOffsiteVerifiedObject] = []
    verified_manifests: list[ResearchArchiveOffsiteVerifiedManifestSummary] = []
    readbacks: list[ResearchArchiveOffsiteReadbackSummary] = []
    verified_at = command.verified_at or datetime.now(UTC)
    for index, bundle in enumerate(bundles, start=1):
        verified_objects.append(
            _verify_remote_size(
                remote_store=remote_store,
                remote_path=bundle.remote_data_relative_path,
                expected_size_bytes=bundle.data_size_bytes,
            )
        )
        verified_objects.append(
            _verify_remote_size(
                remote_store=remote_store,
                remote_path=bundle.remote_manifest_relative_path,
                expected_size_bytes=bundle.manifest_file.stat().st_size,
            )
        )
        readback_verified = index <= command.readback_limit
        if readback_verified:
            readbacks.append(
                _readback_data_file(
                    remote_store=remote_store,
                    remote_path=bundle.remote_data_relative_path,
                    expected_sha256=bundle.data_sha256,
                    expected_size_bytes=bundle.data_size_bytes,
                    expected_row_count=bundle.row_count,
                )
            )
        receipt_file = receipt_store.write_receipt(
            manifest_file=bundle.manifest_file,
            receipt=ResearchArchiveOffsiteVerificationReceipt(
                verified_at=verified_at,
                offsite_url=command.offsite_url,
                offsite_root=command.offsite_root,
                dataset=bundle.dataset,
                layer=bundle.layer,
                row_count=bundle.row_count,
                data_size_bytes=bundle.data_size_bytes,
                data_sha256=bundle.data_sha256,
                manifest_sha256=bundle.manifest_sha256,
                remote_data_path=bundle.remote_data_path,
                remote_manifest_path=bundle.remote_manifest_path,
                verified_object_count=2,
                readback_verified=readback_verified,
            ),
        )
        verified_manifests.append(
            ResearchArchiveOffsiteVerifiedManifestSummary(
                manifest_file=bundle.manifest_file,
                receipt_file=receipt_file,
                dataset=bundle.dataset,
                layer=bundle.layer,
                row_count=bundle.row_count,
                data_size_bytes=bundle.data_size_bytes,
                data_sha256=bundle.data_sha256,
                manifest_sha256=bundle.manifest_sha256,
                remote_data_path=bundle.remote_data_path,
                remote_manifest_path=bundle.remote_manifest_path,
                readback_verified=readback_verified,
            )
        )

    inventory_file = archive_root / "v1" / "inventory" / "archive-inventory.jsonl"
    is_partial_verify = bool(command.manifest_files) or command.limit is not None
    if inventory_file.is_file() and not is_partial_verify:
        inventory_relative_path = inventory_file.relative_to(archive_root).as_posix()
        verified_objects.append(
            _verify_remote_size(
                remote_store=remote_store,
                remote_path=inventory_relative_path,
                expected_size_bytes=inventory_file.stat().st_size,
            )
        )

    return VerifyResearchArchiveOffsiteResult(
        status=RESEARCH_ARCHIVE_OFFSITE_VERIFY_STATUS_SUCCEEDED,
        triggered_by=command.triggered_by,
        archive_dir=archive_root,
        offsite_url=command.offsite_url,
        offsite_root=command.offsite_root,
        scanned_manifest_count=len(bundles),
        verified_manifest_count=len(bundles),
        verified_objects=tuple(verified_objects),
        verified_manifests=tuple(verified_manifests),
        readbacks=tuple(readbacks),
    )


def _verify_remote_size(
    *,
    remote_store: ResearchArchiveOffsiteRemoteStore,
    remote_path: str,
    expected_size_bytes: int,
) -> ResearchArchiveOffsiteVerifiedObject:
    actual_size_bytes = remote_store.get_file_size(remote_path=remote_path)
    if actual_size_bytes != expected_size_bytes:
        raise RuntimeError(
            f"remote research archive object size mismatch for {remote_path}: "
            f"expected={expected_size_bytes} actual={actual_size_bytes}"
        )
    return ResearchArchiveOffsiteVerifiedObject(
        remote_path=remote_path,
        expected_size_bytes=expected_size_bytes,
        actual_size_bytes=actual_size_bytes,
    )


def _readback_data_file(
    *,
    remote_store: ResearchArchiveOffsiteRemoteStore,
    remote_path: str,
    expected_sha256: str,
    expected_size_bytes: int,
    expected_row_count: int,
) -> ResearchArchiveOffsiteReadbackSummary:
    with tempfile.TemporaryDirectory(prefix="hhru-research-archive-readback-") as temp_dir:
        local_file = Path(temp_dir) / Path(remote_path).name
        remote_store.download_file(local_file=local_file, remote_path=remote_path)
        actual_size_bytes = local_file.stat().st_size
        if actual_size_bytes != expected_size_bytes:
            raise RuntimeError(
                f"readback size mismatch for {remote_path}: "
                f"expected={expected_size_bytes} actual={actual_size_bytes}"
            )
        actual_sha256 = _sha256_file(local_file)
        if actual_sha256 != expected_sha256:
            raise RuntimeError(
                f"readback sha256 mismatch for {remote_path}: "
                f"expected={expected_sha256} actual={actual_sha256}"
            )
        actual_row_count = _count_jsonl_gzip_rows(local_file)
        if actual_row_count != expected_row_count:
            raise RuntimeError(
                f"readback row_count mismatch for {remote_path}: "
                f"expected={expected_row_count} actual={actual_row_count}"
            )
        return ResearchArchiveOffsiteReadbackSummary(
            remote_data_path=_join_remote_path("/", remote_path),
            row_count=actual_row_count,
            data_size_bytes=actual_size_bytes,
            data_sha256=actual_sha256,
        )


def _count_jsonl_gzip_rows(path: Path) -> int:
    row_count = 0
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            json.loads(line)
            row_count += 1
    return row_count


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
