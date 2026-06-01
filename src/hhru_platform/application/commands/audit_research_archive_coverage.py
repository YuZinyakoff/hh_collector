from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from hhru_platform.application.commands.export_research_archive import (
    INCREMENTAL_RESEARCH_ARCHIVE_DATASETS,
)
from hhru_platform.infrastructure.observability.operations import (
    log_operation_started,
    record_operation_failed,
    record_operation_succeeded,
)
from hhru_platform.infrastructure.research_archive import (
    ResearchArchiveCheckpoint,
    ResearchArchiveCheckpointDataset,
    ResearchArchiveCheckpointVerificationReceipt,
    ResearchArchiveOffsiteVerificationReceipt,
)

LOGGER = logging.getLogger(__name__)

RESEARCH_ARCHIVE_COVERAGE_STATUS_COMPLETE = "complete"
RESEARCH_ARCHIVE_COVERAGE_STATUS_INCOMPLETE = "incomplete"


@dataclass(slots=True, frozen=True)
class AuditResearchArchiveCoverageCommand:
    archive_dir: Path = Path(".state/archive/research")
    archive_kind: str = "production"
    datasets: tuple[str, ...] = INCREMENTAL_RESEARCH_ARCHIVE_DATASETS
    offsite_url: str = ""
    offsite_root: str = "/hhru-platform/research-archive"
    triggered_by: str = "audit-research-archive-coverage"

    def __post_init__(self) -> None:
        normalized_archive_kind = self.archive_kind.strip()
        normalized_offsite_url = self.offsite_url.strip().rstrip("/")
        normalized_offsite_root = _normalize_offsite_root(self.offsite_root)
        normalized_triggered_by = self.triggered_by.strip()
        normalized_datasets = tuple(dataset.strip() for dataset in self.datasets)
        if not normalized_archive_kind:
            raise ValueError("archive_kind must not be empty")
        if not normalized_triggered_by:
            raise ValueError("triggered_by must not be empty")
        if not normalized_offsite_url:
            raise ValueError("offsite_url must not be empty")
        if not normalized_datasets:
            raise ValueError("datasets must not be empty")
        unsupported = sorted(
            set(normalized_datasets) - set(INCREMENTAL_RESEARCH_ARCHIVE_DATASETS)
        )
        if unsupported:
            raise ValueError(
                "coverage audit supports only append-only datasets: "
                f"{', '.join(INCREMENTAL_RESEARCH_ARCHIVE_DATASETS)}; "
                f"unsupported: {', '.join(unsupported)}"
            )
        object.__setattr__(self, "archive_dir", Path(self.archive_dir))
        object.__setattr__(self, "archive_kind", normalized_archive_kind)
        object.__setattr__(self, "datasets", normalized_datasets)
        object.__setattr__(self, "offsite_url", normalized_offsite_url)
        object.__setattr__(self, "offsite_root", normalized_offsite_root)
        object.__setattr__(self, "triggered_by", normalized_triggered_by)


@dataclass(slots=True, frozen=True)
class ResearchArchiveCoverageIssue:
    dataset: str
    checkpoint_file: Path | None
    message: str


@dataclass(slots=True, frozen=True)
class ResearchArchiveDatasetCoverageSummary:
    dataset: str
    status: str
    scanned_checkpoint_count: int
    verified_checkpoint_count: int
    verified_manifest_count: int
    verified_row_count: int
    source_id_covered: int
    issues: tuple[ResearchArchiveCoverageIssue, ...]


@dataclass(slots=True, frozen=True)
class AuditResearchArchiveCoverageResult:
    status: str
    archive_dir: Path
    archive_kind: str
    triggered_by: str
    summaries: tuple[ResearchArchiveDatasetCoverageSummary, ...]

    @property
    def complete(self) -> bool:
        return self.status == RESEARCH_ARCHIVE_COVERAGE_STATUS_COMPLETE

    @property
    def issue_count(self) -> int:
        return sum(len(summary.issues) for summary in self.summaries)


class ResearchArchiveCheckpointStore(Protocol):
    def load_checkpoints(
        self,
        *,
        archive_dir: Path,
        archive_kind: str,
    ) -> tuple[ResearchArchiveCheckpoint, ...]:
        """Load ordered incremental export checkpoints for one archive kind."""


class ResearchArchiveOffsiteVerificationReceiptStore(Protocol):
    def load_receipt(
        self,
        *,
        manifest_file: Path,
    ) -> ResearchArchiveOffsiteVerificationReceipt | None:
        """Load proof that one archive chunk passed offsite verification."""


class ResearchArchiveCheckpointVerificationReceiptStore(Protocol):
    def load_receipt(
        self,
        *,
        checkpoint_file: Path,
    ) -> ResearchArchiveCheckpointVerificationReceipt | None:
        """Load proof that one checkpoint passed offsite verification."""


def audit_research_archive_coverage(
    command: AuditResearchArchiveCoverageCommand,
    *,
    checkpoint_store: ResearchArchiveCheckpointStore,
    receipt_store: ResearchArchiveOffsiteVerificationReceiptStore,
    checkpoint_receipt_store: ResearchArchiveCheckpointVerificationReceiptStore,
) -> AuditResearchArchiveCoverageResult:
    started_at = log_operation_started(
        LOGGER,
        operation="audit_research_archive_coverage",
        archive_dir=str(command.archive_dir),
        archive_kind=command.archive_kind,
        datasets=",".join(command.datasets),
        offsite_url=command.offsite_url,
        offsite_root=command.offsite_root,
        triggered_by=command.triggered_by,
    )
    try:
        checkpoints = checkpoint_store.load_checkpoints(
            archive_dir=command.archive_dir,
            archive_kind=command.archive_kind,
        )
        summaries = tuple(
            _audit_dataset(
                dataset=dataset,
                archive_kind=command.archive_kind,
                offsite_url=command.offsite_url,
                offsite_root=command.offsite_root,
                checkpoints=checkpoints,
                receipt_store=receipt_store,
                checkpoint_receipt_store=checkpoint_receipt_store,
            )
            for dataset in command.datasets
        )
    except Exception as error:
        record_operation_failed(
            LOGGER,
            operation="audit_research_archive_coverage",
            started_at=started_at,
            error_type=error.__class__.__name__,
            error_message=str(error),
            archive_dir=str(command.archive_dir),
            archive_kind=command.archive_kind,
            offsite_url=command.offsite_url,
            offsite_root=command.offsite_root,
            triggered_by=command.triggered_by,
        )
        raise

    status = (
        RESEARCH_ARCHIVE_COVERAGE_STATUS_COMPLETE
        if all(summary.status == RESEARCH_ARCHIVE_COVERAGE_STATUS_COMPLETE for summary in summaries)
        else RESEARCH_ARCHIVE_COVERAGE_STATUS_INCOMPLETE
    )
    result = AuditResearchArchiveCoverageResult(
        status=status,
        archive_dir=command.archive_dir,
        archive_kind=command.archive_kind,
        triggered_by=command.triggered_by,
        summaries=summaries,
    )
    record_operation_succeeded(
        LOGGER,
        operation="audit_research_archive_coverage",
        started_at=started_at,
        archive_dir=str(result.archive_dir),
        archive_kind=result.archive_kind,
        offsite_url=command.offsite_url,
        offsite_root=command.offsite_root,
        triggered_by=result.triggered_by,
        coverage_status=result.status,
        issue_count=result.issue_count,
    )
    return result


def _audit_dataset(
    *,
    dataset: str,
    archive_kind: str,
    offsite_url: str,
    offsite_root: str,
    checkpoints: tuple[ResearchArchiveCheckpoint, ...],
    receipt_store: ResearchArchiveOffsiteVerificationReceiptStore,
    checkpoint_receipt_store: ResearchArchiveCheckpointVerificationReceiptStore,
) -> ResearchArchiveDatasetCoverageSummary:
    entries = tuple(
        (checkpoint, checkpoint_dataset)
        for checkpoint in checkpoints
        for checkpoint_dataset in checkpoint.datasets
        if checkpoint_dataset.dataset == dataset
    )
    if not entries:
        issue = ResearchArchiveCoverageIssue(
            dataset=dataset,
            checkpoint_file=None,
            message="no incremental checkpoints found",
        )
        return _summary(dataset=dataset, scanned_checkpoint_count=0, issues=(issue,))

    source_id_covered = 0
    verified_checkpoint_count = 0
    verified_manifest_count = 0
    verified_row_count = 0
    seen_manifest_files: set[Path] = set()
    issues: list[ResearchArchiveCoverageIssue] = []
    for checkpoint, checkpoint_dataset in entries:
        checkpoint_issues = _audit_checkpoint_dataset(
            checkpoint=checkpoint,
            checkpoint_dataset=checkpoint_dataset,
            archive_kind=archive_kind,
            offsite_url=offsite_url,
            offsite_root=offsite_root,
            expected_source_id_before=source_id_covered,
            seen_manifest_files=seen_manifest_files,
            receipt_store=receipt_store,
            checkpoint_receipt_store=checkpoint_receipt_store,
        )
        if checkpoint_issues:
            issues.extend(checkpoint_issues)
            break
        source_id_covered = checkpoint_dataset.source_id_after
        verified_checkpoint_count += 1
        verified_manifest_count += checkpoint_dataset.chunk_count
        verified_row_count += checkpoint_dataset.row_count
        seen_manifest_files.update(checkpoint_dataset.manifest_files)

    return _summary(
        dataset=dataset,
        scanned_checkpoint_count=len(entries),
        verified_checkpoint_count=verified_checkpoint_count,
        verified_manifest_count=verified_manifest_count,
        verified_row_count=verified_row_count,
        source_id_covered=source_id_covered,
        issues=tuple(issues),
    )


def _audit_checkpoint_dataset(
    *,
    checkpoint: ResearchArchiveCheckpoint,
    checkpoint_dataset: ResearchArchiveCheckpointDataset,
    archive_kind: str,
    offsite_url: str,
    offsite_root: str,
    expected_source_id_before: int,
    seen_manifest_files: set[Path],
    receipt_store: ResearchArchiveOffsiteVerificationReceiptStore,
    checkpoint_receipt_store: ResearchArchiveCheckpointVerificationReceiptStore,
) -> tuple[ResearchArchiveCoverageIssue, ...]:
    issue = _checkpoint_shape_issue(
        checkpoint=checkpoint,
        checkpoint_dataset=checkpoint_dataset,
        expected_source_id_before=expected_source_id_before,
    )
    if issue is not None:
        return (issue,)
    checkpoint_receipt_issue = _checkpoint_receipt_issue(
        checkpoint=checkpoint,
        offsite_url=offsite_url,
        offsite_root=offsite_root,
        checkpoint_receipt_store=checkpoint_receipt_store,
    )
    if checkpoint_receipt_issue is not None:
        return (
            _issue(
                checkpoint=checkpoint,
                dataset=checkpoint_dataset.dataset,
                message=checkpoint_receipt_issue,
            ),
        )

    row_count = 0
    source_ids: list[int] = []
    for manifest_file in checkpoint_dataset.manifest_files:
        if manifest_file in seen_manifest_files:
            return (
                _issue(
                    checkpoint=checkpoint,
                    dataset=checkpoint_dataset.dataset,
                    message=f"manifest is referenced by multiple checkpoints: {manifest_file}",
                ),
            )
        manifest_issue, manifest_row_count, manifest_source_ids = _audit_manifest(
            checkpoint=checkpoint,
            checkpoint_dataset=checkpoint_dataset,
            archive_kind=archive_kind,
            offsite_url=offsite_url,
            offsite_root=offsite_root,
            manifest_file=manifest_file,
            receipt_store=receipt_store,
        )
        if manifest_issue is not None:
            return (manifest_issue,)
        row_count += manifest_row_count
        source_ids.extend(manifest_source_ids)

    if row_count != checkpoint_dataset.row_count:
        return (
            _issue(
                checkpoint=checkpoint,
                dataset=checkpoint_dataset.dataset,
                message=(
                    f"checkpoint row_count mismatch: expected {checkpoint_dataset.row_count}, "
                    f"got {row_count} from manifests"
                ),
            ),
        )
    if source_ids:
        if min(source_ids) <= checkpoint_dataset.source_id_before:
            return (
                _issue(
                    checkpoint=checkpoint,
                    dataset=checkpoint_dataset.dataset,
                    message="manifest source range overlaps the previous checkpoint cursor",
                ),
            )
        if max(source_ids) != checkpoint_dataset.source_id_after:
            return (
                _issue(
                    checkpoint=checkpoint,
                    dataset=checkpoint_dataset.dataset,
                    message=(
                        "checkpoint source_id_after mismatch: expected "
                        f"{checkpoint_dataset.source_id_after}, got {max(source_ids)} "
                        "from manifests"
                    ),
                ),
            )
    return ()


def _checkpoint_shape_issue(
    *,
    checkpoint: ResearchArchiveCheckpoint,
    checkpoint_dataset: ResearchArchiveCheckpointDataset,
    expected_source_id_before: int,
) -> ResearchArchiveCoverageIssue | None:
    if checkpoint_dataset.source_id_before != expected_source_id_before:
        return _issue(
            checkpoint=checkpoint,
            dataset=checkpoint_dataset.dataset,
            message=(
                f"checkpoint chain break: expected source_id_before={expected_source_id_before}, "
                f"got {checkpoint_dataset.source_id_before}"
            ),
        )
    if checkpoint_dataset.source_id_after < checkpoint_dataset.source_id_before:
        return _issue(
            checkpoint=checkpoint,
            dataset=checkpoint_dataset.dataset,
            message="checkpoint source_id_after must not be lower than source_id_before",
        )
    if checkpoint_dataset.chunk_count != len(checkpoint_dataset.manifest_files):
        return _issue(
            checkpoint=checkpoint,
            dataset=checkpoint_dataset.dataset,
            message="checkpoint chunk_count does not match manifest_files",
        )
    has_progress = checkpoint_dataset.source_id_after > checkpoint_dataset.source_id_before
    if has_progress != bool(checkpoint_dataset.manifest_files):
        return _issue(
            checkpoint=checkpoint,
            dataset=checkpoint_dataset.dataset,
            message="checkpoint cursor progress and manifest_files disagree",
        )
    if bool(checkpoint_dataset.row_count) != bool(checkpoint_dataset.manifest_files):
        return _issue(
            checkpoint=checkpoint,
            dataset=checkpoint_dataset.dataset,
            message="checkpoint row_count and manifest_files disagree",
        )
    return None


def _checkpoint_receipt_issue(
    *,
    checkpoint: ResearchArchiveCheckpoint,
    offsite_url: str,
    offsite_root: str,
    checkpoint_receipt_store: ResearchArchiveCheckpointVerificationReceiptStore,
) -> str | None:
    receipt = checkpoint_receipt_store.load_receipt(
        checkpoint_file=checkpoint.checkpoint_file,
    )
    if receipt is None:
        return f"checkpoint offsite verification receipt not found: {checkpoint.checkpoint_file}"
    if receipt.offsite_url != offsite_url or receipt.offsite_root != offsite_root:
        return (
            "checkpoint offsite verification receipt target mismatch: "
            f"{checkpoint.checkpoint_file}"
        )
    if receipt.checkpoint_size_bytes != checkpoint.checkpoint_file.stat().st_size:
        return (
            "checkpoint offsite verification receipt size mismatch: "
            f"{checkpoint.checkpoint_file}"
        )
    if receipt.checkpoint_sha256 != _sha256(checkpoint.checkpoint_file):
        return (
            "checkpoint offsite verification receipt sha256 mismatch: "
            f"{checkpoint.checkpoint_file}"
        )
    return None


def _audit_manifest(
    *,
    checkpoint: ResearchArchiveCheckpoint,
    checkpoint_dataset: ResearchArchiveCheckpointDataset,
    archive_kind: str,
    offsite_url: str,
    offsite_root: str,
    manifest_file: Path,
    receipt_store: ResearchArchiveOffsiteVerificationReceiptStore,
) -> tuple[ResearchArchiveCoverageIssue | None, int, list[int]]:
    if not manifest_file.is_file():
        return (
            _issue(
                checkpoint=checkpoint,
                dataset=checkpoint_dataset.dataset,
                message=f"manifest file not found: {manifest_file}",
            ),
            0,
            [],
        )
    payload = json.loads(manifest_file.read_text(encoding="utf-8"))
    if payload.get("dataset_key") != checkpoint_dataset.dataset:
        return (
            _issue(
                checkpoint=checkpoint,
                dataset=checkpoint_dataset.dataset,
                message=f"manifest dataset mismatch: {manifest_file}",
            ),
            0,
            [],
        )
    if payload.get("archive_kind") != archive_kind:
        return (
            _issue(
                checkpoint=checkpoint,
                dataset=checkpoint_dataset.dataset,
                message=f"manifest archive_kind mismatch: {manifest_file}",
            ),
            0,
            [],
        )
    receipt = receipt_store.load_receipt(manifest_file=manifest_file)
    manifest_sha256 = _sha256(manifest_file)
    if receipt is None:
        return (
            _issue(
                checkpoint=checkpoint,
                dataset=checkpoint_dataset.dataset,
                message=f"offsite verification receipt not found: {manifest_file}",
            ),
            0,
            [],
        )
    receipt_issue = _receipt_issue(
        manifest_file=manifest_file,
        manifest_payload=payload,
        manifest_sha256=manifest_sha256,
        receipt=receipt,
        offsite_url=offsite_url,
        offsite_root=offsite_root,
    )
    if receipt_issue is not None:
        return (
            _issue(
                checkpoint=checkpoint,
                dataset=checkpoint_dataset.dataset,
                message=receipt_issue,
            ),
            0,
            [],
        )
    return (
        None,
        int(payload["row_count"]),
        [int(str(payload["source_min_id"])), int(str(payload["source_max_id"]))],
    )


def _receipt_issue(
    *,
    manifest_file: Path,
    manifest_payload: dict[str, object],
    manifest_sha256: str,
    receipt: ResearchArchiveOffsiteVerificationReceipt,
    offsite_url: str,
    offsite_root: str,
) -> str | None:
    expected = {
        "dataset": str(manifest_payload["dataset_key"]),
        "layer": str(manifest_payload["layer"]),
        "row_count": int(str(manifest_payload["row_count"])),
        "data_size_bytes": int(str(manifest_payload["data_size_bytes"])),
        "data_sha256": str(manifest_payload["data_sha256"]),
        "manifest_sha256": manifest_sha256,
    }
    actual = {
        "dataset": receipt.dataset,
        "layer": receipt.layer,
        "row_count": receipt.row_count,
        "data_size_bytes": receipt.data_size_bytes,
        "data_sha256": receipt.data_sha256,
        "manifest_sha256": receipt.manifest_sha256,
    }
    if actual != expected:
        return f"offsite verification receipt mismatch: {manifest_file}"
    if receipt.offsite_url != offsite_url or receipt.offsite_root != offsite_root:
        return f"offsite verification receipt target mismatch: {manifest_file}"
    if receipt.verified_object_count < 2:
        return f"offsite verification receipt has insufficient object count: {manifest_file}"
    return None


def _summary(
    *,
    dataset: str,
    scanned_checkpoint_count: int,
    verified_checkpoint_count: int = 0,
    verified_manifest_count: int = 0,
    verified_row_count: int = 0,
    source_id_covered: int = 0,
    issues: tuple[ResearchArchiveCoverageIssue, ...],
) -> ResearchArchiveDatasetCoverageSummary:
    return ResearchArchiveDatasetCoverageSummary(
        dataset=dataset,
        status=(
            RESEARCH_ARCHIVE_COVERAGE_STATUS_COMPLETE
            if not issues
            else RESEARCH_ARCHIVE_COVERAGE_STATUS_INCOMPLETE
        ),
        scanned_checkpoint_count=scanned_checkpoint_count,
        verified_checkpoint_count=verified_checkpoint_count,
        verified_manifest_count=verified_manifest_count,
        verified_row_count=verified_row_count,
        source_id_covered=source_id_covered,
        issues=issues,
    )


def _issue(
    *,
    checkpoint: ResearchArchiveCheckpoint,
    dataset: str,
    message: str,
) -> ResearchArchiveCoverageIssue:
    return ResearchArchiveCoverageIssue(
        dataset=dataset,
        checkpoint_file=checkpoint.checkpoint_file,
        message=message,
    )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _normalize_offsite_root(offsite_root: str) -> str:
    parts = tuple(part for part in offsite_root.strip().split("/") if part)
    if not parts:
        return "/"
    return "/" + "/".join(parts)
