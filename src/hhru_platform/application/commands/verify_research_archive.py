from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

from hhru_platform.infrastructure.observability.operations import (
    log_operation_started,
    record_operation_failed,
    record_operation_succeeded,
)
from hhru_platform.infrastructure.research_archive import (
    ResearchArchiveManifestVerifier,
    ResearchArchiveVerificationSummary,
)

LOGGER = logging.getLogger(__name__)

RESEARCH_ARCHIVE_VERIFY_STATUS_SUCCEEDED = "succeeded"


@dataclass(slots=True, frozen=True)
class VerifyResearchArchiveCommand:
    archive_dir: Path = Path(".state/archive/research")
    manifest_files: tuple[Path, ...] = ()
    limit: int | None = None
    triggered_by: str = "verify-research-archive"

    def __post_init__(self) -> None:
        normalized_triggered_by = self.triggered_by.strip()
        if not normalized_triggered_by:
            raise ValueError("triggered_by must not be empty")
        if self.limit is not None and self.limit < 1:
            raise ValueError("limit must be greater than or equal to one")
        object.__setattr__(self, "archive_dir", Path(self.archive_dir))
        object.__setattr__(
            self, "manifest_files", tuple(Path(path) for path in self.manifest_files)
        )
        object.__setattr__(self, "triggered_by", normalized_triggered_by)


@dataclass(slots=True, frozen=True)
class VerifyResearchArchiveResult:
    status: str
    archive_dir: Path
    triggered_by: str
    scanned_manifest_count: int
    verified_manifest_count: int
    total_row_count: int
    total_data_size_bytes: int
    summaries: tuple[ResearchArchiveVerificationSummary, ...]


def verify_research_archive(
    command: VerifyResearchArchiveCommand,
    *,
    manifest_verifier: ResearchArchiveManifestVerifier,
) -> VerifyResearchArchiveResult:
    started_at = log_operation_started(
        LOGGER,
        operation="verify_research_archive",
        archive_dir=str(command.archive_dir),
        triggered_by=command.triggered_by,
    )
    try:
        summaries = manifest_verifier.verify(
            archive_dir=command.archive_dir,
            manifest_files=command.manifest_files,
            limit=command.limit,
        )
    except Exception as error:
        record_operation_failed(
            LOGGER,
            operation="verify_research_archive",
            started_at=started_at,
            error_type=error.__class__.__name__,
            error_message=str(error),
            archive_dir=str(command.archive_dir),
            triggered_by=command.triggered_by,
        )
        raise

    result = VerifyResearchArchiveResult(
        status=RESEARCH_ARCHIVE_VERIFY_STATUS_SUCCEEDED,
        archive_dir=command.archive_dir,
        triggered_by=command.triggered_by,
        scanned_manifest_count=len(summaries),
        verified_manifest_count=sum(1 for summary in summaries if summary.verified),
        total_row_count=sum(summary.row_count for summary in summaries),
        total_data_size_bytes=sum(summary.data_size_bytes for summary in summaries),
        summaries=summaries,
    )
    record_operation_succeeded(
        LOGGER,
        operation="verify_research_archive",
        started_at=started_at,
        archive_dir=str(result.archive_dir),
        triggered_by=result.triggered_by,
        scanned_manifest_count=result.scanned_manifest_count,
        verified_manifest_count=result.verified_manifest_count,
        total_row_count=result.total_row_count,
        total_data_size_bytes=result.total_data_size_bytes,
    )
    return result
