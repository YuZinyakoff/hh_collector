from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol

from hhru_platform.application.commands.run_housekeeping import (
    TARGET_RAW_API_PAYLOAD,
    TARGET_VACANCY_SNAPSHOT,
    HousekeepingRetentionPolicy,
)
from hhru_platform.infrastructure.housekeeping import RetentionArchiveFileSummary
from hhru_platform.infrastructure.observability.operations import (
    log_operation_started,
    record_operation_failed,
    record_operation_succeeded,
)

LOGGER = logging.getLogger(__name__)

RETENTION_ARCHIVE_STATUS_SUCCEEDED = "succeeded"
SUPPORTED_RETENTION_ARCHIVE_TARGETS = (
    TARGET_RAW_API_PAYLOAD,
    TARGET_VACANCY_SNAPSHOT,
)


@dataclass(slots=True, frozen=True)
class ExportRetentionArchiveCommand:
    retention_policy: HousekeepingRetentionPolicy
    archive_dir: Path = Path(".state/archive/retention")
    targets: tuple[str, ...] = SUPPORTED_RETENTION_ARCHIVE_TARGETS
    triggered_by: str = "export-retention-archive"
    evaluated_at: datetime | None = None

    def __post_init__(self) -> None:
        normalized_triggered_by = self.triggered_by.strip()
        normalized_targets = tuple(target.strip() for target in self.targets)
        if not normalized_triggered_by:
            raise ValueError("triggered_by must not be empty")
        if not normalized_targets:
            raise ValueError("targets must not be empty")
        if any(target not in SUPPORTED_RETENTION_ARCHIVE_TARGETS for target in normalized_targets):
            supported = ", ".join(SUPPORTED_RETENTION_ARCHIVE_TARGETS)
            raise ValueError(f"targets must be drawn from: {supported}")

        object.__setattr__(self, "triggered_by", normalized_triggered_by)
        object.__setattr__(self, "targets", normalized_targets)
        object.__setattr__(self, "archive_dir", Path(self.archive_dir))


@dataclass(slots=True, frozen=True)
class RetentionArchiveTargetSummary:
    target: str
    retention_days: int
    cutoff: datetime | None
    candidate_count: int
    exported_count: int
    archive_file: Path | None
    manifest_file: Path | None
    archive_size_bytes: int
    archive_sha256: str | None
    enabled: bool

    @property
    def limited(self) -> bool:
        return self.exported_count < self.candidate_count


@dataclass(slots=True, frozen=True)
class ExportRetentionArchiveResult:
    status: str
    triggered_by: str
    evaluated_at: datetime
    archive_dir: Path
    summaries: tuple[RetentionArchiveTargetSummary, ...]

    @property
    def total_candidates(self) -> int:
        return sum(summary.candidate_count for summary in self.summaries)

    @property
    def total_exported(self) -> int:
        return sum(summary.exported_count for summary in self.summaries)


class RetentionArchiveRepository(Protocol):
    def count_raw_api_payload_candidates(self, *, cutoff: datetime) -> int:
        """Count raw_api_payload rows eligible for archival."""

    def list_raw_api_payload_ids_for_retention(
        self,
        *,
        cutoff: datetime,
        limit: int,
    ) -> list[int]:
        """List raw_api_payload ids eligible for archival."""

    def list_raw_api_payload_rows_for_archive(
        self,
        *,
        payload_ids: tuple[int, ...],
    ) -> list[dict[str, object]]:
        """Return archive-ready raw_api_payload rows for the selected ids."""

    def count_vacancy_snapshot_candidates(self, *, cutoff: datetime) -> int:
        """Count vacancy_snapshot rows eligible for archival."""

    def list_vacancy_snapshot_ids_for_retention(
        self,
        *,
        cutoff: datetime,
        limit: int,
    ) -> list[int]:
        """List vacancy_snapshot ids eligible for archival."""

    def list_vacancy_snapshot_rows_for_archive(
        self,
        *,
        snapshot_ids: tuple[int, ...],
    ) -> list[dict[str, object]]:
        """Return archive-ready vacancy_snapshot rows for the selected ids."""


class RetentionArchiveStore(Protocol):
    def write_records(
        self,
        *,
        archive_dir: Path,
        target: str,
        evaluated_at: datetime,
        records: tuple[Mapping[str, Any], ...],
        metadata: Mapping[str, Any],
    ) -> RetentionArchiveFileSummary:
        """Persist one compressed archive chunk plus sidecar manifest."""


def export_retention_archive(
    command: ExportRetentionArchiveCommand,
    *,
    retention_archive_repository: RetentionArchiveRepository,
    retention_archive_store: RetentionArchiveStore,
) -> ExportRetentionArchiveResult:
    started_at = log_operation_started(
        LOGGER,
        operation="export_retention_archive",
        archive_dir=str(command.archive_dir),
        targets=",".join(command.targets),
        triggered_by=command.triggered_by,
    )
    evaluated_at = command.evaluated_at or datetime.now(UTC)

    try:
        summaries = tuple(
            _export_target(
                target=target,
                command=command,
                evaluated_at=evaluated_at,
                retention_archive_repository=retention_archive_repository,
                retention_archive_store=retention_archive_store,
            )
            for target in command.targets
        )
    except Exception as error:
        record_operation_failed(
            LOGGER,
            operation="export_retention_archive",
            started_at=started_at,
            error_type=error.__class__.__name__,
            error_message=str(error),
            archive_dir=str(command.archive_dir),
            triggered_by=command.triggered_by,
        )
        raise

    result = ExportRetentionArchiveResult(
        status=RETENTION_ARCHIVE_STATUS_SUCCEEDED,
        triggered_by=command.triggered_by,
        evaluated_at=evaluated_at,
        archive_dir=command.archive_dir,
        summaries=summaries,
    )
    record_operation_succeeded(
        LOGGER,
        operation="export_retention_archive",
        started_at=started_at,
        archive_dir=str(result.archive_dir),
        triggered_by=result.triggered_by,
        targets=",".join(command.targets),
        total_candidates=result.total_candidates,
        total_exported=result.total_exported,
    )
    return result


def _export_target(
    *,
    target: str,
    command: ExportRetentionArchiveCommand,
    evaluated_at: datetime,
    retention_archive_repository: RetentionArchiveRepository,
    retention_archive_store: RetentionArchiveStore,
) -> RetentionArchiveTargetSummary:
    retention_days = _retention_days_for_target(command.retention_policy, target)
    if retention_days <= 0:
        return RetentionArchiveTargetSummary(
            target=target,
            retention_days=retention_days,
            cutoff=None,
            candidate_count=0,
            exported_count=0,
            archive_file=None,
            manifest_file=None,
            archive_size_bytes=0,
            archive_sha256=None,
            enabled=False,
        )

    cutoff = evaluated_at - timedelta(days=retention_days)
    identifiers: tuple[int, ...]
    candidate_count: int
    rows: tuple[dict[str, object], ...]

    if target == TARGET_RAW_API_PAYLOAD:
        candidate_count = retention_archive_repository.count_raw_api_payload_candidates(
            cutoff=cutoff
        )
        identifiers = tuple(
            retention_archive_repository.list_raw_api_payload_ids_for_retention(
                cutoff=cutoff,
                limit=command.retention_policy.delete_limit_per_target,
            )
        )
        rows = tuple(
            retention_archive_repository.list_raw_api_payload_rows_for_archive(
                payload_ids=identifiers
            )
        )
    else:
        candidate_count = retention_archive_repository.count_vacancy_snapshot_candidates(
            cutoff=cutoff
        )
        identifiers = tuple(
            retention_archive_repository.list_vacancy_snapshot_ids_for_retention(
                cutoff=cutoff,
                limit=command.retention_policy.delete_limit_per_target,
            )
        )
        rows = tuple(
            retention_archive_repository.list_vacancy_snapshot_rows_for_archive(
                snapshot_ids=identifiers
            )
        )

    if not rows:
        return RetentionArchiveTargetSummary(
            target=target,
            retention_days=retention_days,
            cutoff=cutoff,
            candidate_count=candidate_count,
            exported_count=0,
            archive_file=None,
            manifest_file=None,
            archive_size_bytes=0,
            archive_sha256=None,
            enabled=True,
        )

    archive_summary = retention_archive_store.write_records(
        archive_dir=command.archive_dir,
        target=target,
        evaluated_at=evaluated_at,
        records=rows,
        metadata={
            "triggered_by": command.triggered_by,
            "cutoff": cutoff,
            "retention_days": retention_days,
            "candidate_count": candidate_count,
            "selected_ids": identifiers,
        },
    )
    return RetentionArchiveTargetSummary(
        target=target,
        retention_days=retention_days,
        cutoff=cutoff,
        candidate_count=candidate_count,
        exported_count=archive_summary.record_count,
        archive_file=archive_summary.archive_file,
        manifest_file=archive_summary.manifest_file,
        archive_size_bytes=archive_summary.archive_size_bytes,
        archive_sha256=archive_summary.archive_sha256,
        enabled=True,
    )


def _retention_days_for_target(policy: HousekeepingRetentionPolicy, target: str) -> int:
    if target == TARGET_RAW_API_PAYLOAD:
        return policy.raw_api_payload_retention_days
    if target == TARGET_VACANCY_SNAPSHOT:
        return policy.vacancy_snapshot_retention_days
    raise ValueError(f"unsupported retention archive target: {target}")
