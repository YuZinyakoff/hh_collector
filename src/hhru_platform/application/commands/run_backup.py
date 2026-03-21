from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol

from hhru_platform.infrastructure.backup.backup_service import (
    BackupArchiveSummary,
    BackupService,
)
from hhru_platform.infrastructure.observability.operations import (
    log_operation_started,
    record_operation_failed,
    record_operation_succeeded,
)

LOGGER = logging.getLogger(__name__)

BACKUP_STATUS_SUCCEEDED = "succeeded"
BACKUP_STATUS_FAILED = "failed"


@dataclass(slots=True, frozen=True)
class RunBackupCommand:
    triggered_by: str = "run-backup"
    recorded_at: datetime | None = None

    def __post_init__(self) -> None:
        normalized_triggered_by = self.triggered_by.strip()
        if not normalized_triggered_by:
            raise ValueError("triggered_by must not be empty")
        object.__setattr__(self, "triggered_by", normalized_triggered_by)


@dataclass(slots=True, frozen=True)
class RunBackupResult:
    status: str
    triggered_by: str
    recorded_at: datetime
    backup_file: Path
    backup_size_bytes: int
    backup_sha256: str
    archive_entry_count: int


class BackupMetricsRecorder(Protocol):
    def record_backup_run(
        self,
        *,
        status: str,
        recorded_at: datetime,
    ) -> None:
        """Persist one backup lifecycle outcome."""


def run_backup(
    command: RunBackupCommand,
    *,
    backup_service: BackupService,
    metrics_recorder: BackupMetricsRecorder | None = None,
) -> RunBackupResult:
    started_at = log_operation_started(
        LOGGER,
        operation="run_backup",
        triggered_by=command.triggered_by,
    )
    recorded_at = command.recorded_at or datetime.now(UTC)

    try:
        backup_summary = backup_service.create_backup()
    except Exception as error:
        if metrics_recorder is not None:
            metrics_recorder.record_backup_run(
                status=BACKUP_STATUS_FAILED,
                recorded_at=recorded_at,
            )
        record_operation_failed(
            LOGGER,
            operation="run_backup",
            started_at=started_at,
            error_type=error.__class__.__name__,
            error_message=str(error),
            triggered_by=command.triggered_by,
        )
        raise

    result = _to_result(
        command=command,
        recorded_at=recorded_at,
        backup_summary=backup_summary,
    )
    if metrics_recorder is not None:
        metrics_recorder.record_backup_run(
            status=result.status,
            recorded_at=result.recorded_at,
        )
    record_operation_succeeded(
        LOGGER,
        operation="run_backup",
        started_at=started_at,
        triggered_by=result.triggered_by,
        backup_file=str(result.backup_file),
        backup_size_bytes=result.backup_size_bytes,
        archive_entry_count=result.archive_entry_count,
    )
    return result


def _to_result(
    *,
    command: RunBackupCommand,
    recorded_at: datetime,
    backup_summary: BackupArchiveSummary,
) -> RunBackupResult:
    return RunBackupResult(
        status=BACKUP_STATUS_SUCCEEDED,
        triggered_by=command.triggered_by,
        recorded_at=recorded_at,
        backup_file=backup_summary.backup_file,
        backup_size_bytes=backup_summary.size_bytes,
        backup_sha256=backup_summary.sha256,
        archive_entry_count=backup_summary.archive_entry_count,
    )
