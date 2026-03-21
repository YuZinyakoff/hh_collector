from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol

from hhru_platform.infrastructure.backup.backup_service import (
    BackupService,
    RestoreDrillSummary,
)
from hhru_platform.infrastructure.observability.operations import (
    log_operation_started,
    record_operation_failed,
    record_operation_succeeded,
)

LOGGER = logging.getLogger(__name__)

RESTORE_DRILL_STATUS_SUCCEEDED = "succeeded"
RESTORE_DRILL_STATUS_FAILED = "failed"


@dataclass(slots=True, frozen=True)
class RunRestoreDrillCommand:
    backup_file: Path
    target_db: str
    drop_target_db: bool = True
    triggered_by: str = "run-restore-drill"
    recorded_at: datetime | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "backup_file", Path(self.backup_file))
        normalized_target_db = self.target_db.strip()
        if not normalized_target_db:
            raise ValueError("target_db must not be empty")
        object.__setattr__(self, "target_db", normalized_target_db)
        normalized_triggered_by = self.triggered_by.strip()
        if not normalized_triggered_by:
            raise ValueError("triggered_by must not be empty")
        object.__setattr__(self, "triggered_by", normalized_triggered_by)


@dataclass(slots=True, frozen=True)
class RunRestoreDrillResult:
    status: str
    triggered_by: str
    recorded_at: datetime
    backup_file: Path
    target_db: str
    archive_entry_count: int
    checked_tables: tuple[str, ...]
    verified_tables_count: int
    schema_verified: bool


class RestoreDrillMetricsRecorder(Protocol):
    def record_restore_drill_run(
        self,
        *,
        status: str,
        recorded_at: datetime,
    ) -> None:
        """Persist one restore drill lifecycle outcome."""


def run_restore_drill(
    command: RunRestoreDrillCommand,
    *,
    backup_service: BackupService,
    metrics_recorder: RestoreDrillMetricsRecorder | None = None,
) -> RunRestoreDrillResult:
    started_at = log_operation_started(
        LOGGER,
        operation="run_restore_drill",
        triggered_by=command.triggered_by,
        target_db=command.target_db,
        drop_target_db=command.drop_target_db,
    )
    recorded_at = command.recorded_at or datetime.now(UTC)

    try:
        restore_summary = backup_service.restore_to_target_db(
            backup_file=command.backup_file,
            target_db=command.target_db,
            drop_existing=command.drop_target_db,
        )
    except Exception as error:
        if metrics_recorder is not None:
            metrics_recorder.record_restore_drill_run(
                status=RESTORE_DRILL_STATUS_FAILED,
                recorded_at=recorded_at,
            )
        record_operation_failed(
            LOGGER,
            operation="run_restore_drill",
            started_at=started_at,
            error_type=error.__class__.__name__,
            error_message=str(error),
            triggered_by=command.triggered_by,
            target_db=command.target_db,
        )
        raise

    result = _to_result(
        command=command,
        recorded_at=recorded_at,
        restore_summary=restore_summary,
    )
    if metrics_recorder is not None:
        metrics_recorder.record_restore_drill_run(
            status=result.status,
            recorded_at=result.recorded_at,
        )
    record_operation_succeeded(
        LOGGER,
        operation="run_restore_drill",
        started_at=started_at,
        triggered_by=result.triggered_by,
        target_db=result.target_db,
        backup_file=str(result.backup_file),
        schema_verified=result.schema_verified,
        verified_tables_count=result.verified_tables_count,
    )
    return result


def _to_result(
    *,
    command: RunRestoreDrillCommand,
    recorded_at: datetime,
    restore_summary: RestoreDrillSummary,
) -> RunRestoreDrillResult:
    return RunRestoreDrillResult(
        status=RESTORE_DRILL_STATUS_SUCCEEDED,
        triggered_by=command.triggered_by,
        recorded_at=recorded_at,
        backup_file=restore_summary.backup_file,
        target_db=restore_summary.target_db,
        archive_entry_count=restore_summary.archive_entry_count,
        checked_tables=restore_summary.required_tables,
        verified_tables_count=restore_summary.present_table_count,
        schema_verified=restore_summary.schema_verified,
    )
