from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol
from uuid import UUID

from hhru_platform.application.commands.run_collection_once_v2 import (
    RunCollectionOnceV2Command,
    RunCollectionOnceV2Result,
)
from hhru_platform.domain.entities.crawl_run import CrawlRun
from hhru_platform.infrastructure.observability.logging import log_event

LOGGER = logging.getLogger(__name__)

TRIGGER_RUN_NOW_STATUS_SKIPPED_OVERLAP = "skipped_overlap"
TRIGGER_RUN_NOW_STATUS_SKIPPED_ACTIVE_RUN = "skipped_active_run"


@dataclass(slots=True, frozen=True)
class TriggerRunNowCommand:
    run_command: RunCollectionOnceV2Command


@dataclass(slots=True, frozen=True)
class TriggerRunNowResult:
    status: str
    ticked_at: datetime
    run_started_at: datetime | None
    run_finished_at: datetime | None
    run_result: RunCollectionOnceV2Result | None
    active_run_id: UUID | None = None
    active_run_status: str | None = None
    error_message: str | None = None

    @property
    def run_id(self) -> UUID | None:
        if self.run_result is None:
            return None
        return self.run_result.run_id

    @property
    def started_run(self) -> bool:
        return self.run_result is not None


class SchedulerAdmissionLease(Protocol):
    def get_active_run(self) -> CrawlRun | None:
        """Return the current active collection run while admission is reserved."""

    def release(self) -> None:
        """Release the admission reservation."""


class SchedulerAdmissionController(Protocol):
    def acquire(self) -> SchedulerAdmissionLease | None:
        """Try to reserve exclusive admission for starting a new collection run."""


class SchedulerMetricsRecorder(Protocol):
    def record_scheduler_tick(
        self,
        *,
        outcome: str,
        ticked_at: datetime,
        run_started_at: datetime | None = None,
        run_finished_at: datetime | None = None,
        triggered_run_at: datetime | None = None,
        observed_run_status: str | None = None,
    ) -> None:
        """Persist scheduler health metrics for one admission attempt."""


def trigger_run_now(
    command: TriggerRunNowCommand,
    *,
    admission_controller: SchedulerAdmissionController,
    run_collection_once_v2_step,
    metrics_recorder: SchedulerMetricsRecorder | None = None,
    now: datetime | None = None,
) -> TriggerRunNowResult:
    ticked_at = now or datetime.now(UTC)
    log_event(
        LOGGER,
        logging.INFO,
        "trigger_run_now.started",
        operation="trigger_run_now",
        status="started",
        triggered_by=command.run_command.triggered_by,
        run_type=command.run_command.run_type,
    )

    lease = admission_controller.acquire()
    if lease is None:
        result = TriggerRunNowResult(
            status=TRIGGER_RUN_NOW_STATUS_SKIPPED_OVERLAP,
            ticked_at=ticked_at,
            run_started_at=None,
            run_finished_at=None,
            run_result=None,
            error_message="collection run admission lock is already held",
        )
        _record_scheduler_tick(metrics_recorder, result)
        log_event(
            LOGGER,
            logging.INFO,
            "trigger_run_now.skipped",
            operation="trigger_run_now",
            status=result.status,
            error_message=result.error_message,
            triggered_by=command.run_command.triggered_by,
            run_type=command.run_command.run_type,
        )
        return result

    try:
        active_run = lease.get_active_run()
        if active_run is not None:
            result = TriggerRunNowResult(
                status=TRIGGER_RUN_NOW_STATUS_SKIPPED_ACTIVE_RUN,
                ticked_at=ticked_at,
                run_started_at=None,
                run_finished_at=None,
                run_result=None,
                active_run_id=active_run.id,
                active_run_status=active_run.status,
                error_message=f"active crawl_run already exists: {active_run.id}",
            )
            _record_scheduler_tick(metrics_recorder, result)
            log_event(
                LOGGER,
                logging.INFO,
                "trigger_run_now.skipped",
                operation="trigger_run_now",
                status=result.status,
                active_run_id=active_run.id,
                active_run_status=active_run.status,
                error_message=result.error_message,
                triggered_by=command.run_command.triggered_by,
                run_type=command.run_command.run_type,
            )
            return result

        run_started_at = datetime.now(UTC)
        try:
            run_result = run_collection_once_v2_step(command.run_command)
        except Exception as error:
            result = TriggerRunNowResult(
                status="failed",
                ticked_at=ticked_at,
                run_started_at=run_started_at,
                run_finished_at=datetime.now(UTC),
                run_result=None,
                error_message=str(error),
            )
            _record_scheduler_tick(metrics_recorder, result)
            log_event(
                LOGGER,
                logging.ERROR,
                "trigger_run_now.failed",
                operation="trigger_run_now",
                status=result.status,
                error_type=error.__class__.__name__,
                error_message=result.error_message,
                triggered_by=command.run_command.triggered_by,
                run_type=command.run_command.run_type,
            )
            return result

        result = TriggerRunNowResult(
            status=run_result.status,
            ticked_at=ticked_at,
            run_started_at=run_started_at,
            run_finished_at=datetime.now(UTC),
            run_result=run_result,
            error_message=run_result.error_message,
        )
        _record_scheduler_tick(metrics_recorder, result)
        log_event(
            LOGGER,
            logging.INFO,
            "trigger_run_now.completed",
            operation="trigger_run_now",
            status=result.status,
            run_id=result.run_id,
            coverage_ratio=run_result.coverage_ratio,
            detail_fetch_failed=run_result.detail_fetch_failed,
            unresolved_partitions=run_result.unresolved_partitions,
            failed_partitions=run_result.failed_partitions,
            triggered_by=command.run_command.triggered_by,
            run_type=command.run_command.run_type,
            error_message=result.error_message,
        )
        return result
    finally:
        lease.release()


def _record_scheduler_tick(
    metrics_recorder: SchedulerMetricsRecorder | None,
    result: TriggerRunNowResult,
) -> None:
    if metrics_recorder is None:
        return

    metrics_recorder.record_scheduler_tick(
        outcome=result.status,
        ticked_at=result.ticked_at,
        run_started_at=result.run_started_at,
        run_finished_at=result.run_finished_at,
        triggered_run_at=result.run_started_at,
        observed_run_status=(
            result.status if result.run_result is not None else None
        ),
    )
