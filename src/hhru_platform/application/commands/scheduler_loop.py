from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Protocol
from uuid import UUID

from hhru_platform.application.commands.run_collection_once_v2 import (
    RunCollectionOnceV2Command,
)
from hhru_platform.application.commands.trigger_run_now import (
    TriggerRunNowCommand,
    TriggerRunNowResult,
)
from hhru_platform.infrastructure.observability.logging import log_event

LOGGER = logging.getLogger(__name__)


@dataclass(slots=True, frozen=True)
class SchedulerLoopCommand:
    interval_seconds: float
    run_command: RunCollectionOnceV2Command
    max_ticks: int | None = None

    def __post_init__(self) -> None:
        if self.interval_seconds <= 0:
            raise ValueError("interval_seconds must be greater than zero")
        if self.max_ticks is not None and self.max_ticks < 1:
            raise ValueError("max_ticks must be greater than or equal to one")


@dataclass(slots=True, frozen=True)
class SchedulerLoopResult:
    tick_results: tuple[TriggerRunNowResult, ...]

    @property
    def ticks_executed(self) -> int:
        return len(self.tick_results)

    @property
    def runs_started(self) -> int:
        return sum(1 for result in self.tick_results if result.started_run)

    @property
    def skipped_overlap_ticks(self) -> int:
        return sum(1 for result in self.tick_results if result.status == "skipped_overlap")

    @property
    def skipped_active_run_ticks(self) -> int:
        return sum(1 for result in self.tick_results if result.status == "skipped_active_run")

    @property
    def succeeded_runs(self) -> int:
        return sum(1 for result in self.tick_results if result.status == "succeeded")

    @property
    def completed_with_detail_errors_runs(self) -> int:
        return sum(
            1
            for result in self.tick_results
            if result.status == "completed_with_detail_errors"
        )

    @property
    def completed_with_unresolved_runs(self) -> int:
        return sum(
            1
            for result in self.tick_results
            if result.status == "completed_with_unresolved"
        )

    @property
    def failed_runs(self) -> int:
        return sum(1 for result in self.tick_results if result.status == "failed")

    @property
    def last_tick_status(self) -> str | None:
        if not self.tick_results:
            return None
        return self.tick_results[-1].status

    @property
    def last_run_id(self) -> UUID | None:
        for result in reversed(self.tick_results):
            if result.run_id is not None:
                return result.run_id
        return None


class TriggerRunNowStep(Protocol):
    def __call__(self, command: TriggerRunNowCommand) -> TriggerRunNowResult:
        """Execute one guarded attempt to start a new collection run."""


class SleepStep(Protocol):
    def __call__(self, seconds: float) -> None:
        """Sleep between scheduler ticks."""


def scheduler_loop(
    command: SchedulerLoopCommand,
    *,
    trigger_run_now_step: TriggerRunNowStep,
    sleep_step: SleepStep,
) -> SchedulerLoopResult:
    log_event(
        LOGGER,
        logging.INFO,
        "scheduler_loop.started",
        operation="scheduler_loop",
        status="started",
        interval_seconds=command.interval_seconds,
        max_ticks=command.max_ticks,
        triggered_by=command.run_command.triggered_by,
        run_type=command.run_command.run_type,
    )
    tick_results: list[TriggerRunNowResult] = []

    while command.max_ticks is None or len(tick_results) < command.max_ticks:
        tick_results.append(
            trigger_run_now_step(TriggerRunNowCommand(run_command=command.run_command))
        )
        if command.max_ticks is not None and len(tick_results) >= command.max_ticks:
            break
        sleep_step(command.interval_seconds)

    result = SchedulerLoopResult(tick_results=tuple(tick_results))
    log_event(
        LOGGER,
        logging.INFO,
        "scheduler_loop.completed",
        operation="scheduler_loop",
        status="completed",
        ticks_executed=result.ticks_executed,
        runs_started=result.runs_started,
        skipped_overlap_ticks=result.skipped_overlap_ticks,
        skipped_active_run_ticks=result.skipped_active_run_ticks,
        succeeded_runs=result.succeeded_runs,
        completed_with_detail_errors_runs=result.completed_with_detail_errors_runs,
        completed_with_unresolved_runs=result.completed_with_unresolved_runs,
        failed_runs=result.failed_runs,
        last_tick_status=result.last_tick_status,
        last_run_id=result.last_run_id,
        triggered_by=command.run_command.triggered_by,
        run_type=command.run_command.run_type,
    )
    return result
