from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from hhru_platform.application.commands.run_collection_once_v2 import (
    RunCollectionOnceV2Command,
)
from hhru_platform.application.commands.scheduler_loop import (
    SchedulerLoopCommand,
    scheduler_loop,
)
from hhru_platform.application.commands.trigger_run_now import TriggerRunNowResult


def test_scheduler_loop_aggregates_tick_outcomes_and_sleeps_between_ticks() -> None:
    tick_results = iter(
        (
            _build_tick_result("skipped_overlap"),
            _build_tick_result("succeeded", run_id=uuid4()),
            _build_tick_result("completed_with_detail_errors", run_id=uuid4()),
        )
    )
    sleep_calls: list[float] = []

    result = scheduler_loop(
        SchedulerLoopCommand(
            interval_seconds=60.0,
            max_ticks=3,
            run_command=RunCollectionOnceV2Command(
                sync_dictionaries=False,
                detail_limit=10,
                detail_refresh_ttl_days=30,
                run_type="weekly_sweep",
                triggered_by="scheduler-loop",
            ),
        ),
        trigger_run_now_step=lambda command: next(tick_results),
        sleep_step=lambda seconds: sleep_calls.append(seconds),
    )

    assert result.ticks_executed == 3
    assert result.runs_started == 2
    assert result.skipped_overlap_ticks == 1
    assert result.skipped_active_run_ticks == 0
    assert result.succeeded_runs == 1
    assert result.completed_with_detail_errors_runs == 1
    assert result.completed_with_unresolved_runs == 0
    assert result.failed_runs == 0
    assert result.last_tick_status == "completed_with_detail_errors"
    assert result.last_run_id is not None
    assert sleep_calls == [60.0, 60.0]


def _build_tick_result(status: str, *, run_id=None) -> TriggerRunNowResult:
    return TriggerRunNowResult(
        status=status,
        ticked_at=datetime(2026, 3, 20, 10, 0, tzinfo=UTC),
        run_started_at=(
            datetime(2026, 3, 20, 10, 0, tzinfo=UTC) if run_id is not None else None
        ),
        run_finished_at=(
            datetime(2026, 3, 20, 10, 5, tzinfo=UTC) if run_id is not None else None
        ),
        run_result=None,
        error_message=None,
    ) if run_id is None else TriggerRunNowResult(
        status=status,
        ticked_at=datetime(2026, 3, 20, 10, 0, tzinfo=UTC),
        run_started_at=datetime(2026, 3, 20, 10, 0, tzinfo=UTC),
        run_finished_at=datetime(2026, 3, 20, 10, 5, tzinfo=UTC),
        run_result=type("RunResult", (), {"run_id": run_id})(),
        error_message=None,
    )
