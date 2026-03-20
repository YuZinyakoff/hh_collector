from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from hhru_platform.application.commands.reconcile_run import ReconcileRunResult
from hhru_platform.application.commands.run_collection_once_v2 import (
    RunCollectionOnceV2Command,
    RunCollectionOnceV2Result,
)
from hhru_platform.application.commands.trigger_run_now import (
    TRIGGER_RUN_NOW_STATUS_SKIPPED_ACTIVE_RUN,
    TRIGGER_RUN_NOW_STATUS_SKIPPED_OVERLAP,
    TriggerRunNowCommand,
    trigger_run_now,
)
from hhru_platform.domain.entities.crawl_run import CrawlRun


class FakeAdmissionLease:
    def __init__(self, active_run: CrawlRun | None) -> None:
        self._active_run = active_run
        self.released = False

    def get_active_run(self) -> CrawlRun | None:
        return self._active_run

    def release(self) -> None:
        self.released = True


class FakeAdmissionController:
    def __init__(self, lease: FakeAdmissionLease | None) -> None:
        self._lease = lease
        self.acquire_calls = 0

    def acquire(self) -> FakeAdmissionLease | None:
        self.acquire_calls += 1
        return self._lease


class RecordingMetricsRecorder:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def record_scheduler_tick(self, **kwargs) -> None:
        self.calls.append(kwargs)


def test_trigger_run_now_skips_when_admission_lock_is_unavailable() -> None:
    metrics_recorder = RecordingMetricsRecorder()

    result = trigger_run_now(
        TriggerRunNowCommand(run_command=_build_run_command()),
        admission_controller=FakeAdmissionController(None),
        run_collection_once_v2_step=lambda command: (_ for _ in ()).throw(
            AssertionError(f"unexpected run {command.triggered_by}")
        ),
        metrics_recorder=metrics_recorder,
        now=datetime(2026, 3, 20, 10, 0, tzinfo=UTC),
    )

    assert result.status == TRIGGER_RUN_NOW_STATUS_SKIPPED_OVERLAP
    assert result.run_result is None
    assert result.error_message == "collection run admission lock is already held"
    assert metrics_recorder.calls[0]["outcome"] == TRIGGER_RUN_NOW_STATUS_SKIPPED_OVERLAP
    assert metrics_recorder.calls[0]["triggered_run_at"] is None
    assert metrics_recorder.calls[0]["observed_run_status"] is None


def test_trigger_run_now_skips_when_active_run_exists() -> None:
    active_run = CrawlRun(
        id=uuid4(),
        run_type="weekly_sweep",
        status="created",
        started_at=datetime(2026, 3, 20, 9, 0, tzinfo=UTC),
        finished_at=None,
        triggered_by="scheduler-loop",
        config_snapshot_json={},
        partitions_total=4,
        partitions_done=0,
        partitions_failed=0,
        notes=None,
    )
    lease = FakeAdmissionLease(active_run=active_run)
    metrics_recorder = RecordingMetricsRecorder()

    result = trigger_run_now(
        TriggerRunNowCommand(run_command=_build_run_command()),
        admission_controller=FakeAdmissionController(lease),
        run_collection_once_v2_step=lambda command: (_ for _ in ()).throw(
            AssertionError(f"unexpected run {command.triggered_by}")
        ),
        metrics_recorder=metrics_recorder,
        now=datetime(2026, 3, 20, 10, 0, tzinfo=UTC),
    )

    assert result.status == TRIGGER_RUN_NOW_STATUS_SKIPPED_ACTIVE_RUN
    assert result.active_run_id == active_run.id
    assert result.active_run_status == "created"
    assert lease.released is True
    assert metrics_recorder.calls[0]["outcome"] == TRIGGER_RUN_NOW_STATUS_SKIPPED_ACTIVE_RUN
    assert metrics_recorder.calls[0]["triggered_run_at"] is None
    assert metrics_recorder.calls[0]["observed_run_status"] is None


def test_trigger_run_now_returns_completed_with_detail_errors() -> None:
    lease = FakeAdmissionLease(active_run=None)
    metrics_recorder = RecordingMetricsRecorder()
    run_id = uuid4()

    result = trigger_run_now(
        TriggerRunNowCommand(run_command=_build_run_command()),
        admission_controller=FakeAdmissionController(lease),
        run_collection_once_v2_step=lambda command: _build_run_result(
            run_id=run_id,
            status="completed_with_detail_errors",
            triggered_by=command.triggered_by,
        ),
        metrics_recorder=metrics_recorder,
        now=datetime(2026, 3, 20, 10, 0, tzinfo=UTC),
    )

    assert result.status == "completed_with_detail_errors"
    assert result.run_result is not None
    assert result.run_id == run_id
    assert result.error_message == "1 detail fetch(es) failed"
    assert lease.released is True
    assert metrics_recorder.calls[0]["outcome"] == "completed_with_detail_errors"
    assert metrics_recorder.calls[0]["run_started_at"] is not None
    assert metrics_recorder.calls[0]["run_finished_at"] is not None
    assert metrics_recorder.calls[0]["triggered_run_at"] is not None
    assert metrics_recorder.calls[0]["observed_run_status"] == "completed_with_detail_errors"


def _build_run_command() -> RunCollectionOnceV2Command:
    return RunCollectionOnceV2Command(
        sync_dictionaries=False,
        detail_limit=25,
        detail_refresh_ttl_days=30,
        run_type="weekly_sweep",
        triggered_by="scheduler-loop",
    )


def _build_run_result(
    *,
    run_id,
    status: str,
    triggered_by: str,
) -> RunCollectionOnceV2Result:
    return RunCollectionOnceV2Result(
        status=status,
        run_id=run_id,
        run_type="weekly_sweep",
        triggered_by=triggered_by,
        dictionary_results=(),
        planned_partition_ids=(uuid4(),),
        list_engine_results=(),
        final_coverage_report=None,
        list_stage_status="completed",
        detail_selection_result=None,
        detail_results=(),
        detail_stage_status=(
            "completed_with_failures"
            if status == "completed_with_detail_errors"
            else "completed"
        ),
        reconciliation_result=ReconcileRunResult(
            crawl_run_id=run_id,
            observed_in_run_count=0,
            missing_updated_count=0,
            marked_inactive_count=0,
            run_status=status,
        ),
        error_message=(
            "1 detail fetch(es) failed"
            if status == "completed_with_detail_errors"
            else None
        ),
        completed_steps=("create_crawl_run", "plan_sweep_v2", "reconcile_run"),
    )
