from __future__ import annotations

from datetime import datetime
from typing import Protocol


class RunTerminalStatusMetricsRecorder(Protocol):
    def record_run_terminal_status(
        self,
        *,
        run_type: str,
        status: str,
        recorded_at: datetime | None = None,
    ) -> None:
        """Persist one crawl_run terminal status transition."""


def publish_run_terminal_status(
    metrics_recorder: RunTerminalStatusMetricsRecorder | None,
    *,
    run_type: str,
    previous_status: str,
    previous_finished_at: datetime | None,
    current_status: str,
    recorded_at: datetime | None,
) -> None:
    if metrics_recorder is None:
        return
    if previous_finished_at is not None and previous_status == current_status:
        return
    metrics_recorder.record_run_terminal_status(
        run_type=run_type,
        status=current_status,
        recorded_at=recorded_at,
    )
