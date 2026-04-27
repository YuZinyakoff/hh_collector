from __future__ import annotations

import fcntl
import json
import logging
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from functools import lru_cache
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from time import time
from typing import Final, TypedDict

from hhru_platform.config.settings import get_settings

LOGGER = logging.getLogger(__name__)
HISTOGRAM_BUCKETS: Final[tuple[float, ...]] = (
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    2.5,
    5.0,
    10.0,
    30.0,
)


class MetricsState(TypedDict):
    operation_total: dict[str, int]
    operation_duration_bucket: dict[str, int]
    operation_duration_count: dict[str, int]
    operation_duration_sum: dict[str, float]
    operation_last_success_timestamp: dict[str, float]
    records_written_total: dict[str, int]
    backup_run_total: dict[str, int]
    backup_gauge: dict[str, float]
    restore_drill_run_total: dict[str, int]
    restore_drill_gauge: dict[str, float]
    run_tree_coverage_gauge: dict[str, float]
    run_terminal_status_total: dict[str, int]
    run_terminal_status_timestamp: dict[str, float]
    scheduler_tick_total: dict[str, int]
    scheduler_gauge: dict[str, float]
    scheduler_status_gauge: dict[str, float]
    resume_attempt_total: dict[str, int]
    detail_repair_attempt_total: dict[str, int]
    detail_repair_gauge: dict[str, float]
    detail_repair_total: dict[str, int]
    first_detail_backlog_gauge: dict[str, float]
    first_detail_drain_attempt_total: dict[str, int]
    first_detail_drain_total: dict[str, int]
    housekeeping_run_total: dict[str, int]
    housekeeping_gauge: dict[str, float]
    housekeeping_status_gauge: dict[str, float]
    housekeeping_mode_gauge: dict[str, float]
    housekeeping_action_gauge: dict[str, float]
    housekeeping_deleted_total: dict[str, int]
    upstream_request_total: dict[str, int]
    upstream_request_duration_bucket: dict[str, int]
    upstream_request_duration_count: dict[str, int]
    upstream_request_duration_sum: dict[str, float]


RUN_TREE_COVERAGE_METRIC_HELP: Final[dict[str, str]] = {
    "hhru_run_tree_coverage_ratio": (
        "Coverage ratio for a crawl_run based on covered terminal partitions."
    ),
    "hhru_run_tree_total_partitions": (
        "Total number of partitions currently present in a crawl_run tree."
    ),
    "hhru_run_tree_covered_terminal_partitions": (
        "Number of covered terminal partitions in a crawl_run tree."
    ),
    "hhru_run_tree_pending_terminal_partitions": (
        "Number of pending terminal partitions in a crawl_run tree."
    ),
    "hhru_run_tree_split_partitions": (
        "Number of split or saturated partitions in a crawl_run tree."
    ),
    "hhru_run_tree_unresolved_partitions": (
        "Number of unresolved partitions in a crawl_run tree."
    ),
    "hhru_run_tree_failed_partitions": (
        "Number of failed partitions in a crawl_run tree."
    ),
}

SCHEDULER_GAUGE_METRIC_HELP: Final[dict[str, str]] = {
    "hhru_scheduler_last_tick_timestamp_seconds": "Timestamp of the latest scheduler tick.",
    "hhru_scheduler_last_run_started_timestamp_seconds": (
        "Timestamp when the latest scheduler-admitted run started."
    ),
    "hhru_scheduler_last_run_finished_timestamp_seconds": (
        "Timestamp when the latest scheduler-admitted run finished."
    ),
    "hhru_scheduler_last_triggered_run_timestamp_seconds": (
        "Timestamp when the latest scheduler-admitted run was triggered."
    ),
}

RUN_TERMINAL_STATUS_TIMESTAMP_METRIC: Final[str] = (
    "hhru_run_terminal_status_last_timestamp_seconds"
)
BACKUP_LAST_SUCCESS_TIMESTAMP_METRIC: Final[str] = (
    "hhru_backup_last_success_timestamp_seconds"
)
RESTORE_DRILL_LAST_SUCCESS_TIMESTAMP_METRIC: Final[str] = (
    "hhru_restore_drill_last_success_timestamp_seconds"
)
SCHEDULER_LAST_OBSERVED_RUN_STATUS_METRIC: Final[str] = (
    "hhru_scheduler_last_observed_run_status"
)
DETAIL_REPAIR_BACKLOG_METRIC: Final[str] = "hhru_detail_repair_backlog_size"
FIRST_DETAIL_BACKLOG_METRIC: Final[str] = "hhru_first_detail_backlog_size"
HOUSEKEEPING_LAST_RUN_TIMESTAMP_METRIC: Final[str] = (
    "hhru_housekeeping_last_run_timestamp_seconds"
)
HOUSEKEEPING_LAST_ACTION_COUNT_METRIC: Final[str] = (
    "hhru_housekeeping_last_action_count"
)
DETAIL_REPAIR_TOTAL_METRIC_HELP: Final[dict[str, str]] = {
    "hhru_detail_repair_retried_total": (
        "Total number of backlog detail fetch retries attempted."
    ),
    "hhru_detail_repair_repaired_total": (
        "Total number of backlog detail fetches repaired successfully."
    ),
    "hhru_detail_repair_still_failing_total": (
        "Total number of backlog detail fetches still failing after retry."
    ),
}
FIRST_DETAIL_DRAIN_TOTAL_METRIC_HELP: Final[dict[str, str]] = {
    "hhru_first_detail_drain_selected_total": (
        "Total number of first-detail backlog items selected for drain."
    ),
    "hhru_first_detail_drain_succeeded_total": (
        "Total number of first-detail backlog items resolved with a detail snapshot."
    ),
    "hhru_first_detail_drain_terminal_total": (
        "Total number of first-detail backlog items resolved by terminal outcome."
    ),
    "hhru_first_detail_drain_failed_total": (
        "Total number of first-detail backlog items still retryable after drain."
    ),
}
HOUSEKEEPING_GAUGE_METRIC_HELP: Final[dict[str, str]] = {
    HOUSEKEEPING_LAST_RUN_TIMESTAMP_METRIC: (
        "Timestamp of the latest housekeeping run completion."
    ),
}
HOUSEKEEPING_DELETED_TOTAL_METRIC: Final[str] = "hhru_housekeeping_deleted_total"
SCHEDULER_OBSERVED_RUN_STATUSES: Final[tuple[str, ...]] = (
    "succeeded",
    "completed_with_detail_errors",
    "completed_with_unresolved",
    "failed",
)
HOUSEKEEPING_RUN_STATUSES: Final[tuple[str, ...]] = (
    "succeeded",
    "failed",
)
HOUSEKEEPING_RUN_MODES: Final[tuple[str, ...]] = (
    "dry_run",
    "execute",
)
BACKUP_RUN_STATUSES: Final[tuple[str, ...]] = (
    "succeeded",
    "failed",
)
BACKUP_GAUGE_METRIC_HELP: Final[dict[str, str]] = {
    BACKUP_LAST_SUCCESS_TIMESTAMP_METRIC: (
        "Timestamp of the latest successful PostgreSQL backup."
    ),
    RESTORE_DRILL_LAST_SUCCESS_TIMESTAMP_METRIC: (
        "Timestamp of the latest successful restore drill."
    ),
}


class FileBackedMetricsRegistry:
    def __init__(self, state_path: str | Path) -> None:
        self._state_path = Path(state_path)

    @property
    def state_path(self) -> Path:
        return self._state_path

    def record_operation(
        self,
        *,
        operation: str,
        status: str,
        duration_seconds: float,
    ) -> None:
        duration = max(duration_seconds, 0.0)
        try:
            with self._mutating_state() as state:
                key = _composite_key(operation, status)
                state["operation_total"][key] = state["operation_total"].get(key, 0) + 1
                _observe_duration(
                    bucket_map=state["operation_duration_bucket"],
                    count_map=state["operation_duration_count"],
                    sum_map=state["operation_duration_sum"],
                    key=key,
                    duration_seconds=duration,
                )
                if status == "succeeded":
                    state["operation_last_success_timestamp"][operation] = time()
        except Exception as error:
            LOGGER.warning("metrics operation recording failed: %s", error)

    def record_records_written(
        self,
        *,
        operation: str,
        record_type: str,
        count: int,
    ) -> None:
        if count <= 0:
            return
        try:
            with self._mutating_state() as state:
                key = _composite_key(operation, record_type)
                state["records_written_total"][key] = (
                    state["records_written_total"].get(key, 0) + count
                )
        except Exception as error:
            LOGGER.warning("metrics record counter update failed: %s", error)

    def record_backup_run(
        self,
        *,
        status: str,
        recorded_at: datetime,
    ) -> None:
        try:
            with self._mutating_state() as state:
                state["backup_run_total"][status] = (
                    state["backup_run_total"].get(status, 0) + 1
                )
                if status == "succeeded":
                    state["backup_gauge"][
                        BACKUP_LAST_SUCCESS_TIMESTAMP_METRIC
                    ] = recorded_at.timestamp()
        except Exception as error:
            LOGGER.warning("metrics backup run update failed: %s", error)

    def record_restore_drill_run(
        self,
        *,
        status: str,
        recorded_at: datetime,
    ) -> None:
        try:
            with self._mutating_state() as state:
                state["restore_drill_run_total"][status] = (
                    state["restore_drill_run_total"].get(status, 0) + 1
                )
                if status == "succeeded":
                    state["restore_drill_gauge"][
                        RESTORE_DRILL_LAST_SUCCESS_TIMESTAMP_METRIC
                    ] = recorded_at.timestamp()
        except Exception as error:
            LOGGER.warning("metrics restore drill update failed: %s", error)

    def set_run_tree_coverage(
        self,
        *,
        run_id: str,
        run_type: str,
        coverage_ratio: float,
        total_partitions: int,
        covered_terminal_partitions: int,
        pending_terminal_partitions: int,
        split_partitions: int,
        unresolved_partitions: int,
        failed_partitions: int,
    ) -> None:
        metric_values = {
            "hhru_run_tree_coverage_ratio": max(coverage_ratio, 0.0),
            "hhru_run_tree_total_partitions": float(max(total_partitions, 0)),
            "hhru_run_tree_covered_terminal_partitions": float(
                max(covered_terminal_partitions, 0)
            ),
            "hhru_run_tree_pending_terminal_partitions": float(
                max(pending_terminal_partitions, 0)
            ),
            "hhru_run_tree_split_partitions": float(max(split_partitions, 0)),
            "hhru_run_tree_unresolved_partitions": float(max(unresolved_partitions, 0)),
            "hhru_run_tree_failed_partitions": float(max(failed_partitions, 0)),
        }
        try:
            with self._mutating_state() as state:
                for metric_name, value in metric_values.items():
                    key = _triple_key(metric_name, run_id, run_type)
                    state["run_tree_coverage_gauge"][key] = value
        except Exception as error:
            LOGGER.warning("metrics run coverage gauge update failed: %s", error)

    def record_run_terminal_status(
        self,
        *,
        run_type: str,
        status: str,
        recorded_at: datetime | None = None,
    ) -> None:
        timestamp = (recorded_at or datetime.now(UTC)).timestamp()
        try:
            with self._mutating_state() as state:
                key = _composite_key(run_type, status)
                state["run_terminal_status_total"][key] = (
                    state["run_terminal_status_total"].get(key, 0) + 1
                )
                state["run_terminal_status_timestamp"][key] = timestamp
        except Exception as error:
            LOGGER.warning("metrics run terminal status update failed: %s", error)

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
        try:
            with self._mutating_state() as state:
                state["scheduler_tick_total"][outcome] = (
                    state["scheduler_tick_total"].get(outcome, 0) + 1
                )
                state["scheduler_gauge"][
                    "hhru_scheduler_last_tick_timestamp_seconds"
                ] = ticked_at.timestamp()
                if run_started_at is not None:
                    state["scheduler_gauge"][
                        "hhru_scheduler_last_run_started_timestamp_seconds"
                    ] = run_started_at.timestamp()
                if run_finished_at is not None:
                    state["scheduler_gauge"][
                        "hhru_scheduler_last_run_finished_timestamp_seconds"
                    ] = run_finished_at.timestamp()
                if triggered_run_at is not None:
                    state["scheduler_gauge"][
                        "hhru_scheduler_last_triggered_run_timestamp_seconds"
                    ] = triggered_run_at.timestamp()
                if observed_run_status is not None:
                    for status in SCHEDULER_OBSERVED_RUN_STATUSES:
                        state["scheduler_status_gauge"][status] = (
                            1.0 if status == observed_run_status else 0.0
                        )
        except Exception as error:
            LOGGER.warning("metrics scheduler tick update failed: %s", error)

    def record_resume_attempt(
        self,
        *,
        run_type: str,
        outcome: str,
    ) -> None:
        try:
            with self._mutating_state() as state:
                key = _composite_key(run_type, outcome)
                state["resume_attempt_total"][key] = (
                    state["resume_attempt_total"].get(key, 0) + 1
                )
        except Exception as error:
            LOGGER.warning("metrics resume attempt update failed: %s", error)

    def set_detail_repair_backlog(
        self,
        *,
        run_id: str,
        run_type: str,
        backlog_size: int,
    ) -> None:
        try:
            with self._mutating_state() as state:
                key = _triple_key(DETAIL_REPAIR_BACKLOG_METRIC, run_id, run_type)
                state["detail_repair_gauge"][key] = float(max(backlog_size, 0))
        except Exception as error:
            LOGGER.warning("metrics detail repair backlog update failed: %s", error)

    def record_detail_repair_attempt(
        self,
        *,
        run_type: str,
        outcome: str,
        retried_count: int,
        repaired_count: int,
        still_failing_count: int,
    ) -> None:
        try:
            with self._mutating_state() as state:
                attempt_key = _composite_key(run_type, outcome)
                state["detail_repair_attempt_total"][attempt_key] = (
                    state["detail_repair_attempt_total"].get(attempt_key, 0) + 1
                )
                for metric_name, count in (
                    ("hhru_detail_repair_retried_total", retried_count),
                    ("hhru_detail_repair_repaired_total", repaired_count),
                    ("hhru_detail_repair_still_failing_total", still_failing_count),
                ):
                    if count <= 0:
                        continue
                    total_key = _composite_key(metric_name, run_type)
                    state["detail_repair_total"][total_key] = (
                        state["detail_repair_total"].get(total_key, 0) + count
                    )
        except Exception as error:
            LOGGER.warning("metrics detail repair attempt update failed: %s", error)

    def set_first_detail_backlog(
        self,
        *,
        include_inactive: bool,
        backlog_size: int,
    ) -> None:
        scope = _first_detail_scope(include_inactive=include_inactive)
        try:
            with self._mutating_state() as state:
                state["first_detail_backlog_gauge"][scope] = float(max(backlog_size, 0))
        except Exception as error:
            LOGGER.warning("metrics first detail backlog update failed: %s", error)

    def record_first_detail_drain_attempt(
        self,
        *,
        include_inactive: bool,
        outcome: str,
        selected_count: int,
        succeeded_count: int,
        terminal_count: int,
        failed_count: int,
    ) -> None:
        scope = _first_detail_scope(include_inactive=include_inactive)
        try:
            with self._mutating_state() as state:
                attempt_key = _composite_key(scope, outcome)
                state["first_detail_drain_attempt_total"][attempt_key] = (
                    state["first_detail_drain_attempt_total"].get(attempt_key, 0) + 1
                )
                for metric_name, count in (
                    ("hhru_first_detail_drain_selected_total", selected_count),
                    ("hhru_first_detail_drain_succeeded_total", succeeded_count),
                    ("hhru_first_detail_drain_terminal_total", terminal_count),
                    ("hhru_first_detail_drain_failed_total", failed_count),
                ):
                    if count <= 0:
                        continue
                    total_key = _composite_key(metric_name, scope)
                    state["first_detail_drain_total"][total_key] = (
                        state["first_detail_drain_total"].get(total_key, 0) + count
                    )
        except Exception as error:
            LOGGER.warning("metrics first detail drain attempt update failed: %s", error)

    def record_housekeeping_run(
        self,
        *,
        mode: str,
        status: str,
        recorded_at: datetime,
    ) -> None:
        try:
            with self._mutating_state() as state:
                key = _composite_key(mode, status)
                state["housekeeping_run_total"][key] = (
                    state["housekeeping_run_total"].get(key, 0) + 1
                )
                state["housekeeping_gauge"][
                    HOUSEKEEPING_LAST_RUN_TIMESTAMP_METRIC
                ] = recorded_at.timestamp()
                for recorded_status in HOUSEKEEPING_RUN_STATUSES:
                    state["housekeeping_status_gauge"][recorded_status] = (
                        1.0 if recorded_status == status else 0.0
                    )
                for recorded_mode in HOUSEKEEPING_RUN_MODES:
                    state["housekeeping_mode_gauge"][recorded_mode] = (
                        1.0 if recorded_mode == mode else 0.0
                    )
        except Exception as error:
            LOGGER.warning("metrics housekeeping run update failed: %s", error)

    def set_housekeeping_last_action_count(
        self,
        *,
        target: str,
        mode: str,
        count: int,
    ) -> None:
        try:
            with self._mutating_state() as state:
                key = _composite_key(target, mode)
                state["housekeeping_action_gauge"][key] = float(max(count, 0))
        except Exception as error:
            LOGGER.warning("metrics housekeeping action gauge update failed: %s", error)

    def record_housekeeping_deleted(
        self,
        *,
        target: str,
        count: int,
    ) -> None:
        if count <= 0:
            return
        try:
            with self._mutating_state() as state:
                state["housekeeping_deleted_total"][target] = (
                    state["housekeeping_deleted_total"].get(target, 0) + count
                )
        except Exception as error:
            LOGGER.warning("metrics housekeeping deleted counter update failed: %s", error)

    def record_upstream_request(
        self,
        *,
        endpoint: str,
        status_code: int,
        duration_seconds: float,
        error_type: str | None = None,
    ) -> None:
        duration = max(duration_seconds, 0.0)
        status_class = _status_class(status_code=status_code, error_type=error_type)
        try:
            with self._mutating_state() as state:
                key = _composite_key(endpoint, status_class)
                state["upstream_request_total"][key] = (
                    state["upstream_request_total"].get(key, 0) + 1
                )
                _observe_duration(
                    bucket_map=state["upstream_request_duration_bucket"],
                    count_map=state["upstream_request_duration_count"],
                    sum_map=state["upstream_request_duration_sum"],
                    key=key,
                    duration_seconds=duration,
                )
        except Exception as error:
            LOGGER.warning("metrics upstream request recording failed: %s", error)

    def render_prometheus(self) -> str:
        state = self._read_state()
        lines = [
            "# HELP hhru_operation_total Total number of application operations.",
            "# TYPE hhru_operation_total counter",
        ]
        for key, value in sorted(state["operation_total"].items()):
            operation, status = _split_composite_key(key)
            lines.append(
                "hhru_operation_total"
                f'{{operation="{_label_value(operation)}",status="{_label_value(status)}"}} '
                f"{value}"
            )

        lines.extend(
            [
                "# HELP hhru_operation_duration_seconds Duration of application operations.",
                "# TYPE hhru_operation_duration_seconds histogram",
            ]
        )
        for key, count in sorted(state["operation_duration_count"].items()):
            operation, status = _split_composite_key(key)
            for bucket in HISTOGRAM_BUCKETS:
                bucket_key = _bucket_key(key, bucket)
                bucket_count = state["operation_duration_bucket"].get(bucket_key, 0)
                lines.append(
                    "hhru_operation_duration_seconds_bucket"
                    f'{{operation="{_label_value(operation)}",status="{_label_value(status)}",'
                    f'le="{bucket}"}} {bucket_count}'
                )
            lines.append(
                "hhru_operation_duration_seconds_bucket"
                f'{{operation="{_label_value(operation)}",status="{_label_value(status)}",'
                'le="+Inf"} '
                f"{count}"
            )
            lines.append(
                "hhru_operation_duration_seconds_count"
                f'{{operation="{_label_value(operation)}",status="{_label_value(status)}"}} '
                f"{count}"
            )
            lines.append(
                "hhru_operation_duration_seconds_sum"
                f'{{operation="{_label_value(operation)}",status="{_label_value(status)}"}} '
                f"{state['operation_duration_sum'].get(key, 0.0):.6f}"
            )

        lines.extend(
            [
                (
                    "# HELP hhru_operation_last_success_timestamp_seconds "
                    "Last successful application operation timestamp."
                ),
                "# TYPE hhru_operation_last_success_timestamp_seconds gauge",
            ]
        )
        for operation, timestamp_value in sorted(state["operation_last_success_timestamp"].items()):
            lines.append(
                "hhru_operation_last_success_timestamp_seconds"
                f'{{operation="{_label_value(operation)}"}} {timestamp_value:.3f}'
            )

        lines.extend(
            [
                (
                    "# HELP hhru_records_written_total "
                    "Number of rows written by application operations."
                ),
                "# TYPE hhru_records_written_total counter",
            ]
        )
        for key, value in sorted(state["records_written_total"].items()):
            operation, record_type = _split_composite_key(key)
            lines.append(
                "hhru_records_written_total"
                f'{{operation="{_label_value(operation)}",'
                f'record_type="{_label_value(record_type)}"}} '
                f"{value}"
            )

        lines.extend(
            [
                "# HELP hhru_backup_run_total Total number of PostgreSQL backup runs by status.",
                "# TYPE hhru_backup_run_total counter",
            ]
        )
        for status, value in sorted(state["backup_run_total"].items()):
            lines.append(
                "hhru_backup_run_total"
                f'{{status="{_label_value(status)}"}} {value}'
            )

        lines.extend(
            [
                "# HELP hhru_restore_drill_run_total Total number of restore drill runs by status.",
                "# TYPE hhru_restore_drill_run_total counter",
            ]
        )
        for status, value in sorted(state["restore_drill_run_total"].items()):
            lines.append(
                "hhru_restore_drill_run_total"
                f'{{status="{_label_value(status)}"}} {value}'
            )

        for metric_name, help_text in BACKUP_GAUGE_METRIC_HELP.items():
            lines.extend(
                [
                    f"# HELP {metric_name} {help_text}",
                    f"# TYPE {metric_name} gauge",
                ]
            )
            gauge_map = (
                state["backup_gauge"]
                if metric_name == BACKUP_LAST_SUCCESS_TIMESTAMP_METRIC
                else state["restore_drill_gauge"]
            )
            if metric_name in gauge_map:
                lines.append(f"{metric_name} {gauge_map[metric_name]:.3f}")

        for metric_name, help_text in RUN_TREE_COVERAGE_METRIC_HELP.items():
            lines.extend(
                [
                    f"# HELP {metric_name} {help_text}",
                    f"# TYPE {metric_name} gauge",
                ]
            )
            for key, gauge_value in sorted(state["run_tree_coverage_gauge"].items()):
                recorded_metric_name, run_id, run_type = _split_triple_key(key)
                if recorded_metric_name != metric_name:
                    continue
                lines.append(
                    f'{metric_name}{{run_id="{_label_value(run_id)}",'
                    f'run_type="{_label_value(run_type)}"}} {gauge_value:.6f}'
                )

        lines.extend(
            [
                (
                    "# HELP hhru_run_terminal_status_total "
                    "Total number of crawl_run terminal status publications."
                ),
                "# TYPE hhru_run_terminal_status_total counter",
            ]
        )
        for key, value in sorted(state["run_terminal_status_total"].items()):
            run_type, status = _split_composite_key(key)
            lines.append(
                "hhru_run_terminal_status_total"
                f'{{run_type="{_label_value(run_type)}",status="{_label_value(status)}"}} '
                f"{value}"
            )

        lines.extend(
            [
                (
                    f"# HELP {RUN_TERMINAL_STATUS_TIMESTAMP_METRIC} "
                    "Last crawl_run terminal status publication timestamp."
                ),
                f"# TYPE {RUN_TERMINAL_STATUS_TIMESTAMP_METRIC} gauge",
            ]
        )
        for key, timestamp_value in sorted(state["run_terminal_status_timestamp"].items()):
            run_type, status = _split_composite_key(key)
            lines.append(
                f"{RUN_TERMINAL_STATUS_TIMESTAMP_METRIC}"
                f'{{run_type="{_label_value(run_type)}",status="{_label_value(status)}"}} '
                f"{timestamp_value:.3f}"
            )

        lines.extend(
            [
                "# HELP hhru_scheduler_tick_total Total number of scheduler admission ticks.",
                "# TYPE hhru_scheduler_tick_total counter",
            ]
        )
        for outcome, value in sorted(state["scheduler_tick_total"].items()):
            lines.append(
                "hhru_scheduler_tick_total"
                f'{{outcome="{_label_value(outcome)}"}} {value}'
            )

        for metric_name, help_text in SCHEDULER_GAUGE_METRIC_HELP.items():
            lines.extend(
                [
                    f"# HELP {metric_name} {help_text}",
                    f"# TYPE {metric_name} gauge",
                ]
            )
            if metric_name in state["scheduler_gauge"]:
                lines.append(f"{metric_name} {state['scheduler_gauge'][metric_name]:.3f}")

        lines.extend(
            [
                (
                    f"# HELP {SCHEDULER_LAST_OBSERVED_RUN_STATUS_METRIC} "
                    "One-hot gauge for the latest scheduler-observed run terminal status."
                ),
                f"# TYPE {SCHEDULER_LAST_OBSERVED_RUN_STATUS_METRIC} gauge",
            ]
        )
        for status, gauge_value in sorted(state["scheduler_status_gauge"].items()):
            lines.append(
                f"{SCHEDULER_LAST_OBSERVED_RUN_STATUS_METRIC}"
                f'{{status="{_label_value(status)}"}} {gauge_value:.1f}'
            )

        lines.extend(
            [
                (
                    "# HELP hhru_resume_run_v2_attempt_total "
                    "Total number of resume-run-v2 attempts by outcome."
                ),
                "# TYPE hhru_resume_run_v2_attempt_total counter",
            ]
        )
        for key, value in sorted(state["resume_attempt_total"].items()):
            run_type, outcome = _split_composite_key(key)
            lines.append(
                "hhru_resume_run_v2_attempt_total"
                f'{{run_type="{_label_value(run_type)}",outcome="{_label_value(outcome)}"}} '
                f"{value}"
            )

        lines.extend(
            [
                (
                    "# HELP hhru_detail_repair_attempt_total "
                    "Total number of detail repair attempts by outcome."
                ),
                "# TYPE hhru_detail_repair_attempt_total counter",
            ]
        )
        for key, value in sorted(state["detail_repair_attempt_total"].items()):
            run_type, outcome = _split_composite_key(key)
            lines.append(
                "hhru_detail_repair_attempt_total"
                f'{{run_type="{_label_value(run_type)}",outcome="{_label_value(outcome)}"}} '
                f"{value}"
            )

        lines.extend(
            [
                (
                    f"# HELP {DETAIL_REPAIR_BACKLOG_METRIC} "
                    "Current size of the derived detail repair backlog for a crawl_run."
                ),
                f"# TYPE {DETAIL_REPAIR_BACKLOG_METRIC} gauge",
            ]
        )
        for key, gauge_value in sorted(state["detail_repair_gauge"].items()):
            metric_name, run_id, run_type = _split_triple_key(key)
            if metric_name != DETAIL_REPAIR_BACKLOG_METRIC:
                continue
            lines.append(
                f"{DETAIL_REPAIR_BACKLOG_METRIC}"
                f'{{run_id="{_label_value(run_id)}",run_type="{_label_value(run_type)}"}} '
                f"{gauge_value:.6f}"
            )

        for metric_name, help_text in DETAIL_REPAIR_TOTAL_METRIC_HELP.items():
            lines.extend(
                [
                    f"# HELP {metric_name} {help_text}",
                    f"# TYPE {metric_name} counter",
                ]
            )
            for key, value in sorted(state["detail_repair_total"].items()):
                recorded_metric_name, run_type = _split_composite_key(key)
                if recorded_metric_name != metric_name:
                    continue
                lines.append(
                    f"{metric_name}"
                    f'{{run_type="{_label_value(run_type)}"}} '
                    f"{value}"
                )

        lines.extend(
            [
                (
                    f"# HELP {FIRST_DETAIL_BACKLOG_METRIC} "
                    "Current size of the global first-detail backlog."
                ),
                f"# TYPE {FIRST_DETAIL_BACKLOG_METRIC} gauge",
            ]
        )
        for scope, gauge_value in sorted(state["first_detail_backlog_gauge"].items()):
            lines.append(
                f"{FIRST_DETAIL_BACKLOG_METRIC}"
                f'{{scope="{_label_value(scope)}"}} {gauge_value:.6f}'
            )

        lines.extend(
            [
                (
                    "# HELP hhru_first_detail_drain_attempt_total "
                    "Total number of first-detail drain attempts by scope and outcome."
                ),
                "# TYPE hhru_first_detail_drain_attempt_total counter",
            ]
        )
        for key, value in sorted(state["first_detail_drain_attempt_total"].items()):
            scope, outcome = _split_composite_key(key)
            lines.append(
                "hhru_first_detail_drain_attempt_total"
                f'{{scope="{_label_value(scope)}",outcome="{_label_value(outcome)}"}} '
                f"{value}"
            )

        for metric_name, help_text in FIRST_DETAIL_DRAIN_TOTAL_METRIC_HELP.items():
            lines.extend(
                [
                    f"# HELP {metric_name} {help_text}",
                    f"# TYPE {metric_name} counter",
                ]
            )
            for key, value in sorted(state["first_detail_drain_total"].items()):
                recorded_metric_name, scope = _split_composite_key(key)
                if recorded_metric_name != metric_name:
                    continue
                lines.append(
                    f"{metric_name}"
                    f'{{scope="{_label_value(scope)}"}} '
                    f"{value}"
                )

        lines.extend(
            [
                (
                    "# HELP hhru_housekeeping_run_total "
                    "Total number of housekeeping runs by mode and status."
                ),
                "# TYPE hhru_housekeeping_run_total counter",
            ]
        )
        for key, value in sorted(state["housekeeping_run_total"].items()):
            mode, status = _split_composite_key(key)
            lines.append(
                "hhru_housekeeping_run_total"
                f'{{mode="{_label_value(mode)}",status="{_label_value(status)}"}} '
                f"{value}"
            )

        for metric_name, help_text in HOUSEKEEPING_GAUGE_METRIC_HELP.items():
            lines.extend(
                [
                    f"# HELP {metric_name} {help_text}",
                    f"# TYPE {metric_name} gauge",
                ]
            )
            if metric_name in state["housekeeping_gauge"]:
                lines.append(f"{metric_name} {state['housekeeping_gauge'][metric_name]:.3f}")

        lines.extend(
            [
                (
                    "# HELP hhru_housekeeping_last_run_status "
                    "One-hot gauge for the latest housekeeping run status."
                ),
                "# TYPE hhru_housekeeping_last_run_status gauge",
            ]
        )
        for status, gauge_value in sorted(state["housekeeping_status_gauge"].items()):
            lines.append(
                "hhru_housekeeping_last_run_status"
                f'{{status="{_label_value(status)}"}} {gauge_value:.1f}'
            )

        lines.extend(
            [
                (
                    "# HELP hhru_housekeeping_last_run_mode "
                    "One-hot gauge for the latest housekeeping run mode."
                ),
                "# TYPE hhru_housekeeping_last_run_mode gauge",
            ]
        )
        for mode, gauge_value in sorted(state["housekeeping_mode_gauge"].items()):
            lines.append(
                "hhru_housekeeping_last_run_mode"
                f'{{mode="{_label_value(mode)}"}} {gauge_value:.1f}'
            )

        lines.extend(
            [
                (
                    f"# HELP {HOUSEKEEPING_LAST_ACTION_COUNT_METRIC} "
                    "Affected row or file count in the latest housekeeping run by target and mode."
                ),
                f"# TYPE {HOUSEKEEPING_LAST_ACTION_COUNT_METRIC} gauge",
            ]
        )
        for key, gauge_value in sorted(state["housekeeping_action_gauge"].items()):
            target, mode = _split_composite_key(key)
            lines.append(
                f"{HOUSEKEEPING_LAST_ACTION_COUNT_METRIC}"
                f'{{target="{_label_value(target)}",mode="{_label_value(mode)}"}} '
                f"{gauge_value:.6f}"
            )

        lines.extend(
            [
                (
                    f"# HELP {HOUSEKEEPING_DELETED_TOTAL_METRIC} "
                    "Total number of rows or files deleted by housekeeping per target."
                ),
                f"# TYPE {HOUSEKEEPING_DELETED_TOTAL_METRIC} counter",
            ]
        )
        for target, value in sorted(state["housekeeping_deleted_total"].items()):
            lines.append(
                f"{HOUSEKEEPING_DELETED_TOTAL_METRIC}"
                f'{{target="{_label_value(target)}"}} {value}'
            )

        lines.extend(
            [
                "# HELP hhru_upstream_request_total Total number of upstream hh API requests.",
                "# TYPE hhru_upstream_request_total counter",
            ]
        )
        for key, value in sorted(state["upstream_request_total"].items()):
            endpoint, status_class = _split_composite_key(key)
            lines.append(
                "hhru_upstream_request_total"
                f'{{endpoint="{_label_value(endpoint)}",'
                f'status_class="{_label_value(status_class)}"}} '
                f"{value}"
            )

        lines.extend(
            [
                (
                    "# HELP hhru_upstream_request_duration_seconds "
                    "Duration of upstream hh API requests."
                ),
                "# TYPE hhru_upstream_request_duration_seconds histogram",
            ]
        )
        for key, count in sorted(state["upstream_request_duration_count"].items()):
            endpoint, status_class = _split_composite_key(key)
            for bucket in HISTOGRAM_BUCKETS:
                bucket_key = _bucket_key(key, bucket)
                bucket_count = state["upstream_request_duration_bucket"].get(bucket_key, 0)
                lines.append(
                    "hhru_upstream_request_duration_seconds_bucket"
                    f'{{endpoint="{_label_value(endpoint)}",status_class="{_label_value(status_class)}",'
                    f'le="{bucket}"}} {bucket_count}'
                )
            lines.append(
                "hhru_upstream_request_duration_seconds_bucket"
                f'{{endpoint="{_label_value(endpoint)}",status_class="{_label_value(status_class)}",'
                'le="+Inf"} '
                f"{count}"
            )
            lines.append(
                "hhru_upstream_request_duration_seconds_count"
                f'{{endpoint="{_label_value(endpoint)}",'
                f'status_class="{_label_value(status_class)}"}} '
                f"{count}"
            )
            lines.append(
                "hhru_upstream_request_duration_seconds_sum"
                f'{{endpoint="{_label_value(endpoint)}",'
                f'status_class="{_label_value(status_class)}"}} '
                f"{state['upstream_request_duration_sum'].get(key, 0.0):.6f}"
            )

        return "\n".join(lines) + "\n"

    def reset(self) -> None:
        try:
            self._state_path.parent.mkdir(parents=True, exist_ok=True)
            with self._state_path.open("w", encoding="utf-8") as file_handle:
                fcntl.flock(file_handle.fileno(), fcntl.LOCK_EX)
                try:
                    json.dump(_empty_state(), file_handle, sort_keys=True)
                    file_handle.write("\n")
                    file_handle.flush()
                finally:
                    fcntl.flock(file_handle.fileno(), fcntl.LOCK_UN)
        except Exception as error:
            LOGGER.warning("metrics reset failed: %s", error)

    @contextmanager
    def _mutating_state(self) -> Iterator[MetricsState]:
        self._state_path.parent.mkdir(parents=True, exist_ok=True)
        with self._state_path.open("a+", encoding="utf-8") as file_handle:
            fcntl.flock(file_handle.fileno(), fcntl.LOCK_EX)
            try:
                file_handle.seek(0)
                raw_state = file_handle.read()
                state = _deserialize_state(raw_state)
                yield state
                file_handle.seek(0)
                file_handle.truncate()
                json.dump(state, file_handle, sort_keys=True)
                file_handle.write("\n")
                file_handle.flush()
            finally:
                fcntl.flock(file_handle.fileno(), fcntl.LOCK_UN)

    def _read_state(self) -> MetricsState:
        if not self._state_path.exists():
            return _empty_state()
        try:
            with self._state_path.open("r", encoding="utf-8") as file_handle:
                fcntl.flock(file_handle.fileno(), fcntl.LOCK_SH)
                try:
                    return _deserialize_state(file_handle.read())
                finally:
                    fcntl.flock(file_handle.fileno(), fcntl.LOCK_UN)
        except Exception as error:
            LOGGER.warning("metrics read failed: %s", error)
            return _empty_state()


class _MetricsHTTPServer(ThreadingHTTPServer):
    def __init__(
        self,
        server_address: tuple[str, int],
        registry: FileBackedMetricsRegistry,
    ) -> None:
        super().__init__(server_address, _MetricsRequestHandler)
        self.registry = registry


class _MetricsRequestHandler(BaseHTTPRequestHandler):
    server: _MetricsHTTPServer

    def do_GET(self) -> None:  # noqa: N802
        if self.path in {"/metrics", "/metrics/"}:
            payload = self.server.registry.render_prometheus().encode("utf-8")
            self._write_response(
                status=HTTPStatus.OK,
                content_type="text/plain; version=0.0.4; charset=utf-8",
                payload=payload,
            )
            return

        if self.path in {"/healthz", "/healthz/"}:
            self._write_response(
                status=HTTPStatus.OK,
                content_type="text/plain; charset=utf-8",
                payload=b"ok\n",
            )
            return

        self._write_response(
            status=HTTPStatus.NOT_FOUND,
            content_type="text/plain; charset=utf-8",
            payload=b"not found\n",
        )

    def log_message(self, format: str, *args: object) -> None:
        return

    def _write_response(
        self,
        *,
        status: HTTPStatus,
        content_type: str,
        payload: bytes,
    ) -> None:
        self.send_response(status.value)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


@lru_cache(maxsize=1)
def get_metrics_registry() -> FileBackedMetricsRegistry:
    settings = get_settings()
    return FileBackedMetricsRegistry(settings.metrics_state_path)


def serve_metrics_http(
    *,
    host: str,
    port: int,
    registry: FileBackedMetricsRegistry | None = None,
) -> None:
    metrics_registry = registry or get_metrics_registry()
    server = _MetricsHTTPServer((host, port), metrics_registry)
    try:
        server.serve_forever()
    finally:
        server.server_close()


def _empty_state() -> MetricsState:
    return MetricsState(
        operation_total={},
        operation_duration_bucket={},
        operation_duration_count={},
        operation_duration_sum={},
        operation_last_success_timestamp={},
        records_written_total={},
        backup_run_total={},
        backup_gauge={},
        restore_drill_run_total={},
        restore_drill_gauge={},
        run_tree_coverage_gauge={},
        run_terminal_status_total={},
        run_terminal_status_timestamp={},
        scheduler_tick_total={},
        scheduler_gauge={},
        scheduler_status_gauge={},
        resume_attempt_total={},
        detail_repair_attempt_total={},
        detail_repair_gauge={},
        detail_repair_total={},
        first_detail_backlog_gauge={},
        first_detail_drain_attempt_total={},
        first_detail_drain_total={},
        housekeeping_run_total={},
        housekeeping_gauge={},
        housekeeping_status_gauge={},
        housekeeping_mode_gauge={},
        housekeeping_action_gauge={},
        housekeeping_deleted_total={},
        upstream_request_total={},
        upstream_request_duration_bucket={},
        upstream_request_duration_count={},
        upstream_request_duration_sum={},
    )


def _deserialize_state(raw_state: str) -> MetricsState:
    normalized_state = raw_state.strip()
    if not normalized_state or not normalized_state.strip("\x00"):
        return _empty_state()

    try:
        loaded = json.loads(normalized_state)
    except json.JSONDecodeError:
        return _empty_state()

    if not isinstance(loaded, dict):
        return _empty_state()

    return MetricsState(
        operation_total=_coerce_int_map(loaded.get("operation_total")),
        operation_duration_bucket=_coerce_int_map(loaded.get("operation_duration_bucket")),
        operation_duration_count=_coerce_int_map(loaded.get("operation_duration_count")),
        operation_duration_sum=_coerce_float_map(loaded.get("operation_duration_sum")),
        operation_last_success_timestamp=_coerce_float_map(
            loaded.get("operation_last_success_timestamp")
        ),
        records_written_total=_coerce_int_map(loaded.get("records_written_total")),
        backup_run_total=_coerce_int_map(loaded.get("backup_run_total")),
        backup_gauge=_coerce_float_map(loaded.get("backup_gauge")),
        restore_drill_run_total=_coerce_int_map(loaded.get("restore_drill_run_total")),
        restore_drill_gauge=_coerce_float_map(loaded.get("restore_drill_gauge")),
        run_tree_coverage_gauge=_coerce_float_map(loaded.get("run_tree_coverage_gauge")),
        run_terminal_status_total=_coerce_int_map(loaded.get("run_terminal_status_total")),
        run_terminal_status_timestamp=_coerce_float_map(
            loaded.get("run_terminal_status_timestamp")
        ),
        scheduler_tick_total=_coerce_int_map(loaded.get("scheduler_tick_total")),
        scheduler_gauge=_coerce_float_map(loaded.get("scheduler_gauge")),
        scheduler_status_gauge=_coerce_float_map(loaded.get("scheduler_status_gauge")),
        resume_attempt_total=_coerce_int_map(loaded.get("resume_attempt_total")),
        detail_repair_attempt_total=_coerce_int_map(
            loaded.get("detail_repair_attempt_total")
        ),
        detail_repair_gauge=_coerce_float_map(loaded.get("detail_repair_gauge")),
        detail_repair_total=_coerce_int_map(loaded.get("detail_repair_total")),
        first_detail_backlog_gauge=_coerce_float_map(
            loaded.get("first_detail_backlog_gauge")
        ),
        first_detail_drain_attempt_total=_coerce_int_map(
            loaded.get("first_detail_drain_attempt_total")
        ),
        first_detail_drain_total=_coerce_int_map(
            loaded.get("first_detail_drain_total")
        ),
        housekeeping_run_total=_coerce_int_map(loaded.get("housekeeping_run_total")),
        housekeeping_gauge=_coerce_float_map(loaded.get("housekeeping_gauge")),
        housekeeping_status_gauge=_coerce_float_map(
            loaded.get("housekeeping_status_gauge")
        ),
        housekeeping_mode_gauge=_coerce_float_map(loaded.get("housekeeping_mode_gauge")),
        housekeeping_action_gauge=_coerce_float_map(
            loaded.get("housekeeping_action_gauge")
        ),
        housekeeping_deleted_total=_coerce_int_map(
            loaded.get("housekeeping_deleted_total")
        ),
        upstream_request_total=_coerce_int_map(loaded.get("upstream_request_total")),
        upstream_request_duration_bucket=_coerce_int_map(
            loaded.get("upstream_request_duration_bucket")
        ),
        upstream_request_duration_count=_coerce_int_map(
            loaded.get("upstream_request_duration_count")
        ),
        upstream_request_duration_sum=_coerce_float_map(
            loaded.get("upstream_request_duration_sum")
        ),
    )


def _coerce_int_map(raw_map: object) -> dict[str, int]:
    if not isinstance(raw_map, dict):
        return {}
    coerced: dict[str, int] = {}
    for key, value in raw_map.items():
        if not isinstance(key, str):
            continue
        if isinstance(value, int):
            coerced[key] = value
        elif isinstance(value, float):
            coerced[key] = int(value)
    return coerced


def _coerce_float_map(raw_map: object) -> dict[str, float]:
    if not isinstance(raw_map, dict):
        return {}
    coerced: dict[str, float] = {}
    for key, value in raw_map.items():
        if not isinstance(key, str):
            continue
        if isinstance(value, int | float):
            coerced[key] = float(value)
    return coerced


def _observe_duration(
    *,
    bucket_map: dict[str, int],
    count_map: dict[str, int],
    sum_map: dict[str, float],
    key: str,
    duration_seconds: float,
) -> None:
    count_map[key] = count_map.get(key, 0) + 1
    sum_map[key] = sum_map.get(key, 0.0) + duration_seconds
    for bucket in HISTOGRAM_BUCKETS:
        if duration_seconds <= bucket:
            bucket_key = _bucket_key(key, bucket)
            bucket_map[bucket_key] = bucket_map.get(bucket_key, 0) + 1


def _status_class(*, status_code: int, error_type: str | None) -> str:
    if error_type is not None and status_code == 0:
        return "network_error"
    if status_code <= 0:
        return "unknown"
    return f"{status_code // 100}xx"


def _first_detail_scope(*, include_inactive: bool) -> str:
    return "all" if include_inactive else "active"


def _composite_key(left: str, right: str) -> str:
    return f"{left}|{right}"


def _split_composite_key(key: str) -> tuple[str, str]:
    left, right = key.split("|", maxsplit=1)
    return left, right


def _bucket_key(key: str, bucket: float) -> str:
    return f"{key}|{bucket}"


def _triple_key(left: str, middle: str, right: str) -> str:
    return f"{left}|{middle}|{right}"


def _split_triple_key(key: str) -> tuple[str, str, str]:
    left, middle, right = key.split("|", maxsplit=2)
    return left, middle, right


def _label_value(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")
