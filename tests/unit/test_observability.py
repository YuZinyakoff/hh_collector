from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from io import StringIO
from uuid import uuid4

from hhru_platform.infrastructure.observability.logging import (
    JsonLogFormatter,
    ServiceContextFilter,
    log_event,
)
from hhru_platform.infrastructure.observability.metrics import FileBackedMetricsRegistry


def test_file_backed_metrics_registry_renders_prometheus_snapshot(tmp_path) -> None:
    registry = FileBackedMetricsRegistry(tmp_path / "metrics.json")
    registry.record_operation(
        operation="process_list_page",
        status="succeeded",
        duration_seconds=0.42,
    )
    registry.record_records_written(
        operation="process_list_page",
        record_type="vacancy",
        count=5,
    )
    registry.record_backup_run(
        status="succeeded",
        recorded_at=datetime(2026, 3, 20, 9, 30, tzinfo=UTC),
    )
    registry.record_restore_drill_run(
        status="succeeded",
        recorded_at=datetime(2026, 3, 20, 9, 45, tzinfo=UTC),
    )
    registry.record_upstream_request(
        endpoint="/vacancies",
        status_code=200,
        duration_seconds=0.17,
    )
    registry.set_run_tree_coverage(
        run_id="run-1",
        run_type="weekly_sweep",
        coverage_ratio=0.5,
        total_partitions=6,
        covered_terminal_partitions=3,
        pending_terminal_partitions=3,
        split_partitions=1,
        unresolved_partitions=0,
        failed_partitions=1,
    )
    registry.record_run_terminal_status(
        run_type="weekly_sweep",
        status="completed_with_detail_errors",
        recorded_at=datetime(2026, 3, 20, 10, 6, tzinfo=UTC),
    )
    registry.record_scheduler_tick(
        outcome="completed_with_detail_errors",
        ticked_at=datetime(2026, 3, 20, 10, 0, tzinfo=UTC),
        run_started_at=datetime(2026, 3, 20, 10, 1, tzinfo=UTC),
        run_finished_at=datetime(2026, 3, 20, 10, 5, tzinfo=UTC),
        triggered_run_at=datetime(2026, 3, 20, 10, 1, tzinfo=UTC),
        observed_run_status="completed_with_detail_errors",
    )
    registry.record_resume_attempt(
        run_type="weekly_sweep",
        outcome="completed_with_unresolved",
    )
    registry.set_detail_repair_backlog(
        run_id="run-1",
        run_type="weekly_sweep",
        backlog_size=2,
    )
    registry.record_detail_repair_attempt(
        run_type="weekly_sweep",
        outcome="completed_with_detail_errors",
        retried_count=2,
        repaired_count=1,
        still_failing_count=1,
    )
    registry.set_first_detail_backlog(
        include_inactive=False,
        backlog_size=42,
    )
    registry.record_first_detail_drain_attempt(
        include_inactive=False,
        outcome="succeeded",
        selected_count=10,
        succeeded_count=8,
        terminal_count=2,
        failed_count=0,
    )
    registry.record_housekeeping_run(
        mode="dry_run",
        status="succeeded",
        recorded_at=datetime(2026, 3, 21, 8, 0, tzinfo=UTC),
    )
    registry.set_housekeeping_last_action_count(
        target="raw_api_payload",
        mode="dry_run",
        count=12,
    )
    registry.record_housekeeping_deleted(
        target="crawl_partition",
        count=5,
    )

    rendered = registry.render_prometheus()

    assert 'hhru_operation_total{operation="process_list_page",status="succeeded"} 1' in rendered
    assert (
        'hhru_records_written_total{operation="process_list_page",record_type="vacancy"} 5'
        in rendered
    )
    assert 'hhru_backup_run_total{status="succeeded"} 1' in rendered
    assert "hhru_backup_last_success_timestamp_seconds" in rendered
    assert 'hhru_restore_drill_run_total{status="succeeded"} 1' in rendered
    assert "hhru_restore_drill_last_success_timestamp_seconds" in rendered
    assert 'hhru_upstream_request_total{endpoint="/vacancies",status_class="2xx"} 1' in rendered
    assert (
        'hhru_run_tree_total_partitions{run_id="run-1",run_type="weekly_sweep"} 6.000000'
        in rendered
    )
    assert (
        'hhru_run_tree_coverage_ratio{run_id="run-1",run_type="weekly_sweep"} 0.500000'
        in rendered
    )
    assert (
        'hhru_run_tree_pending_terminal_partitions{run_id="run-1",run_type="weekly_sweep"} '
        "3.000000" in rendered
    )
    assert (
        'hhru_run_tree_failed_partitions{run_id="run-1",run_type="weekly_sweep"} 1.000000'
        in rendered
    )
    assert (
        'hhru_run_terminal_status_total{run_type="weekly_sweep",'
        'status="completed_with_detail_errors"} 1' in rendered
    )
    assert 'hhru_scheduler_tick_total{outcome="completed_with_detail_errors"} 1' in rendered
    assert "hhru_scheduler_last_tick_timestamp_seconds" in rendered
    assert "hhru_scheduler_last_run_started_timestamp_seconds" in rendered
    assert "hhru_scheduler_last_run_finished_timestamp_seconds" in rendered
    assert "hhru_scheduler_last_triggered_run_timestamp_seconds" in rendered
    assert (
        'hhru_scheduler_last_observed_run_status{status="completed_with_detail_errors"} 1.0'
        in rendered
    )
    assert (
        'hhru_resume_run_v2_attempt_total{run_type="weekly_sweep",'
        'outcome="completed_with_unresolved"} 1' in rendered
    )
    assert (
        'hhru_detail_repair_backlog_size{run_id="run-1",run_type="weekly_sweep"} 2.000000'
        in rendered
    )
    assert (
        'hhru_detail_repair_attempt_total{run_type="weekly_sweep",'
        'outcome="completed_with_detail_errors"} 1' in rendered
    )
    assert 'hhru_detail_repair_retried_total{run_type="weekly_sweep"} 2' in rendered
    assert 'hhru_detail_repair_repaired_total{run_type="weekly_sweep"} 1' in rendered
    assert 'hhru_detail_repair_still_failing_total{run_type="weekly_sweep"} 1' in rendered
    assert 'hhru_first_detail_backlog_size{scope="active"} 42.000000' in rendered
    assert (
        'hhru_first_detail_drain_attempt_total{scope="active",outcome="succeeded"} 1'
        in rendered
    )
    assert 'hhru_first_detail_drain_selected_total{scope="active"} 10' in rendered
    assert 'hhru_first_detail_drain_succeeded_total{scope="active"} 8' in rendered
    assert 'hhru_first_detail_drain_terminal_total{scope="active"} 2' in rendered
    assert 'hhru_housekeeping_run_total{mode="dry_run",status="succeeded"} 1' in rendered
    assert "hhru_housekeeping_last_run_timestamp_seconds" in rendered
    assert 'hhru_housekeeping_last_run_status{status="succeeded"} 1.0' in rendered
    assert 'hhru_housekeeping_last_run_mode{mode="dry_run"} 1.0' in rendered
    assert (
        'hhru_housekeeping_last_action_count{target="raw_api_payload",mode="dry_run"} 12.000000'
        in rendered
    )
    assert 'hhru_housekeeping_deleted_total{target="crawl_partition"} 5' in rendered
    assert "hhru_operation_duration_seconds_count" in rendered
    assert "hhru_upstream_request_duration_seconds_count" in rendered


def test_file_backed_metrics_registry_recovers_from_zero_filled_state(tmp_path) -> None:
    metrics_file = tmp_path / "metrics.json"
    metrics_file.write_bytes(b"\x00" * 1024)
    registry = FileBackedMetricsRegistry(metrics_file)

    registry.record_operation(
        operation="drain_first_detail_backlog",
        status="succeeded",
        duration_seconds=0.1,
    )

    rendered = registry.render_prometheus()

    assert (
        'hhru_operation_total{operation="drain_first_detail_backlog",status="succeeded"} 1'
        in rendered
    )


def test_json_log_formatter_keeps_structured_fields() -> None:
    output = StringIO()
    handler = logging.StreamHandler(output)
    handler.setFormatter(JsonLogFormatter())
    handler.addFilter(ServiceContextFilter(service_name="hhru-platform", env="test"))

    logger = logging.Logger("test-observability")
    logger.handlers = [handler]
    logger.setLevel(logging.INFO)
    logger.propagate = False

    log_event(
        logger,
        logging.INFO,
        "process_list_page.succeeded",
        operation="process_list_page",
        status="succeeded",
        run_id=uuid4(),
        partition_id=uuid4(),
        duration_ms=123,
    )

    payload = json.loads(output.getvalue())

    assert payload["event"] == "process_list_page.succeeded"
    assert payload["operation"] == "process_list_page"
    assert payload["status"] == "succeeded"
    assert payload["duration_ms"] == 123
    assert payload["service_name"] == "hhru-platform"
    assert payload["env"] == "test"
