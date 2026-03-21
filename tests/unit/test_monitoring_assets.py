from __future__ import annotations

import json
import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


def test_grafana_dashboards_are_valid_json_assets() -> None:
    dashboard_paths = (
        REPO_ROOT / "monitoring" / "grafana" / "dashboards" / "collector-overview.json",
        REPO_ROOT / "monitoring" / "grafana" / "dashboards" / "hh-api-ingest-health.json",
        REPO_ROOT
        / "monitoring"
        / "grafana"
        / "dashboards"
        / "scheduler-recovery-health.json",
    )

    for dashboard_path in dashboard_paths:
        payload = json.loads(dashboard_path.read_text(encoding="utf-8"))

        assert isinstance(payload["title"], str)
        assert payload["title"]
        assert isinstance(payload["panels"], list)
        assert payload["panels"]
        assert payload["uid"]


def test_grafana_provisioning_points_to_prometheus_and_repo_dashboards() -> None:
    datasource_config = (
        REPO_ROOT / "monitoring" / "grafana" / "provisioning" / "datasources" / "prometheus.yml"
    ).read_text(encoding="utf-8")
    dashboards_config = (
        REPO_ROOT / "monitoring" / "grafana" / "provisioning" / "dashboards" / "dashboards.yml"
    ).read_text(encoding="utf-8")

    assert "url: http://prometheus:9090" in datasource_config
    assert "uid: prometheus" in datasource_config
    assert "path: /var/lib/grafana/dashboards" in dashboards_config


def test_grafana_range_stat_panels_query_selected_interval_as_instant_values() -> None:
    collector_dashboard = _load_dashboard("collector-overview.json")
    failures_panel = _find_panel(collector_dashboard, "Failures In Range")
    failures_target = failures_panel["targets"][0]

    assert (
        failures_target["expr"]
        == 'sum(increase(hhru_operation_total{status="failed"}[$__range]))'
    )
    assert failures_target["instant"] is True

    ingest_dashboard = _load_dashboard("hh-api-ingest-health.json")
    upstream_errors_panel = _find_panel(ingest_dashboard, "Upstream Errors In Range")
    upstream_errors_target = upstream_errors_panel["targets"][0]

    assert (
        upstream_errors_target["expr"]
        == (
            'sum(increase(hhru_upstream_request_total'
            '{status_class=~"4xx|5xx|network_error|timeout|transport_error"}[$__range]))'
        )
    )
    assert upstream_errors_target["instant"] is True


def test_grafana_last_success_tables_render_operation_and_wall_clock_timestamp() -> None:
    collector_dashboard = _load_dashboard("collector-overview.json")
    collector_panel = _find_panel(collector_dashboard, "Last Success Timestamps")

    assert collector_panel["targets"][0]["expr"] == (
        "sort_desc(1000 * max by (operation) "
        "(hhru_operation_last_success_timestamp_seconds))"
    )
    assert collector_panel["targets"][0]["format"] == "table"
    assert collector_panel["targets"][0]["instant"] is True
    assert collector_panel["transformations"][0]["options"]["excludeByName"]["Time"] is True
    assert collector_panel["transformations"][0]["options"]["renameByName"] == {
        "operation": "Operation",
        "Value": "Last Success",
    }
    assert _field_override(collector_panel, "Last Success")["properties"] == [
        {"id": "unit", "value": "dateTimeAsIso"}
    ]

    ingest_dashboard = _load_dashboard("hh-api-ingest-health.json")
    ingest_panel = _find_panel(ingest_dashboard, "Last Success By Critical Operation")

    assert ingest_panel["targets"][0]["expr"] == (
        "sort_desc(1000 * max by (operation) "
        '(hhru_operation_last_success_timestamp_seconds{operation=~"sync_dictionary|process_list_page|fetch_vacancy_detail|reconcile_run"}))'
    )
    assert ingest_panel["targets"][0]["format"] == "table"
    assert ingest_panel["targets"][0]["instant"] is True
    assert ingest_panel["transformations"][0]["options"]["excludeByName"]["Time"] is True
    assert ingest_panel["transformations"][0]["options"]["renameByName"] == {
        "operation": "Operation",
        "Value": "Last Success",
    }
    assert _field_override(ingest_panel, "Last Success")["properties"] == [
        {"id": "unit", "value": "dateTimeAsIso"}
    ]


def test_grafana_run_coverage_panels_use_run_tree_metrics() -> None:
    collector_dashboard = _load_dashboard("collector-overview.json")

    coverage_table = _find_panel(collector_dashboard, "Planner V2 Coverage By Run")
    assert coverage_table["targets"][0]["expr"] == "sort_desc(hhru_run_tree_coverage_ratio)"
    assert coverage_table["targets"][0]["format"] == "table"
    assert coverage_table["targets"][0]["instant"] is True
    assert coverage_table["transformations"][0]["options"]["renameByName"] == {
        "run_id": "Run ID",
        "run_type": "Run Type",
        "Value": "Coverage Ratio",
    }
    assert _field_override(coverage_table, "Coverage Ratio")["properties"] == [
        {"id": "unit", "value": "percentunit"}
    ]

    assert _find_panel(collector_dashboard, "Total Partitions")["targets"][0]["expr"] == (
        "sum(hhru_run_tree_total_partitions)"
    )
    assert _find_panel(collector_dashboard, "Covered Terminal Partitions")["targets"][0][
        "expr"
    ] == "sum(hhru_run_tree_covered_terminal_partitions)"
    assert _find_panel(collector_dashboard, "Pending Terminal Partitions")["targets"][0][
        "expr"
    ] == "sum(hhru_run_tree_pending_terminal_partitions)"
    assert _find_panel(collector_dashboard, "Failed Partitions")["targets"][0]["expr"] == (
        "sum(hhru_run_tree_failed_partitions)"
    )
    assert _find_panel(collector_dashboard, "Split Partitions")["targets"][0][
        "expr"
    ] == "sum(hhru_run_tree_split_partitions)"
    assert _find_panel(collector_dashboard, "Unresolved Partitions")["targets"][0][
        "expr"
    ] == "sum(hhru_run_tree_unresolved_partitions)"


def test_grafana_scheduler_panels_use_scheduler_metrics() -> None:
    collector_dashboard = _load_dashboard("collector-overview.json")

    overlap_panel = _find_panel(collector_dashboard, "Scheduler Overlap Skips In Range")
    assert overlap_panel["targets"][0]["expr"] == (
        'sum(increase(hhru_scheduler_tick_total{outcome="skipped_overlap"}[$__range]))'
    )
    assert overlap_panel["targets"][0]["instant"] is True

    active_run_panel = _find_panel(collector_dashboard, "Scheduler Active-Run Skips In Range")
    assert active_run_panel["targets"][0]["expr"] == (
        'sum(increase(hhru_scheduler_tick_total{outcome="skipped_active_run"}[$__range]))'
    )
    assert active_run_panel["targets"][0]["instant"] is True

    last_tick_panel = _find_panel(collector_dashboard, "Scheduler Last Tick")
    assert last_tick_panel["targets"][0]["expr"] == (
        "1000 * hhru_scheduler_last_tick_timestamp_seconds"
    )
    assert last_tick_panel["fieldConfig"]["defaults"]["unit"] == "dateTimeAsIso"

    last_triggered_panel = _find_panel(collector_dashboard, "Scheduler Last Triggered Run")
    assert last_triggered_panel["targets"][0]["expr"] == (
        "1000 * hhru_scheduler_last_triggered_run_timestamp_seconds"
    )
    assert last_triggered_panel["fieldConfig"]["defaults"]["unit"] == "dateTimeAsIso"

    last_run_panel = _find_panel(collector_dashboard, "Scheduler Last Run Finished")
    assert last_run_panel["targets"][0]["expr"] == (
        "1000 * hhru_scheduler_last_run_finished_timestamp_seconds"
    )
    assert last_run_panel["fieldConfig"]["defaults"]["unit"] == "dateTimeAsIso"

    last_status_panel = _find_panel(collector_dashboard, "Scheduler Last Observed Run Status")
    assert last_status_panel["targets"][0]["expr"] == (
        "sort_desc(max by (status) (hhru_scheduler_last_observed_run_status == 1))"
    )
    assert last_status_panel["targets"][0]["format"] == "table"
    assert last_status_panel["targets"][0]["instant"] is True


def test_grafana_recovery_panels_use_lifecycle_metrics() -> None:
    collector_dashboard = _load_dashboard("collector-overview.json")

    run_status_panel = _find_panel(collector_dashboard, "Run Terminal Statuses In Range")
    assert run_status_panel["targets"][0]["expr"] == (
        "sort_desc(sum by (status) (increase(hhru_run_terminal_status_total[$__range])))"
    )
    assert run_status_panel["targets"][0]["format"] == "table"
    assert run_status_panel["targets"][0]["instant"] is True

    backlog_panel = _find_panel(collector_dashboard, "Detail Repair Backlog")
    assert backlog_panel["targets"][0]["expr"] == "sum(hhru_detail_repair_backlog_size)"
    assert backlog_panel["targets"][0]["instant"] is True

    repaired_panel = _find_panel(collector_dashboard, "Detail Repaired In Range")
    assert repaired_panel["targets"][0]["expr"] == (
        "sum(increase(hhru_detail_repair_repaired_total[$__range]))"
    )
    assert repaired_panel["targets"][0]["instant"] is True

    still_failing_panel = _find_panel(collector_dashboard, "Detail Still Failing In Range")
    assert still_failing_panel["targets"][0]["expr"] == (
        "sum(increase(hhru_detail_repair_still_failing_total[$__range]))"
    )
    assert still_failing_panel["targets"][0]["instant"] is True

    resume_attempts_panel = _find_panel(collector_dashboard, "Resume Attempts In Range")
    assert resume_attempts_panel["targets"][0]["expr"] == (
        "sum(increase(hhru_resume_run_v2_attempt_total[$__range]))"
    )
    assert resume_attempts_panel["targets"][0]["instant"] is True

    repair_attempts_panel = _find_panel(collector_dashboard, "Detail Repair Attempts In Range")
    assert repair_attempts_panel["targets"][0]["expr"] == (
        "sum(increase(hhru_detail_repair_attempt_total[$__range]))"
    )
    assert repair_attempts_panel["targets"][0]["instant"] is True


def test_scheduler_recovery_dashboard_uses_recording_rules_and_debt_tables() -> None:
    dashboard = _load_dashboard("scheduler-recovery-health.json")

    tick_age_panel = _find_panel(dashboard, "Scheduler Tick Age")
    assert tick_age_panel["targets"][0]["expr"] == "hhru:scheduler_tick_age_seconds"
    assert tick_age_panel["fieldConfig"]["defaults"]["unit"] == "s"

    triggered_age_panel = _find_panel(dashboard, "Last Triggered Run Age")
    assert triggered_age_panel["targets"][0]["expr"] == (
        "hhru:scheduler_last_triggered_run_age_seconds"
    )
    assert triggered_age_panel["fieldConfig"]["defaults"]["unit"] == "s"

    failed_panel = _find_panel(dashboard, "Open Failed Partitions")
    assert failed_panel["targets"][0]["expr"] == "hhru:coverage_failed_partitions_open"

    unresolved_panel = _find_panel(dashboard, "Open Unresolved Partitions")
    assert unresolved_panel["targets"][0]["expr"] == "hhru:coverage_unresolved_partitions_open"

    backlog_panel = _find_panel(dashboard, "Open Detail Repair Backlog")
    assert backlog_panel["targets"][0]["expr"] == "hhru:detail_repair_backlog_open"

    resume_again_panel = _find_panel(dashboard, "Resume Unresolved Again In 12h")
    assert resume_again_panel["targets"][0]["expr"] == (
        'sum(increase(hhru_resume_run_v2_attempt_total{outcome="completed_with_unresolved"}[12h]))'
    )

    failed_runs_table = _find_panel(dashboard, "Runs With Failed Partitions")
    assert failed_runs_table["targets"][0]["expr"] == (
        "sort_desc(hhru_run_tree_failed_partitions > 0)"
    )
    assert failed_runs_table["targets"][0]["format"] == "table"

    unresolved_runs_table = _find_panel(dashboard, "Runs With Unresolved Partitions")
    assert unresolved_runs_table["targets"][0]["expr"] == (
        "sort_desc(hhru_run_tree_unresolved_partitions > 0)"
    )

    backlog_runs_table = _find_panel(dashboard, "Runs With Detail Repair Backlog")
    assert backlog_runs_table["targets"][0]["expr"] == (
        "sort_desc(hhru_detail_repair_backlog_size > 0)"
    )

    resume_outcomes_panel = _find_panel(dashboard, "Resume Outcomes In Range")
    assert resume_outcomes_panel["targets"][0]["expr"] == (
        "sort_desc(sum by (outcome) (increase(hhru_resume_run_v2_attempt_total[$__range])))"
    )

    repair_outcomes_panel = _find_panel(dashboard, "Detail Repair Outcomes In Range")
    assert repair_outcomes_panel["targets"][0]["expr"] == (
        "sort_desc(sum by (outcome) (increase(hhru_detail_repair_attempt_total[$__range])))"
    )

    housekeeping_age_panel = _find_panel(dashboard, "Housekeeping Last Run Age")
    assert housekeeping_age_panel["targets"][0]["expr"] == (
        "hhru:housekeeping_last_run_age_seconds"
    )
    assert housekeeping_age_panel["fieldConfig"]["defaults"]["unit"] == "s"

    housekeeping_status_panel = _find_panel(dashboard, "Housekeeping Last Run Status")
    assert housekeeping_status_panel["targets"][0]["expr"] == (
        "sort_desc(max by (status) (hhru_housekeeping_last_run_status == 1))"
    )

    housekeeping_mode_panel = _find_panel(dashboard, "Housekeeping Last Run Mode")
    assert housekeeping_mode_panel["targets"][0]["expr"] == (
        "sort_desc(max by (mode) (hhru_housekeeping_last_run_mode == 1))"
    )

    housekeeping_deleted_panel = _find_panel(dashboard, "Housekeeping Deletions In Range")
    assert housekeeping_deleted_panel["targets"][0]["expr"] == (
        "sort_desc(sum by (target) (increase(hhru_housekeeping_deleted_total[$__range])))"
    )

    backup_age_panel = _find_panel(dashboard, "Backup Last Success Age")
    assert backup_age_panel["targets"][0]["expr"] == "hhru:backup_last_success_age_seconds"
    assert backup_age_panel["fieldConfig"]["defaults"]["unit"] == "s"

    backup_runs_panel = _find_panel(dashboard, "Backup Runs In Range")
    assert backup_runs_panel["targets"][0]["expr"] == (
        "sort_desc(sum by (status) (increase(hhru_backup_run_total[$__range])))"
    )

    restore_drill_age_panel = _find_panel(dashboard, "Restore Drill Last Success Age")
    assert restore_drill_age_panel["targets"][0]["expr"] == (
        "hhru:restore_drill_last_success_age_seconds"
    )
    assert restore_drill_age_panel["fieldConfig"]["defaults"]["unit"] == "s"

    restore_drill_runs_panel = _find_panel(dashboard, "Restore Drill Runs In Range")
    assert restore_drill_runs_panel["targets"][0]["expr"] == (
        "sort_desc(sum by (status) (increase(hhru_restore_drill_run_total[$__range])))"
    )


def test_prometheus_alert_rules_cover_scheduler_and_recovery_risks() -> None:
    rules_text = (
        REPO_ROOT / "monitoring" / "alerting" / "rules.yml"
    ).read_text(encoding="utf-8")

    assert "record: hhru:scheduler_tick_age_seconds" in rules_text
    assert "record: hhru:scheduler_last_triggered_run_age_seconds" in rules_text
    assert "record: hhru:coverage_failed_partitions_open" in rules_text
    assert "record: hhru:coverage_unresolved_partitions_open" in rules_text
    assert "record: hhru:detail_repair_backlog_open" in rules_text
    assert "record: hhru:housekeeping_last_run_age_seconds" in rules_text
    assert "record: hhru:backup_last_success_age_seconds" in rules_text
    assert "record: hhru:restore_drill_last_success_age_seconds" in rules_text

    alert_names = set(re.findall(r"alert:\s+([A-Za-z0-9]+)", rules_text))
    assert {
        "HHRUPlatformMetricsEndpointDown",
        "HHRUPlatformOperationFailures",
        "HHRUPlatformNoRecentReconciliation",
        "HHRUPlatformSchedulerTickStale",
        "HHRUPlatformSchedulerTriggeredRunStale",
        "HHRUPlatformFailedPartitionsPresent",
        "HHRUPlatformUnresolvedPartitionsStuck",
        "HHRUPlatformDetailRepairBacklogStuck",
        "HHRUPlatformResumeUnresolvedRepeatedly",
        "HHRUPlatformHousekeepingStale",
        "HHRUPlatformBackupStale",
    }.issubset(alert_names)

    assert "expr: hhru:scheduler_tick_age_seconds > 7200" in rules_text
    assert "expr: hhru:scheduler_last_triggered_run_age_seconds > 86400" in rules_text
    assert "expr: hhru:coverage_failed_partitions_open > 0" in rules_text
    assert "expr: hhru:coverage_unresolved_partitions_open > 0" in rules_text
    assert "expr: hhru:detail_repair_backlog_open > 0" in rules_text
    assert "expr: hhru:housekeeping_last_run_age_seconds > 604800" in rules_text
    assert "expr: hhru:backup_last_success_age_seconds > 259200" in rules_text
    assert (
        'expr: increase(hhru_resume_run_v2_attempt_total'
        '{outcome="completed_with_unresolved"}[12h]) >= 2'
        in rules_text
    )


def _load_dashboard(filename: str) -> dict:
    return json.loads(
        (REPO_ROOT / "monitoring" / "grafana" / "dashboards" / filename).read_text(
            encoding="utf-8"
        )
    )


def _find_panel(dashboard: dict, title: str) -> dict:
    for panel in dashboard["panels"]:
        if panel["title"] == title:
            return panel

    raise AssertionError(f"panel not found: {title}")


def _field_override(panel: dict, field_name: str) -> dict:
    for override in panel["fieldConfig"]["overrides"]:
        if override["matcher"] == {"id": "byName", "options": field_name}:
            return override

    raise AssertionError(f"override not found for field: {field_name}")
