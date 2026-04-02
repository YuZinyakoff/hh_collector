from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from notebooks import hh_api_probe_harness as harness


def test_redact_headers_masks_authorization() -> None:
    sanitized = harness.redact_headers(
        {
            "Authorization": "Bearer secret-token",
            "User-Agent": "probe-bot/0.1",
        }
    )

    assert sanitized["Authorization"] == "<redacted>"
    assert sanitized["User-Agent"] == "probe-bot/0.1"


def test_default_auth_mode_prefers_application_token_when_configured(
    monkeypatch,
) -> None:
    monkeypatch.setattr(harness, "APPLICATION_TOKEN", "secret-token")

    assert harness.default_auth_mode() == "application_token"
    assert harness.resolve_auth_mode() == "application_token"


def test_default_auth_mode_falls_back_to_anonymous_without_token(
    monkeypatch,
) -> None:
    monkeypatch.setattr(harness, "APPLICATION_TOKEN", None)

    assert harness.default_auth_mode() == "anonymous"
    assert harness.resolve_auth_mode() == "anonymous"
    assert harness.build_auth_headers() == {}


def test_probe_report_tracks_timing_and_captcha_metadata() -> None:
    records: list[dict[str, object]] = []

    first = {
        "timestamp_utc": "2026-03-28T10:00:00+00:00",
        "status_code": 200,
        "latency_ms": 150,
        "endpoint": "/vacancies",
        "endpoint_kind": "search",
        "auth_mode": "anonymous",
        "params": {"area": "1003", "page": 0, "per_page": 20},
        "request_headers": {"User-Agent": "probe-bot/0.1"},
        "header_mode": "dual",
        "response_headers": {},
        "error_type": None,
        "error_value": None,
        "captcha_url": None,
        "captcha_url_with_backurl": None,
        "request_id": None,
        "items_count": 20,
        "found": 100,
        "pages": 5,
    }
    harness.annotate_probe_record(
        first,
        records,
        scenario_label="sequential-area-1003",
        scenario_type="sequential_area",
        workers=1,
        pause_seconds=2.0,
    )
    records.append(first)

    second = {
        "timestamp_utc": "2026-03-28T10:00:02+00:00",
        "status_code": 403,
        "latency_ms": 220,
        "endpoint": "/vacancies",
        "endpoint_kind": "search",
        "auth_mode": "anonymous",
        "params": {"area": "1003", "page": 1, "per_page": 20},
        "request_headers": {"Authorization": "Bearer secret-token", "User-Agent": "probe-bot/0.1"},
        "header_mode": "dual",
        "response_headers": {},
        "error_type": "captcha_required",
        "error_value": "captcha_required",
        "captcha_url": "https://hh.example/captcha",
        "captcha_url_with_backurl": "https://hh.example/captcha?backurl=1",
        "request_id": "request-123",
        "items_count": 0,
        "found": 100,
        "pages": 5,
    }
    harness.annotate_probe_record(
        second,
        records,
        scenario_label="sequential-area-1003",
        scenario_type="sequential_area",
        workers=1,
        pause_seconds=2.0,
    )
    records.append(second)

    assert first["request_index_from_run_start"] == 1
    assert first["seconds_since_previous_request"] is None
    assert second["request_index_from_run_start"] == 2
    assert second["seconds_since_previous_request"] == 2.0
    assert second["minutes_since_first_captcha"] == 0.0
    assert second["request_headers"]["Authorization"] == "<redacted>"

    report = harness.build_probe_report(records)
    summary = report["summary"]
    transition = report["transition"]

    assert summary["scenario_label"] == "sequential-area-1003"
    assert summary["scenario_type"] == "sequential_area"
    assert summary["endpoint_kind"] == "search"
    assert summary["auth_mode"] == "anonymous"
    assert summary["header_mode"] == "dual"
    assert summary["requests_until_first_403"] == 2
    assert summary["requests_until_first_captcha"] == 2
    assert summary["wall_clock_until_first_captcha_seconds"] == 2.0
    assert summary["latency_ms_p50"] == 185.0
    assert summary["first_captcha_request_id"] == "request-123"
    assert transition["first_403"]["error_type"] == "captcha_required"
    assert transition["last_success"]["params"] == {"area": "1003", "page": 0, "per_page": 20}


def test_extract_detail_ids_from_records_preserves_unique_order() -> None:
    records = [
        {
            "payload": {
                "items": [
                    {"id": "10"},
                    {"id": "20"},
                    {"id": "10"},
                ]
            }
        },
        {
            "payload": {
                "items": [
                    {"id": 30},
                    {"id": "20"},
                ]
            }
        },
    ]

    assert harness.extract_detail_ids_from_records(records) == ["10", "20", "30"]


def test_build_search_after_coverage_plan_appends_detail_phase() -> None:
    search_sequence = [
        {
            "params": {"area": "1", "page": 0, "per_page": 20},
            "payload": {"items": [{"id": "100"}, {"id": "200"}]},
            "source_request_log_id": "req-1",
        },
        {
            "params": {"area": "1", "page": 1, "per_page": 20},
            "payload": {"items": [{"id": "300"}]},
            "source_request_log_id": "req-2",
        },
    ]

    plan = harness.build_search_after_coverage_plan(search_sequence, detail_budget=2)

    assert [item["endpoint"] for item in plan] == [
        "/vacancies",
        "/vacancies",
        "/vacancies/100",
        "/vacancies/200",
    ]
    assert plan[0]["extra_fields"]["plan_segment"] == "search_phase"
    assert plan[2]["extra_fields"]["plan_segment"] == "detail_phase"


def test_build_small_detail_budget_plan_interleaves_details() -> None:
    search_sequence = [
        {"params": {"page": 0}, "payload": {"items": [{"id": "100"}]}},
        {"params": {"page": 1}, "payload": {"items": [{"id": "200"}]}},
        {"params": {"page": 2}, "payload": {"items": [{"id": "300"}]}},
        {"params": {"page": 3}, "payload": {"items": [{"id": "400"}]}},
    ]

    plan = harness.build_small_detail_budget_plan(
        search_sequence,
        every_n_search=2,
        max_detail_requests=2,
    )

    assert [item["endpoint"] for item in plan] == [
        "/vacancies",
        "/vacancies",
        "/vacancies/100",
        "/vacancies",
        "/vacancies",
        "/vacancies/200",
    ]
    assert plan[2]["extra_fields"]["detail_index"] == 1
    assert plan[5]["extra_fields"]["detail_index"] == 2


def test_mixed_workload_summary_aggregates_endpoint_breakdown() -> None:
    records = [
        {
            "scenario_label": "mixed-check",
            "endpoint_kind": "search",
            "status_code": 200,
            "error_type": None,
            "request_index_from_run_start": 1,
        },
        {
            "scenario_label": "mixed-check",
            "endpoint_kind": "detail",
            "status_code": 200,
            "error_type": None,
            "request_index_from_run_start": 2,
        },
        {
            "scenario_label": "mixed-check",
            "endpoint_kind": "search",
            "status_code": 403,
            "error_type": "captcha_required",
            "request_index_from_run_start": 3,
        },
    ]

    summary = harness.build_mixed_workload_summary(records, mixed_mode="small_detail_budget")

    assert summary["mixed_mode"] == "small_detail_budget"
    assert summary["endpoint_breakdown"]["search"]["requests"] == 2
    assert summary["endpoint_breakdown"]["search"]["status_counts"] == {"200": 1, "403": 1}
    assert summary["endpoint_breakdown"]["search"]["first_captcha_request_index"] == 3
    assert summary["endpoint_breakdown"]["detail"]["requests"] == 1
