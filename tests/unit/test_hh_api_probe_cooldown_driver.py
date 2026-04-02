from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from notebooks import hh_api_probe_cooldown_driver as driver


def _probe(
    *,
    status_code: int | None,
    error_type: str | None = None,
    is_clean_200: bool | None = None,
) -> dict[str, object]:
    if is_clean_200 is None:
        is_clean_200 = status_code == 200 and error_type is None
    return {
        "status_code": status_code,
        "error_type": error_type,
        "is_clean_200": is_clean_200,
    }


def test_parse_probe_windows_accepts_csv() -> None:
    assert driver.parse_probe_windows("0,300,900") == (0, 300, 900)


def test_build_session_aggregate_identifies_first_recovery_windows() -> None:
    state = {
        "trigger_run": {
            "summary": {
                "total_requests": 130,
                "requests_until_first_captcha": 119,
                "status_counts": {"200": 118, "403": 1},
            },
            "transport_error_count": 0,
        },
        "windows": [
            {
                "window_seconds": 0,
                "status": "completed",
                "search_probe": _probe(status_code=403, error_type="captcha_required"),
                "detail_probe": _probe(status_code=200),
                "dictionary_probe": _probe(status_code=200),
            },
            {
                "window_seconds": 300,
                "status": "completed",
                "search_probe": _probe(status_code=200),
                "detail_probe": _probe(status_code=200),
                "dictionary_probe": _probe(status_code=200),
            },
        ],
    }

    aggregate = driver.build_session_aggregate(state)

    assert aggregate["trigger_total_requests"] == 130
    assert aggregate["trigger_requests_until_first_captcha"] == 119
    assert aggregate["completed_windows"] == 2
    assert aggregate["search_clean_windows"] == 1
    assert aggregate["detail_clean_windows"] == 2
    assert aggregate["dictionary_clean_windows"] == 2
    assert aggregate["first_search_recovered_window_seconds"] == 300
    assert aggregate["first_detail_recovered_window_seconds"] == 0
    assert aggregate["first_dictionary_recovered_window_seconds"] == 0


def test_render_session_markdown_includes_window_rows() -> None:
    state = {
        "status": "completed",
        "run_label": "20260331T120000Z-test",
        "started_at_utc": "2026-03-31T12:00:00+00:00",
        "finished_at_utc": "2026-03-31T14:00:00+00:00",
        "source_sequence_path": ".state/reports/hh-api-probe/sample.jsonl",
        "auth_mode": "application_token",
        "trigger_workers": 4,
        "trigger_burst_pause_seconds": 0.0,
        "probe_windows_seconds": [0, 300],
        "trigger_run": {
            "summary": {
                "total_requests": 130,
                "requests_until_first_captcha": None,
                "status_counts": {"200": 125, "None": 5},
            },
            "transport_error_count": 5,
        },
        "windows": [
            {
                "window_seconds": 0,
                "status": "completed",
                "search_probe": _probe(status_code=200),
                "detail_probe": _probe(status_code=200),
                "dictionary_probe": _probe(status_code=200),
            }
        ],
    }

    markdown = driver.render_session_markdown(state)

    assert "# HH API Probe Cooldown Driver" in markdown
    assert "- trigger_transport_error_count: 5" in markdown
    assert "| 0 | completed | 200 | None | 200 | None | 200 | None |" in markdown


def test_parse_args_defaults_auth_mode_from_harness(monkeypatch) -> None:
    monkeypatch.setattr(driver.h, "default_auth_mode", lambda: "application_token")

    config = driver.parse_args([])

    assert config.auth_mode == "application_token"
