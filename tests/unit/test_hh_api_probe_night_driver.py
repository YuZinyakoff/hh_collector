from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from notebooks import hh_api_probe_night_driver as driver


def _run_result(
    *,
    requests_until_first_captcha: int | None,
    search_ok: int,
    captcha_idx: int | None,
) -> dict[str, object]:
    return {
        "summary": {
            "total_requests": 144,
            "requests_until_first_captcha": requests_until_first_captcha,
            "latency_ms_p50": 480.0,
            "latency_ms_p95": 700.0,
        },
        "search_ok_before_first_search_captcha": search_ok,
        "first_search_captcha_search_request_index": captcha_idx,
        "detail_200_count": 24,
    }


def _gate(attempts: int) -> dict[str, object]:
    return {
        "satisfied": True,
        "attempts": [{"attempt": idx + 1} for idx in range(attempts)],
    }


def test_build_session_aggregate_captures_recovery_improvement() -> None:
    state = {
        "slots": [
            {
                "slot_id": "slot-001",
                "status": "completed",
                "seed_preflight": _gate(3),
                "recovery_preflight": _gate(3),
                "seed_run": _run_result(
                    requests_until_first_captcha=141,
                    search_ok=117,
                    captcha_idx=118,
                ),
                "recovery_run": _run_result(
                    requests_until_first_captcha=None,
                    search_ok=120,
                    captcha_idx=None,
                ),
            },
            {
                "slot_id": "slot-002",
                "status": "completed",
                "seed_preflight": _gate(4),
                "recovery_preflight": _gate(3),
                "seed_run": _run_result(
                    requests_until_first_captcha=142,
                    search_ok=118,
                    captcha_idx=119,
                ),
                "recovery_run": _run_result(
                    requests_until_first_captcha=141,
                    search_ok=117,
                    captcha_idx=118,
                ),
            },
        ]
    }

    aggregate = driver.build_session_aggregate(state)

    assert aggregate["total_slots"] == 2
    assert aggregate["completed_slots"] == 2
    assert aggregate["seed_clean_count"] == 0
    assert aggregate["recovery_clean_count"] == 1
    assert aggregate["recovery_better_than_seed_count"] == 1
    assert aggregate["avg_seed_gate_attempts"] == 3.5
    assert aggregate["avg_recovery_gate_attempts"] == 3.0
    assert aggregate["min_recovery_search_ok"] == 117
    assert aggregate["max_recovery_search_ok"] == 120


def test_render_session_markdown_includes_slot_rows() -> None:
    state = {
        "status": "running",
        "run_label": "20260330T010203Z-test",
        "started_at_utc": "2026-03-30T01:02:03+00:00",
        "finished_at_utc": None,
        "source_sequence_path": ".state/reports/hh-api-probe/sample.jsonl",
        "slot_profile_sequence": ["control-short", "aggr-long"],
        "slots": [
            {
                "slot_id": "slot-001",
                "slot_profile_name": "aggr-long",
                "workers": 4,
                "recovery_window_seconds": 300,
                "status": "completed",
                "seed_preflight": _gate(3),
                "recovery_preflight": _gate(3),
                "seed_run": _run_result(
                    requests_until_first_captcha=141,
                    search_ok=117,
                    captcha_idx=118,
                ),
                "recovery_run": _run_result(
                    requests_until_first_captcha=None,
                    search_ok=120,
                    captcha_idx=None,
                ),
            }
        ],
    }

    markdown = driver.render_session_markdown(state)

    assert "# HH API Probe Night Driver" in markdown
    assert "- recovery_clean_count: 1" in markdown
    assert "- slot_profile_sequence: control-short, aggr-long" in markdown
    assert (
        "| slot-001 | aggr-long | 4 | 300 | completed | 3 | 117 | 118 | 3 | 120 | None |"
        in markdown
    )


def test_parse_args_accepts_slot_profile_sequence() -> None:
    config = driver.parse_args(
        [
            "--slot-profile-sequence",
            "control-short,aggr-short,control-long,aggr-long",
            "--control-workers",
            "3",
            "--long-recovery-window-seconds",
            "300",
        ]
    )

    assert config.control_workers == 3
    assert config.long_recovery_window_seconds == 300
    assert config.slot_profile_sequence == (
        "control-short",
        "aggr-short",
        "control-long",
        "aggr-long",
    )


def test_build_slot_profiles_exposes_control_and_aggressive_variants() -> None:
    config = driver.NightDriverConfig(
        workers=4,
        control_workers=3,
        recovery_window_seconds=120,
        long_recovery_window_seconds=300,
        slot_profile_sequence=("default",),
    )

    profiles = driver.build_slot_profiles(config)

    assert profiles["default"].workers == 4
    assert profiles["aggr-short"].recovery_window_seconds == 120
    assert profiles["control-short"].workers == 3
    assert profiles["aggr-long"].recovery_window_seconds == 300


def test_parse_args_defaults_auth_mode_from_harness(monkeypatch) -> None:
    monkeypatch.setattr(driver.h, "default_auth_mode", lambda: "application_token")

    config = driver.parse_args([])

    assert config.auth_mode == "application_token"
