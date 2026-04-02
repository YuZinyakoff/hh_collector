from __future__ import annotations

import argparse
import json
import signal
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from notebooks import hh_api_probe_harness as h


DEFAULT_SOURCE_SEQUENCE_PATH = (
    h.RESULTS_DIR / "20260328T114056Z-cbc1c13a-shape-historical-replay-prefix-130-0s.jsonl"
)
DEFAULT_PROBE_WINDOWS_SECONDS = (0, 300, 900, 1800, 3600, 7200)


@dataclass(slots=True)
class CooldownDriverConfig:
    source_sequence_path: Path = DEFAULT_SOURCE_SEQUENCE_PATH
    trigger_prefix: int = 130
    trigger_workers: int = 4
    trigger_burst_pause_seconds: float = 0.0
    header_mode: str = "dual"
    auth_mode: str = field(default_factory=h.default_auth_mode)
    stop_on_captcha: bool = True
    probe_windows_seconds: tuple[int, ...] = DEFAULT_PROBE_WINDOWS_SECONDS
    detail_fallback_id: str | None = None
    suppress_probe_logs: bool = True


class StopController:
    def __init__(self) -> None:
        self.stop_requested = False
        self.stop_signal_name: str | None = None

    def install(self) -> None:
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

    def _handle_signal(self, signum: int, _frame: Any) -> None:
        signal_name = signal.Signals(signum).name
        if self.stop_requested:
            raise KeyboardInterrupt(f"received repeated {signal_name}")
        self.stop_requested = True
        self.stop_signal_name = signal_name
        log(f"Stop requested via {signal_name}; finishing current step before exit.")


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def local_now_iso() -> str:
    return datetime.now().astimezone().isoformat()


def log(message: str) -> None:
    print(f"[{local_now_iso()}] {message}", flush=True)


def json_default(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"Object of type {type(value)!r} is not JSON serializable")


def parse_probe_windows(raw_value: str | None) -> tuple[int, ...]:
    if raw_value is None:
        return DEFAULT_PROBE_WINDOWS_SECONDS
    values: list[int] = []
    for part in raw_value.split(","):
        stripped = part.strip()
        if not stripped:
            continue
        value = int(stripped)
        if value < 0:
            raise ValueError("probe windows must be >= 0")
        values.append(value)
    if not values:
        raise ValueError("probe windows must contain at least one value")
    return tuple(dict.fromkeys(values))


def _build_search_plan_item(
    source_record: dict[str, Any],
    *,
    search_index: int,
) -> dict[str, Any]:
    return {
        "endpoint": "/vacancies",
        "params": dict(source_record.get("params") or {}),
        "extra_fields": {
            "plan_step_kind": "search",
            "plan_segment": "search_phase",
            "search_index": search_index,
            "source_request_log_id": source_record.get("source_request_log_id"),
            "source_partition_id": source_record.get("source_partition_id"),
            "source_status_code": source_record.get("status_code"),
            "source_timestamp_utc": source_record.get("timestamp_utc"),
            "source_request_headers": source_record.get("request_headers"),
        },
    }


def load_search_sequence(config: CooldownDriverConfig) -> list[dict[str, Any]]:
    sequence = h.load_jsonl(config.source_sequence_path)
    return sequence[: config.trigger_prefix]


def build_trigger_plan(search_sequence: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        _build_search_plan_item(source_record, search_index=index)
        for index, source_record in enumerate(search_sequence, start=1)
    ]


def first_captcha_timestamp(records: list[dict[str, Any]]) -> str | None:
    for record in records:
        if record.get("error_type") == "captcha_required":
            return record.get("timestamp_utc")
    return None


def select_detail_id(
    trigger_records: list[dict[str, Any]],
    search_sequence: list[dict[str, Any]],
    *,
    fallback_id: str | None = None,
) -> str | None:
    if fallback_id:
        return fallback_id
    detail_ids = h.extract_detail_ids_from_records(trigger_records, limit=1)
    if detail_ids:
        return detail_ids[0]
    fallback_ids = h.extract_detail_ids_from_records(search_sequence, limit=1)
    if fallback_ids:
        return fallback_ids[0]
    return None


def analyze_probe(records: list[dict[str, Any]], *, records_path: Path) -> dict[str, Any]:
    report = h.build_probe_report(records, records_path=records_path)
    summary = report["summary"]
    record = records[0] if records else {}
    return {
        "records_path": str(records_path),
        "report_path": str(h.make_probe_report_path(records_path)),
        "summary": summary,
        "endpoint": record.get("endpoint"),
        "endpoint_kind": record.get("endpoint_kind"),
        "status_code": record.get("status_code"),
        "error_type": record.get("error_type"),
        "network_error": record.get("network_error"),
        "minutes_since_first_captcha": record.get("minutes_since_first_captcha"),
        "is_clean_200": record.get("status_code") == 200 and record.get("error_type") is None,
    }


def run_single_probe(
    *,
    endpoint: str,
    params: dict[str, Any],
    label: str,
    config: CooldownDriverConfig,
    cooldown_origin_timestamp_utc: str | None,
) -> dict[str, Any]:
    records, path = h.run_fixed_request_probe(
        params=params,
        repeats=1,
        sleep_seconds=0.0,
        header_mode=config.header_mode,
        auth_mode=config.auth_mode,
        stop_on_captcha=True,
        label=label,
        endpoint=endpoint,
        cooldown_origin_timestamp_utc=cooldown_origin_timestamp_utc,
    )
    return analyze_probe(records, records_path=path)


def wait_until_probe_window(
    origin_timestamp_utc: str,
    *,
    window_seconds: int,
    stopper: StopController,
) -> bool:
    origin = h.parse_timestamp_utc(origin_timestamp_utc)
    if origin is None:
        return True
    deadline = origin.timestamp() + window_seconds
    while True:
        if stopper.stop_requested:
            return False
        remaining = deadline - time.time()
        if remaining <= 0:
            return True
        time.sleep(min(1.0, remaining))


def build_window_compact_summary(window: dict[str, Any]) -> dict[str, Any]:
    search_probe = window.get("search_probe") or {}
    detail_probe = window.get("detail_probe") or {}
    dictionary_probe = window.get("dictionary_probe") or {}
    return {
        "window_seconds": window.get("window_seconds"),
        "status": window.get("status"),
        "search_status": search_probe.get("status_code"),
        "search_error_type": search_probe.get("error_type"),
        "detail_status": detail_probe.get("status_code"),
        "detail_error_type": detail_probe.get("error_type"),
        "dictionary_status": dictionary_probe.get("status_code"),
        "dictionary_error_type": dictionary_probe.get("error_type"),
    }


def _first_clean_window_seconds(windows: list[dict[str, Any]], key: str) -> int | None:
    for window in windows:
        probe = window.get(key) or {}
        if probe.get("is_clean_200"):
            return window.get("window_seconds")
    return None


def build_session_aggregate(state: dict[str, Any]) -> dict[str, Any]:
    trigger_run = state.get("trigger_run") or {}
    trigger_summary = trigger_run.get("summary") or {}
    windows = state.get("windows") or []
    completed_windows = [window for window in windows if window.get("status") == "completed"]
    return {
        "trigger_total_requests": trigger_summary.get("total_requests"),
        "trigger_requests_until_first_captcha": trigger_summary.get("requests_until_first_captcha"),
        "trigger_status_counts": trigger_summary.get("status_counts"),
        "trigger_transport_error_count": trigger_run.get("transport_error_count"),
        "completed_windows": len(completed_windows),
        "search_clean_windows": sum(
            1 for window in windows if (window.get("search_probe") or {}).get("is_clean_200")
        ),
        "detail_clean_windows": sum(
            1 for window in windows if (window.get("detail_probe") or {}).get("is_clean_200")
        ),
        "dictionary_clean_windows": sum(
            1 for window in windows if (window.get("dictionary_probe") or {}).get("is_clean_200")
        ),
        "first_search_recovered_window_seconds": _first_clean_window_seconds(
            windows, "search_probe"
        ),
        "first_detail_recovered_window_seconds": _first_clean_window_seconds(
            windows, "detail_probe"
        ),
        "first_dictionary_recovered_window_seconds": _first_clean_window_seconds(
            windows, "dictionary_probe"
        ),
        "windows": [build_window_compact_summary(window) for window in windows],
    }


def render_session_markdown(state: dict[str, Any]) -> str:
    aggregate = build_session_aggregate(state)
    lines = [
        "# HH API Probe Cooldown Driver",
        "",
        f"- status: {state.get('status')}",
        f"- run_label: {state.get('run_label')}",
        f"- started_at_utc: {state.get('started_at_utc')}",
        f"- finished_at_utc: {state.get('finished_at_utc')}",
        f"- current_time_utc: {utc_now_iso()}",
        f"- source_sequence_path: {state.get('source_sequence_path')}",
        f"- auth_mode: {state.get('auth_mode')}",
        f"- trigger_workers: {state.get('trigger_workers')}",
        f"- trigger_burst_pause_seconds: {state.get('trigger_burst_pause_seconds')}",
        f"- probe_windows_seconds: {', '.join(str(value) for value in state.get('probe_windows_seconds') or [])}",
        "",
        "## Aggregate",
        "",
        f"- trigger_total_requests: {aggregate['trigger_total_requests']}",
        f"- trigger_requests_until_first_captcha: {aggregate['trigger_requests_until_first_captcha']}",
        f"- trigger_transport_error_count: {aggregate['trigger_transport_error_count']}",
        f"- completed_windows: {aggregate['completed_windows']}",
        f"- search_clean_windows: {aggregate['search_clean_windows']}",
        f"- detail_clean_windows: {aggregate['detail_clean_windows']}",
        f"- dictionary_clean_windows: {aggregate['dictionary_clean_windows']}",
        f"- first_search_recovered_window_seconds: {aggregate['first_search_recovered_window_seconds']}",
        f"- first_detail_recovered_window_seconds: {aggregate['first_detail_recovered_window_seconds']}",
        f"- first_dictionary_recovered_window_seconds: {aggregate['first_dictionary_recovered_window_seconds']}",
        "",
        "## Windows",
        "",
        "| window seconds | status | search | search error | detail | detail error | dictionary | dictionary error |",
        "| --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for window in aggregate["windows"]:
        lines.append(
            "| "
            f"{window['window_seconds']} | "
            f"{window['status']} | "
            f"{window['search_status']} | "
            f"{window['search_error_type']} | "
            f"{window['detail_status']} | "
            f"{window['detail_error_type']} | "
            f"{window['dictionary_status']} | "
            f"{window['dictionary_error_type']} |"
        )
    lines.append("")
    return "\n".join(lines)


def persist_state(state: dict[str, Any], summary_path: Path, markdown_path: Path) -> None:
    state["aggregate"] = build_session_aggregate(state)
    summary_path.write_text(
        json.dumps(state, indent=2, ensure_ascii=False, default=json_default) + "\n",
        encoding="utf-8",
    )
    markdown_path.write_text(render_session_markdown(state), encoding="utf-8")


def run_trigger(
    *,
    search_sequence: list[dict[str, Any]],
    config: CooldownDriverConfig,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    plan = build_trigger_plan(search_sequence)
    label = (
        "cooldown-trigger-search-only-prefix-"
        f"{config.trigger_prefix}-workers-{config.trigger_workers}-"
        f"burst{int(config.trigger_burst_pause_seconds)}s-{config.header_mode}-{config.auth_mode}"
    )
    records, path = h.run_request_plan(
        plan,
        workers=config.trigger_workers,
        sleep_seconds=0.0,
        burst_pause_seconds=config.trigger_burst_pause_seconds,
        header_mode=config.header_mode,
        auth_mode=config.auth_mode,
        stop_on_captcha=config.stop_on_captcha,
        label=label,
        scenario_type="cooldown_trigger",
    )
    report = h.build_probe_report(records, records_path=path)
    trigger_run = {
        "records_path": str(path),
        "report_path": str(h.make_probe_report_path(path)),
        "summary": report["summary"],
        "transport_error_count": sum(1 for record in records if record.get("status_code") is None),
        "first_captcha_timestamp_utc": first_captcha_timestamp(records),
    }
    return records, trigger_run


def run_window(
    *,
    window_seconds: int,
    search_params: dict[str, Any],
    detail_id: str | None,
    config: CooldownDriverConfig,
    cooldown_origin_timestamp_utc: str,
    stopper: StopController,
) -> dict[str, Any]:
    window: dict[str, Any] = {
        "window_seconds": window_seconds,
        "status": "running",
        "scheduled_for_utc": cooldown_origin_timestamp_utc,
    }
    if not wait_until_probe_window(
        cooldown_origin_timestamp_utc,
        window_seconds=window_seconds,
        stopper=stopper,
    ):
        window["status"] = "aborted"
        window["finished_at_utc"] = utc_now_iso()
        return window

    prefix = f"cooldown-window-{window_seconds}s"
    window["started_at_utc"] = utc_now_iso()
    window["search_probe"] = run_single_probe(
        endpoint="/vacancies",
        params=search_params,
        label=f"{prefix}-search",
        config=config,
        cooldown_origin_timestamp_utc=cooldown_origin_timestamp_utc,
    )
    if detail_id:
        window["detail_probe"] = run_single_probe(
            endpoint=f"/vacancies/{detail_id}",
            params={},
            label=f"{prefix}-detail-{detail_id}",
            config=config,
            cooldown_origin_timestamp_utc=cooldown_origin_timestamp_utc,
        )
    else:
        window["detail_probe"] = None
    window["dictionary_probe"] = run_single_probe(
        endpoint="/dictionaries",
        params={},
        label=f"{prefix}-dictionaries",
        config=config,
        cooldown_origin_timestamp_utc=cooldown_origin_timestamp_utc,
    )
    window["status"] = "completed"
    window["finished_at_utc"] = utc_now_iso()
    return window


def run_session(config: CooldownDriverConfig) -> tuple[dict[str, Any], Path, Path]:
    stopper = StopController()
    stopper.install()
    summary_path = h.RESULTS_DIR / f"{h.RUN_LABEL}-cooldown-driver-summary.json"
    markdown_path = h.RESULTS_DIR / f"{h.RUN_LABEL}-cooldown-driver-summary.md"
    state: dict[str, Any] = {
        "run_label": h.RUN_LABEL,
        "status": "running",
        "started_at_utc": utc_now_iso(),
        "finished_at_utc": None,
        "source_sequence_path": str(config.source_sequence_path),
        "auth_mode": config.auth_mode,
        "header_mode": config.header_mode,
        "trigger_prefix": config.trigger_prefix,
        "trigger_workers": config.trigger_workers,
        "trigger_burst_pause_seconds": config.trigger_burst_pause_seconds,
        "probe_windows_seconds": list(config.probe_windows_seconds),
        "config": asdict(config),
        "trigger_run": None,
        "detail_probe_vacancy_id": None,
        "windows": [],
    }
    persist_state(state, summary_path, markdown_path)

    search_sequence = load_search_sequence(config)
    search_params = dict(search_sequence[0].get("params") or {})
    trigger_records, trigger_run = run_trigger(search_sequence=search_sequence, config=config)
    state["trigger_run"] = trigger_run

    cooldown_origin_timestamp_utc = trigger_run.get("first_captcha_timestamp_utc")
    if cooldown_origin_timestamp_utc is None:
        state["status"] = "completed_no_captcha"
        state["finished_at_utc"] = utc_now_iso()
        persist_state(state, summary_path, markdown_path)
        return state, summary_path, markdown_path

    detail_id = select_detail_id(
        trigger_records,
        search_sequence,
        fallback_id=config.detail_fallback_id,
    )
    state["detail_probe_vacancy_id"] = detail_id

    for window_seconds in config.probe_windows_seconds:
        if stopper.stop_requested:
            state["status"] = "stopped_by_operator"
            break
        window = run_window(
            window_seconds=window_seconds,
            search_params=search_params,
            detail_id=detail_id,
            config=config,
            cooldown_origin_timestamp_utc=cooldown_origin_timestamp_utc,
            stopper=stopper,
        )
        state["windows"].append(window)
        persist_state(state, summary_path, markdown_path)

    if state["status"] == "running":
        state["status"] = "completed"
    state["finished_at_utc"] = utc_now_iso()
    persist_state(state, summary_path, markdown_path)
    return state, summary_path, markdown_path


def parse_args(argv: list[str] | None = None) -> CooldownDriverConfig:
    parser = argparse.ArgumentParser(description="Headless HH API cooldown/recovery driver")
    parser.add_argument("--source-sequence-path", type=Path, default=DEFAULT_SOURCE_SEQUENCE_PATH)
    parser.add_argument("--trigger-prefix", type=int, default=130)
    parser.add_argument("--trigger-workers", type=int, default=4)
    parser.add_argument("--trigger-burst-pause-seconds", type=float, default=0.0)
    parser.add_argument("--header-mode", default="dual")
    parser.add_argument("--auth-mode", default=h.default_auth_mode())
    parser.add_argument("--probe-windows-seconds", default=None)
    parser.add_argument("--detail-fallback-id", default=None)
    parser.add_argument(
        "--no-suppress-probe-logs",
        action="store_true",
        help="Keep per-request probe logs enabled.",
    )
    args = parser.parse_args(argv)
    return CooldownDriverConfig(
        source_sequence_path=args.source_sequence_path,
        trigger_prefix=args.trigger_prefix,
        trigger_workers=args.trigger_workers,
        trigger_burst_pause_seconds=args.trigger_burst_pause_seconds,
        header_mode=args.header_mode,
        auth_mode=args.auth_mode,
        probe_windows_seconds=parse_probe_windows(args.probe_windows_seconds),
        detail_fallback_id=args.detail_fallback_id,
        suppress_probe_logs=not args.no_suppress_probe_logs,
    )


def main(argv: list[str] | None = None) -> int:
    config = parse_args(argv)
    if config.suppress_probe_logs:
        h._log_probe_record = lambda *_args, **_kwargs: None
    log(
        "Cooldown driver started. Summary: "
        f"{h.RESULTS_DIR / f'{h.RUN_LABEL}-cooldown-driver-summary.json'}"
    )
    state, summary_path, markdown_path = run_session(config)
    log(f"Cooldown driver finished with status={state['status']}. Summary: {summary_path}")
    log(f"Markdown summary: {markdown_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
