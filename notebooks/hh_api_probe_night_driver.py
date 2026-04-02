from __future__ import annotations

import argparse
import json
import signal
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from notebooks import hh_api_probe_harness as h


DEFAULT_SOURCE_SEQUENCE_PATH = (
    h.RESULTS_DIR / "20260328T114056Z-cbc1c13a-shape-historical-replay-prefix-130-0s.jsonl"
)


@dataclass(frozen=True, slots=True)
class SlotProfile:
    name: str
    workers: int
    recovery_window_seconds: int


@dataclass(slots=True)
class NightDriverConfig:
    source_sequence_path: Path = DEFAULT_SOURCE_SEQUENCE_PATH
    search_prefix: int = 120
    every_n_search: int = 5
    max_detail_requests: int = 24
    workers: int = 4
    control_workers: int = 3
    burst_pause_seconds: float = 1.0
    header_mode: str = "dual"
    auth_mode: str = field(default_factory=h.default_auth_mode)
    stop_on_captcha: bool = True
    gate_required_clean_probes: int = 3
    gate_max_attempts: int = 24
    gate_probe_sleep_seconds: float = 10.0
    recovery_window_seconds: int = 120
    long_recovery_window_seconds: int = 300
    slot_profile_sequence: tuple[str, ...] = ("default",)
    slot_interval_seconds: int = 7200
    first_slot_delay_seconds: int = 0
    max_slots: int | None = None
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


def parse_slot_profile_sequence(raw_value: str | None) -> tuple[str, ...]:
    if raw_value is None:
        return ("default",)
    names = tuple(part.strip() for part in raw_value.split(",") if part.strip())
    if not names:
        raise ValueError("slot profile sequence must contain at least one profile name")
    return names


def build_slot_profiles(config: NightDriverConfig) -> dict[str, SlotProfile]:
    return {
        "default": SlotProfile(
            name="default",
            workers=config.workers,
            recovery_window_seconds=config.recovery_window_seconds,
        ),
        "aggr-short": SlotProfile(
            name="aggr-short",
            workers=config.workers,
            recovery_window_seconds=config.recovery_window_seconds,
        ),
        "control-short": SlotProfile(
            name="control-short",
            workers=config.control_workers,
            recovery_window_seconds=config.recovery_window_seconds,
        ),
        "aggr-long": SlotProfile(
            name="aggr-long",
            workers=config.workers,
            recovery_window_seconds=config.long_recovery_window_seconds,
        ),
        "control-long": SlotProfile(
            name="control-long",
            workers=config.control_workers,
            recovery_window_seconds=config.long_recovery_window_seconds,
        ),
    }


def resolve_slot_profile_sequence(config: NightDriverConfig) -> tuple[dict[str, SlotProfile], tuple[str, ...]]:
    available_profiles = build_slot_profiles(config)
    missing = [name for name in config.slot_profile_sequence if name not in available_profiles]
    if missing:
        available = ", ".join(sorted(available_profiles))
        missing_str = ", ".join(missing)
        raise ValueError(
            f"unknown slot profile(s): {missing_str}. Available profiles: {available}"
        )
    return available_profiles, config.slot_profile_sequence


def scenario_is_clean(result: dict[str, Any] | None) -> bool:
    if not result:
        return False
    summary = result.get("summary") or {}
    return summary.get("requests_until_first_captcha") is None


def scenario_search_ok(result: dict[str, Any] | None) -> int | None:
    if not result:
        return None
    return result.get("search_ok_before_first_search_captcha")


def scenario_search_captcha_index(result: dict[str, Any] | None) -> int | None:
    if not result:
        return None
    return result.get("first_search_captcha_search_request_index")


def gate_attempts(gate: dict[str, Any] | None) -> int | None:
    if not gate:
        return None
    attempts = gate.get("attempts") or []
    return len(attempts)


def build_slot_compact_summary(slot: dict[str, Any]) -> dict[str, Any]:
    seed_run = slot.get("seed_run")
    recovery_run = slot.get("recovery_run")
    seed_gate = slot.get("seed_preflight")
    recovery_gate = slot.get("recovery_preflight")
    return {
        "slot_id": slot.get("slot_id"),
        "profile": slot.get("slot_profile_name"),
        "workers": slot.get("workers"),
        "recovery_window_seconds": slot.get("recovery_window_seconds"),
        "planned_start_utc": slot.get("planned_start_utc"),
        "started_at_utc": slot.get("started_at_utc"),
        "status": slot.get("status"),
        "seed_gate_attempts": gate_attempts(seed_gate),
        "recovery_gate_attempts": gate_attempts(recovery_gate),
        "seed_clean": scenario_is_clean(seed_run),
        "recovery_clean": scenario_is_clean(recovery_run),
        "seed_search_ok": scenario_search_ok(seed_run),
        "recovery_search_ok": scenario_search_ok(recovery_run),
        "seed_search_captcha_idx": scenario_search_captcha_index(seed_run),
        "recovery_search_captcha_idx": scenario_search_captcha_index(recovery_run),
    }


def _average_int(values: list[int]) -> float | None:
    if not values:
        return None
    return round(sum(values) / len(values), 2)


def build_session_aggregate(state: dict[str, Any]) -> dict[str, Any]:
    slots = state.get("slots") or []
    completed_slots = [slot for slot in slots if slot.get("status") == "completed"]
    seed_gate_attempts_values = [
        attempts
        for attempts in (gate_attempts(slot.get("seed_preflight")) for slot in slots)
        if attempts is not None
    ]
    recovery_gate_attempts_values = [
        attempts
        for attempts in (gate_attempts(slot.get("recovery_preflight")) for slot in slots)
        if attempts is not None
    ]
    seed_search_ok_values = [
        value for value in (scenario_search_ok(slot.get("seed_run")) for slot in slots) if value is not None
    ]
    recovery_search_ok_values = [
        value
        for value in (scenario_search_ok(slot.get("recovery_run")) for slot in slots)
        if value is not None
    ]
    recovery_better_than_seed = 0
    for slot in slots:
        seed_value = scenario_search_ok(slot.get("seed_run"))
        recovery_value = scenario_search_ok(slot.get("recovery_run"))
        if seed_value is not None and recovery_value is not None and recovery_value > seed_value:
            recovery_better_than_seed += 1

    return {
        "total_slots": len(slots),
        "completed_slots": len(completed_slots),
        "aborted_slots": sum(1 for slot in slots if slot.get("status") == "aborted"),
        "seed_clean_count": sum(1 for slot in slots if scenario_is_clean(slot.get("seed_run"))),
        "recovery_clean_count": sum(1 for slot in slots if scenario_is_clean(slot.get("recovery_run"))),
        "recovery_better_than_seed_count": recovery_better_than_seed,
        "avg_seed_gate_attempts": _average_int(seed_gate_attempts_values),
        "avg_recovery_gate_attempts": _average_int(recovery_gate_attempts_values),
        "min_seed_search_ok": min(seed_search_ok_values) if seed_search_ok_values else None,
        "max_seed_search_ok": max(seed_search_ok_values) if seed_search_ok_values else None,
        "min_recovery_search_ok": min(recovery_search_ok_values) if recovery_search_ok_values else None,
        "max_recovery_search_ok": max(recovery_search_ok_values) if recovery_search_ok_values else None,
        "slots": [build_slot_compact_summary(slot) for slot in slots],
    }


def render_session_markdown(state: dict[str, Any]) -> str:
    aggregate = build_session_aggregate(state)
    lines = [
        "# HH API Probe Night Driver",
        "",
        f"- status: {state.get('status')}",
        f"- run_label: {state.get('run_label')}",
        f"- started_at_utc: {state.get('started_at_utc')}",
        f"- finished_at_utc: {state.get('finished_at_utc')}",
        f"- current_time_utc: {utc_now_iso()}",
        f"- source_sequence_path: {state.get('source_sequence_path')}",
        f"- slot_profile_sequence: {', '.join(state.get('slot_profile_sequence') or ['default'])}",
        "",
        "## Aggregate",
        "",
        f"- total_slots: {aggregate['total_slots']}",
        f"- completed_slots: {aggregate['completed_slots']}",
        f"- aborted_slots: {aggregate['aborted_slots']}",
        f"- seed_clean_count: {aggregate['seed_clean_count']}",
        f"- recovery_clean_count: {aggregate['recovery_clean_count']}",
        f"- recovery_better_than_seed_count: {aggregate['recovery_better_than_seed_count']}",
        f"- avg_seed_gate_attempts: {aggregate['avg_seed_gate_attempts']}",
        f"- avg_recovery_gate_attempts: {aggregate['avg_recovery_gate_attempts']}",
        f"- min_seed_search_ok: {aggregate['min_seed_search_ok']}",
        f"- max_seed_search_ok: {aggregate['max_seed_search_ok']}",
        f"- min_recovery_search_ok: {aggregate['min_recovery_search_ok']}",
        f"- max_recovery_search_ok: {aggregate['max_recovery_search_ok']}",
        "",
        "## Slots",
        "",
        "| slot | profile | workers | recovery window | status | seed gate | seed ok | seed captcha idx | recovery gate | recovery ok | recovery captcha idx |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for slot in aggregate["slots"]:
        lines.append(
            "| "
            f"{slot['slot_id']} | "
            f"{slot['profile']} | "
            f"{slot['workers']} | "
            f"{slot['recovery_window_seconds']} | "
            f"{slot['status']} | "
            f"{slot['seed_gate_attempts']} | "
            f"{slot['seed_search_ok']} | "
            f"{slot['seed_search_captcha_idx']} | "
            f"{slot['recovery_gate_attempts']} | "
            f"{slot['recovery_search_ok']} | "
            f"{slot['recovery_search_captcha_idx']} |"
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


def load_search_sequence(config: NightDriverConfig) -> list[dict[str, Any]]:
    sequence = h.load_jsonl(config.source_sequence_path)
    return sequence[: config.search_prefix]


def build_plan(config: NightDriverConfig, search_sequence: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return h.build_small_detail_budget_plan(
        search_sequence,
        every_n_search=config.every_n_search,
        max_detail_requests=config.max_detail_requests,
    )


def analyze_records(records: list[dict[str, Any]]) -> dict[str, Any]:
    endpoint_breakdown = h.endpoint_breakdown(records)
    search_request_index = 0
    first_search_captcha_search_index = None
    search_ok_before_first_search_captcha = 0
    for record in records:
        if record.get("endpoint_kind") != "search":
            continue
        search_request_index += 1
        if first_search_captcha_search_index is None and record.get("error_type") == "captcha_required":
            first_search_captcha_search_index = search_request_index
        if first_search_captcha_search_index is None and record.get("status_code") == 200:
            search_ok_before_first_search_captcha += 1

    detail_status_counts = endpoint_breakdown.get("detail", {}).get("status_counts", {})
    return {
        "endpoint_breakdown": endpoint_breakdown,
        "search_ok_before_first_search_captcha": search_ok_before_first_search_captcha,
        "first_search_captcha_search_request_index": first_search_captcha_search_index,
        "detail_200_count": detail_status_counts.get("200", 0),
        "detail_403_count": detail_status_counts.get("403", 0),
    }


def run_interleaved(
    plan: list[dict[str, Any]],
    *,
    config: NightDriverConfig,
    slot_profile: SlotProfile,
    label_suffix: str,
) -> dict[str, Any]:
    label = (
        "batched-mixed-search-small-detail-budget-historical-prefix-"
        f"{config.search_prefix}-every{config.every_n_search}-max{config.max_detail_requests}-"
        f"workers-{slot_profile.workers}-burst{int(config.burst_pause_seconds)}s-"
        f"{config.header_mode}-{label_suffix}"
    )
    records, path = h.run_request_plan(
        plan,
        workers=slot_profile.workers,
        sleep_seconds=0.0,
        burst_pause_seconds=config.burst_pause_seconds,
        header_mode=config.header_mode,
        auth_mode=config.auth_mode,
        stop_on_captcha=config.stop_on_captcha,
        label=label,
        scenario_type="request_plan",
    )
    report = h.build_probe_report(records, records_path=path)
    mixed_summary_path = h.write_mixed_workload_summary(
        records,
        path,
        mixed_mode="small_detail_budget",
        extra_fields={
            "search_prefix": config.search_prefix,
            "workers": slot_profile.workers,
            "burst_pause_seconds": config.burst_pause_seconds,
            "header_mode": config.header_mode,
            "auth_mode": config.auth_mode,
            "every_n_search": config.every_n_search,
            "max_detail_requests": config.max_detail_requests,
            "slot_profile": slot_profile.name,
            "recovery_window_seconds": slot_profile.recovery_window_seconds,
            "label_suffix": label_suffix,
        },
    )
    result = {
        "records_path": str(path),
        "report_path": str(h.make_probe_report_path(path)),
        "mixed_summary_path": str(mixed_summary_path),
        "summary": report["summary"],
    }
    result.update(analyze_records(records))
    return result


def single_probe(
    *,
    probe_params: dict[str, Any],
    config: NightDriverConfig,
    label: str,
) -> dict[str, Any]:
    records, path = h.run_fixed_request_probe(
        params=probe_params,
        repeats=1,
        sleep_seconds=0.0,
        header_mode=config.header_mode,
        auth_mode=config.auth_mode,
        stop_on_captcha=True,
        label=label,
    )
    report = h.build_probe_report(records, records_path=path)
    summary = report["summary"]
    return {
        "records_path": str(path),
        "report_path": str(h.make_probe_report_path(path)),
        "summary": summary,
        "is_clean": summary["status_counts"].get("200") == 1
        and summary["requests_until_first_captcha"] is None,
    }


def sleep_with_stop(seconds: float, stopper: StopController) -> bool:
    deadline = time.monotonic() + max(seconds, 0.0)
    while True:
        if stopper.stop_requested:
            return False
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return True
        time.sleep(min(1.0, remaining))


def wait_for_consecutive_clean(
    *,
    prefix: str,
    probe_params: dict[str, Any],
    config: NightDriverConfig,
    stopper: StopController,
) -> dict[str, Any]:
    attempts: list[dict[str, Any]] = []
    consecutive = 0
    started_at = utc_now_iso()
    for attempt in range(1, config.gate_max_attempts + 1):
        if stopper.stop_requested:
            return {
                "started_at_utc": started_at,
                "finished_at_utc": utc_now_iso(),
                "required_consecutive_clean": config.gate_required_clean_probes,
                "sleep_seconds_between_attempts": config.gate_probe_sleep_seconds,
                "max_attempts": config.gate_max_attempts,
                "satisfied": False,
                "stop_requested": True,
                "attempts": attempts,
            }

        probe = single_probe(
            probe_params=probe_params,
            config=config,
            label=f"{prefix}-probe{attempt}",
        )
        attempts.append({"attempt": attempt, **probe})
        consecutive = consecutive + 1 if probe["is_clean"] else 0
        if consecutive >= config.gate_required_clean_probes:
            return {
                "started_at_utc": started_at,
                "finished_at_utc": utc_now_iso(),
                "required_consecutive_clean": config.gate_required_clean_probes,
                "sleep_seconds_between_attempts": config.gate_probe_sleep_seconds,
                "max_attempts": config.gate_max_attempts,
                "satisfied": True,
                "stop_requested": False,
                "attempts": attempts,
            }
        if attempt < config.gate_max_attempts and not sleep_with_stop(
            config.gate_probe_sleep_seconds, stopper
        ):
            return {
                "started_at_utc": started_at,
                "finished_at_utc": utc_now_iso(),
                "required_consecutive_clean": config.gate_required_clean_probes,
                "sleep_seconds_between_attempts": config.gate_probe_sleep_seconds,
                "max_attempts": config.gate_max_attempts,
                "satisfied": False,
                "stop_requested": True,
                "attempts": attempts,
            }

    return {
        "started_at_utc": started_at,
        "finished_at_utc": utc_now_iso(),
        "required_consecutive_clean": config.gate_required_clean_probes,
        "sleep_seconds_between_attempts": config.gate_probe_sleep_seconds,
        "max_attempts": config.gate_max_attempts,
        "satisfied": False,
        "stop_requested": False,
        "attempts": attempts,
    }


def run_slot(
    *,
    slot_index: int,
    planned_start_utc: str,
    search_sequence: list[dict[str, Any]],
    plan: list[dict[str, Any]],
    config: NightDriverConfig,
    slot_profile: SlotProfile,
    stopper: StopController,
    state: dict[str, Any],
    summary_path: Path,
    markdown_path: Path,
) -> None:
    probe_params = dict(search_sequence[0]["params"])
    slot_id = f"slot-{slot_index:03d}"
    slot: dict[str, Any] = {
        "slot_id": slot_id,
        "slot_index": slot_index,
        "planned_start_utc": planned_start_utc,
        "started_at_utc": utc_now_iso(),
        "slot_profile_name": slot_profile.name,
        "workers": slot_profile.workers,
        "recovery_window_seconds": slot_profile.recovery_window_seconds,
        "status": "running",
    }
    state["slots"].append(slot)
    persist_state(state, summary_path, markdown_path)

    log(
        f"{slot_id}: starting profile={slot_profile.name} "
        f"workers={slot_profile.workers} "
        f"recovery_window={slot_profile.recovery_window_seconds}s."
    )

    slot["seed_preflight"] = wait_for_consecutive_clean(
        prefix=f"{slot_id}-seed-preflight",
        probe_params=probe_params,
        config=config,
        stopper=stopper,
    )
    persist_state(state, summary_path, markdown_path)
    if not slot["seed_preflight"]["satisfied"]:
        slot["status"] = "aborted"
        slot["aborted_reason"] = (
            "stop_requested_during_seed_preflight"
            if slot["seed_preflight"].get("stop_requested")
            else "seed_preflight_not_satisfied"
        )
        slot["finished_at_utc"] = utc_now_iso()
        persist_state(state, summary_path, markdown_path)
        log(f"{slot_id}: stopped before seed run ({slot['aborted_reason']}).")
        return

    if stopper.stop_requested:
        slot["status"] = "aborted"
        slot["aborted_reason"] = "stop_requested_before_seed_run"
        slot["finished_at_utc"] = utc_now_iso()
        persist_state(state, summary_path, markdown_path)
        log(f"{slot_id}: stop requested before seed run.")
        return

    log(f"{slot_id}: seed run.")
    slot["seed_run"] = run_interleaved(
        plan,
        config=config,
        slot_profile=slot_profile,
        label_suffix=f"{slot_id}-{slot_profile.name}-seed",
    )
    persist_state(state, summary_path, markdown_path)

    log(f"{slot_id}: waiting {slot_profile.recovery_window_seconds}s before recovery gate.")
    if not sleep_with_stop(slot_profile.recovery_window_seconds, stopper):
        slot["status"] = "aborted"
        slot["aborted_reason"] = "stop_requested_during_recovery_wait"
        slot["finished_at_utc"] = utc_now_iso()
        persist_state(state, summary_path, markdown_path)
        log(f"{slot_id}: stop requested during recovery wait.")
        return

    slot["recovery_preflight"] = wait_for_consecutive_clean(
        prefix=f"{slot_id}-recovery-preflight",
        probe_params=probe_params,
        config=config,
        stopper=stopper,
    )
    persist_state(state, summary_path, markdown_path)
    if not slot["recovery_preflight"]["satisfied"]:
        slot["status"] = "aborted"
        slot["aborted_reason"] = (
            "stop_requested_during_recovery_preflight"
            if slot["recovery_preflight"].get("stop_requested")
            else "recovery_preflight_not_satisfied"
        )
        slot["finished_at_utc"] = utc_now_iso()
        persist_state(state, summary_path, markdown_path)
        log(f"{slot_id}: stopped before recovery run ({slot['aborted_reason']}).")
        return

    if stopper.stop_requested:
        slot["status"] = "aborted"
        slot["aborted_reason"] = "stop_requested_before_recovery_run"
        slot["finished_at_utc"] = utc_now_iso()
        persist_state(state, summary_path, markdown_path)
        log(f"{slot_id}: stop requested before recovery run.")
        return

    log(f"{slot_id}: recovery run.")
    slot["recovery_run"] = run_interleaved(
        plan,
        config=config,
        slot_profile=slot_profile,
        label_suffix=f"{slot_id}-{slot_profile.name}-recovery",
    )
    slot["status"] = "completed"
    slot["finished_at_utc"] = utc_now_iso()
    persist_state(state, summary_path, markdown_path)
    log(f"{slot_id}: completed.")


def parse_args(argv: list[str] | None = None) -> NightDriverConfig:
    parser = argparse.ArgumentParser(
        description="Run overnight HH API probe slots and persist aggregate summaries."
    )
    parser.add_argument("--source-sequence-path", type=Path, default=DEFAULT_SOURCE_SEQUENCE_PATH)
    parser.add_argument("--search-prefix", type=int, default=120)
    parser.add_argument("--every-n-search", type=int, default=5)
    parser.add_argument("--max-detail-requests", type=int, default=24)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--control-workers", type=int, default=3)
    parser.add_argument("--burst-pause-seconds", type=float, default=1.0)
    parser.add_argument("--header-mode", default="dual")
    parser.add_argument("--auth-mode", default=h.default_auth_mode())
    parser.add_argument("--gate-required-clean-probes", type=int, default=3)
    parser.add_argument("--gate-max-attempts", type=int, default=24)
    parser.add_argument("--gate-probe-sleep-seconds", type=float, default=10.0)
    parser.add_argument("--recovery-window-seconds", type=int, default=120)
    parser.add_argument("--long-recovery-window-seconds", type=int, default=300)
    parser.add_argument(
        "--slot-profile-sequence",
        default="default",
        help=(
            "Comma-separated built-in slot profiles. "
            "Available: default, aggr-short, control-short, aggr-long, control-long."
        ),
    )
    parser.add_argument("--slot-interval-seconds", type=int, default=7200)
    parser.add_argument("--first-slot-delay-seconds", type=int, default=0)
    parser.add_argument("--max-slots", type=int, default=None)
    parser.add_argument(
        "--show-probe-logs",
        action="store_true",
        help="Keep per-request probe logging enabled.",
    )
    args = parser.parse_args(argv)
    return NightDriverConfig(
        source_sequence_path=args.source_sequence_path,
        search_prefix=args.search_prefix,
        every_n_search=args.every_n_search,
        max_detail_requests=args.max_detail_requests,
        workers=args.workers,
        control_workers=args.control_workers,
        burst_pause_seconds=args.burst_pause_seconds,
        header_mode=args.header_mode,
        auth_mode=args.auth_mode,
        gate_required_clean_probes=args.gate_required_clean_probes,
        gate_max_attempts=args.gate_max_attempts,
        gate_probe_sleep_seconds=args.gate_probe_sleep_seconds,
        recovery_window_seconds=args.recovery_window_seconds,
        long_recovery_window_seconds=args.long_recovery_window_seconds,
        slot_profile_sequence=parse_slot_profile_sequence(args.slot_profile_sequence),
        slot_interval_seconds=args.slot_interval_seconds,
        first_slot_delay_seconds=args.first_slot_delay_seconds,
        max_slots=args.max_slots,
        suppress_probe_logs=not args.show_probe_logs,
    )


def run_driver(config: NightDriverConfig) -> tuple[Path, Path]:
    if config.suppress_probe_logs:
        h._log_probe_record = lambda *args, **kwargs: None

    stopper = StopController()
    stopper.install()

    search_sequence = load_search_sequence(config)
    if not search_sequence:
        raise ValueError("search sequence is empty")
    plan = build_plan(config, search_sequence)
    available_profiles, slot_profile_sequence = resolve_slot_profile_sequence(config)

    summary_path = h.RESULTS_DIR / f"{h.RUN_LABEL}-night-driver-summary.json"
    markdown_path = h.RESULTS_DIR / f"{h.RUN_LABEL}-night-driver-summary.md"
    state: dict[str, Any] = {
        "run_label": h.RUN_LABEL,
        "driver_name": "hh_api_probe_night_driver",
        "status": "running",
        "started_at_utc": utc_now_iso(),
        "finished_at_utc": None,
        "stopped_by_signal": None,
        "source_sequence_path": str(config.source_sequence_path),
        "summary_path": str(summary_path),
        "markdown_path": str(markdown_path),
        "config": asdict(config),
        "slot_profile_sequence": list(slot_profile_sequence),
        "resolved_slot_profiles": {
            name: asdict(profile) for name, profile in available_profiles.items()
        },
        "slots": [],
    }
    persist_state(state, summary_path, markdown_path)

    log(f"Night driver started. Summary: {summary_path}")
    slot_index = 1
    next_planned_start = datetime.now(UTC) + timedelta(seconds=config.first_slot_delay_seconds)

    while True:
        if config.max_slots is not None and slot_index > config.max_slots:
            state["status"] = "completed"
            break
        if stopper.stop_requested:
            state["status"] = "stopped_by_operator"
            break

        seconds_until_start = max(0.0, (next_planned_start - datetime.now(UTC)).total_seconds())
        if seconds_until_start > 0:
            log(
                f"Waiting {round(seconds_until_start, 2)}s until "
                f"{next_planned_start.isoformat()} for slot-{slot_index:03d}."
            )
            if not sleep_with_stop(seconds_until_start, stopper):
                state["status"] = "stopped_by_operator"
                break

        run_slot(
            slot_index=slot_index,
            planned_start_utc=next_planned_start.isoformat(),
            search_sequence=search_sequence,
            plan=plan,
            config=config,
            slot_profile=available_profiles[
                slot_profile_sequence[(slot_index - 1) % len(slot_profile_sequence)]
            ],
            stopper=stopper,
            state=state,
            summary_path=summary_path,
            markdown_path=markdown_path,
        )
        slot_index += 1
        next_planned_start = next_planned_start + timedelta(seconds=config.slot_interval_seconds)

        if stopper.stop_requested:
            state["status"] = "stopped_by_operator"
            break

    state["finished_at_utc"] = utc_now_iso()
    state["stopped_by_signal"] = stopper.stop_signal_name
    if state["status"] == "running":
        state["status"] = "completed"
    persist_state(state, summary_path, markdown_path)
    log(f"Night driver finished with status={state['status']}.")
    return summary_path, markdown_path


def main(argv: list[str] | None = None) -> int:
    config = parse_args(argv)
    run_driver(config)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
