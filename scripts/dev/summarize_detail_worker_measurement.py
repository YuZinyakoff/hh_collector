from __future__ import annotations

import argparse
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build a markdown summary for a detail worker measurement run."
    )
    parser.add_argument("--run-ts", required=True)
    parser.add_argument("--status", required=True)
    parser.add_argument("--triggered-by", required=True)
    parser.add_argument("--batch-size", required=True)
    parser.add_argument("--max-ticks", required=True)
    parser.add_argument("--include-inactive", required=True)
    parser.add_argument("--retry-cooldown-seconds", required=True)
    parser.add_argument("--max-retry-cooldown-seconds", required=True)
    parser.add_argument("--log-path", required=True)
    parser.add_argument("--preflight-path", required=True)
    parser.add_argument("--postflight-path", required=True)
    parser.add_argument("--summary-path", required=True)
    args = parser.parse_args()

    log_path = Path(args.log_path)
    preflight_path = Path(args.preflight_path)
    postflight_path = Path(args.postflight_path)
    summary_path = Path(args.summary_path)

    preflight = _read_key_values(preflight_path)
    postflight = _read_key_values(postflight_path)
    tick_totals = _read_tick_totals(log_path)

    pre_backlog = _read_int(preflight, "active_backlog_size")
    post_backlog = _read_int(postflight, "active_backlog_size")
    pre_ready = _read_int(preflight, "active_ready_backlog_size")
    post_ready = _read_int(postflight, "active_ready_backlog_size")
    pre_db_size = _read_int(preflight, "db_size_bytes")
    post_db_size = _read_int(postflight, "db_size_bytes")

    summary_path.write_text(
        "\n".join(
            [
                "# Detail Worker Measurement",
                "",
                f"- run_ts: `{args.run_ts}`",
                f"- status: `{args.status}`",
                f"- triggered_by: `{args.triggered_by}`",
                f"- batch_size: `{args.batch_size}`",
                f"- max_ticks: `{args.max_ticks}`",
                f"- include_inactive: `{args.include_inactive}`",
                f"- retry_cooldown_seconds: `{args.retry_cooldown_seconds}`",
                f"- max_retry_cooldown_seconds: `{args.max_retry_cooldown_seconds}`",
                f"- ticks_observed: `{tick_totals['ticks_observed']}`",
                f"- selected_total: `{tick_totals['selected_count']}`",
                f"- succeeded_total: `{tick_totals['detail_fetch_succeeded']}`",
                f"- terminal_total: `{tick_totals['detail_fetch_terminal']}`",
                f"- failed_total: `{tick_totals['detail_fetch_failed']}`",
                f"- active_backlog: `{pre_backlog} -> {post_backlog}`",
                f"- active_backlog_delta: `{post_backlog - pre_backlog}`",
                f"- active_ready_backlog: `{pre_ready} -> {post_ready}`",
                f"- active_ready_backlog_delta: `{post_ready - pre_ready}`",
                f"- db_size_bytes: `{pre_db_size} -> {post_db_size}`",
                f"- db_size_delta_bytes: `{post_db_size - pre_db_size}`",
                f"- log: `{log_path}`",
                f"- preflight: `{preflight_path}`",
                f"- postflight: `{postflight_path}`",
                "",
            ]
        ),
        encoding="utf-8",
    )
    print(f"summary={summary_path}")
    return 0


def _read_key_values(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key] = value
    return values


def _read_int(values: dict[str, str], key: str) -> int:
    return int(values.get(key, "0"))


def _read_tick_totals(path: Path) -> dict[str, int]:
    totals = {
        "ticks_observed": 0,
        "selected_count": 0,
        "detail_fetch_succeeded": 0,
        "detail_fetch_terminal": 0,
        "detail_fetch_failed": 0,
    }
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.startswith("detail_worker_tick "):
            continue
        totals["ticks_observed"] += 1
        fields = _parse_tick_fields(line)
        for key in (
            "selected_count",
            "detail_fetch_succeeded",
            "detail_fetch_terminal",
            "detail_fetch_failed",
        ):
            totals[key] += int(fields.get(key, "0"))
    return totals


def _parse_tick_fields(line: str) -> dict[str, str]:
    fields: dict[str, str] = {}
    for part in line.split()[1:]:
        if "=" not in part:
            continue
        key, value = part.split("=", 1)
        fields[key] = value
    return fields


if __name__ == "__main__":
    raise SystemExit(main())
