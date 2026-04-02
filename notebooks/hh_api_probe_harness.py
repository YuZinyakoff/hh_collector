from __future__ import annotations

import json
import math
import os
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

SENSITIVE_HEADER_NAMES = frozenset({"authorization", "proxy-authorization"})


def find_repo_root(start: Path) -> Path:
    for candidate in [start, *start.parents]:
        if (candidate / ".git").exists() and (candidate / "pyproject.toml").exists():
            return candidate
    return start


REPO_ROOT = find_repo_root(Path.cwd().resolve())


def load_simple_dotenv(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


for key, value in load_simple_dotenv(REPO_ROOT / ".env").items():
    os.environ.setdefault(key, value)


def _resolve_first_present_env_var(*names: str) -> str | None:
    for name in names:
        value = os.environ.get(name)
        if value:
            return value
    return None


API_ROOT = os.environ.get("HHRU_HH_API_BASE_URL", "https://api.hh.ru").rstrip("/")
USER_AGENT = os.environ.get(
    "HHRU_HH_API_USER_AGENT",
    "hh-api-captcha-probe/0.1 (contact: change-me@example.com)",
)
APPLICATION_TOKEN = _resolve_first_present_env_var(
    "HH_API_APPLICATION_TOKEN",
    "HHRU_HH_API_APPLICATION_TOKEN",
)
CAPTCHA_BACKURL = os.environ.get(
    "HH_CAPTCHA_BACKURL",
    "http://localhost:8888/lab/tree/notebooks/hh_api_captcha_probe.ipynb",
)
TIMEOUT_SECONDS = 30.0
POSTGRES_CONNECT_TIMEOUT_SECONDS = 5
RESULTS_DIR = REPO_ROOT / ".state" / "reports" / "hh-api-probe"
RESULTS_DIR.mkdir(parents=True, exist_ok=True)
RUN_LABEL = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]
DEFAULT_SOAK_RUN_ID = os.environ.get(
    "HH_API_PROBE_SOAK_RUN_ID",
    "2b5f4211-e614-463c-a041-00e440044297",
)


def notebook_config() -> dict[str, Any]:
    return {
        "api_root": API_ROOT,
        "user_agent": USER_AGENT,
        "timeout_seconds": TIMEOUT_SECONDS,
        "default_soak_run_id": DEFAULT_SOAK_RUN_ID,
        "results_dir": str(RESULTS_DIR),
        "run_label": RUN_LABEL,
        "application_token_configured": APPLICATION_TOKEN is not None,
        "default_auth_mode": default_auth_mode(),
    }


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def safe_json_loads(raw_text: str) -> Any:
    try:
        return json.loads(raw_text)
    except json.JSONDecodeError:
        return None


def response_headers_to_dict(headers: Any) -> dict[str, str]:
    return {key: value for key, value in headers.items()}


def redact_headers(headers: dict[str, str] | None) -> dict[str, str]:
    sanitized: dict[str, str] = {}
    for key, value in (headers or {}).items():
        if key.lower() in SENSITIVE_HEADER_NAMES:
            sanitized[key] = "<redacted>"
            continue
        sanitized[key] = value
    return sanitized


def extract_first_error(payload: Any) -> dict[str, Any]:
    if isinstance(payload, dict):
        errors = payload.get("errors")
        if isinstance(errors, list) and errors:
            first = errors[0]
            if isinstance(first, dict):
                return first
    return {}


def default_headers(header_mode: str = "dual") -> dict[str, str]:
    headers = {
        "Accept": "application/json",
    }
    if header_mode == "app_like":
        headers["User-Agent"] = USER_AGENT
        return headers
    if header_mode == "dual":
        headers["User-Agent"] = USER_AGENT
        headers["HH-User-Agent"] = USER_AGENT
        return headers
    if header_mode == "hh_only":
        headers["HH-User-Agent"] = USER_AGENT
        return headers
    raise ValueError(f"Unsupported header_mode: {header_mode}")


def infer_header_mode(headers: dict[str, str] | None) -> str | None:
    lowered = {key.lower(): value for key, value in (headers or {}).items()}
    has_user_agent = "user-agent" in lowered
    has_hh_user_agent = "hh-user-agent" in lowered
    if has_user_agent and has_hh_user_agent:
        return "dual"
    if has_user_agent:
        return "app_like"
    if has_hh_user_agent:
        return "hh_only"
    return None


def default_auth_mode() -> str:
    return "application_token" if APPLICATION_TOKEN else "anonymous"


def resolve_auth_mode(auth_mode: str | None = None) -> str:
    if auth_mode is None or auth_mode == "default":
        return default_auth_mode()
    if auth_mode in {"anonymous", "application_token"}:
        return auth_mode
    raise ValueError(f"Unsupported auth_mode: {auth_mode}")


def build_auth_headers(auth_mode: str | None = None) -> dict[str, str]:
    auth_mode = resolve_auth_mode(auth_mode)
    if auth_mode == "anonymous":
        return {}
    if auth_mode == "application_token":
        if not APPLICATION_TOKEN:
            raise ValueError(
                "auth_mode='application_token' requires HH_API_APPLICATION_TOKEN "
                "or HHRU_HH_API_APPLICATION_TOKEN in the environment"
            )
        return {"Authorization": f"Bearer {APPLICATION_TOKEN}"}
    raise ValueError(f"Unsupported auth_mode: {auth_mode}")


def infer_endpoint_kind(endpoint: str) -> str:
    normalized = endpoint.strip()
    if normalized == "/vacancies":
        return "search"
    if normalized.startswith("/vacancies/"):
        return "detail"
    return "dictionary"


def build_captcha_backurl(captcha_url: str | None, backurl: str = CAPTCHA_BACKURL) -> str | None:
    if not captcha_url:
        return None
    parts = urllib.parse.urlsplit(captcha_url)
    query = urllib.parse.parse_qsl(parts.query, keep_blank_values=True)
    query.append(("backurl", backurl))
    updated_query = urllib.parse.urlencode(query, doseq=True)
    return urllib.parse.urlunsplit(
        (parts.scheme, parts.netloc, parts.path, updated_query, parts.fragment)
    )


def parse_timestamp_utc(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _seconds_between(
    previous_timestamp_utc: str | None,
    current_timestamp_utc: str | None,
) -> float | None:
    previous = parse_timestamp_utc(previous_timestamp_utc)
    current = parse_timestamp_utc(current_timestamp_utc)
    if previous is None or current is None:
        return None
    return round(max((current - previous).total_seconds(), 0.0), 6)


def _resolve_first_captcha_timestamp(
    records: list[dict[str, Any]],
    *,
    cooldown_origin_timestamp_utc: str | None = None,
) -> str | None:
    if cooldown_origin_timestamp_utc:
        return cooldown_origin_timestamp_utc
    for record in records:
        if record.get("error_type") == "captcha_required":
            return record.get("timestamp_utc")
    return None


def _minutes_since_origin(
    current_timestamp_utc: str | None,
    origin_timestamp_utc: str | None,
) -> float | None:
    current = parse_timestamp_utc(current_timestamp_utc)
    origin = parse_timestamp_utc(origin_timestamp_utc)
    if current is None or origin is None or current < origin:
        return None
    return round((current - origin).total_seconds() / 60.0, 3)


def annotate_probe_record(
    record: dict[str, Any],
    records_so_far: list[dict[str, Any]],
    *,
    scenario_label: str,
    scenario_type: str,
    workers: int = 1,
    pause_seconds: float | None = None,
    burst_pause_seconds: float | None = None,
    cooldown_origin_timestamp_utc: str | None = None,
    extra_fields: dict[str, Any] | None = None,
) -> dict[str, Any]:
    previous_record = records_so_far[-1] if records_so_far else None
    first_captcha_timestamp_utc = _resolve_first_captcha_timestamp(
        records_so_far,
        cooldown_origin_timestamp_utc=cooldown_origin_timestamp_utc,
    )
    if first_captcha_timestamp_utc is None and record.get("error_type") == "captcha_required":
        first_captcha_timestamp_utc = record.get("timestamp_utc")

    record["request_headers"] = redact_headers(record.get("request_headers"))
    record["response_headers"] = redact_headers(record.get("response_headers"))
    record["run_label"] = RUN_LABEL
    record["scenario_label"] = scenario_label
    record["scenario_type"] = scenario_type
    record["request_index_from_run_start"] = len(records_so_far) + 1
    record["seconds_since_previous_request"] = _seconds_between(
        None if previous_record is None else previous_record.get("timestamp_utc"),
        record.get("timestamp_utc"),
    )
    record["workers"] = workers
    record["pause_seconds"] = pause_seconds
    record["burst_pause_seconds"] = burst_pause_seconds
    record["cooldown_origin_timestamp_utc"] = cooldown_origin_timestamp_utc
    record["first_captcha_timestamp_utc"] = first_captcha_timestamp_utc
    record["minutes_since_first_captcha"] = _minutes_since_origin(
        record.get("timestamp_utc"),
        first_captcha_timestamp_utc,
    )
    if extra_fields:
        record.update(extra_fields)
    return record


def hh_get(
    endpoint: str = "/vacancies",
    params: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    header_mode: str = "dual",
    auth_mode: str | None = None,
    timeout_seconds: float = TIMEOUT_SECONDS,
) -> dict[str, Any]:
    params = params or {}
    auth_mode = resolve_auth_mode(auth_mode)
    request_headers = default_headers(header_mode=header_mode)
    request_headers.update(build_auth_headers(auth_mode=auth_mode))
    if headers:
        request_headers.update(headers)

    query = urllib.parse.urlencode(params, doseq=True)
    base_url = f"{API_ROOT}{endpoint}"
    url = f"{base_url}?{query}" if query else base_url
    request = urllib.request.Request(url, headers=request_headers, method="GET")

    started_at = utc_now_iso()
    started_perf = time.perf_counter()
    endpoint_kind = infer_endpoint_kind(endpoint)

    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            raw_bytes = response.read()
            status_code = response.status
            response_headers = response_headers_to_dict(response.headers)
    except urllib.error.HTTPError as exc:
        raw_bytes = exc.read()
        status_code = exc.code
        response_headers = response_headers_to_dict(exc.headers)
    except Exception as exc:
        latency_ms = round((time.perf_counter() - started_perf) * 1000, 2)
        return {
            "timestamp_utc": started_at,
            "status_code": None,
            "latency_ms": latency_ms,
            "endpoint": endpoint,
            "endpoint_kind": endpoint_kind,
            "auth_mode": auth_mode,
            "url": url,
            "params": params,
            "request_headers": redact_headers(request_headers),
            "header_mode": header_mode,
            "response_headers": {},
            "network_error": repr(exc),
            "payload": None,
            "body_text": None,
            "error_type": None,
            "error_value": None,
            "captcha_url": None,
            "captcha_url_with_backurl": None,
            "fallback_url": None,
            "request_id": None,
            "items_count": None,
            "found": None,
            "pages": None,
        }

    latency_ms = round((time.perf_counter() - started_perf) * 1000, 2)
    body_text = raw_bytes.decode("utf-8", errors="replace")
    payload = safe_json_loads(body_text)
    first_error = extract_first_error(payload)

    items_count = None
    found = None
    pages = None
    request_id = None
    if isinstance(payload, dict):
        items = payload.get("items")
        if isinstance(items, list):
            items_count = len(items)
        found = payload.get("found")
        pages = payload.get("pages")
        request_id = payload.get("request_id")

    captcha_url = first_error.get("captcha_url")
    return {
        "timestamp_utc": started_at,
        "status_code": status_code,
        "latency_ms": latency_ms,
        "endpoint": endpoint,
        "endpoint_kind": endpoint_kind,
        "auth_mode": auth_mode,
        "url": url,
        "params": params,
        "request_headers": redact_headers(request_headers),
        "header_mode": header_mode,
        "response_headers": redact_headers(response_headers),
        "network_error": None,
        "payload": payload,
        "body_text": body_text,
        "error_type": first_error.get("type"),
        "error_value": first_error.get("value"),
        "captcha_url": captcha_url,
        "captcha_url_with_backurl": build_captcha_backurl(captcha_url),
        "fallback_url": first_error.get("fallback_url"),
        "request_id": request_id,
        "items_count": items_count,
        "found": found,
        "pages": pages,
    }


def append_jsonl(path: Path, record: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=False) + "\n")


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def make_probe_path(name: str) -> Path:
    return RESULTS_DIR / f"{RUN_LABEL}-{name}.jsonl"


def make_probe_report_path(path: Path) -> Path:
    return path.parent / f"{path.stem}-report.json"


def make_mixed_summary_path(path: Path) -> Path:
    return path.parent / f"{path.stem}-mixed-summary.json"


def _percentile(values: list[float], quantile: float) -> float | None:
    if not values:
        return None
    if len(values) == 1:
        return round(values[0], 2)
    ordered = sorted(values)
    rank = (len(ordered) - 1) * quantile
    lower_index = math.floor(rank)
    upper_index = math.ceil(rank)
    if lower_index == upper_index:
        return round(ordered[lower_index], 2)
    lower_value = ordered[lower_index]
    upper_value = ordered[upper_index]
    interpolated = lower_value + (upper_value - lower_value) * (rank - lower_index)
    return round(interpolated, 2)


def _first_matching_index(
    records: list[dict[str, Any]],
    predicate: Any,
) -> int | None:
    for index, record in enumerate(records):
        if predicate(record):
            return index
    return None


def summarize_records(records: list[dict[str, Any]]) -> dict[str, Any]:
    status_counts: dict[str, int] = {}
    error_type_counts: dict[str, int] = {}
    latency_values: list[float] = []
    first_403_index = _first_matching_index(
        records,
        lambda record: record.get("status_code") == 403,
    )
    first_captcha_index = _first_matching_index(
        records,
        lambda record: record.get("error_type") == "captcha_required",
    )

    for record in records:
        status_key = str(record.get("status_code"))
        status_counts[status_key] = status_counts.get(status_key, 0) + 1
        error_type_key = str(record.get("error_type"))
        error_type_counts[error_type_key] = error_type_counts.get(error_type_key, 0) + 1
        latency_ms = record.get("latency_ms")
        if isinstance(latency_ms, int | float):
            latency_values.append(float(latency_ms))

    ok_before_first_403 = 0
    ok_after_first_403 = 0
    forbidden_after_first_403 = 0
    if first_403_index is None:
        ok_before_first_403 = sum(1 for record in records if record.get("status_code") == 200)
    else:
        ok_before_first_403 = sum(
            1 for record in records[:first_403_index] if record.get("status_code") == 200
        )
        ok_after_first_403 = sum(
            1 for record in records[first_403_index + 1 :] if record.get("status_code") == 200
        )
        forbidden_after_first_403 = sum(
            1 for record in records[first_403_index + 1 :] if record.get("status_code") == 403
        )

    first_record = records[0] if records else {}
    first_captcha_record = next(
        (record for record in records if record.get("error_type") == "captcha_required"),
        None,
    )
    first_403_record = None if first_403_index is None else records[first_403_index]

    first_timestamp_utc = None if not records else records[0].get("timestamp_utc")
    last_timestamp_utc = None if not records else records[-1].get("timestamp_utc")

    wall_clock_until_first_403_seconds = _seconds_between(
        first_timestamp_utc,
        None if first_403_record is None else first_403_record.get("timestamp_utc"),
    )
    wall_clock_until_first_captcha_seconds = _seconds_between(
        first_timestamp_utc,
        None if first_captcha_record is None else first_captcha_record.get("timestamp_utc"),
    )

    minutes_since_first_captcha_values = [
        float(record["minutes_since_first_captcha"])
        for record in records
        if isinstance(record.get("minutes_since_first_captcha"), int | float)
    ]

    return {
        "run_label": first_record.get("run_label"),
        "scenario_label": first_record.get("scenario_label"),
        "scenario_type": first_record.get("scenario_type"),
        "endpoint_kind": first_record.get("endpoint_kind"),
        "auth_mode": first_record.get("auth_mode"),
        "header_mode": first_record.get("header_mode"),
        "workers": first_record.get("workers"),
        "pause_seconds": first_record.get("pause_seconds"),
        "burst_pause_seconds": first_record.get("burst_pause_seconds"),
        "cooldown_origin_timestamp_utc": first_record.get("cooldown_origin_timestamp_utc"),
        "window_started_at_utc": first_timestamp_utc,
        "window_finished_at_utc": last_timestamp_utc,
        "total_requests": len(records),
        "status_counts": status_counts,
        "error_type_counts": error_type_counts,
        "latency_ms_p50": _percentile(latency_values, 0.50),
        "latency_ms_p95": _percentile(latency_values, 0.95),
        "first_403_index": first_403_index,
        "first_captcha_index": first_captcha_index,
        "first_403_request_index": None if first_403_index is None else first_403_index + 1,
        "first_captcha_request_index": None
        if first_captcha_index is None
        else first_captcha_index + 1,
        "requests_until_first_403": None if first_403_index is None else first_403_index + 1,
        "requests_until_first_captcha": None
        if first_captcha_index is None
        else first_captcha_index + 1,
        "wall_clock_until_first_403_seconds": wall_clock_until_first_403_seconds,
        "wall_clock_until_first_captcha_seconds": wall_clock_until_first_captcha_seconds,
        "ok_before_first_403": ok_before_first_403,
        "ok_after_first_403": ok_after_first_403,
        "forbidden_after_first_403": forbidden_after_first_403,
        "first_captcha_request_id": None
        if first_captcha_record is None
        else first_captcha_record.get("request_id"),
        "first_captcha_url": None
        if first_captcha_record is None
        else first_captcha_record.get("captcha_url"),
        "first_captcha_url_with_backurl": None
        if first_captcha_record is None
        else first_captcha_record.get("captcha_url_with_backurl"),
        "max_minutes_since_first_captcha": None
        if not minutes_since_first_captcha_values
        else round(max(minutes_since_first_captcha_values), 3),
    }


def last_success_vs_first_403(records: list[dict[str, Any]]) -> dict[str, Any]:
    first_403_index = _first_matching_index(
        records,
        lambda record: record.get("status_code") == 403,
    )
    if first_403_index is None:
        return {"last_success": None, "first_403": None}

    last_success = None
    for record in reversed(records[:first_403_index]):
        if record.get("status_code") == 200:
            last_success = {
                "timestamp_utc": record.get("timestamp_utc"),
                "params": record.get("params"),
                "status_code": record.get("status_code"),
                "latency_ms": record.get("latency_ms"),
                "found": record.get("found"),
                "pages": record.get("pages"),
                "items_count": record.get("items_count"),
                "request_id": record.get("request_id"),
            }
            break

    first_403_record = records[first_403_index]
    first_403 = {
        "timestamp_utc": first_403_record.get("timestamp_utc"),
        "params": first_403_record.get("params"),
        "status_code": first_403_record.get("status_code"),
        "latency_ms": first_403_record.get("latency_ms"),
        "error_type": first_403_record.get("error_type"),
        "error_value": first_403_record.get("error_value"),
        "captcha_url": first_403_record.get("captcha_url"),
        "captcha_url_with_backurl": first_403_record.get("captcha_url_with_backurl"),
        "request_id": first_403_record.get("request_id"),
    }
    return {"last_success": last_success, "first_403": first_403}


def build_probe_report(
    records: list[dict[str, Any]],
    *,
    records_path: Path | None = None,
) -> dict[str, Any]:
    return {
        "generated_at_utc": utc_now_iso(),
        "records_path": None if records_path is None else str(records_path),
        "summary": summarize_records(records),
        "transition": last_success_vs_first_403(records),
    }


def write_probe_report(records: list[dict[str, Any]], records_path: Path) -> Path:
    report = build_probe_report(records, records_path=records_path)
    report_path = make_probe_report_path(records_path)
    report_path.write_text(
        json.dumps(report, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return report_path


def endpoint_breakdown(records: list[dict[str, Any]]) -> dict[str, Any]:
    breakdown: dict[str, Any] = {}
    for record in records:
        endpoint_kind = str(record.get("endpoint_kind") or "unknown")
        entry = breakdown.setdefault(
            endpoint_kind,
            {
                "requests": 0,
                "status_counts": {},
                "error_type_counts": {},
                "first_captcha_request_index": None,
            },
        )
        entry["requests"] += 1

        status_key = str(record.get("status_code"))
        entry["status_counts"][status_key] = entry["status_counts"].get(status_key, 0) + 1

        error_type_key = str(record.get("error_type"))
        entry["error_type_counts"][error_type_key] = (
            entry["error_type_counts"].get(error_type_key, 0) + 1
        )

        if (
            entry["first_captcha_request_index"] is None
            and record.get("error_type") == "captcha_required"
        ):
            entry["first_captcha_request_index"] = record.get("request_index_from_run_start")
    return breakdown


def build_mixed_workload_summary(
    records: list[dict[str, Any]],
    *,
    mixed_mode: str,
    records_path: Path | None = None,
    extra_fields: dict[str, Any] | None = None,
) -> dict[str, Any]:
    first_record = records[0] if records else {}
    summary: dict[str, Any] = {
        "scenario_label": first_record.get("scenario_label"),
        "mixed_mode": mixed_mode,
        "total_requests": len(records),
        "endpoint_breakdown": endpoint_breakdown(records),
        "records_path": None if records_path is None else str(records_path),
        "report_path": None if records_path is None else str(make_probe_report_path(records_path)),
    }
    if extra_fields:
        summary.update(extra_fields)
    return summary


def write_mixed_workload_summary(
    records: list[dict[str, Any]],
    records_path: Path,
    *,
    mixed_mode: str,
    extra_fields: dict[str, Any] | None = None,
) -> Path:
    summary = build_mixed_workload_summary(
        records,
        mixed_mode=mixed_mode,
        records_path=records_path,
        extra_fields=extra_fields,
    )
    summary_path = make_mixed_summary_path(records_path)
    summary_path.write_text(
        json.dumps(summary, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return summary_path


def print_summary(records: list[dict[str, Any]], *, records_path: Path | None = None) -> None:
    report = build_probe_report(records, records_path=records_path)
    print(json.dumps(report["summary"], indent=2, ensure_ascii=False))
    print(json.dumps(report["transition"], indent=2, ensure_ascii=False))


def postgres_connect_kwargs() -> dict[str, Any]:
    return {
        "host": os.environ.get("HHRU_DB_HOST", "localhost"),
        "port": int(os.environ.get("HHRU_DB_PORT", "5432")),
        "dbname": os.environ.get("HHRU_DB_NAME", "hhru_platform"),
        "user": os.environ.get("HHRU_DB_USER", "hhru"),
        "password": os.environ.get("HHRU_DB_PASSWORD", "hhru"),
        "connect_timeout": POSTGRES_CONNECT_TIMEOUT_SECONDS,
    }


def load_soak_search_sequence(
    crawl_run_id: str = DEFAULT_SOAK_RUN_ID,
    *,
    limit: int | None = None,
    stop_before_first_403: bool = False,
    include_first_403: bool = False,
) -> list[dict[str, Any]]:
    import psycopg

    query = """
        SELECT
            l.id,
            l.crawl_partition_id::text,
            l.requested_at,
            l.status_code,
            l.latency_ms,
            l.params_json,
            l.request_headers_json,
            l.error_type,
            l.error_message,
            p.payload_json
        FROM api_request_log AS l
        LEFT JOIN raw_api_payload AS p
            ON p.api_request_log_id = l.id
        WHERE l.crawl_run_id = %s
          AND l.endpoint = '/vacancies'
          AND l.request_type = 'vacancy_search'
        ORDER BY l.requested_at, l.id
    """

    sequence: list[dict[str, Any]] = []
    with psycopg.connect(**postgres_connect_kwargs()) as conn:
        with conn.cursor() as cur:
            cur.execute(query, (crawl_run_id,))
            rows = cur.fetchall()

    for row in rows:
        payload = row[9]
        request_id = None
        items_count = None
        found = None
        pages = None
        error_value = None
        captcha_url = None
        fallback_url = None
        if isinstance(payload, dict):
            request_id = payload.get("request_id")
            items = payload.get("items")
            if isinstance(items, list):
                items_count = len(items)
            found = payload.get("found")
            pages = payload.get("pages")
            first_error = extract_first_error(payload)
            error_value = first_error.get("value")
            captcha_url = first_error.get("captcha_url")
            fallback_url = first_error.get("fallback_url")
        raw_request_headers = row[6] or {}
        record = {
            "source_request_log_id": row[0],
            "source_partition_id": row[1],
            "timestamp_utc": row[2].isoformat(),
            "status_code": row[3],
            "latency_ms": row[4],
            "params": row[5],
            "request_headers": redact_headers(raw_request_headers),
            "header_mode": infer_header_mode(raw_request_headers),
            "endpoint": "/vacancies",
            "endpoint_kind": "search",
            "auth_mode": "unknown_source",
            "scenario_label": f"source-crawl-run-{crawl_run_id}",
            "scenario_type": "loaded_soak_sequence",
            "error_type": row[7],
            "error_value": error_value,
            "error_message": row[8],
            "captcha_url": captcha_url,
            "captcha_url_with_backurl": build_captcha_backurl(captcha_url),
            "fallback_url": fallback_url,
            "payload": payload,
            "request_id": request_id,
            "items_count": items_count,
            "found": found,
            "pages": pages,
        }
        annotate_probe_record(
            record,
            sequence,
            scenario_label=f"source-crawl-run-{crawl_run_id}",
            scenario_type="loaded_soak_sequence",
            workers=1,
            pause_seconds=None,
            extra_fields={"run_label": f"source-{crawl_run_id}"},
        )
        if stop_before_first_403 and record["status_code"] == 403:
            break
        sequence.append(record)
        if include_first_403 and record["status_code"] == 403:
            break
        if limit is not None and len(sequence) >= limit:
            break
    return sequence


def _log_probe_record(record: dict[str, Any], *fields: str) -> None:
    preview = {field: record.get(field) for field in fields}
    print(preview)


def extract_detail_ids_from_records(
    records: list[dict[str, Any]],
    *,
    limit: int | None = None,
) -> list[str]:
    vacancy_ids: list[str] = []
    seen_ids: set[str] = set()
    for record in records:
        payload = record.get("payload")
        if not isinstance(payload, dict):
            continue
        items = payload.get("items")
        if not isinstance(items, list):
            continue
        for item in items:
            if not isinstance(item, dict):
                continue
            vacancy_id = item.get("id")
            if vacancy_id is None:
                continue
            normalized = str(vacancy_id)
            if normalized in seen_ids:
                continue
            seen_ids.add(normalized)
            vacancy_ids.append(normalized)
            if limit is not None and len(vacancy_ids) >= limit:
                return vacancy_ids
    return vacancy_ids


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


def _build_detail_plan_item(
    vacancy_id: str,
    *,
    detail_index: int,
) -> dict[str, Any]:
    return {
        "endpoint": f"/vacancies/{vacancy_id}",
        "params": {},
        "extra_fields": {
            "plan_step_kind": "detail",
            "plan_segment": "detail_phase",
            "detail_index": detail_index,
            "vacancy_id": vacancy_id,
        },
    }


def build_search_after_coverage_plan(
    search_sequence: list[dict[str, Any]],
    *,
    detail_budget: int,
    detail_ids: list[str] | None = None,
) -> list[dict[str, Any]]:
    if detail_budget < 0:
        raise ValueError("detail_budget must be >= 0")
    selected_detail_ids = (
        detail_ids or extract_detail_ids_from_records(search_sequence)
    )[:detail_budget]

    plan: list[dict[str, Any]] = []
    for search_index, source_record in enumerate(search_sequence, start=1):
        plan.append(_build_search_plan_item(source_record, search_index=search_index))
    for detail_index, vacancy_id in enumerate(selected_detail_ids, start=1):
        plan.append(_build_detail_plan_item(vacancy_id, detail_index=detail_index))
    return plan


def build_small_detail_budget_plan(
    search_sequence: list[dict[str, Any]],
    *,
    every_n_search: int,
    max_detail_requests: int | None = None,
    detail_ids: list[str] | None = None,
) -> list[dict[str, Any]]:
    if every_n_search < 1:
        raise ValueError("every_n_search must be >= 1")
    if max_detail_requests is not None and max_detail_requests < 0:
        raise ValueError("max_detail_requests must be >= 0")

    available_detail_ids = detail_ids or extract_detail_ids_from_records(search_sequence)
    next_detail_index = 0
    detail_requests_emitted = 0
    plan: list[dict[str, Any]] = []

    for search_index, source_record in enumerate(search_sequence, start=1):
        plan.append(_build_search_plan_item(source_record, search_index=search_index))

        should_emit_detail = search_index % every_n_search == 0
        within_budget = (
            max_detail_requests is None or detail_requests_emitted < max_detail_requests
        )
        has_next_detail = next_detail_index < len(available_detail_ids)
        if not (should_emit_detail and within_budget and has_next_detail):
            continue

        vacancy_id = available_detail_ids[next_detail_index]
        next_detail_index += 1
        detail_requests_emitted += 1
        plan.append(_build_detail_plan_item(vacancy_id, detail_index=detail_requests_emitted))

    return plan


def run_request_plan(
    plan: list[dict[str, Any]],
    *,
    workers: int = 1,
    sleep_seconds: float = 0.0,
    burst_pause_seconds: float = 0.0,
    header_mode: str = "dual",
    auth_mode: str | None = None,
    stop_on_captcha: bool = True,
    label: str = "request-plan",
    scenario_type: str = "request_plan",
    cooldown_origin_timestamp_utc: str | None = None,
) -> tuple[list[dict[str, Any]], Path]:
    if workers < 1:
        raise ValueError("workers must be >= 1")

    path = make_probe_path(label)
    records: list[dict[str, Any]] = []

    if workers == 1:
        for index, plan_item in enumerate(plan, start=1):
            record = hh_get(
                endpoint=str(plan_item.get("endpoint") or "/vacancies"),
                params=dict(plan_item.get("params") or {}),
                header_mode=header_mode,
                auth_mode=auth_mode,
            )
            extra_fields = dict(plan_item.get("extra_fields") or {})
            extra_fields["plan_index"] = index
            annotate_probe_record(
                record,
                records,
                scenario_label=label,
                scenario_type=scenario_type,
                workers=1,
                pause_seconds=sleep_seconds,
                cooldown_origin_timestamp_utc=cooldown_origin_timestamp_utc,
                extra_fields=extra_fields,
            )
            append_jsonl(path, record)
            records.append(record)
            _log_probe_record(
                record,
                "plan_index",
                "plan_step_kind",
                "endpoint_kind",
                "status_code",
                "error_type",
                "request_index_from_run_start",
                "seconds_since_previous_request",
            )
            if stop_on_captcha and record.get("error_type") == "captcha_required":
                break
            if index < len(plan) and sleep_seconds > 0:
                time.sleep(sleep_seconds)
        write_probe_report(records, path)
        return records, path

    for batch_start in range(0, len(plan), workers):
        batch = plan[batch_start : batch_start + workers]
        future_map = {}
        with ThreadPoolExecutor(max_workers=workers) as executor:
            for offset, plan_item in enumerate(batch, start=batch_start + 1):
                future = executor.submit(
                    hh_get,
                    str(plan_item.get("endpoint") or "/vacancies"),
                    params=dict(plan_item.get("params") or {}),
                    header_mode=header_mode,
                    auth_mode=auth_mode,
                )
                future_map[future] = (offset, plan_item)

            batch_records: list[tuple[int, dict[str, Any], dict[str, Any]]] = []
            for future in as_completed(future_map):
                plan_index, plan_item = future_map[future]
                batch_records.append((plan_index, plan_item, future.result()))

        batch_records.sort(key=lambda item: item[0])
        batch_has_captcha = False
        for plan_index, plan_item, record in batch_records:
            extra_fields = dict(plan_item.get("extra_fields") or {})
            extra_fields["plan_index"] = plan_index
            annotate_probe_record(
                record,
                records,
                scenario_label=label,
                scenario_type=scenario_type,
                workers=workers,
                pause_seconds=None,
                burst_pause_seconds=burst_pause_seconds,
                cooldown_origin_timestamp_utc=cooldown_origin_timestamp_utc,
                extra_fields=extra_fields,
            )
            append_jsonl(path, record)
            records.append(record)
            _log_probe_record(
                record,
                "plan_index",
                "plan_step_kind",
                "endpoint_kind",
                "status_code",
                "error_type",
                "request_index_from_run_start",
                "seconds_since_previous_request",
            )
            if stop_on_captcha and record.get("error_type") == "captcha_required":
                batch_has_captcha = True
        if batch_has_captcha:
            break
        if batch_start + workers < len(plan) and burst_pause_seconds > 0:
            time.sleep(burst_pause_seconds)

    write_probe_report(records, path)
    return records, path


def run_fixed_request_probe(
    *,
    params: dict[str, Any],
    repeats: int = 10,
    sleep_seconds: float = 5.0,
    header_mode: str = "dual",
    auth_mode: str | None = None,
    stop_on_captcha: bool = True,
    label: str = "fixed-request",
    endpoint: str = "/vacancies",
    cooldown_origin_timestamp_utc: str | None = None,
) -> tuple[list[dict[str, Any]], Path]:
    path = make_probe_path(label)
    records: list[dict[str, Any]] = []
    for attempt in range(repeats):
        record = hh_get(
            endpoint=endpoint,
            params=params,
            header_mode=header_mode,
            auth_mode=auth_mode,
        )
        annotate_probe_record(
            record,
            records,
            scenario_label=label,
            scenario_type="fixed_request",
            workers=1,
            pause_seconds=sleep_seconds,
            cooldown_origin_timestamp_utc=cooldown_origin_timestamp_utc,
            extra_fields={"attempt": attempt + 1},
        )
        append_jsonl(path, record)
        records.append(record)
        _log_probe_record(
            record,
            "attempt",
            "endpoint_kind",
            "status_code",
            "latency_ms",
            "items_count",
            "error_type",
            "request_index_from_run_start",
            "seconds_since_previous_request",
        )
        if stop_on_captcha and record["error_type"] == "captcha_required":
            break
        if attempt < repeats - 1 and sleep_seconds > 0:
            time.sleep(sleep_seconds)
    write_probe_report(records, path)
    return records, path


def replay_request_sequence(
    sequence: list[dict[str, Any]],
    *,
    sleep_seconds: float = 0.0,
    header_mode: str = "dual",
    auth_mode: str | None = None,
    stop_on_captcha: bool = True,
    label: str = "replay-sequence",
    cooldown_origin_timestamp_utc: str | None = None,
) -> tuple[list[dict[str, Any]], Path]:
    path = make_probe_path(label)
    records: list[dict[str, Any]] = []
    for index, source_record in enumerate(sequence, start=1):
        params = dict(source_record.get("params") or {})
        record = hh_get(
            "/vacancies",
            params=params,
            header_mode=header_mode,
            auth_mode=auth_mode,
        )
        annotate_probe_record(
            record,
            records,
            scenario_label=label,
            scenario_type="replay_sequence",
            workers=1,
            pause_seconds=sleep_seconds,
            cooldown_origin_timestamp_utc=cooldown_origin_timestamp_utc,
            extra_fields={
                "replay_index": index,
                "source_request_log_id": source_record.get("source_request_log_id"),
                "source_partition_id": source_record.get("source_partition_id"),
                "source_status_code": source_record.get("status_code"),
                "source_timestamp_utc": source_record.get("timestamp_utc"),
                "source_request_headers": source_record.get("request_headers"),
            },
        )
        append_jsonl(path, record)
        records.append(record)
        _log_probe_record(
            record,
            "replay_index",
            "source_request_log_id",
            "endpoint_kind",
            "status_code",
            "error_type",
            "request_index_from_run_start",
            "seconds_since_previous_request",
        )
        if stop_on_captcha and record["error_type"] == "captcha_required":
            break
        if index < len(sequence) and sleep_seconds > 0:
            time.sleep(sleep_seconds)
    write_probe_report(records, path)
    return records, path


def burst_replay_request_sequence(
    sequence: list[dict[str, Any]],
    *,
    workers: int = 4,
    burst_pause_seconds: float = 0.0,
    header_mode: str = "app_like",
    auth_mode: str | None = None,
    stop_on_captcha: bool = True,
    label: str = "burst-replay-sequence",
    cooldown_origin_timestamp_utc: str | None = None,
) -> tuple[list[dict[str, Any]], Path]:
    if workers < 1:
        raise ValueError("workers must be >= 1")
    path = make_probe_path(label)
    records: list[dict[str, Any]] = []
    for batch_start in range(0, len(sequence), workers):
        batch = sequence[batch_start : batch_start + workers]
        future_map = {}
        with ThreadPoolExecutor(max_workers=workers) as executor:
            for offset, source_record in enumerate(batch, start=batch_start + 1):
                params = dict(source_record.get("params") or {})
                future = executor.submit(
                    hh_get,
                    "/vacancies",
                    params=params,
                    header_mode=header_mode,
                    auth_mode=auth_mode,
                )
                future_map[future] = (offset, source_record)

            batch_records: list[dict[str, Any]] = []
            for future in as_completed(future_map):
                index, source_record = future_map[future]
                record = future.result()
                batch_records.append(
                    (
                        index,
                        source_record,
                        record,
                    )
                )

        batch_records.sort(key=lambda item: int(item[0]))
        for index, source_record, record in batch_records:
            annotate_probe_record(
                record,
                records,
                scenario_label=label,
                scenario_type="burst_replay_sequence",
                workers=workers,
                pause_seconds=None,
                burst_pause_seconds=burst_pause_seconds,
                cooldown_origin_timestamp_utc=cooldown_origin_timestamp_utc,
                extra_fields={
                    "replay_index": index,
                    "source_request_log_id": source_record.get("source_request_log_id"),
                    "source_partition_id": source_record.get("source_partition_id"),
                    "source_status_code": source_record.get("status_code"),
                    "source_timestamp_utc": source_record.get("timestamp_utc"),
                    "source_request_headers": source_record.get("request_headers"),
                },
            )
            append_jsonl(path, record)
            records.append(record)
            _log_probe_record(
                record,
                "replay_index",
                "source_request_log_id",
                "endpoint_kind",
                "status_code",
                "error_type",
                "request_index_from_run_start",
                "seconds_since_previous_request",
            )
        if stop_on_captcha and any(
            record.get("error_type") == "captcha_required"
            for _, _, record in batch_records
        ):
            break
        if batch_start + workers < len(sequence) and burst_pause_seconds > 0:
            time.sleep(burst_pause_seconds)
    write_probe_report(records, path)
    return records, path


def run_sequential_page_probe(
    area: str,
    *,
    start_page: int = 0,
    pages: int = 10,
    per_page: int = 20,
    sleep_seconds: float = 2.0,
    header_mode: str = "dual",
    auth_mode: str | None = None,
    stop_on_captcha: bool = True,
    extra_params: dict[str, Any] | None = None,
    label: str | None = None,
    cooldown_origin_timestamp_utc: str | None = None,
) -> tuple[list[dict[str, Any]], Path]:
    scenario_label = label or f"sequential-area-{area}"
    path = make_probe_path(scenario_label)
    records: list[dict[str, Any]] = []
    for page in range(start_page, start_page + pages):
        params: dict[str, Any] = {
            "area": area,
            "page": page,
            "per_page": per_page,
        }
        if extra_params:
            params.update(extra_params)
        record = hh_get(
            "/vacancies",
            params=params,
            header_mode=header_mode,
            auth_mode=auth_mode,
        )
        annotate_probe_record(
            record,
            records,
            scenario_label=scenario_label,
            scenario_type="sequential_area",
            workers=1,
            pause_seconds=sleep_seconds,
            cooldown_origin_timestamp_utc=cooldown_origin_timestamp_utc,
        )
        append_jsonl(path, record)
        records.append(record)
        _log_probe_record(
            record,
            "endpoint_kind",
            "status_code",
            "latency_ms",
            "items_count",
            "error_type",
            "request_index_from_run_start",
            "seconds_since_previous_request",
        )
        if stop_on_captcha and record["error_type"] == "captcha_required":
            break
        if page < start_page + pages - 1 and sleep_seconds > 0:
            time.sleep(sleep_seconds)
    write_probe_report(records, path)
    return records, path


def run_round_robin_probe(
    areas: list[str],
    *,
    pages_per_area: int = 3,
    per_page: int = 20,
    sleep_seconds: float = 1.0,
    header_mode: str = "dual",
    auth_mode: str | None = None,
    stop_on_captcha: bool = True,
    extra_params: dict[str, Any] | None = None,
    label: str = "round-robin",
    cooldown_origin_timestamp_utc: str | None = None,
) -> tuple[list[dict[str, Any]], Path]:
    path = make_probe_path(label)
    records: list[dict[str, Any]] = []
    for page in range(pages_per_area):
        for area in areas:
            params: dict[str, Any] = {
                "area": area,
                "page": page,
                "per_page": per_page,
            }
            if extra_params:
                params.update(extra_params)
            record = hh_get(
                "/vacancies",
                params=params,
                header_mode=header_mode,
                auth_mode=auth_mode,
            )
            annotate_probe_record(
                record,
                records,
                scenario_label=label,
                scenario_type="round_robin",
                workers=1,
                pause_seconds=sleep_seconds,
                cooldown_origin_timestamp_utc=cooldown_origin_timestamp_utc,
            )
            append_jsonl(path, record)
            records.append(record)
            _log_probe_record(
                record,
                "endpoint_kind",
                "status_code",
                "latency_ms",
                "error_type",
                "request_index_from_run_start",
                "seconds_since_previous_request",
            )
            if stop_on_captcha and record["error_type"] == "captcha_required":
                write_probe_report(records, path)
                return records, path
            if sleep_seconds > 0:
                time.sleep(sleep_seconds)
    write_probe_report(records, path)
    return records, path


__all__ = [
    "API_ROOT",
    "APPLICATION_TOKEN",
    "CAPTCHA_BACKURL",
    "DEFAULT_SOAK_RUN_ID",
    "POSTGRES_CONNECT_TIMEOUT_SECONDS",
    "RESULTS_DIR",
    "RUN_LABEL",
    "TIMEOUT_SECONDS",
    "USER_AGENT",
    "append_jsonl",
    "build_auth_headers",
    "build_mixed_workload_summary",
    "build_search_after_coverage_plan",
    "build_small_detail_budget_plan",
    "build_probe_report",
    "build_captcha_backurl",
    "burst_replay_request_sequence",
    "default_headers",
    "default_auth_mode",
    "endpoint_breakdown",
    "extract_detail_ids_from_records",
    "extract_first_error",
    "hh_get",
    "infer_endpoint_kind",
    "infer_header_mode",
    "load_jsonl",
    "load_simple_dotenv",
    "load_soak_search_sequence",
    "make_probe_path",
    "make_probe_report_path",
    "make_mixed_summary_path",
    "notebook_config",
    "postgres_connect_kwargs",
    "print_summary",
    "redact_headers",
    "replay_request_sequence",
    "resolve_auth_mode",
    "response_headers_to_dict",
    "run_fixed_request_probe",
    "run_request_plan",
    "run_round_robin_probe",
    "run_sequential_page_probe",
    "safe_json_loads",
    "summarize_records",
    "utc_now_iso",
    "write_mixed_workload_summary",
    "write_probe_report",
]
