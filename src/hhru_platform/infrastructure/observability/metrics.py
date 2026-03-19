from __future__ import annotations

import fcntl
import json
import logging
from collections.abc import Iterator
from contextlib import contextmanager
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
    upstream_request_total: dict[str, int]
    upstream_request_duration_bucket: dict[str, int]
    upstream_request_duration_count: dict[str, int]
    upstream_request_duration_sum: dict[str, float]


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
        for operation, timestamp_value in sorted(
            state["operation_last_success_timestamp"].items()
        ):
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
        upstream_request_total={},
        upstream_request_duration_bucket={},
        upstream_request_duration_count={},
        upstream_request_duration_sum={},
    )


def _deserialize_state(raw_state: str) -> MetricsState:
    if not raw_state.strip():
        return _empty_state()

    loaded = json.loads(raw_state)
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


def _composite_key(left: str, right: str) -> str:
    return f"{left}|{right}"


def _split_composite_key(key: str) -> tuple[str, str]:
    left, right = key.split("|", maxsplit=1)
    return left, right


def _bucket_key(key: str, bucket: float) -> str:
    return f"{key}|{bucket}"


def _label_value(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")
