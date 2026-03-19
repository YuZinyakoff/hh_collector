from __future__ import annotations

import json
import logging
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
    registry.record_upstream_request(
        endpoint="/vacancies",
        status_code=200,
        duration_seconds=0.17,
    )

    rendered = registry.render_prometheus()

    assert 'hhru_operation_total{operation="process_list_page",status="succeeded"} 1' in rendered
    assert (
        'hhru_records_written_total{operation="process_list_page",record_type="vacancy"} 5'
        in rendered
    )
    assert 'hhru_upstream_request_total{endpoint="/vacancies",status_class="2xx"} 1' in rendered
    assert "hhru_operation_duration_seconds_count" in rendered
    assert "hhru_upstream_request_duration_seconds_count" in rendered


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
