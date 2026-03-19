from __future__ import annotations

import logging
from collections.abc import Mapping
from time import perf_counter

from hhru_platform.infrastructure.observability.logging import log_event
from hhru_platform.infrastructure.observability.metrics import get_metrics_registry


def log_operation_started(
    logger: logging.Logger,
    *,
    operation: str,
    **fields: object,
) -> float:
    log_event(
        logger,
        logging.INFO,
        f"{operation}.started",
        operation=operation,
        status="started",
        **fields,
    )
    return perf_counter()


def record_operation_succeeded(
    logger: logging.Logger,
    *,
    operation: str,
    started_at: float,
    records_written: Mapping[str, int] | None = None,
    **fields: object,
) -> None:
    duration_seconds = max(perf_counter() - started_at, 0.0)
    metrics_registry = get_metrics_registry()
    metrics_registry.record_operation(
        operation=operation,
        status="succeeded",
        duration_seconds=duration_seconds,
    )
    if records_written is not None:
        for record_type, count in records_written.items():
            metrics_registry.record_records_written(
                operation=operation,
                record_type=record_type,
                count=count,
            )
    log_event(
        logger,
        logging.INFO,
        f"{operation}.succeeded",
        operation=operation,
        status="succeeded",
        duration_ms=int(duration_seconds * 1000),
        **fields,
    )


def record_operation_failed(
    logger: logging.Logger,
    *,
    operation: str,
    started_at: float,
    error_type: str,
    error_message: str,
    level: int = logging.ERROR,
    **fields: object,
) -> None:
    duration_seconds = max(perf_counter() - started_at, 0.0)
    get_metrics_registry().record_operation(
        operation=operation,
        status="failed",
        duration_seconds=duration_seconds,
    )
    log_event(
        logger,
        level,
        f"{operation}.failed",
        operation=operation,
        status="failed",
        duration_ms=int(duration_seconds * 1000),
        error_type=error_type,
        error_message=error_message,
        **fields,
    )
