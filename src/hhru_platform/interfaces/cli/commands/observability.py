from __future__ import annotations

import argparse
import logging

from hhru_platform.config.settings import get_settings
from hhru_platform.infrastructure.observability.logging import log_event
from hhru_platform.infrastructure.observability.metrics import (
    get_metrics_registry,
    serve_metrics_http,
)

LOGGER = logging.getLogger(__name__)


def register_observability_commands(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    show_parser = subparsers.add_parser(
        "show-metrics",
        help="Print the current Prometheus metrics snapshot.",
    )
    show_parser.set_defaults(handler=handle_show_metrics)

    serve_parser = subparsers.add_parser(
        "serve-metrics",
        help="Expose Prometheus metrics over HTTP.",
    )
    serve_parser.add_argument(
        "--host",
        default=None,
        help="Bind host. Defaults to HHRU_METRICS_HOST.",
    )
    serve_parser.add_argument(
        "--port",
        type=int,
        default=None,
        help="Bind port. Defaults to HHRU_METRICS_PORT.",
    )
    serve_parser.set_defaults(handler=handle_serve_metrics)


def handle_show_metrics(_: argparse.Namespace) -> int:
    print(get_metrics_registry().render_prometheus(), end="")
    return 0


def handle_serve_metrics(args: argparse.Namespace) -> int:
    settings = get_settings()
    host = str(args.host or settings.metrics_host)
    port = int(args.port or settings.metrics_port)

    log_event(
        LOGGER,
        logging.INFO,
        "serve_metrics.started",
        operation="serve_metrics",
        host=host,
        port=port,
        status="running",
    )
    try:
        serve_metrics_http(host=host, port=port)
    except KeyboardInterrupt:
        log_event(
            LOGGER,
            logging.INFO,
            "serve_metrics.stopped",
            operation="serve_metrics",
            host=host,
            port=port,
            status="stopped",
        )
    return 0
