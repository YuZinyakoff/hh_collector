import logging

from hhru_platform.config.settings import get_settings
from hhru_platform.infrastructure.observability.logging import (
    JsonLogFormatter,
    ServiceContextFilter,
)


def configure_logging() -> None:
    settings = get_settings()
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(getattr(logging, settings.log_level.upper(), logging.INFO))

    handler = logging.StreamHandler()
    handler.addFilter(ServiceContextFilter(service_name="hhru-platform", env=settings.env))
    if settings.log_format.lower() == "json":
        handler.setFormatter(JsonLogFormatter())
    else:
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))

    root_logger.addHandler(handler)
    root_logger.propagate = False
