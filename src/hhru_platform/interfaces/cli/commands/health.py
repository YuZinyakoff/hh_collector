from __future__ import annotations

import argparse

from hhru_platform.config.settings import get_settings
from hhru_platform.infrastructure.hh_api.user_agent import (
    HHApiUserAgentValidationError,
    validate_live_vacancy_search_user_agent,
)


def register_health_commands(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    parser = subparsers.add_parser("health-check", help="Show basic platform configuration status.")
    parser.set_defaults(handler=handle_health_check)


def handle_health_check(_: argparse.Namespace) -> int:
    settings = get_settings()
    user_agent_validation_error: str | None = None
    try:
        validate_live_vacancy_search_user_agent(settings.hh_api_user_agent)
    except HHApiUserAgentValidationError as error:
        user_agent_validation_error = str(error)

    print(f"env={settings.env}")
    print(f"database_url={settings.database_url}")
    print(f"redis_url={settings.redis_url}")
    print(f"hh_api_base_url={settings.hh_api_base_url}")
    print(f"hh_api_timeout_seconds={settings.hh_api_timeout_seconds}")
    print(f"hh_api_user_agent={settings.hh_api_user_agent}")
    print(
        "hh_api_user_agent_live_search_valid="
        f"{'no' if user_agent_validation_error is not None else 'yes'}"
    )
    print(f"hh_api_user_agent_live_search_error={user_agent_validation_error or '-'}")
    print(f"metrics_state_path={settings.metrics_state_path}")
    print(f"metrics_endpoint=http://{settings.metrics_host}:{settings.metrics_port}/metrics")
    print(f"backup_dir={settings.backup_dir}")
    print(f"backup_retention_days={settings.backup_retention_days}")
    return 0
