from __future__ import annotations

import argparse

from hhru_platform.config.settings import get_settings


def register_health_commands(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    parser = subparsers.add_parser("health-check", help="Show basic platform configuration status.")
    parser.set_defaults(handler=handle_health_check)


def handle_health_check(_: argparse.Namespace) -> int:
    settings = get_settings()
    print(f"env={settings.env}")
    print(f"database_url={settings.database_url}")
    print(f"redis_url={settings.redis_url}")
    print(f"hh_api_base_url={settings.hh_api_base_url}")
    print(f"hh_api_timeout_seconds={settings.hh_api_timeout_seconds}")
    print(f"hh_api_user_agent={settings.hh_api_user_agent}")
    print(f"metrics_state_path={settings.metrics_state_path}")
    print(f"metrics_endpoint=http://{settings.metrics_host}:{settings.metrics_port}/metrics")
    print(f"backup_dir={settings.backup_dir}")
    print(f"backup_retention_days={settings.backup_retention_days}")
    return 0
