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
    return 0
