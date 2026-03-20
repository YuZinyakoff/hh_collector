import argparse

from hhru_platform.config.logging import configure_logging
from hhru_platform.interfaces.cli.commands.detail import register_detail_commands
from hhru_platform.interfaces.cli.commands.dictionary import register_dictionary_commands
from hhru_platform.interfaces.cli.commands.health import register_health_commands
from hhru_platform.interfaces.cli.commands.list_engine import register_list_engine_commands
from hhru_platform.interfaces.cli.commands.list_page import register_list_page_commands
from hhru_platform.interfaces.cli.commands.observability import (
    register_observability_commands,
)
from hhru_platform.interfaces.cli.commands.partition import register_partition_commands
from hhru_platform.interfaces.cli.commands.reconcile import register_reconcile_commands
from hhru_platform.interfaces.cli.commands.reporting import register_reporting_commands
from hhru_platform.interfaces.cli.commands.research import register_research_commands
from hhru_platform.interfaces.cli.commands.run import register_run_commands
from hhru_platform.interfaces.cli.commands.run_once import register_run_once_commands
from hhru_platform.interfaces.cli.commands.scheduler import register_scheduler_commands


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="hhru-platform")
    subparsers = parser.add_subparsers(dest="command")
    register_health_commands(subparsers)
    register_run_commands(subparsers)
    register_run_once_commands(subparsers)
    register_scheduler_commands(subparsers)
    register_partition_commands(subparsers)
    register_reporting_commands(subparsers)
    register_dictionary_commands(subparsers)
    register_list_page_commands(subparsers)
    register_list_engine_commands(subparsers)
    register_detail_commands(subparsers)
    register_reconcile_commands(subparsers)
    register_research_commands(subparsers)
    register_observability_commands(subparsers)
    return parser


def main() -> int:
    configure_logging()
    parser = build_parser()
    args = parser.parse_args()
    handler = getattr(args, "handler", None)
    if handler is not None:
        return int(handler(args))

    parser.print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
