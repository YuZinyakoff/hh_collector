import argparse

from hhru_platform.config.logging import configure_logging
from hhru_platform.interfaces.cli.commands.dictionary import register_dictionary_commands
from hhru_platform.interfaces.cli.commands.health import register_health_commands
from hhru_platform.interfaces.cli.commands.partition import register_partition_commands
from hhru_platform.interfaces.cli.commands.run import register_run_commands


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="hhru-platform")
    subparsers = parser.add_subparsers(dest="command")
    register_health_commands(subparsers)
    register_run_commands(subparsers)
    register_partition_commands(subparsers)
    register_dictionary_commands(subparsers)
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
