import argparse

from hhru_platform.config.logging import configure_logging
from hhru_platform.config.settings import get_settings


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="hhru-platform")
    subparsers = parser.add_subparsers(dest="command")
    subparsers.add_parser("health-check", help="Show basic platform configuration status.")
    return parser


def main() -> int:
    configure_logging()
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "health-check":
        settings = get_settings()
        print(f"env={settings.env}")
        print(f"database_url={settings.database_url}")
        print(f"redis_url={settings.redis_url}")
        return 0

    parser.print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
