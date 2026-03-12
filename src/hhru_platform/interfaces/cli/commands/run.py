from __future__ import annotations

import argparse

from hhru_platform.application.commands.create_crawl_run import (
    CreateCrawlRunCommand,
    create_crawl_run,
)
from hhru_platform.infrastructure.db.repositories.crawl_run_repo import (
    SqlAlchemyCrawlRunRepository,
)
from hhru_platform.infrastructure.db.session import session_scope


def register_run_commands(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser("create-run", help="Create a crawl_run record.")
    parser.add_argument("--run-type", required=True, help="Logical type of the run to create.")
    parser.add_argument(
        "--triggered-by",
        required=True,
        help="Actor or subsystem that initiated the run.",
    )
    parser.set_defaults(handler=handle_create_run)


def handle_create_run(args: argparse.Namespace) -> int:
    command = CreateCrawlRunCommand(
        run_type=args.run_type,
        triggered_by=args.triggered_by,
    )

    with session_scope() as session:
        repository = SqlAlchemyCrawlRunRepository(session)
        crawl_run = create_crawl_run(command, repository)

    print("created crawl_run")
    print(f"id={crawl_run.id}")
    print(f"run_type={crawl_run.run_type}")
    print(f"status={crawl_run.status}")
    print(f"triggered_by={crawl_run.triggered_by}")
    print(f"started_at={crawl_run.started_at.isoformat()}")
    return 0
