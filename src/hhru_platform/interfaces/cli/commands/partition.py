from __future__ import annotations

import argparse
import sys
from uuid import UUID

from hhru_platform.application.commands.plan_sweep import (
    CrawlRunNotFoundError,
    PlanRunCommand,
    plan_sweep,
)
from hhru_platform.application.policies.planner import SinglePartitionPlannerPolicyV1
from hhru_platform.infrastructure.db.repositories.crawl_partition_repo import (
    SqlAlchemyCrawlPartitionRepository,
)
from hhru_platform.infrastructure.db.repositories.crawl_run_repo import (
    SqlAlchemyCrawlRunRepository,
)
from hhru_platform.infrastructure.db.session import session_scope


def register_partition_commands(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    parser = subparsers.add_parser("plan-run", help="Create crawl_partitions for a crawl_run.")
    parser.add_argument(
        "--run-id",
        type=UUID,
        required=True,
        help="Existing crawl_run identifier.",
    )
    parser.set_defaults(handler=handle_plan_run)


def handle_plan_run(args: argparse.Namespace) -> int:
    command = PlanRunCommand(crawl_run_id=args.run_id)

    try:
        with session_scope() as session:
            crawl_run_repository = SqlAlchemyCrawlRunRepository(session)
            crawl_partition_repository = SqlAlchemyCrawlPartitionRepository(session)
            result = plan_sweep(
                command,
                crawl_run_repository,
                crawl_partition_repository,
                SinglePartitionPlannerPolicyV1(),
            )
    except CrawlRunNotFoundError as error:
        print(str(error), file=sys.stderr)
        return 1

    print("planned crawl partitions")
    print(f"run_id={result.crawl_run_id}")
    print(f"partitions_created={len(result.created_partitions)}")
    for partition in result.partitions:
        print(f"partition={partition.id} key={partition.partition_key} status={partition.status}")
    return 0
