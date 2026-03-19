from __future__ import annotations

import argparse
import sys
from uuid import UUID

from hhru_platform.application.commands.plan_sweep import (
    CrawlRunNotFoundError,
    PlanRunCommand,
    plan_sweep,
)
from hhru_platform.application.commands.plan_sweep_v2 import (
    PlannerV2AreasNotReadyError,
    PlanRunV2Command,
    plan_sweep_v2,
)
from hhru_platform.application.commands.split_partition import (
    CrawlPartitionNotFoundError,
    SplitPartitionCommand,
    UnsupportedPartitionSplitError,
    split_partition,
)
from hhru_platform.application.policies.planner import SinglePartitionPlannerPolicyV1
from hhru_platform.infrastructure.db.repositories import (
    SqlAlchemyAreaRepository,
    SqlAlchemyCrawlPartitionRepository,
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

    planner_v2_parser = subparsers.add_parser(
        "plan-run-v2",
        help="Create planner v2 area-root crawl_partitions for a crawl_run.",
    )
    planner_v2_parser.add_argument(
        "--run-id",
        type=UUID,
        required=True,
        help="Existing crawl_run identifier.",
    )
    planner_v2_parser.set_defaults(handler=handle_plan_run_v2)

    split_parser = subparsers.add_parser(
        "split-partition",
        help="Split one planner v2 saturated crawl_partition into child area partitions.",
    )
    split_parser.add_argument(
        "--partition-id",
        type=UUID,
        required=True,
        help="Existing crawl_partition identifier.",
    )
    split_parser.set_defaults(handler=handle_split_partition)


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


def handle_plan_run_v2(args: argparse.Namespace) -> int:
    command = PlanRunV2Command(crawl_run_id=args.run_id)

    try:
        with session_scope() as session:
            crawl_run_repository = SqlAlchemyCrawlRunRepository(session)
            crawl_partition_repository = SqlAlchemyCrawlPartitionRepository(session)
            area_repository = SqlAlchemyAreaRepository(session)
            result = plan_sweep_v2(
                command,
                crawl_run_repository,
                crawl_partition_repository,
                area_repository,
            )
    except (CrawlRunNotFoundError, PlannerV2AreasNotReadyError) as error:
        print(str(error), file=sys.stderr)
        return 1

    print("planned crawl partitions with planner v2")
    print(f"run_id={result.crawl_run_id}")
    print(f"partitions_created={len(result.created_partitions)}")
    for partition in result.partitions:
        print(
            "partition="
            f"{partition.id} "
            f"key={partition.partition_key} "
            f"scope_key={partition.scope_key or '-'} "
            f"depth={partition.depth} "
            f"parent={partition.parent_partition_id or '-'} "
            f"status={partition.status}"
        )
    return 0


def handle_split_partition(args: argparse.Namespace) -> int:
    command = SplitPartitionCommand(partition_id=args.partition_id)

    try:
        with session_scope() as session:
            crawl_partition_repository = SqlAlchemyCrawlPartitionRepository(session)
            crawl_run_repository = SqlAlchemyCrawlRunRepository(session)
            area_repository = SqlAlchemyAreaRepository(session)
            result = split_partition(
                command,
                crawl_partition_repository,
                crawl_run_repository,
                area_repository,
            )
    except (CrawlPartitionNotFoundError, UnsupportedPartitionSplitError, LookupError) as error:
        print(str(error), file=sys.stderr)
        return 1

    print("split crawl partition")
    print(f"partition_id={result.parent_partition.id}")
    print(f"run_id={result.parent_partition.crawl_run_id}")
    print(f"status={result.parent_partition.status}")
    print(f"children_created={len(result.created_children)}")
    print(f"children_total={len(result.children)}")
    print(f"resolution_message={result.resolution_message or '-'}")
    for child in result.children:
        print(
            "child="
            f"{child.id} "
            f"key={child.partition_key} "
            f"scope_key={child.scope_key or '-'} "
            f"depth={child.depth} "
            f"parent={child.parent_partition_id or '-'} "
            f"status={child.status}"
        )
    return 0
