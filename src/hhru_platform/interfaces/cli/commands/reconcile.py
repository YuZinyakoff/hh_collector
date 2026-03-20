from __future__ import annotations

import argparse
import sys
from uuid import UUID

from hhru_platform.application.commands.reconcile_run import (
    CrawlRunNotFoundError,
    ReconcileRunCommand,
    reconcile_run,
)
from hhru_platform.application.policies.reconciliation import (
    MissingRunsReconciliationPolicyV1,
)
from hhru_platform.infrastructure.db.repositories import (
    SqlAlchemyCrawlPartitionRepository,
    SqlAlchemyCrawlRunRepository,
    SqlAlchemyVacancyCurrentStateRepository,
    SqlAlchemyVacancySeenEventRepository,
)
from hhru_platform.infrastructure.db.session import session_scope
from hhru_platform.infrastructure.observability.metrics import get_metrics_registry


def register_reconcile_commands(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    parser = subparsers.add_parser(
        "reconcile-run",
        help="Reconcile vacancy state for one crawl_run.",
    )
    parser.add_argument(
        "--run-id",
        type=UUID,
        required=True,
        help="Existing crawl_run identifier.",
    )
    parser.set_defaults(handler=handle_reconcile_run)


def handle_reconcile_run(args: argparse.Namespace) -> int:
    command = ReconcileRunCommand(crawl_run_id=args.run_id)

    try:
        with session_scope() as session:
            result = reconcile_run(
                command,
                crawl_run_repository=SqlAlchemyCrawlRunRepository(session),
                crawl_partition_repository=SqlAlchemyCrawlPartitionRepository(session),
                vacancy_seen_event_repository=SqlAlchemyVacancySeenEventRepository(session),
                vacancy_current_state_repository=SqlAlchemyVacancyCurrentStateRepository(session),
                reconciliation_policy=MissingRunsReconciliationPolicyV1(),
                metrics_recorder=get_metrics_registry(),
            )
    except CrawlRunNotFoundError as error:
        print(str(error), file=sys.stderr)
        return 1

    print("reconciled crawl run")
    print(f"run_id={result.crawl_run_id}")
    print(f"vacancies_observed_in_run={result.observed_in_run_count}")
    print(f"missing_updated={result.missing_updated_count}")
    print(f"marked_inactive={result.marked_inactive_count}")
    print(f"status={result.run_status}")
    return 0
