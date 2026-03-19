from __future__ import annotations

import argparse
import sys

from hhru_platform.application.commands.create_crawl_run import (
    CreateCrawlRunCommand,
    create_crawl_run,
)
from hhru_platform.application.commands.fetch_vacancy_detail import (
    FetchVacancyDetailCommand,
    fetch_vacancy_detail,
)
from hhru_platform.application.commands.plan_sweep import PlanRunCommand, plan_sweep
from hhru_platform.application.commands.process_list_page import (
    ProcessListPageCommand,
    process_list_page,
)
from hhru_platform.application.commands.reconcile_run import (
    ReconcileRunCommand,
    reconcile_run,
)
from hhru_platform.application.commands.run_collection_once import (
    RunCollectionOnceCommand,
    RunCollectionOnceResult,
    run_collection_once,
)
from hhru_platform.application.commands.sync_dictionary import (
    SyncDictionaryCommand,
    sync_dictionary,
)
from hhru_platform.application.policies.planner import SinglePartitionPlannerPolicyV1
from hhru_platform.application.policies.reconciliation import (
    MissingRunsReconciliationPolicyV1,
)
from hhru_platform.infrastructure.db.repositories import (
    SqlAlchemyApiRequestLogRepository,
    SqlAlchemyAreaRepository,
    SqlAlchemyCrawlPartitionRepository,
    SqlAlchemyCrawlRunRepository,
    SqlAlchemyDetailFetchAttemptRepository,
    SqlAlchemyDictionaryStore,
    SqlAlchemyDictionarySyncRunRepository,
    SqlAlchemyProfessionalRoleRepository,
    SqlAlchemyRawApiPayloadRepository,
    SqlAlchemyVacancyCurrentStateRepository,
    SqlAlchemyVacancyRepository,
    SqlAlchemyVacancySeenEventRepository,
    SqlAlchemyVacancySnapshotRepository,
)
from hhru_platform.infrastructure.db.session import session_scope
from hhru_platform.infrastructure.hh_api.client import HHApiClient


def register_run_once_commands(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    parser = subparsers.add_parser(
        "run-once",
        help="Execute one orchestration-lite collection flow using the existing MVP slices.",
    )
    parser.add_argument(
        "--sync-dictionaries",
        choices=("yes", "no"),
        default="no",
        help="Sync all supported dictionaries before the run. Defaults to no.",
    )
    parser.add_argument(
        "--pages-per-partition",
        type=int,
        default=1,
        help="How many list pages to process per partition. Defaults to 1.",
    )
    parser.add_argument(
        "--detail-limit",
        type=int,
        default=0,
        help="How many discovered vacancies to fetch in detail. Defaults to 0.",
    )
    parser.add_argument(
        "--run-type",
        default="weekly_sweep",
        help="Logical run_type for the created crawl_run. Defaults to weekly_sweep.",
    )
    parser.add_argument(
        "--triggered-by",
        default="run-once",
        help="Actor or subsystem that initiated the flow. Defaults to run-once.",
    )
    parser.set_defaults(handler=handle_run_once)


def handle_run_once(args: argparse.Namespace) -> int:
    command = RunCollectionOnceCommand(
        sync_dictionaries=_parse_yes_no(str(args.sync_dictionaries)),
        pages_per_partition=int(args.pages_per_partition),
        detail_limit=int(args.detail_limit),
        run_type=str(args.run_type),
        triggered_by=str(args.triggered_by),
    )
    api_client = HHApiClient.from_settings()
    planner_policy = SinglePartitionPlannerPolicyV1()
    reconciliation_policy = MissingRunsReconciliationPolicyV1()

    try:
        result = run_collection_once(
            command,
            sync_dictionary_step=lambda step_command: _execute_sync_dictionary_step(
                step_command,
                api_client=api_client,
            ),
            create_crawl_run_step=_execute_create_crawl_run_step,
            plan_run_step=lambda step_command: _execute_plan_run_step(
                step_command,
                planner_policy=planner_policy,
            ),
            process_list_page_step=lambda step_command: _execute_process_list_page_step(
                step_command,
                api_client=api_client,
            ),
            fetch_vacancy_detail_step=lambda step_command: _execute_fetch_detail_step(
                step_command,
                api_client=api_client,
            ),
            reconcile_run_step=lambda step_command: _execute_reconcile_run_step(
                step_command,
                reconciliation_policy=reconciliation_policy,
            ),
        )
    except Exception as error:
        print(str(error), file=sys.stderr)
        return 1

    _print_run_once_summary(result)
    return 0


def _parse_yes_no(value: str) -> bool:
    normalized_value = value.strip().lower()
    if normalized_value == "yes":
        return True
    if normalized_value == "no":
        return False
    raise ValueError("sync-dictionaries must be either yes or no")


def _execute_sync_dictionary_step(
    command: SyncDictionaryCommand,
    *,
    api_client: HHApiClient,
):
    with session_scope() as session:
        return sync_dictionary(
            command,
            api_client=api_client,
            sync_run_repository=SqlAlchemyDictionarySyncRunRepository(session),
            api_request_log_repository=SqlAlchemyApiRequestLogRepository(session),
            raw_api_payload_repository=SqlAlchemyRawApiPayloadRepository(session),
            dictionary_store=SqlAlchemyDictionaryStore(
                area_repository=SqlAlchemyAreaRepository(session),
                professional_role_repository=SqlAlchemyProfessionalRoleRepository(session),
            ),
        )


def _execute_create_crawl_run_step(command: CreateCrawlRunCommand):
    with session_scope() as session:
        return create_crawl_run(command, SqlAlchemyCrawlRunRepository(session))


def _execute_plan_run_step(
    command: PlanRunCommand,
    *,
    planner_policy: SinglePartitionPlannerPolicyV1,
):
    with session_scope() as session:
        return plan_sweep(
            command,
            SqlAlchemyCrawlRunRepository(session),
            SqlAlchemyCrawlPartitionRepository(session),
            planner_policy,
        )


def _execute_process_list_page_step(
    command: ProcessListPageCommand,
    *,
    api_client: HHApiClient,
):
    with session_scope() as session:
        return process_list_page(
            command,
            crawl_partition_repository=SqlAlchemyCrawlPartitionRepository(session),
            api_client=api_client,
            api_request_log_repository=SqlAlchemyApiRequestLogRepository(session),
            raw_api_payload_repository=SqlAlchemyRawApiPayloadRepository(session),
            vacancy_repository=SqlAlchemyVacancyRepository(session),
            vacancy_seen_event_repository=SqlAlchemyVacancySeenEventRepository(session),
            vacancy_current_state_repository=SqlAlchemyVacancyCurrentStateRepository(session),
        )


def _execute_fetch_detail_step(
    command: FetchVacancyDetailCommand,
    *,
    api_client: HHApiClient,
):
    with session_scope() as session:
        return fetch_vacancy_detail(
            command,
            vacancy_repository=SqlAlchemyVacancyRepository(session),
            api_client=api_client,
            detail_fetch_attempt_repository=SqlAlchemyDetailFetchAttemptRepository(session),
            api_request_log_repository=SqlAlchemyApiRequestLogRepository(session),
            raw_api_payload_repository=SqlAlchemyRawApiPayloadRepository(session),
            vacancy_snapshot_repository=SqlAlchemyVacancySnapshotRepository(session),
            vacancy_current_state_repository=SqlAlchemyVacancyCurrentStateRepository(session),
        )


def _execute_reconcile_run_step(
    command: ReconcileRunCommand,
    *,
    reconciliation_policy: MissingRunsReconciliationPolicyV1,
):
    with session_scope() as session:
        return reconcile_run(
            command,
            crawl_run_repository=SqlAlchemyCrawlRunRepository(session),
            crawl_partition_repository=SqlAlchemyCrawlPartitionRepository(session),
            vacancy_seen_event_repository=SqlAlchemyVacancySeenEventRepository(session),
            vacancy_current_state_repository=SqlAlchemyVacancyCurrentStateRepository(session),
            reconciliation_policy=reconciliation_policy,
        )


def _print_run_once_summary(result: RunCollectionOnceResult) -> None:
    print("completed run-once collection")
    print(f"run_id={result.run_id}")
    print(f"run_type={result.run_type}")
    print(f"triggered_by={result.triggered_by}")
    print(f"dictionaries_synced={len(result.dictionary_results)}")
    print(f"partitions_planned={result.partitions_planned}")
    print(f"partitions_processed={result.partitions_processed}")
    print(f"list_pages_processed={result.list_pages_processed}")
    print(f"vacancies_found={result.vacancies_found}")
    print(f"detail_fetch_attempted={result.detail_fetch_attempted}")
    print(f"detail_fetch_succeeded={result.detail_fetch_succeeded}")
    print(f"detail_fetch_failed={result.detail_fetch_failed}")
    print(f"reconciliation_status={result.reconciliation_status}")
