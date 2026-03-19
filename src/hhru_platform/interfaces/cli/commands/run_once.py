from __future__ import annotations

import argparse
import sys
from uuid import UUID

from hhru_platform.application.commands.create_crawl_run import (
    CreateCrawlRunCommand,
    create_crawl_run,
)
from hhru_platform.application.commands.fetch_vacancy_detail import (
    FetchVacancyDetailCommand,
    FetchVacancyDetailResult,
    fetch_vacancy_detail,
)
from hhru_platform.application.commands.plan_sweep import (
    PlanRunCommand,
    PlanRunResult,
    plan_sweep,
)
from hhru_platform.application.commands.plan_sweep_v2 import (
    PlanRunV2Command,
    plan_sweep_v2,
)
from hhru_platform.application.commands.process_list_page import (
    ProcessListPageCommand,
    ProcessListPageResult,
    process_list_page,
)
from hhru_platform.application.commands.process_partition_v2 import (
    ProcessPartitionV2Command,
    ProcessPartitionV2Result,
    process_partition_v2,
)
from hhru_platform.application.commands.reconcile_run import (
    ReconcileRunCommand,
    ReconcileRunResult,
    reconcile_run,
)
from hhru_platform.application.commands.report_run_coverage import (
    ReportRunCoverageCommand,
    RunCoverageReport,
    report_run_coverage,
)
from hhru_platform.application.commands.run_collection_once import (
    RunCollectionOnceCommand,
    RunCollectionOnceResult,
    run_collection_once,
)
from hhru_platform.application.commands.run_collection_once_v2 import (
    RunCollectionOnceV2Command,
    RunCollectionOnceV2Result,
    run_collection_once_v2,
)
from hhru_platform.application.commands.run_list_engine_v2 import (
    RunListEngineV2Command,
    RunListEngineV2Result,
    run_list_engine_v2,
)
from hhru_platform.application.commands.select_detail_candidates import (
    SelectDetailCandidatesCommand,
    SelectDetailCandidatesResult,
    select_detail_candidates,
)
from hhru_platform.application.commands.split_partition import split_partition
from hhru_platform.application.commands.sync_dictionary import (
    SyncDictionaryCommand,
    SyncDictionaryResult,
    sync_dictionary,
)
from hhru_platform.application.policies.list_engine import PartitionSaturationPolicyV1
from hhru_platform.application.policies.planner import SinglePartitionPlannerPolicyV1
from hhru_platform.application.policies.reconciliation import (
    MissingRunsReconciliationPolicyV1,
)
from hhru_platform.domain.entities.crawl_run import CrawlRun
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
from hhru_platform.infrastructure.observability.metrics import get_metrics_registry


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

    v2_parser = subparsers.add_parser(
        "run-once-v2",
        help=(
            "Execute one tree-aware planner-v2 collection flow with exhaustive list "
            "coverage semantics."
        ),
    )
    v2_parser.add_argument(
        "--sync-dictionaries",
        choices=("yes", "no"),
        default="no",
        help="Sync all supported dictionaries before the run. Defaults to no.",
    )
    v2_parser.add_argument(
        "--detail-limit",
        type=int,
        default=100,
        help=(
            "Maximum number of selective detail fetches after list coverage completes. "
            "Defaults to 100."
        ),
    )
    v2_parser.add_argument(
        "--detail-refresh-ttl-days",
        type=int,
        default=30,
        help="TTL in days for selective detail refreshes. Defaults to 30.",
    )
    v2_parser.add_argument(
        "--run-type",
        default="weekly_sweep",
        help="Logical run_type for the created crawl_run. Defaults to weekly_sweep.",
    )
    v2_parser.add_argument(
        "--triggered-by",
        default="run-once-v2",
        help="Actor or subsystem that initiated the flow. Defaults to run-once-v2.",
    )
    v2_parser.set_defaults(handler=handle_run_once_v2)


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
    return 0 if result.status == "succeeded" else 1


def handle_run_once_v2(args: argparse.Namespace) -> int:
    command = RunCollectionOnceV2Command(
        sync_dictionaries=_parse_yes_no(str(args.sync_dictionaries)),
        detail_limit=int(args.detail_limit),
        detail_refresh_ttl_days=int(args.detail_refresh_ttl_days),
        run_type=str(args.run_type),
        triggered_by=str(args.triggered_by),
    )
    api_client = HHApiClient.from_settings()
    saturation_policy = PartitionSaturationPolicyV1()
    reconciliation_policy = MissingRunsReconciliationPolicyV1()

    try:
        result = run_collection_once_v2(
            command,
            sync_dictionary_step=lambda step_command: _execute_sync_dictionary_step(
                step_command,
                api_client=api_client,
            ),
            create_crawl_run_step=_execute_create_crawl_run_step,
            plan_run_v2_step=_execute_plan_run_v2_step,
            run_list_engine_v2_step=lambda step_command: _execute_run_list_engine_v2_step(
                step_command,
                api_client=api_client,
                saturation_policy=saturation_policy,
            ),
            report_run_coverage_step=_execute_report_run_coverage_step,
            select_detail_candidates_step=_execute_select_detail_candidates_step,
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

    _print_run_once_v2_summary(result)
    return 0 if result.status == "succeeded" else 1


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
) -> SyncDictionaryResult:
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


def _execute_create_crawl_run_step(command: CreateCrawlRunCommand) -> CrawlRun:
    with session_scope() as session:
        return create_crawl_run(command, SqlAlchemyCrawlRunRepository(session))


def _execute_plan_run_step(
    command: PlanRunCommand,
    *,
    planner_policy: SinglePartitionPlannerPolicyV1,
) -> PlanRunResult:
    with session_scope() as session:
        return plan_sweep(
            command,
            SqlAlchemyCrawlRunRepository(session),
            SqlAlchemyCrawlPartitionRepository(session),
            planner_policy,
        )


def _execute_plan_run_v2_step(command: PlanRunV2Command) -> PlanRunResult:
    with session_scope() as session:
        return plan_sweep_v2(
            command,
            crawl_run_repository=SqlAlchemyCrawlRunRepository(session),
            crawl_partition_repository=SqlAlchemyCrawlPartitionRepository(session),
            area_repository=SqlAlchemyAreaRepository(session),
        )


def _execute_process_list_page_step(
    command: ProcessListPageCommand,
    *,
    api_client: HHApiClient,
) -> ProcessListPageResult:
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
) -> FetchVacancyDetailResult:
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


def _execute_run_list_engine_v2_step(
    command: RunListEngineV2Command,
    *,
    api_client: HHApiClient,
    saturation_policy: PartitionSaturationPolicyV1,
) -> RunListEngineV2Result:
    return run_list_engine_v2(
        command,
        crawl_run_repository=_SessionlessCrawlRunRepository(),
        crawl_partition_repository=_SessionlessCrawlPartitionRepository(),
        process_partition_v2_step=lambda step_command: _execute_process_partition_v2_step(
            step_command,
            api_client=api_client,
            saturation_policy=saturation_policy,
        ),
    )


def _execute_process_partition_v2_step(
    command: ProcessPartitionV2Command,
    *,
    api_client: HHApiClient,
    saturation_policy: PartitionSaturationPolicyV1,
) -> ProcessPartitionV2Result:
    with session_scope() as session:
        crawl_partition_repository = SqlAlchemyCrawlPartitionRepository(session)
        return process_partition_v2(
            command,
            crawl_partition_repository=crawl_partition_repository,
            process_list_page_step=lambda step_command: process_list_page(
                step_command,
                crawl_partition_repository=crawl_partition_repository,
                api_client=api_client,
                api_request_log_repository=SqlAlchemyApiRequestLogRepository(session),
                raw_api_payload_repository=SqlAlchemyRawApiPayloadRepository(session),
                vacancy_repository=SqlAlchemyVacancyRepository(session),
                vacancy_seen_event_repository=SqlAlchemyVacancySeenEventRepository(session),
                vacancy_current_state_repository=SqlAlchemyVacancyCurrentStateRepository(session),
            ),
            split_partition_step=lambda step_command: split_partition(
                step_command,
                crawl_partition_repository=crawl_partition_repository,
                crawl_run_repository=SqlAlchemyCrawlRunRepository(session),
                area_repository=SqlAlchemyAreaRepository(session),
            ),
            saturation_policy=saturation_policy,
        )


def _execute_report_run_coverage_step(command: ReportRunCoverageCommand) -> RunCoverageReport:
    with session_scope() as session:
        return report_run_coverage(
            command,
            crawl_run_repository=SqlAlchemyCrawlRunRepository(session),
            crawl_partition_repository=SqlAlchemyCrawlPartitionRepository(session),
            metrics_recorder=get_metrics_registry(),
        )


def _execute_select_detail_candidates_step(
    command: SelectDetailCandidatesCommand,
) -> SelectDetailCandidatesResult:
    with session_scope() as session:
        return select_detail_candidates(
            command,
            vacancy_current_state_repository=SqlAlchemyVacancyCurrentStateRepository(session),
            vacancy_seen_event_repository=SqlAlchemyVacancySeenEventRepository(session),
        )


def _execute_reconcile_run_step(
    command: ReconcileRunCommand,
    *,
    reconciliation_policy: MissingRunsReconciliationPolicyV1,
) -> ReconcileRunResult:
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
    if result.status == "succeeded":
        print("completed run-once collection")
    else:
        print("failed run-once collection")
    print(f"status={result.status}")
    print(f"run_id={result.run_id}")
    print(f"run_type={result.run_type}")
    print(f"triggered_by={result.triggered_by}")
    print(f"dictionaries_synced={len(result.dictionary_results)}")
    print(f"partitions_planned={result.partitions_planned}")
    print(f"partitions_attempted={result.partitions_attempted}")
    print(f"partitions_processed={result.partitions_processed}")
    print(f"partitions_failed={result.partitions_failed}")
    print(f"list_pages_attempted={result.list_pages_attempted}")
    print(f"list_pages_processed={result.list_pages_processed}")
    print(f"list_pages_failed={result.list_pages_failed}")
    print(f"vacancies_found={result.vacancies_found}")
    print(f"detail_fetch_attempted={result.detail_fetch_attempted}")
    print(f"detail_fetch_succeeded={result.detail_fetch_succeeded}")
    print(f"detail_fetch_failed={result.detail_fetch_failed}")
    print(f"reconciliation_status={result.reconciliation_status}")
    print(f"completed_steps={','.join(result.completed_steps) or '-'}")
    print(f"skipped_steps={','.join(result.skipped_steps) or '-'}")
    print(f"failed_step={result.failed_step or '-'}")
    print(f"error={result.error_message or '-'}")


def _print_run_once_v2_summary(result: RunCollectionOnceV2Result) -> None:
    if result.status == "succeeded":
        print("completed run-once-v2 collection")
    elif result.status == "completed_with_unresolved":
        print("completed run-once-v2 collection with unresolved coverage")
    else:
        print("failed run-once-v2 collection")

    print(f"status={result.status}")
    print(f"run_id={result.run_id}")
    print(f"run_type={result.run_type}")
    print(f"triggered_by={result.triggered_by}")
    print(f"dictionaries_synced={len(result.dictionary_results)}")
    print(f"partitions_planned={result.partitions_planned}")
    print(f"list_engine_iterations={result.list_engine_iterations}")
    print(f"total_partitions={result.total_partitions}")
    print(f"covered_terminal_partitions={result.covered_terminal_partitions}")
    print(f"pending_terminal_partitions={result.pending_terminal_partitions}")
    print(f"split_partitions={result.split_partitions}")
    print(f"unresolved_partitions={result.unresolved_partitions}")
    print(f"failed_partitions={result.failed_partitions}")
    print(f"coverage_ratio={result.coverage_ratio:.4f}")
    print(f"list_stage_status={result.list_stage_status}")
    print(f"detail_stage_status={result.detail_stage_status}")
    print(f"detail_candidates_selected={result.detail_candidates_selected}")
    print(f"detail_fetch_attempted={result.detail_fetch_attempted}")
    print(f"detail_fetch_succeeded={result.detail_fetch_succeeded}")
    print(f"detail_fetch_failed={result.detail_fetch_failed}")
    print(f"reconciliation_status={result.reconciliation_status}")
    print(f"completed_steps={','.join(result.completed_steps) or '-'}")
    print(f"skipped_steps={','.join(result.skipped_steps) or '-'}")
    print(f"failed_step={result.failed_step or '-'}")
    print(f"error={result.error_message or '-'}")


class _SessionlessCrawlRunRepository:
    def get(self, run_id: UUID) -> CrawlRun | None:
        with session_scope() as session:
            return SqlAlchemyCrawlRunRepository(session).get(run_id)


class _SessionlessCrawlPartitionRepository:
    def list_pending_terminal_by_run_id(
        self,
        run_id: UUID,
        *,
        limit: int | None = None,
    ) -> list:
        with session_scope() as session:
            return SqlAlchemyCrawlPartitionRepository(session).list_pending_terminal_by_run_id(
                run_id,
                limit=limit,
            )
