from __future__ import annotations

import argparse
import sys
from uuid import UUID

from hhru_platform.application.commands.process_list_page import process_list_page
from hhru_platform.application.commands.process_partition_v2 import (
    CrawlPartitionNotFoundError,
    ProcessPartitionV2Command,
    ProcessPartitionV2Result,
    UnsupportedPartitionExecutionError,
    process_partition_v2,
)
from hhru_platform.application.commands.run_list_engine_v2 import (
    CrawlRunNotFoundError,
    RunListEngineV2Command,
    RunListEngineV2Result,
    run_list_engine_v2,
)
from hhru_platform.application.commands.split_partition import split_partition
from hhru_platform.application.policies.list_engine import PartitionSaturationPolicyV1
from hhru_platform.infrastructure.db.repositories import (
    SqlAlchemyApiRequestLogRepository,
    SqlAlchemyAreaRepository,
    SqlAlchemyCrawlPartitionRepository,
    SqlAlchemyCrawlRunRepository,
    SqlAlchemyRawApiPayloadRepository,
    SqlAlchemyVacancyCurrentStateRepository,
    SqlAlchemyVacancyRepository,
    SqlAlchemyVacancySeenEventRepository,
)
from hhru_platform.infrastructure.db.session import session_scope
from hhru_platform.infrastructure.hh_api.client import HHApiClient


def register_list_engine_commands(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    process_partition_parser = subparsers.add_parser(
        "process-partition-v2",
        help="Process one planner v2 terminal partition with pagination and saturation handling.",
    )
    process_partition_parser.add_argument(
        "--partition-id",
        type=UUID,
        required=True,
        help="Existing planner v2 crawl_partition identifier.",
    )
    process_partition_parser.set_defaults(handler=handle_process_partition_v2)

    run_engine_parser = subparsers.add_parser(
        "run-list-engine-v2",
        help="Run list engine v2 for pending terminal partitions in one crawl_run.",
    )
    run_engine_parser.add_argument(
        "--run-id",
        type=UUID,
        required=True,
        help="Existing crawl_run identifier.",
    )
    run_engine_parser.add_argument(
        "--partition-limit",
        type=int,
        default=None,
        help="Optional limit on how many terminal partitions to process in this invocation.",
    )
    run_engine_parser.set_defaults(handler=handle_run_list_engine_v2)


def handle_process_partition_v2(args: argparse.Namespace) -> int:
    command = ProcessPartitionV2Command(partition_id=args.partition_id)
    api_client = HHApiClient.from_settings()
    saturation_policy = PartitionSaturationPolicyV1()

    try:
        result = _execute_process_partition_v2_step(
            command,
            api_client=api_client,
            saturation_policy=saturation_policy,
        )
    except (CrawlPartitionNotFoundError, UnsupportedPartitionExecutionError) as error:
        print(str(error), file=sys.stderr)
        return 1

    _print_process_partition_v2_summary(result)
    return 0 if result.status == "succeeded" else 1


def handle_run_list_engine_v2(args: argparse.Namespace) -> int:
    command = RunListEngineV2Command(
        crawl_run_id=args.run_id,
        partition_limit=args.partition_limit,
    )
    api_client = HHApiClient.from_settings()
    saturation_policy = PartitionSaturationPolicyV1()

    try:
        result = run_list_engine_v2(
            command,
            crawl_run_repository=_SessionlessCrawlRunRepository(),
            crawl_partition_repository=_SessionlessCrawlPartitionRepository(),
            process_partition_v2_step=lambda step_command: _execute_process_partition_v2_step(
                step_command,
                api_client=api_client,
                saturation_policy=saturation_policy,
            ),
        )
    except CrawlRunNotFoundError as error:
        print(str(error), file=sys.stderr)
        return 1

    _print_run_list_engine_v2_summary(result)
    return 0 if result.status == "succeeded" else 1


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


def _print_process_partition_v2_summary(result: ProcessPartitionV2Result) -> None:
    print("processed crawl partition with list engine v2")
    print(f"partition_id={result.partition_id}")
    print(f"run_id={result.crawl_run_id}")
    print(f"status={result.status}")
    print(f"partition_final_status={result.final_partition_status}")
    print(f"coverage_status={result.final_coverage_status}")
    print(f"pages_attempted={result.pages_attempted}")
    print(f"pages_processed={result.pages_processed}")
    print(f"vacancies_found={result.vacancies_found}")
    print(f"vacancies_created={result.vacancies_created}")
    print(f"seen_events_created={result.seen_events_created}")
    print(f"saturated={'yes' if result.saturated else 'no'}")
    print(f"children_created={result.children_created_count}")
    print(f"children_total={result.children_total_count}")
    print(f"saturation_reason={result.saturation_reason or '-'}")
    print(f"error={result.error_message or '-'}")


def _print_run_list_engine_v2_summary(result: RunListEngineV2Result) -> None:
    print("completed list engine v2 run")
    print(f"status={result.status}")
    print(f"run_id={result.crawl_run_id}")
    print(f"partitions_attempted={result.partitions_attempted}")
    print(f"partitions_completed={result.partitions_completed}")
    print(f"partitions_failed={result.partitions_failed}")
    print(f"pages_attempted={result.pages_attempted}")
    print(f"pages_processed={result.pages_processed}")
    print(f"vacancies_found={result.vacancies_found}")
    print(f"vacancies_created={result.vacancies_created}")
    print(f"seen_events_created={result.seen_events_created}")
    print(f"saturated_partitions={result.saturated_partitions}")
    print(f"children_created_total={result.children_created_total}")
    print(f"remaining_pending_terminal_partitions={result.remaining_pending_terminal_count}")
    for partition_result in result.partition_results:
        print(
            "partition="
            f"{partition_result.partition_id} "
            f"final_status={partition_result.final_partition_status} "
            f"coverage_status={partition_result.final_coverage_status} "
            f"pages_processed={partition_result.pages_processed} "
            f"saturated={'yes' if partition_result.saturated else 'no'} "
            f"children_created={partition_result.children_created_count}"
        )


class _SessionlessCrawlRunRepository:
    def get(self, run_id: UUID):
        with session_scope() as session:
            return SqlAlchemyCrawlRunRepository(session).get(run_id)


class _SessionlessCrawlPartitionRepository:
    def list_pending_terminal_by_run_id(self, run_id: UUID, *, limit: int | None = None):
        with session_scope() as session:
            return SqlAlchemyCrawlPartitionRepository(session).list_pending_terminal_by_run_id(
                run_id,
                limit=limit,
            )
