from __future__ import annotations

import argparse
import sys
from uuid import UUID

from hhru_platform.application.commands.process_list_page import (
    CrawlPartitionNotFoundError,
    ProcessListPageCommand,
    process_list_page,
)
from hhru_platform.infrastructure.db.repositories import (
    SqlAlchemyApiRequestLogRepository,
    SqlAlchemyCrawlPartitionRepository,
    SqlAlchemyRawApiPayloadRepository,
    SqlAlchemyVacancyCurrentStateRepository,
    SqlAlchemyVacancyRepository,
    SqlAlchemyVacancySeenEventRepository,
)
from hhru_platform.infrastructure.db.session import session_scope
from hhru_platform.infrastructure.hh_api.client import HHApiClient


def register_list_page_commands(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    parser = subparsers.add_parser(
        "process-list-page",
        help="Process one hh vacancies search page for a crawl partition.",
    )
    parser.add_argument(
        "--partition-id",
        type=UUID,
        required=True,
        help="Existing crawl_partition identifier.",
    )
    parser.add_argument(
        "--page",
        type=int,
        default=None,
        help="Optional search page override. Defaults to the partition or page 0.",
    )
    parser.set_defaults(handler=handle_process_list_page)


def handle_process_list_page(args: argparse.Namespace) -> int:
    command = ProcessListPageCommand(partition_id=args.partition_id, page=args.page)

    try:
        with session_scope() as session:
            result = process_list_page(
                command,
                crawl_partition_repository=SqlAlchemyCrawlPartitionRepository(session),
                api_client=HHApiClient.from_settings(),
                api_request_log_repository=SqlAlchemyApiRequestLogRepository(session),
                raw_api_payload_repository=SqlAlchemyRawApiPayloadRepository(session),
                vacancy_repository=SqlAlchemyVacancyRepository(session),
                vacancy_seen_event_repository=SqlAlchemyVacancySeenEventRepository(session),
                vacancy_current_state_repository=SqlAlchemyVacancyCurrentStateRepository(session),
            )
    except CrawlPartitionNotFoundError as error:
        print(str(error), file=sys.stderr)
        return 1

    print("processed list page")
    print(f"partition_id={result.partition_id}")
    print(f"status={result.partition_status}")
    print(f"page={result.page}")
    print(f"pages_total_expected={result.pages_total_expected}")
    print(f"vacancies_processed={result.vacancies_processed}")
    print(f"vacancies_created={result.vacancies_created}")
    print(f"seen_events_created={result.seen_events_created}")
    print(f"request_log_id={result.request_log_id}")
    print(f"raw_payload_id={result.raw_payload_id}")
    for vacancy in result.processed_vacancies:
        print(f"vacancy={vacancy.id} hh_vacancy_id={vacancy.hh_vacancy_id}")

    if result.error_message is not None:
        print(f"error={result.error_message}", file=sys.stderr)
        return 1

    return 0
