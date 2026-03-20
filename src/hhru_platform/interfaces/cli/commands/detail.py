from __future__ import annotations

import argparse
import sys
from uuid import UUID

from hhru_platform.application.commands.fetch_vacancy_detail import (
    FetchVacancyDetailCommand,
    VacancyNotFoundError,
    fetch_vacancy_detail,
)
from hhru_platform.application.commands.retry_failed_details import (
    RetryFailedDetailsCommand,
    RetryFailedDetailsResult,
    retry_failed_details,
)
from hhru_platform.infrastructure.db.repositories import (
    SqlAlchemyApiRequestLogRepository,
    SqlAlchemyCrawlRunRepository,
    SqlAlchemyDetailFetchAttemptRepository,
    SqlAlchemyRawApiPayloadRepository,
    SqlAlchemyVacancyCurrentStateRepository,
    SqlAlchemyVacancyRepository,
    SqlAlchemyVacancySnapshotRepository,
)
from hhru_platform.infrastructure.db.session import session_scope
from hhru_platform.infrastructure.hh_api.client import HHApiClient
from hhru_platform.infrastructure.observability.metrics import get_metrics_registry


def register_detail_commands(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    parser = subparsers.add_parser(
        "fetch-vacancy-detail",
        help="Fetch one vacancy detail card from the official hh API.",
    )
    parser.add_argument(
        "--vacancy-id",
        type=UUID,
        required=True,
        help="Existing internal vacancy identifier.",
    )
    parser.add_argument(
        "--reason",
        default="manual_refetch",
        help="Logical reason for the detail fetch. Defaults to manual_refetch.",
    )
    parser.set_defaults(handler=handle_fetch_vacancy_detail)

    retry_parser = subparsers.add_parser(
        "retry-failed-details",
        help="Retry the derived detail repair backlog for one crawl_run.",
    )
    retry_parser.add_argument(
        "--run-id",
        type=UUID,
        required=True,
        help="Existing crawl_run identifier with detail repair backlog.",
    )
    retry_parser.add_argument(
        "--triggered-by",
        default="retry-failed-details",
        help=(
            "Actor or subsystem that initiated the detail repair flow. "
            "Defaults to retry-failed-details."
        ),
    )
    retry_parser.set_defaults(handler=handle_retry_failed_details)


def handle_fetch_vacancy_detail(args: argparse.Namespace) -> int:
    command = FetchVacancyDetailCommand(
        vacancy_id=args.vacancy_id,
        reason=str(args.reason),
    )

    try:
        with session_scope() as session:
            result = fetch_vacancy_detail(
                command,
                vacancy_repository=SqlAlchemyVacancyRepository(session),
                api_client=HHApiClient.from_settings(),
                detail_fetch_attempt_repository=SqlAlchemyDetailFetchAttemptRepository(session),
                api_request_log_repository=SqlAlchemyApiRequestLogRepository(session),
                raw_api_payload_repository=SqlAlchemyRawApiPayloadRepository(session),
                vacancy_snapshot_repository=SqlAlchemyVacancySnapshotRepository(session),
                vacancy_current_state_repository=SqlAlchemyVacancyCurrentStateRepository(session),
            )
    except VacancyNotFoundError as error:
        print(str(error), file=sys.stderr)
        return 1

    print("fetched vacancy detail")
    print(f"vacancy_id={result.vacancy_id}")
    print(f"hh_vacancy_id={result.hh_vacancy_id}")
    print(f"detail_fetch_status={result.detail_fetch_status}")
    print(f"snapshot_id={result.snapshot_id}")
    print(f"request_log_id={result.request_log_id}")
    print(f"raw_payload_id={result.raw_payload_id}")
    print(f"detail_fetch_attempt_id={result.detail_fetch_attempt_id}")

    if result.error_message is not None:
        print(f"error={result.error_message}", file=sys.stderr)
        return 1

    return 0


def handle_retry_failed_details(args: argparse.Namespace) -> int:
    command = RetryFailedDetailsCommand(
        crawl_run_id=args.run_id,
        triggered_by=str(args.triggered_by),
    )
    api_client = HHApiClient.from_settings()

    try:
        with session_scope() as session:
            result = retry_failed_details(
                command,
                crawl_run_repository=SqlAlchemyCrawlRunRepository(session),
                detail_fetch_attempt_repository=SqlAlchemyDetailFetchAttemptRepository(session),
                fetch_vacancy_detail_step=lambda step_command: _execute_fetch_detail_step(
                    step_command,
                    api_client=api_client,
                ),
                metrics_recorder=get_metrics_registry(),
            )
    except (LookupError, VacancyNotFoundError, ValueError) as error:
        print(str(error), file=sys.stderr)
        return 1

    _print_retry_failed_details_summary(result)
    return 0 if result.status == "succeeded" else 1


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


def _print_retry_failed_details_summary(result: RetryFailedDetailsResult) -> None:
    if result.status == "succeeded":
        print("completed retry-failed-details")
    else:
        print("completed retry-failed-details with remaining backlog")
    print(f"status={result.status}")
    print(f"run_id={result.run_id}")
    print(f"run_type={result.run_type}")
    print(f"triggered_by={result.triggered_by}")
    print(f"run_status_before={result.run_status_before}")
    print(f"run_status_after={result.run_status_after}")
    print(f"backlog_size={result.backlog_size}")
    print(f"retried_count={result.retried_count}")
    print(f"repaired_count={result.repaired_count}")
    print(f"still_failing_count={result.still_failing_count}")
    print(f"remaining_backlog_count={result.remaining_backlog_count}")
    print(f"error={result.error_message or '-'}")
    for detail_result in result.detail_results:
        print(
            "vacancy="
            f"{detail_result.vacancy_id} "
            f"detail_fetch_status={detail_result.detail_fetch_status} "
            f"attempt_id={detail_result.detail_fetch_attempt_id} "
            f"error={detail_result.error_message or '-'}"
        )
