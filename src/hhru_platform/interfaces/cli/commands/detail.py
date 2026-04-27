from __future__ import annotations

import argparse
import sys
from uuid import UUID

from hhru_platform.application.commands.backfill_vacancy_snapshots import (
    BackfillVacancySnapshotsCommand,
    backfill_vacancy_snapshots,
)
from hhru_platform.application.commands.drain_first_detail_backlog import (
    DRAIN_FIRST_DETAIL_BACKLOG_STATUS_SUCCEEDED,
    DrainFirstDetailBacklogCommand,
    DrainFirstDetailBacklogResult,
    drain_first_detail_backlog,
)
from hhru_platform.application.commands.fetch_vacancy_detail import (
    FetchVacancyDetailCommand,
    FetchVacancyDetailResult,
    VacancyNotFoundError,
    fetch_vacancy_detail,
)
from hhru_platform.application.commands.retry_failed_details import (
    RetryFailedDetailsCommand,
    RetryFailedDetailsResult,
    retry_failed_details,
)
from hhru_platform.config.settings import get_settings
from hhru_platform.infrastructure.db.repositories import (
    SqlAlchemyApiRequestLogRepository,
    SqlAlchemyCrawlRunRepository,
    SqlAlchemyDetailFetchAttemptRepository,
    SqlAlchemyRawApiPayloadRepository,
    SqlAlchemyVacancyCurrentStateRepository,
    SqlAlchemyVacancyRepository,
    SqlAlchemyVacancySnapshotBackfillRepository,
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

    drain_parser = subparsers.add_parser(
        "drain-first-detail-backlog",
        help="Fetch details for vacancies that still have no successful detail payload.",
    )
    drain_parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Maximum number of backlog vacancies to fetch. Defaults to 100.",
    )
    drain_parser.add_argument(
        "--include-inactive",
        choices=("yes", "no"),
        default="no",
        help="Whether to include probably inactive vacancies. Defaults to no.",
    )
    drain_parser.add_argument(
        "--triggered-by",
        default="drain-first-detail-backlog",
        help="Actor or subsystem that initiated the backlog drain.",
    )
    settings = get_settings()
    retry_cooldown_seconds = settings.detail_worker_retry_cooldown_seconds
    max_retry_cooldown_seconds = settings.detail_worker_max_retry_cooldown_seconds
    drain_parser.add_argument(
        "--retry-cooldown-seconds",
        type=int,
        default=retry_cooldown_seconds,
        help=(
            "Base cooldown after a retryable detail failure. "
            "Each repeated failed attempt doubles this value. "
            f"Defaults to {retry_cooldown_seconds}."
        ),
    )
    drain_parser.add_argument(
        "--max-retry-cooldown-seconds",
        type=int,
        default=max_retry_cooldown_seconds,
        help=(
            "Maximum cooldown cap for repeated retryable detail failures. "
            f"Defaults to {max_retry_cooldown_seconds}."
        ),
    )
    drain_parser.set_defaults(handler=handle_drain_first_detail_backlog)

    backfill_parser = subparsers.add_parser(
        "backfill-vacancy-snapshots",
        help="Backfill lossless vacancy snapshots from retained raw payloads.",
    )
    backfill_parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="How many rows to process per repository batch. Defaults to 500.",
    )
    backfill_parser.add_argument(
        "--max-batches",
        type=int,
        default=None,
        help="Optional limit on repository batches per phase.",
    )
    backfill_parser.add_argument(
        "--triggered-by",
        default="backfill-vacancy-snapshots",
        help="Actor or subsystem that initiated the backfill flow.",
    )
    backfill_parser.set_defaults(handler=handle_backfill_vacancy_snapshots)


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


def handle_drain_first_detail_backlog(args: argparse.Namespace) -> int:
    command = DrainFirstDetailBacklogCommand(
        limit=int(args.limit),
        include_inactive=_parse_yes_no(str(args.include_inactive)),
        triggered_by=str(args.triggered_by),
        retry_cooldown_seconds=int(args.retry_cooldown_seconds),
        max_retry_cooldown_seconds=int(args.max_retry_cooldown_seconds),
    )
    api_client = HHApiClient.from_settings()

    try:
        with session_scope() as session:
            result = drain_first_detail_backlog(
                command,
                vacancy_current_state_repository=SqlAlchemyVacancyCurrentStateRepository(
                    session
                ),
                detail_fetch_attempt_repository=SqlAlchemyDetailFetchAttemptRepository(session),
                fetch_vacancy_detail_step=lambda step_command: _execute_fetch_detail_step(
                    step_command,
                    api_client=api_client,
                ),
                metrics_recorder=get_metrics_registry(),
            )
    except ValueError as error:
        print(str(error), file=sys.stderr)
        return 1

    _print_drain_first_detail_backlog_summary(result)
    return 0 if result.status == DRAIN_FIRST_DETAIL_BACKLOG_STATUS_SUCCEEDED else 1


def handle_backfill_vacancy_snapshots(args: argparse.Namespace) -> int:
    command = BackfillVacancySnapshotsCommand(
        batch_size=int(args.batch_size),
        max_batches=args.max_batches,
        triggered_by=str(args.triggered_by),
    )
    try:
        with session_scope() as session:
            result = backfill_vacancy_snapshots(
                command,
                repository=SqlAlchemyVacancySnapshotBackfillRepository(session),
            )
    except ValueError as error:
        print(str(error), file=sys.stderr)
        return 1

    print("completed vacancy snapshot backfill")
    print(f"status={result.status}")
    print(f"triggered_by={result.triggered_by}")
    print(f"detail_candidates_seen={result.detail_candidates_seen}")
    print(f"detail_snapshots_updated={result.detail_snapshots_updated}")
    print(f"short_candidates_seen={result.short_candidates_seen}")
    print(f"short_snapshots_created={result.short_snapshots_created}")
    print(f"skipped_missing_raw_payload={result.skipped_missing_raw_payload}")
    print(f"skipped_missing_search_item={result.skipped_missing_search_item}")
    print(f"batches_processed={result.batches_processed}")
    return 0


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


def _print_drain_first_detail_backlog_summary(
    result: DrainFirstDetailBacklogResult,
) -> None:
    if result.status == DRAIN_FIRST_DETAIL_BACKLOG_STATUS_SUCCEEDED:
        print("completed drain-first-detail-backlog")
    else:
        print("completed drain-first-detail-backlog with failures")
    print(f"status={result.status}")
    print(f"triggered_by={result.triggered_by}")
    print(f"include_inactive={'yes' if result.include_inactive else 'no'}")
    print(f"limit={result.limit}")
    print(f"retry_cooldown_seconds={result.retry_cooldown_seconds}")
    print(f"max_retry_cooldown_seconds={result.max_retry_cooldown_seconds}")
    print(f"backlog_size_before={result.backlog_size_before}")
    print(f"ready_backlog_size_before={result.ready_backlog_size_before}")
    print(f"cooldown_skipped_before={result.cooldown_skipped_before}")
    print(f"selected_count={result.selected_count}")
    print(f"detail_fetch_attempted={result.detail_fetch_attempted}")
    print(f"detail_fetch_succeeded={result.detail_fetch_succeeded}")
    print(f"detail_fetch_terminal={result.detail_fetch_terminal}")
    print(f"detail_fetch_failed={result.detail_fetch_failed}")
    print(f"backlog_size_after={result.backlog_size_after}")
    print(f"ready_backlog_size_after={result.ready_backlog_size_after}")
    print(f"cooldown_skipped_after={result.cooldown_skipped_after}")
    for item in result.item_results:
        print(
            "vacancy="
            f"{item.vacancy_id} "
            f"detail_fetch_status={item.detail_fetch_status} "
            f"attempt_id={item.detail_fetch_attempt_id or '-'} "
            f"error={item.error_message or '-'}"
        )


def _parse_yes_no(value: str) -> bool:
    normalized_value = value.strip().lower()
    if normalized_value == "yes":
        return True
    if normalized_value == "no":
        return False
    raise ValueError("value must be either yes or no")
