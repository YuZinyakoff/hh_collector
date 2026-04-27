from __future__ import annotations

import argparse
import logging
import time

from hhru_platform.application.commands.drain_first_detail_backlog import (
    DrainFirstDetailBacklogCommand,
    DrainFirstDetailBacklogResult,
    drain_first_detail_backlog,
)
from hhru_platform.application.commands.fetch_vacancy_detail import (
    FetchVacancyDetailCommand,
    FetchVacancyDetailResult,
    fetch_vacancy_detail,
)
from hhru_platform.config.logging import configure_logging
from hhru_platform.config.settings import get_settings
from hhru_platform.infrastructure.db.repositories import (
    SqlAlchemyApiRequestLogRepository,
    SqlAlchemyDetailFetchAttemptRepository,
    SqlAlchemyRawApiPayloadRepository,
    SqlAlchemyVacancyCurrentStateRepository,
    SqlAlchemyVacancyRepository,
    SqlAlchemyVacancySnapshotRepository,
)
from hhru_platform.infrastructure.db.session import session_scope
from hhru_platform.infrastructure.hh_api.client import HHApiClient
from hhru_platform.infrastructure.observability.metrics import get_metrics_registry

LOGGER = logging.getLogger(__name__)


def main() -> int:
    configure_logging()
    settings = get_settings()
    parser = _build_parser(
        default_batch_size=settings.detail_worker_batch_size,
        default_interval_seconds=settings.detail_worker_interval_seconds,
        default_include_inactive=settings.detail_worker_include_inactive,
        default_triggered_by=settings.detail_worker_triggered_by,
        default_retry_cooldown_seconds=settings.detail_worker_retry_cooldown_seconds,
        default_max_retry_cooldown_seconds=settings.detail_worker_max_retry_cooldown_seconds,
    )
    args = parser.parse_args()
    return _run_loop(args)


def _build_parser(
    *,
    default_batch_size: int,
    default_interval_seconds: float,
    default_include_inactive: bool,
    default_triggered_by: str,
    default_retry_cooldown_seconds: int,
    default_max_retry_cooldown_seconds: int,
) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="hhru-detail-worker",
        description="Drain the persistent first-detail backlog in bounded batches.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=default_batch_size,
        help=f"Maximum vacancies to fetch per tick. Defaults to {default_batch_size}.",
    )
    parser.add_argument(
        "--interval-seconds",
        type=float,
        default=default_interval_seconds,
        help=(
            "Seconds between drain ticks. "
            f"Defaults to {default_interval_seconds}."
        ),
    )
    parser.add_argument(
        "--max-ticks",
        type=int,
        default=None,
        help="Optional number of ticks before exit.",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run exactly one drain tick and exit.",
    )
    parser.add_argument(
        "--include-inactive",
        choices=("yes", "no"),
        default="yes" if default_include_inactive else "no",
        help="Whether to include probably inactive vacancies. Defaults to no.",
    )
    parser.add_argument(
        "--triggered-by",
        default=default_triggered_by,
        help=f"Actor name recorded in logs. Defaults to {default_triggered_by}.",
    )
    parser.add_argument(
        "--retry-cooldown-seconds",
        type=int,
        default=default_retry_cooldown_seconds,
        help=(
            "Base cooldown after a retryable detail failure. "
            "Each repeated failed attempt doubles this value. "
            f"Defaults to {default_retry_cooldown_seconds}."
        ),
    )
    parser.add_argument(
        "--max-retry-cooldown-seconds",
        type=int,
        default=default_max_retry_cooldown_seconds,
        help=(
            "Maximum cooldown cap for repeated retryable detail failures. "
            f"Defaults to {default_max_retry_cooldown_seconds}."
        ),
    )
    return parser


def _run_loop(args: argparse.Namespace) -> int:
    batch_size = int(args.batch_size)
    interval_seconds = float(args.interval_seconds)
    max_ticks = 1 if bool(args.once) else args.max_ticks
    include_inactive = _parse_yes_no(str(args.include_inactive))
    triggered_by = str(args.triggered_by)
    retry_cooldown_seconds = int(args.retry_cooldown_seconds)
    max_retry_cooldown_seconds = int(args.max_retry_cooldown_seconds)
    api_client = HHApiClient.from_settings()

    tick = 0
    while max_ticks is None or tick < int(max_ticks):
        tick += 1
        result = _drain_once(
            batch_size=batch_size,
            include_inactive=include_inactive,
            triggered_by=triggered_by,
            retry_cooldown_seconds=retry_cooldown_seconds,
            max_retry_cooldown_seconds=max_retry_cooldown_seconds,
            api_client=api_client,
        )
        _print_tick_summary(tick=tick, result=result)

        if max_ticks is not None and tick >= int(max_ticks):
            break
        time.sleep(interval_seconds)

    return 0


def _drain_once(
    *,
    batch_size: int,
    include_inactive: bool,
    triggered_by: str,
    retry_cooldown_seconds: int,
    max_retry_cooldown_seconds: int,
    api_client: HHApiClient,
) -> DrainFirstDetailBacklogResult:
    with session_scope() as session:
        return drain_first_detail_backlog(
            DrainFirstDetailBacklogCommand(
                limit=batch_size,
                include_inactive=include_inactive,
                triggered_by=triggered_by,
                retry_cooldown_seconds=retry_cooldown_seconds,
                max_retry_cooldown_seconds=max_retry_cooldown_seconds,
            ),
            vacancy_current_state_repository=SqlAlchemyVacancyCurrentStateRepository(session),
            detail_fetch_attempt_repository=SqlAlchemyDetailFetchAttemptRepository(session),
            fetch_vacancy_detail_step=lambda command: _execute_fetch_detail_step(
                command,
                api_client=api_client,
            ),
            metrics_recorder=get_metrics_registry(),
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


def _print_tick_summary(*, tick: int, result: DrainFirstDetailBacklogResult) -> None:
    print(
        "detail_worker_tick "
        f"tick={tick} "
        f"status={result.status} "
        f"backlog_size_before={result.backlog_size_before} "
        f"ready_backlog_size_before={result.ready_backlog_size_before} "
        f"cooldown_skipped_before={result.cooldown_skipped_before} "
        f"selected_count={result.selected_count} "
        f"detail_fetch_succeeded={result.detail_fetch_succeeded} "
        f"detail_fetch_terminal={result.detail_fetch_terminal} "
        f"detail_fetch_failed={result.detail_fetch_failed} "
        f"backlog_size_after={result.backlog_size_after} "
        f"ready_backlog_size_after={result.ready_backlog_size_after} "
        f"cooldown_skipped_after={result.cooldown_skipped_after}",
        flush=True,
    )
    LOGGER.info(
        "detail worker tick completed",
        extra={
            "tick": tick,
            "status": result.status,
            "backlog_size_before": result.backlog_size_before,
            "ready_backlog_size_before": result.ready_backlog_size_before,
            "cooldown_skipped_before": result.cooldown_skipped_before,
            "selected_count": result.selected_count,
            "detail_fetch_succeeded": result.detail_fetch_succeeded,
            "detail_fetch_terminal": result.detail_fetch_terminal,
            "detail_fetch_failed": result.detail_fetch_failed,
            "backlog_size_after": result.backlog_size_after,
            "ready_backlog_size_after": result.ready_backlog_size_after,
            "cooldown_skipped_after": result.cooldown_skipped_after,
            "retry_cooldown_seconds": result.retry_cooldown_seconds,
            "max_retry_cooldown_seconds": result.max_retry_cooldown_seconds,
        },
    )


def _parse_yes_no(value: str) -> bool:
    normalized_value = value.strip().lower()
    if normalized_value == "yes":
        return True
    if normalized_value == "no":
        return False
    raise ValueError("value must be either yes or no")


if __name__ == "__main__":
    raise SystemExit(main())
