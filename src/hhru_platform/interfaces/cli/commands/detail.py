from __future__ import annotations

import argparse
import sys
from uuid import UUID

from hhru_platform.application.commands.fetch_vacancy_detail import (
    FetchVacancyDetailCommand,
    VacancyNotFoundError,
    fetch_vacancy_detail,
)
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
