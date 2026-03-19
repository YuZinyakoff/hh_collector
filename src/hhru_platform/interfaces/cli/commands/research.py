from __future__ import annotations

import argparse
import sys
from pathlib import Path
from uuid import UUID

from hhru_platform.application.commands.fetch_vacancy_detail import (
    fetch_vacancy_detail,
)
from hhru_platform.application.commands.study_detail_payloads import (
    StudyDetailPayloadsCommand,
    study_detail_payloads,
)
from hhru_platform.infrastructure.db.repositories.api_request_log_repo import (
    SqlAlchemyApiRequestLogRepository,
)
from hhru_platform.infrastructure.db.repositories.detail_fetch_attempt_repo import (
    SqlAlchemyDetailFetchAttemptRepository,
)
from hhru_platform.infrastructure.db.repositories.detail_payload_study_repo import (
    SqlAlchemyDetailPayloadStudyRepository,
)
from hhru_platform.infrastructure.db.repositories.raw_payload_repo import (
    SqlAlchemyRawApiPayloadRepository,
)
from hhru_platform.infrastructure.db.repositories.vacancy_current_state_repo import (
    SqlAlchemyVacancyCurrentStateRepository,
)
from hhru_platform.infrastructure.db.repositories.vacancy_repo import (
    SqlAlchemyVacancyRepository,
)
from hhru_platform.infrastructure.db.repositories.vacancy_snapshot_repo import (
    SqlAlchemyVacancySnapshotRepository,
)
from hhru_platform.infrastructure.db.session import session_scope
from hhru_platform.infrastructure.hh_api.client import HHApiClient


def register_research_commands(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    parser = subparsers.add_parser(
        "study-detail-payloads",
        help=(
            "Run a repeated detail-fetch study on vacancies selected from recent search crawl data."
        ),
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=5,
        help="How many vacancies to sample from the selected crawl_run. Defaults to 5.",
    )
    parser.add_argument(
        "--repeat-fetches",
        type=int,
        default=2,
        help=(
            "How many additional detail refetch rounds to run after the initial detail fetch. "
            "Defaults to 2."
        ),
    )
    parser.add_argument(
        "--pause-seconds",
        type=float,
        default=0.0,
        help="Optional pause between repeated detail rounds. Defaults to 0.",
    )
    parser.add_argument(
        "--crawl-run-id",
        type=UUID,
        help="Optional crawl_run to sample from. Defaults to the latest run with search payloads.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path(".state/reports/detail-payload-study"),
        help="Directory where report.json and summary.md will be written.",
    )
    parser.set_defaults(handler=handle_study_detail_payloads)


def handle_study_detail_payloads(args: argparse.Namespace) -> int:
    command = StudyDetailPayloadsCommand(
        sample_size=int(args.sample_size),
        repeat_fetches=int(args.repeat_fetches),
        pause_seconds=float(args.pause_seconds),
        crawl_run_id=args.crawl_run_id,
        output_dir=Path(args.output_dir),
    )

    try:
        result = study_detail_payloads(
            command,
            resolve_latest_crawl_run_id_step=_resolve_latest_crawl_run_id_with_search_payloads,
            load_candidates_step=_load_candidates,
            load_raw_payload_step=_load_raw_payload,
            fetch_detail_step=_fetch_detail,
        )
    except Exception as error:
        print(str(error), file=sys.stderr)
        return 1

    print("completed detail payload study")
    print(f"crawl_run_id={result.crawl_run_id}")
    print(f"sample_size_requested={result.sample_size_requested}")
    print(f"sample_size_selected={result.sample_size_selected}")
    print(f"vacancies_with_search_sample={result.vacancies_with_search_sample}")
    print(f"vacancies_with_successful_detail={result.vacancies_with_successful_detail}")
    print(f"raw_comparable_pairs={result.raw_comparable_pairs}")
    print(f"raw_changed_pairs={result.raw_changed_pairs}")
    print(f"normalized_comparable_pairs={result.normalized_comparable_pairs}")
    print(f"normalized_changed_pairs={result.normalized_changed_pairs}")
    print(
        "detail_only_research_fields="
        f"{','.join(result.detail_only_research_fields) or '-'}"
    )
    print(f"recommended_policy={result.recommendation}")
    print(f"report_directory={result.report_directory}")
    print(f"report_json={result.report_json_path}")
    print(f"summary_markdown={result.summary_markdown_path}")
    return 0


def _resolve_latest_crawl_run_id_with_search_payloads() -> UUID | None:
    with session_scope() as session:
        repository = SqlAlchemyDetailPayloadStudyRepository(session)
        return repository.get_latest_crawl_run_id_with_search_payloads()


def _load_candidates(crawl_run_id: UUID, sample_size: int):
    with session_scope() as session:
        repository = SqlAlchemyDetailPayloadStudyRepository(session)
        return repository.list_recent_candidates(
            crawl_run_id=crawl_run_id,
            limit=sample_size,
        )


def _load_raw_payload(payload_id: int):
    with session_scope() as session:
        repository = SqlAlchemyDetailPayloadStudyRepository(session)
        return repository.get_raw_payload(payload_id)


def _fetch_detail(command):
    with session_scope() as session:
        return fetch_vacancy_detail(
            command,
            vacancy_repository=SqlAlchemyVacancyRepository(session),
            api_client=HHApiClient.from_settings(),
            detail_fetch_attempt_repository=SqlAlchemyDetailFetchAttemptRepository(session),
            api_request_log_repository=SqlAlchemyApiRequestLogRepository(session),
            raw_api_payload_repository=SqlAlchemyRawApiPayloadRepository(session),
            vacancy_snapshot_repository=SqlAlchemyVacancySnapshotRepository(session),
            vacancy_current_state_repository=SqlAlchemyVacancyCurrentStateRepository(session),
        )
