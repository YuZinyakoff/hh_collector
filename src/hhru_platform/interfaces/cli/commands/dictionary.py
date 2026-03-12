from __future__ import annotations

import argparse
import sys

from hhru_platform.application.commands.sync_dictionary import (
    SyncDictionaryCommand,
    sync_dictionary,
)
from hhru_platform.application.dto import SUPPORTED_DICTIONARY_NAMES
from hhru_platform.infrastructure.db.repositories import (
    SqlAlchemyApiRequestLogRepository,
    SqlAlchemyAreaRepository,
    SqlAlchemyDictionaryStore,
    SqlAlchemyDictionarySyncRunRepository,
    SqlAlchemyProfessionalRoleRepository,
    SqlAlchemyRawApiPayloadRepository,
)
from hhru_platform.infrastructure.db.session import session_scope
from hhru_platform.infrastructure.hh_api.client import HHApiClient


def register_dictionary_commands(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    parser = subparsers.add_parser(
        "sync-dictionaries",
        help="Sync reference dictionaries from the official hh API.",
    )
    parser.add_argument(
        "--name",
        choices=(*SUPPORTED_DICTIONARY_NAMES, "all"),
        default="all",
        help="Dictionary to sync. Defaults to all supported dictionaries.",
    )
    parser.set_defaults(handler=handle_sync_dictionaries)


def handle_sync_dictionaries(args: argparse.Namespace) -> int:
    dictionary_names = (
        SUPPORTED_DICTIONARY_NAMES if args.name == "all" else (str(args.name),)
    )
    results = []

    with session_scope() as session:
        sync_run_repository = SqlAlchemyDictionarySyncRunRepository(session)
        api_request_log_repository = SqlAlchemyApiRequestLogRepository(session)
        raw_api_payload_repository = SqlAlchemyRawApiPayloadRepository(session)
        dictionary_store = SqlAlchemyDictionaryStore(
            area_repository=SqlAlchemyAreaRepository(session),
            professional_role_repository=SqlAlchemyProfessionalRoleRepository(session),
        )
        api_client = HHApiClient()

        for dictionary_name in dictionary_names:
            results.append(
                sync_dictionary(
                    SyncDictionaryCommand(dictionary_name=dictionary_name),
                    api_client=api_client,
                    sync_run_repository=sync_run_repository,
                    api_request_log_repository=api_request_log_repository,
                    raw_api_payload_repository=raw_api_payload_repository,
                    dictionary_store=dictionary_store,
                )
            )

    has_failures = False
    for result in results:
        print("dictionary sync result")
        print(f"name={result.dictionary_name}")
        print(f"sync_run_id={result.sync_run_id}")
        print(f"status={result.status}")
        print(f"source_status_code={result.source_status_code}")
        print(f"created={result.created_count}")
        print(f"updated={result.updated_count}")
        print(f"deactivated={result.deactivated_count}")
        print(f"request_log_id={result.request_log_id}")
        print(f"raw_payload_id={result.raw_payload_id}")
        if result.error_message is not None:
            print(f"error={result.error_message}", file=sys.stderr)
            has_failures = True

    return 1 if has_failures else 0
