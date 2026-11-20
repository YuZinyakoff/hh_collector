from __future__ import annotations

from typing import Never
from unittest.mock import Mock
from uuid import uuid4

import pytest

from hhru_platform.application.commands.run_collection_once_v2 import (
    RunCollectionOnceV2Command,
    run_collection_once_v2,
)
from hhru_platform.application.commands.sync_dictionary import (
    SyncDictionaryCommand,
    SyncDictionaryResult,
)


def _sync_result(name: str, *, failed: bool = False) -> SyncDictionaryResult:
    return SyncDictionaryResult(
        dictionary_name=name,
        sync_run_id=uuid4(),
        status="failed" if failed else "succeeded",
        created_count=0,
        updated_count=0,
        deactivated_count=0,
        source_status_code=503 if failed else 200,
        request_log_id=1,
        raw_payload_id=1,
        error_message="upstream unavailable" if failed else None,
    )


@pytest.mark.parametrize("failing_name", ["areas", "professional_roles"])
def test_failed_dictionary_sync_prevents_run_creation_and_planning(
    failing_name: str,
) -> None:
    calls: list[str] = []

    def sync_step(command: SyncDictionaryCommand) -> SyncDictionaryResult:
        calls.append(command.dictionary_name)
        return _sync_result(command.dictionary_name, failed=command.dictionary_name == failing_name)

    unexpected = Mock(side_effect=AssertionError("collection must not start after failed sync"))
    result = run_collection_once_v2(
        RunCollectionOnceV2Command(sync_dictionaries=True, detail_limit=0),
        sync_dictionary_step=sync_step,
        create_crawl_run_step=unexpected,
        plan_run_v2_step=unexpected,
        run_list_engine_v2_step=unexpected,
        report_run_coverage_step=unexpected,
        select_detail_candidates_step=unexpected,
        fetch_vacancy_detail_step=unexpected,
        reconcile_run_step=unexpected,
        finalize_crawl_run_step=unexpected,
    )

    assert result.status == "failed"
    assert result.failed_step == "sync_dictionaries"
    assert result.run_id is None
    assert result.completed_steps == ()
    assert calls == (["areas"] if failing_name == "areas" else ["areas", "professional_roles"])
    unexpected.assert_not_called()


def test_both_dictionaries_are_synced_before_creating_run() -> None:
    calls: list[str] = []

    def sync_step(command: SyncDictionaryCommand) -> SyncDictionaryResult:
        calls.append(command.dictionary_name)
        return _sync_result(command.dictionary_name)

    def stop_at_create(command: object) -> Never:
        calls.append("create")
        raise RuntimeError("test stops after dictionary sync")

    unexpected = Mock(side_effect=AssertionError("test must stop before planning"))
    result = run_collection_once_v2(
        RunCollectionOnceV2Command(sync_dictionaries=True, detail_limit=0),
        sync_dictionary_step=sync_step,
        create_crawl_run_step=stop_at_create,
        plan_run_v2_step=unexpected,
        run_list_engine_v2_step=unexpected,
        report_run_coverage_step=unexpected,
        select_detail_candidates_step=unexpected,
        fetch_vacancy_detail_step=unexpected,
        reconcile_run_step=unexpected,
        finalize_crawl_run_step=unexpected,
    )

    assert calls == ["areas", "professional_roles", "create"]
    assert result.status == "failed"
    assert result.failed_step == "create_crawl_run"
    assert result.completed_steps == ("sync_dictionaries",)
    unexpected.assert_not_called()


def test_dictionary_sync_exception_does_not_start_collection() -> None:
    sync_step = Mock(side_effect=RuntimeError("dictionary sync failed unexpectedly"))
    unexpected = Mock(side_effect=AssertionError("collection must not start after failed sync"))

    with pytest.raises(RuntimeError, match="dictionary sync failed unexpectedly"):
        run_collection_once_v2(
            RunCollectionOnceV2Command(sync_dictionaries=True, detail_limit=0),
            sync_dictionary_step=sync_step,
            create_crawl_run_step=unexpected,
            plan_run_v2_step=unexpected,
            run_list_engine_v2_step=unexpected,
            report_run_coverage_step=unexpected,
            select_detail_candidates_step=unexpected,
            fetch_vacancy_detail_step=unexpected,
            reconcile_run_step=unexpected,
            finalize_crawl_run_step=unexpected,
        )

    sync_step.assert_called_once()
    unexpected.assert_not_called()
