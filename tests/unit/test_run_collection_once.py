from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from hhru_platform.application.commands.fetch_vacancy_detail import (
    FetchVacancyDetailResult,
)
from hhru_platform.application.commands.plan_sweep import PlanRunResult
from hhru_platform.application.commands.process_list_page import ProcessListPageResult
from hhru_platform.application.commands.reconcile_run import ReconcileRunResult
from hhru_platform.application.commands.run_collection_once import (
    RunCollectionOnceCommand,
    run_collection_once,
)
from hhru_platform.application.commands.sync_dictionary import SyncDictionaryResult
from hhru_platform.application.dto import StoredVacancyReference
from hhru_platform.domain.entities.crawl_partition import CrawlPartition
from hhru_platform.domain.entities.crawl_run import CrawlRun


def test_run_collection_once_sequences_existing_slices_and_builds_summary() -> None:
    run_id = uuid4()
    partition_id = uuid4()
    first_dictionary_sync_id = uuid4()
    second_dictionary_sync_id = uuid4()
    vacancy_one = StoredVacancyReference(
        id=uuid4(),
        hh_vacancy_id="hh-1",
        name_current="Vacancy One",
    )
    vacancy_two = StoredVacancyReference(
        id=uuid4(),
        hh_vacancy_id="hh-2",
        name_current="Vacancy Two",
    )
    vacancy_three = StoredVacancyReference(
        id=uuid4(),
        hh_vacancy_id="hh-3",
        name_current="Vacancy Three",
    )
    events: list[tuple[object, ...]] = []

    def sync_dictionary_step(command):
        events.append(("sync", command.dictionary_name))
        sync_run_id = (
            first_dictionary_sync_id
            if command.dictionary_name == "areas"
            else second_dictionary_sync_id
        )
        return SyncDictionaryResult(
            dictionary_name=command.dictionary_name,
            sync_run_id=sync_run_id,
            status="succeeded",
            created_count=1,
            updated_count=0,
            deactivated_count=0,
            source_status_code=200,
            request_log_id=1,
            raw_payload_id=1,
            error_message=None,
        )

    def create_crawl_run_step(command):
        events.append(("create", command.run_type, command.triggered_by))
        return CrawlRun(
            id=run_id,
            run_type=command.run_type,
            status="created",
            started_at=datetime(2026, 3, 13, 12, 0, tzinfo=UTC),
            finished_at=None,
            triggered_by=command.triggered_by,
            config_snapshot_json={},
            partitions_total=0,
            partitions_done=0,
            partitions_failed=0,
            notes=None,
        )

    def plan_run_step(command):
        events.append(("plan", command.crawl_run_id))
        partition = CrawlPartition(
            id=partition_id,
            crawl_run_id=command.crawl_run_id,
            partition_key="global-default",
            params_json={"scope": "global"},
            status="pending",
            pages_total_expected=None,
            pages_processed=0,
            items_seen=0,
            retry_count=0,
            started_at=None,
            finished_at=None,
            last_error_message=None,
            created_at=datetime(2026, 3, 13, 12, 1, tzinfo=UTC),
        )
        return PlanRunResult(
            crawl_run_id=command.crawl_run_id,
            created_partitions=[partition],
            partitions=[partition],
        )

    def process_list_page_step(command):
        events.append(("page", command.partition_id, command.page))
        if command.page == 0:
            return ProcessListPageResult(
                partition_id=command.partition_id,
                partition_status="done",
                page=0,
                pages_total_expected=3,
                vacancies_processed=2,
                vacancies_created=2,
                seen_events_created=2,
                request_log_id=10,
                raw_payload_id=20,
                processed_vacancies=[vacancy_one, vacancy_two],
                error_message=None,
            )
        if command.page == 1:
            return ProcessListPageResult(
                partition_id=command.partition_id,
                partition_status="done",
                page=1,
                pages_total_expected=3,
                vacancies_processed=2,
                vacancies_created=1,
                seen_events_created=2,
                request_log_id=11,
                raw_payload_id=21,
                processed_vacancies=[vacancy_two, vacancy_three],
                error_message=None,
            )
        raise AssertionError(f"unexpected page {command.page}")

    def fetch_vacancy_detail_step(command):
        events.append(("detail", command.vacancy_id, command.crawl_run_id, command.reason))
        return FetchVacancyDetailResult(
            vacancy_id=command.vacancy_id,
            hh_vacancy_id=f"detail-{command.vacancy_id}",
            detail_fetch_status="succeeded",
            snapshot_id=100,
            request_log_id=200,
            raw_payload_id=300,
            detail_fetch_attempt_id=400,
            error_message=None,
        )

    def reconcile_run_step(command):
        events.append(("reconcile", command.crawl_run_id))
        return ReconcileRunResult(
            crawl_run_id=command.crawl_run_id,
            observed_in_run_count=3,
            missing_updated_count=0,
            marked_inactive_count=0,
            run_status="completed",
        )

    result = run_collection_once(
        RunCollectionOnceCommand(
            sync_dictionaries=True,
            pages_per_partition=2,
            detail_limit=2,
            run_type="weekly_sweep",
            triggered_by="cli",
        ),
        sync_dictionary_step=sync_dictionary_step,
        create_crawl_run_step=create_crawl_run_step,
        plan_run_step=plan_run_step,
        process_list_page_step=process_list_page_step,
        fetch_vacancy_detail_step=fetch_vacancy_detail_step,
        reconcile_run_step=reconcile_run_step,
    )

    assert events == [
        ("sync", "areas"),
        ("sync", "professional_roles"),
        ("create", "weekly_sweep", "cli"),
        ("plan", run_id),
        ("page", partition_id, 0),
        ("page", partition_id, 1),
        ("detail", vacancy_one.id, run_id, "run_once"),
        ("detail", vacancy_two.id, run_id, "run_once"),
        ("reconcile", run_id),
    ]
    assert result.status == "succeeded"
    assert result.run_id == run_id
    assert result.partitions_planned == 1
    assert result.partitions_attempted == 1
    assert result.partitions_processed == 1
    assert result.partitions_failed == 0
    assert result.list_pages_attempted == 2
    assert result.list_pages_processed == 2
    assert result.list_pages_failed == 0
    assert result.vacancies_found == 3
    assert result.detail_fetch_attempted == 2
    assert result.detail_fetch_succeeded == 2
    assert result.detail_fetch_failed == 0
    assert result.reconciliation_status == "completed"
    assert result.failed_step is None
    assert result.error_message is None
    assert result.completed_steps == (
        "sync_dictionaries",
        "create_crawl_run",
        "plan_sweep",
        "process_list_page",
        "fetch_vacancy_detail",
        "reconcile_run",
    )
    assert result.skipped_steps == ()


def test_run_collection_once_fails_fast_when_process_list_page_returns_failed_result() -> None:
    run_id = uuid4()
    partition_id = uuid4()
    events: list[tuple[object, ...]] = []

    def sync_dictionary_step(command):
        raise AssertionError(f"unexpected dictionary sync {command.dictionary_name}")

    def create_crawl_run_step(command):
        events.append(("create", command.run_type, command.triggered_by))
        return CrawlRun(
            id=run_id,
            run_type=command.run_type,
            status="created",
            started_at=datetime(2026, 3, 13, 12, 0, tzinfo=UTC),
            finished_at=None,
            triggered_by=command.triggered_by,
            config_snapshot_json={},
            partitions_total=0,
            partitions_done=0,
            partitions_failed=0,
            notes=None,
        )

    def plan_run_step(command):
        events.append(("plan", command.crawl_run_id))
        partition = CrawlPartition(
            id=partition_id,
            crawl_run_id=command.crawl_run_id,
            partition_key="global-default",
            params_json={"scope": "global"},
            status="pending",
            pages_total_expected=None,
            pages_processed=0,
            items_seen=0,
            retry_count=0,
            started_at=None,
            finished_at=None,
            last_error_message=None,
            created_at=datetime(2026, 3, 13, 12, 1, tzinfo=UTC),
        )
        return PlanRunResult(
            crawl_run_id=command.crawl_run_id,
            created_partitions=[partition],
            partitions=[partition],
        )

    def process_list_page_step(command):
        events.append(("page", command.partition_id, command.page))
        return ProcessListPageResult(
            partition_id=command.partition_id,
            partition_status="failed",
            page=0,
            pages_total_expected=None,
            vacancies_processed=0,
            vacancies_created=0,
            seen_events_created=0,
            request_log_id=None,
            raw_payload_id=None,
            processed_vacancies=[],
            error_message="Invalid HH API User-Agent for live vacancy search",
        )

    def fetch_vacancy_detail_step(command):
        raise AssertionError(f"unexpected detail fetch {command.vacancy_id}")

    def reconcile_run_step(command):
        raise AssertionError(f"unexpected reconcile {command.crawl_run_id}")

    result = run_collection_once(
        RunCollectionOnceCommand(
            sync_dictionaries=False,
            pages_per_partition=1,
            detail_limit=3,
            run_type="weekly_sweep",
            triggered_by="cli",
        ),
        sync_dictionary_step=sync_dictionary_step,
        create_crawl_run_step=create_crawl_run_step,
        plan_run_step=plan_run_step,
        process_list_page_step=process_list_page_step,
        fetch_vacancy_detail_step=fetch_vacancy_detail_step,
        reconcile_run_step=reconcile_run_step,
    )

    assert events == [
        ("create", "weekly_sweep", "cli"),
        ("plan", run_id),
        ("page", partition_id, 0),
    ]
    assert result.status == "failed"
    assert result.run_id == run_id
    assert result.failed_step == "process_list_page"
    assert result.error_message is not None
    assert "Invalid HH API User-Agent" in result.error_message
    assert result.partitions_planned == 1
    assert result.partitions_attempted == 1
    assert result.partitions_processed == 0
    assert result.partitions_failed == 1
    assert result.list_pages_attempted == 1
    assert result.list_pages_processed == 0
    assert result.list_pages_failed == 1
    assert result.detail_fetch_attempted == 0
    assert result.reconciliation_status == "skipped"
    assert result.completed_steps == ("create_crawl_run", "plan_sweep")
    assert result.skipped_steps == ("fetch_vacancy_detail", "reconcile_run")
