from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from uuid import UUID

from hhru_platform.application.commands.create_crawl_run import (
    CreateCrawlRunCommand,
)
from hhru_platform.application.commands.fetch_vacancy_detail import (
    FetchVacancyDetailCommand,
    FetchVacancyDetailResult,
)
from hhru_platform.application.commands.plan_sweep import PlanRunCommand, PlanRunResult
from hhru_platform.application.commands.process_list_page import (
    ProcessListPageCommand,
    ProcessListPageResult,
)
from hhru_platform.application.commands.reconcile_run import (
    ReconcileRunCommand,
    ReconcileRunResult,
)
from hhru_platform.application.commands.sync_dictionary import (
    SyncDictionaryCommand,
    SyncDictionaryResult,
)
from hhru_platform.application.dto import SUPPORTED_DICTIONARY_NAMES, StoredVacancyReference
from hhru_platform.domain.entities.crawl_run import CrawlRun
from hhru_platform.domain.value_objects.enums import CrawlPartitionStatus, DictionarySyncStatus
from hhru_platform.infrastructure.observability.operations import (
    log_operation_started,
    record_operation_failed,
    record_operation_succeeded,
)

LOGGER = logging.getLogger(__name__)


class RunCollectionOnceStepError(RuntimeError):
    def __init__(self, *, step: str, message: str, run_id: UUID | None = None) -> None:
        super().__init__(message)
        self.step = step
        self.run_id = run_id


@dataclass(slots=True, frozen=True)
class RunCollectionOnceCommand:
    sync_dictionaries: bool = False
    pages_per_partition: int = 1
    detail_limit: int = 0
    run_type: str = "weekly_sweep"
    triggered_by: str = "run-once"
    dictionary_names: tuple[str, ...] = SUPPORTED_DICTIONARY_NAMES

    def __post_init__(self) -> None:
        normalized_run_type = self.run_type.strip()
        normalized_triggered_by = self.triggered_by.strip()
        normalized_dictionary_names = tuple(name.strip() for name in self.dictionary_names)

        if not normalized_run_type:
            raise ValueError("run_type must not be empty")
        if not normalized_triggered_by:
            raise ValueError("triggered_by must not be empty")
        if self.pages_per_partition < 1:
            raise ValueError("pages_per_partition must be greater than or equal to one")
        if self.detail_limit < 0:
            raise ValueError("detail_limit must be greater than or equal to zero")
        if any(name not in SUPPORTED_DICTIONARY_NAMES for name in normalized_dictionary_names):
            supported = ", ".join(SUPPORTED_DICTIONARY_NAMES)
            raise ValueError(f"dictionary_names must be drawn from: {supported}")

        object.__setattr__(self, "run_type", normalized_run_type)
        object.__setattr__(self, "triggered_by", normalized_triggered_by)
        object.__setattr__(self, "dictionary_names", normalized_dictionary_names)


@dataclass(slots=True, frozen=True)
class RunCollectionOnceResult:
    run_id: UUID
    run_type: str
    triggered_by: str
    dictionary_results: tuple[SyncDictionaryResult, ...]
    planned_partition_ids: tuple[UUID, ...]
    list_page_results: tuple[ProcessListPageResult, ...]
    detail_results: tuple[FetchVacancyDetailResult, ...]
    reconciliation_result: ReconcileRunResult

    @property
    def partitions_planned(self) -> int:
        return len(self.planned_partition_ids)

    @property
    def partitions_processed(self) -> int:
        return len({result.partition_id for result in self.list_page_results})

    @property
    def list_pages_processed(self) -> int:
        return len(self.list_page_results)

    @property
    def vacancies_found(self) -> int:
        return len(_collect_unique_vacancies(self.list_page_results))

    @property
    def detail_fetch_attempted(self) -> int:
        return len(self.detail_results)

    @property
    def detail_fetch_succeeded(self) -> int:
        return sum(1 for result in self.detail_results if result.error_message is None)

    @property
    def detail_fetch_failed(self) -> int:
        return self.detail_fetch_attempted - self.detail_fetch_succeeded

    @property
    def reconciliation_status(self) -> str:
        return self.reconciliation_result.run_status


SyncDictionaryStep = Callable[[SyncDictionaryCommand], SyncDictionaryResult]
CreateCrawlRunStep = Callable[[CreateCrawlRunCommand], CrawlRun]
PlanRunStep = Callable[[PlanRunCommand], PlanRunResult]
ProcessListPageStep = Callable[[ProcessListPageCommand], ProcessListPageResult]
FetchVacancyDetailStep = Callable[[FetchVacancyDetailCommand], FetchVacancyDetailResult]
ReconcileRunStep = Callable[[ReconcileRunCommand], ReconcileRunResult]


def run_collection_once(
    command: RunCollectionOnceCommand,
    *,
    sync_dictionary_step: SyncDictionaryStep,
    create_crawl_run_step: CreateCrawlRunStep,
    plan_run_step: PlanRunStep,
    process_list_page_step: ProcessListPageStep,
    fetch_vacancy_detail_step: FetchVacancyDetailStep,
    reconcile_run_step: ReconcileRunStep,
) -> RunCollectionOnceResult:
    started_at = log_operation_started(
        LOGGER,
        operation="run_collection_once",
        sync_dictionaries=command.sync_dictionaries,
        pages_per_partition=command.pages_per_partition,
        detail_limit=command.detail_limit,
        run_type=command.run_type,
        triggered_by=command.triggered_by,
    )
    current_run_id: UUID | None = None

    try:
        dictionary_results = tuple(_sync_requested_dictionaries(command, sync_dictionary_step))

        crawl_run = create_crawl_run_step(
            CreateCrawlRunCommand(
                run_type=command.run_type,
                triggered_by=command.triggered_by,
            )
        )
        current_run_id = crawl_run.id

        plan_result = plan_run_step(PlanRunCommand(crawl_run_id=crawl_run.id))
        list_page_results = tuple(
            _process_partitions(
                plan_result=plan_result,
                pages_per_partition=command.pages_per_partition,
                process_list_page_step=process_list_page_step,
            )
        )

        detail_candidates = list(_collect_unique_vacancies(list_page_results).values())
        detail_results = tuple(
            fetch_vacancy_detail_step(
                FetchVacancyDetailCommand(
                    vacancy_id=vacancy.id,
                    reason="run_once",
                    attempt=1,
                    crawl_run_id=crawl_run.id,
                )
            )
            for vacancy in detail_candidates[: command.detail_limit]
        )

        reconciliation_result = reconcile_run_step(ReconcileRunCommand(crawl_run_id=crawl_run.id))
    except Exception as error:
        record_operation_failed(
            LOGGER,
            operation="run_collection_once",
            started_at=started_at,
            error_type=error.__class__.__name__,
            error_message=str(error),
            run_id=current_run_id,
            sync_dictionaries=command.sync_dictionaries,
            pages_per_partition=command.pages_per_partition,
            detail_limit=command.detail_limit,
            run_type=command.run_type,
            triggered_by=command.triggered_by,
            failed_step=getattr(error, "step", None),
        )
        raise

    result = RunCollectionOnceResult(
        run_id=crawl_run.id,
        run_type=command.run_type,
        triggered_by=command.triggered_by,
        dictionary_results=dictionary_results,
        planned_partition_ids=tuple(partition.id for partition in plan_result.partitions),
        list_page_results=list_page_results,
        detail_results=detail_results,
        reconciliation_result=reconciliation_result,
    )
    record_operation_succeeded(
        LOGGER,
        operation="run_collection_once",
        started_at=started_at,
        run_id=result.run_id,
        run_type=result.run_type,
        triggered_by=result.triggered_by,
        dictionaries_synced=len(result.dictionary_results),
        partitions_planned=result.partitions_planned,
        partitions_processed=result.partitions_processed,
        list_pages_processed=result.list_pages_processed,
        vacancies_found=result.vacancies_found,
        detail_fetch_attempted=result.detail_fetch_attempted,
        detail_fetch_failed=result.detail_fetch_failed,
        reconciliation_status=result.reconciliation_status,
    )
    return result


def _sync_requested_dictionaries(
    command: RunCollectionOnceCommand,
    sync_dictionary_step: SyncDictionaryStep,
) -> list[SyncDictionaryResult]:
    if not command.sync_dictionaries:
        return []

    results: list[SyncDictionaryResult] = []
    for dictionary_name in command.dictionary_names:
        result = sync_dictionary_step(SyncDictionaryCommand(dictionary_name=dictionary_name))
        results.append(result)
        if (
            result.status != DictionarySyncStatus.SUCCEEDED.value
            or result.error_message is not None
        ):
            raise RunCollectionOnceStepError(
                step="sync_dictionary",
                message=(
                    f"dictionary sync failed for {dictionary_name}: "
                    f"{result.error_message or result.status}"
                ),
            )
    return results


def _process_partitions(
    *,
    plan_result: PlanRunResult,
    pages_per_partition: int,
    process_list_page_step: ProcessListPageStep,
) -> list[ProcessListPageResult]:
    results: list[ProcessListPageResult] = []
    for partition in plan_result.partitions:
        expected_pages: int | None = None
        for page_number in range(pages_per_partition):
            if expected_pages is not None and page_number >= expected_pages:
                break

            page_result = process_list_page_step(
                ProcessListPageCommand(
                    partition_id=partition.id,
                    page=page_number,
                )
            )
            results.append(page_result)
            if page_result.pages_total_expected is not None:
                expected_pages = page_result.pages_total_expected
            if (
                page_result.partition_status == CrawlPartitionStatus.FAILED.value
                or page_result.error_message is not None
            ):
                break
    return results


def _collect_unique_vacancies(
    list_page_results: tuple[ProcessListPageResult, ...] | list[ProcessListPageResult],
) -> dict[UUID, StoredVacancyReference]:
    vacancies_by_id: dict[UUID, StoredVacancyReference] = {}
    for page_result in list_page_results:
        for vacancy in page_result.processed_vacancies:
            vacancies_by_id.setdefault(vacancy.id, vacancy)
    return vacancies_by_id
