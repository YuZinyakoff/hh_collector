from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol
from uuid import UUID

from hhru_platform.application.dto import (
    NormalizedVacancySearchPage,
    NormalizedVacancyShortRecord,
    ObservedVacancyRecord,
    StoredVacancyReference,
    VacancySearchResponse,
    VacancyUpsertResult,
)
from hhru_platform.domain.entities.crawl_partition import CrawlPartition
from hhru_platform.domain.value_objects.enums import CrawlPartitionStatus
from hhru_platform.infrastructure.hh_api.user_agent import (
    HHApiUserAgentValidationError,
)
from hhru_platform.infrastructure.normalization.vacancy_short_normalizer import (
    VacancySearchNormalizationError,
    normalize_vacancy_search_page,
)
from hhru_platform.infrastructure.observability.operations import (
    log_operation_started,
    record_operation_failed,
    record_operation_succeeded,
)

DEFAULT_SEARCH_PAGE = 0
DEFAULT_SEARCH_PER_PAGE = 20
SUPPORTED_LIST_SEARCH_PARAMS = {
    "area",
    "date_from",
    "date_to",
    "only_with_salary",
    "order_by",
    "period",
    "professional_role",
    "search_field",
    "text",
}

LOGGER = logging.getLogger(__name__)


class CrawlPartitionNotFoundError(LookupError):
    def __init__(self, crawl_partition_id: UUID) -> None:
        super().__init__(f"crawl_partition not found: {crawl_partition_id}")
        self.crawl_partition_id = crawl_partition_id


@dataclass(slots=True, frozen=True)
class ProcessListPageCommand:
    partition_id: UUID
    page: int | None = None

    def __post_init__(self) -> None:
        if self.page is not None and self.page < 0:
            raise ValueError("page must be greater than or equal to zero")


@dataclass(slots=True, frozen=True)
class ProcessListPageResult:
    partition_id: UUID
    partition_status: str
    page: int
    pages_total_expected: int | None
    vacancies_processed: int
    vacancies_created: int
    seen_events_created: int
    request_log_id: int | None
    raw_payload_id: int | None
    processed_vacancies: list[StoredVacancyReference]
    error_message: str | None


class CrawlPartitionRepository(Protocol):
    def get(self, partition_id: UUID) -> CrawlPartition | None:
        """Return a crawl partition by id."""

    def mark_running(self, partition_id: UUID) -> CrawlPartition:
        """Mark a crawl partition as running."""

    def record_page_processed(
        self,
        *,
        partition_id: UUID,
        pages_total_expected: int | None,
        items_seen_delta: int,
        status: str,
    ) -> CrawlPartition:
        """Persist a successful page-processing result for a partition."""

    def mark_failed(self, *, partition_id: UUID, error_message: str) -> CrawlPartition:
        """Persist a failed partition state."""


class VacancySearchApiClient(Protocol):
    def search_vacancies(self, params_json: dict[str, object]) -> VacancySearchResponse:
        """Request one page of the hh vacancies search endpoint."""


class ApiRequestLogRepository(Protocol):
    def add(
        self,
        *,
        crawl_run_id: UUID | None,
        crawl_partition_id: UUID | None,
        requested_at: datetime,
        request_type: str,
        endpoint: str,
        method: str,
        params_json: dict[str, object],
        request_headers_json: dict[str, str] | None,
        status_code: int,
        latency_ms: int,
        response_received_at: datetime | None,
        error_type: str | None,
        error_message: str | None,
    ) -> int:
        """Persist an API request log row and return its identifier."""


class RawApiPayloadRepository(Protocol):
    def add(
        self,
        *,
        api_request_log_id: int,
        received_at: datetime,
        endpoint_type: str,
        entity_hh_id: str | None,
        payload_json: object,
    ) -> int:
        """Persist a raw payload row and return its identifier."""


class VacancyRepository(Protocol):
    def upsert_many(
        self,
        records: list[NormalizedVacancyShortRecord],
    ) -> VacancyUpsertResult:
        """Create or update vacancy rows from normalized search items."""


class VacancySeenEventRepository(Protocol):
    def add_many(
        self,
        *,
        crawl_run_id: UUID,
        crawl_partition_id: UUID,
        seen_at: datetime,
        short_payload_ref_id: int | None,
        observations: list[ObservedVacancyRecord],
    ) -> int:
        """Persist vacancy seen events and return the number of rows created."""


class VacancyCurrentStateRepository(Protocol):
    def observe_many(
        self,
        *,
        crawl_run_id: UUID,
        observed_at: datetime,
        observations: list[ObservedVacancyRecord],
    ) -> int:
        """Create or update vacancy_current_state rows."""


def process_list_page(
    command: ProcessListPageCommand,
    crawl_partition_repository: CrawlPartitionRepository,
    api_client: VacancySearchApiClient,
    api_request_log_repository: ApiRequestLogRepository,
    raw_api_payload_repository: RawApiPayloadRepository,
    vacancy_repository: VacancyRepository,
    vacancy_seen_event_repository: VacancySeenEventRepository,
    vacancy_current_state_repository: VacancyCurrentStateRepository,
) -> ProcessListPageResult:
    started_at = log_operation_started(
        LOGGER,
        operation="process_list_page",
        partition_id=command.partition_id,
        page=command.page,
    )
    try:
        partition = crawl_partition_repository.get(command.partition_id)
        if partition is None:
            raise CrawlPartitionNotFoundError(command.partition_id)

        crawl_partition_repository.mark_running(command.partition_id)

        search_params = _build_search_params(partition, page_override=command.page)
        page_number = search_params["page"]
        if not isinstance(page_number, int):
            raise TypeError("search parameter page must be an integer")

        try:
            response = api_client.search_vacancies(search_params)
        except HHApiUserAgentValidationError as error:
            failed_partition = crawl_partition_repository.mark_failed(
                partition_id=partition.id,
                error_message=str(error),
            )
            result = ProcessListPageResult(
                partition_id=partition.id,
                partition_status=failed_partition.status,
                page=page_number,
                pages_total_expected=None,
                vacancies_processed=0,
                vacancies_created=0,
                seen_events_created=0,
                request_log_id=None,
                raw_payload_id=None,
                processed_vacancies=[],
                error_message=str(error),
            )
            record_operation_failed(
                LOGGER,
                operation="process_list_page",
                started_at=started_at,
                error_type=error.__class__.__name__,
                error_message=str(error),
                level=logging.ERROR,
                run_id=partition.crawl_run_id,
                partition_id=partition.id,
                page=page_number,
            )
            return result

        request_log_id = api_request_log_repository.add(
            crawl_run_id=partition.crawl_run_id,
            crawl_partition_id=partition.id,
            requested_at=response.requested_at,
            request_type="vacancy_search",
            endpoint=response.endpoint,
            method=response.method,
            params_json=dict(response.params_json),
            request_headers_json=dict(response.request_headers_json),
            status_code=response.status_code,
            latency_ms=response.latency_ms,
            response_received_at=response.response_received_at,
            error_type=response.error_type,
            error_message=response.error_message,
        )

        raw_payload_id: int | None = None
        if _payload_is_present(response.payload_json):
            raw_payload_id = raw_api_payload_repository.add(
                api_request_log_id=request_log_id,
                received_at=response.response_received_at or response.requested_at,
                endpoint_type="vacancies.search",
                entity_hh_id=None,
                payload_json=response.payload_json,
            )

        try:
            normalized_page = _normalize_response(response)
        except VacancySearchNormalizationError as error:
            failed_partition = crawl_partition_repository.mark_failed(
                partition_id=partition.id,
                error_message=str(error),
            )
            result = ProcessListPageResult(
                partition_id=partition.id,
                partition_status=failed_partition.status,
                page=page_number,
                pages_total_expected=None,
                vacancies_processed=0,
                vacancies_created=0,
                seen_events_created=0,
                request_log_id=request_log_id,
                raw_payload_id=raw_payload_id,
                processed_vacancies=[],
                error_message=str(error),
            )
            record_operation_failed(
                LOGGER,
                operation="process_list_page",
                started_at=started_at,
                error_type=error.__class__.__name__,
                error_message=str(error),
                level=logging.WARNING,
                run_id=partition.crawl_run_id,
                partition_id=partition.id,
                request_log_id=request_log_id,
                raw_payload_id=raw_payload_id,
                page=page_number,
            )
            return result

        upsert_result = vacancy_repository.upsert_many(normalized_page.items)
        observed_at = response.response_received_at or datetime.now(UTC)
        observed_records = _build_observed_records(normalized_page.items, upsert_result)
        seen_events_created = vacancy_seen_event_repository.add_many(
            crawl_run_id=partition.crawl_run_id,
            crawl_partition_id=partition.id,
            seen_at=observed_at,
            short_payload_ref_id=raw_payload_id,
            observations=observed_records,
        )
        vacancy_current_state_repository.observe_many(
            crawl_run_id=partition.crawl_run_id,
            observed_at=observed_at,
            observations=observed_records,
        )
        updated_partition = crawl_partition_repository.record_page_processed(
            partition_id=partition.id,
            pages_total_expected=normalized_page.pages,
            items_seen_delta=len(observed_records),
            status=CrawlPartitionStatus.DONE.value,
        )
    except Exception as error:
        record_operation_failed(
            LOGGER,
            operation="process_list_page",
            started_at=started_at,
            error_type=error.__class__.__name__,
            error_message=str(error),
            level=logging.WARNING
            if isinstance(error, CrawlPartitionNotFoundError)
            else logging.ERROR,
            partition_id=command.partition_id,
            page=command.page,
        )
        raise

    result = ProcessListPageResult(
        partition_id=updated_partition.id,
        partition_status=updated_partition.status,
        page=normalized_page.page,
        pages_total_expected=updated_partition.pages_total_expected,
        vacancies_processed=len(observed_records),
        vacancies_created=upsert_result.created_count,
        seen_events_created=seen_events_created,
        request_log_id=request_log_id,
        raw_payload_id=raw_payload_id,
        processed_vacancies=upsert_result.vacancies,
        error_message=None,
    )
    record_operation_succeeded(
        LOGGER,
        operation="process_list_page",
        started_at=started_at,
        records_written={
            "vacancy": result.vacancies_processed,
            "vacancy_seen_event": result.seen_events_created,
        },
        run_id=partition.crawl_run_id,
        partition_id=result.partition_id,
        page=result.page,
        partition_status=result.partition_status,
        vacancies_processed=result.vacancies_processed,
        vacancies_created=result.vacancies_created,
        seen_events_created=result.seen_events_created,
        request_log_id=result.request_log_id,
        raw_payload_id=result.raw_payload_id,
    )
    return result


def _build_search_params(
    partition: CrawlPartition,
    *,
    page_override: int | None,
) -> dict[str, object]:
    params_source = partition.params_json.get("params")
    search_source = params_source if isinstance(params_source, dict) else partition.params_json

    page = page_override
    if page is None:
        page_value = search_source.get("page")
        page = (
            page_value
            if isinstance(page_value, int) and page_value >= 0
            else DEFAULT_SEARCH_PAGE
        )

    per_page_value = search_source.get("per_page")
    per_page = (
        per_page_value
        if isinstance(per_page_value, int) and per_page_value > 0
        else DEFAULT_SEARCH_PER_PAGE
    )

    search_params: dict[str, object] = {"page": page, "per_page": per_page}
    for key in SUPPORTED_LIST_SEARCH_PARAMS:
        value = search_source.get(key)
        if value is not None:
            search_params[key] = value

    return search_params


def _normalize_response(response: VacancySearchResponse) -> NormalizedVacancySearchPage:
    if response.status_code != 200:
        raise VacancySearchNormalizationError(f"Unexpected status code: {response.status_code}")

    if response.error_type is not None:
        raise VacancySearchNormalizationError(
            f"{response.error_type}: {response.error_message or 'vacancy search request failed'}"
        )

    if not _payload_is_present(response.payload_json):
        raise VacancySearchNormalizationError("vacancy search payload is empty")

    return normalize_vacancy_search_page(response.payload_json)


def _build_observed_records(
    normalized_records: list[NormalizedVacancyShortRecord],
    upsert_result: VacancyUpsertResult,
) -> list[ObservedVacancyRecord]:
    stored_vacancies_by_hh_id = {
        vacancy.hh_vacancy_id: vacancy.id for vacancy in upsert_result.vacancies
    }
    return [
        ObservedVacancyRecord(
            vacancy_id=stored_vacancies_by_hh_id[record.hh_vacancy_id],
            hh_vacancy_id=record.hh_vacancy_id,
            list_position=record.list_position,
            short_hash=record.short_hash,
        )
        for record in normalized_records
    ]


def _payload_is_present(payload_json: object | None) -> bool:
    return isinstance(payload_json, dict | list)
