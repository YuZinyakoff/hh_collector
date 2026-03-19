from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol
from uuid import UUID

from hhru_platform.application.dto import NormalizedVacancyDetail, VacancyDetailResponse
from hhru_platform.domain.entities.vacancy import Vacancy
from hhru_platform.domain.value_objects.enums import DetailFetchStatus, VacancySnapshotType
from hhru_platform.infrastructure.normalization.vacancy_detail_normalizer import (
    VacancyDetailNormalizationError,
    normalize_vacancy_detail,
)
from hhru_platform.infrastructure.observability.operations import (
    log_operation_started,
    record_operation_failed,
    record_operation_succeeded,
)

LOGGER = logging.getLogger(__name__)


class VacancyNotFoundError(LookupError):
    def __init__(self, vacancy_id: UUID) -> None:
        super().__init__(f"vacancy not found: {vacancy_id}")
        self.vacancy_id = vacancy_id


@dataclass(slots=True, frozen=True)
class FetchVacancyDetailCommand:
    vacancy_id: UUID
    reason: str = "manual_refetch"
    attempt: int = 1
    crawl_run_id: UUID | None = None

    def __post_init__(self) -> None:
        normalized_reason = self.reason.strip()
        if not normalized_reason:
            raise ValueError("reason must be a non-empty string")
        if self.attempt < 1:
            raise ValueError("attempt must be greater than or equal to one")

        object.__setattr__(self, "reason", normalized_reason)


@dataclass(slots=True, frozen=True)
class FetchVacancyDetailResult:
    vacancy_id: UUID
    hh_vacancy_id: str
    detail_fetch_status: str
    snapshot_id: int | None
    request_log_id: int
    raw_payload_id: int | None
    detail_fetch_attempt_id: int
    error_message: str | None


class VacancyRepository(Protocol):
    def get(self, vacancy_id: UUID) -> Vacancy | None:
        """Return a vacancy by internal identifier."""

    def apply_detail_update(
        self,
        *,
        vacancy_id: UUID,
        detail: NormalizedVacancyDetail,
    ) -> Vacancy:
        """Persist canonical vacancy fields from a normalized detail payload."""


class VacancyDetailApiClient(Protocol):
    def fetch_vacancy_detail(self, hh_vacancy_id: str) -> VacancyDetailResponse:
        """Request one vacancy detail card from the hh API."""


class DetailFetchAttemptRepository(Protocol):
    def start(
        self,
        *,
        vacancy_id: UUID,
        crawl_run_id: UUID | None,
        reason: str,
        attempt: int,
        requested_at: datetime,
        status: str,
    ) -> int:
        """Persist a detail fetch attempt and return its identifier."""

    def finish(
        self,
        *,
        detail_fetch_attempt_id: int,
        status: str,
        finished_at: datetime,
        error_message: str | None,
    ) -> int:
        """Finalize a detail fetch attempt."""


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


class VacancySnapshotRepository(Protocol):
    def add(
        self,
        *,
        vacancy_id: UUID,
        crawl_run_id: UUID | None,
        snapshot_type: str,
        captured_at: datetime,
        detail_hash: str | None,
        detail_payload_ref_id: int | None,
        normalized_json: dict[str, object] | None,
        change_reason: str | None,
    ) -> int:
        """Persist a vacancy snapshot row and return its identifier."""


class VacancyCurrentStateRepository(Protocol):
    def record_detail_fetch(
        self,
        *,
        vacancy_id: UUID,
        recorded_at: datetime,
        detail_hash: str | None,
        detail_fetch_status: str,
    ) -> None:
        """Create or update vacancy_current_state after a detail fetch."""


def fetch_vacancy_detail(
    command: FetchVacancyDetailCommand,
    vacancy_repository: VacancyRepository,
    api_client: VacancyDetailApiClient,
    detail_fetch_attempt_repository: DetailFetchAttemptRepository,
    api_request_log_repository: ApiRequestLogRepository,
    raw_api_payload_repository: RawApiPayloadRepository,
    vacancy_snapshot_repository: VacancySnapshotRepository,
    vacancy_current_state_repository: VacancyCurrentStateRepository,
) -> FetchVacancyDetailResult:
    started_at = log_operation_started(
        LOGGER,
        operation="fetch_vacancy_detail",
        vacancy_id=command.vacancy_id,
        attempt=command.attempt,
        run_id=command.crawl_run_id,
        reason=command.reason,
    )
    try:
        vacancy = vacancy_repository.get(command.vacancy_id)
        if vacancy is None:
            raise VacancyNotFoundError(command.vacancy_id)

        attempt_requested_at = datetime.now(UTC)
        detail_fetch_attempt_id = detail_fetch_attempt_repository.start(
            vacancy_id=vacancy.id,
            crawl_run_id=command.crawl_run_id,
            reason=command.reason,
            attempt=command.attempt,
            requested_at=attempt_requested_at,
            status=DetailFetchStatus.RUNNING.value,
        )
        response = api_client.fetch_vacancy_detail(vacancy.hh_vacancy_id)
        request_log_id = api_request_log_repository.add(
            crawl_run_id=command.crawl_run_id,
            crawl_partition_id=None,
            requested_at=response.requested_at,
            request_type="vacancy_detail",
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
                endpoint_type="vacancies.detail",
                entity_hh_id=vacancy.hh_vacancy_id,
                payload_json=response.payload_json,
            )

        try:
            normalized_detail = _normalize_response(response)
        except VacancyDetailNormalizationError as error:
            failed_at = response.response_received_at or datetime.now(UTC)
            vacancy_current_state_repository.record_detail_fetch(
                vacancy_id=vacancy.id,
                recorded_at=failed_at,
                detail_hash=None,
                detail_fetch_status=DetailFetchStatus.FAILED.value,
            )
            detail_fetch_attempt_repository.finish(
                detail_fetch_attempt_id=detail_fetch_attempt_id,
                status=DetailFetchStatus.FAILED.value,
                finished_at=failed_at,
                error_message=str(error),
            )
            result = FetchVacancyDetailResult(
                vacancy_id=vacancy.id,
                hh_vacancy_id=vacancy.hh_vacancy_id,
                detail_fetch_status=DetailFetchStatus.FAILED.value,
                snapshot_id=None,
                request_log_id=request_log_id,
                raw_payload_id=raw_payload_id,
                detail_fetch_attempt_id=detail_fetch_attempt_id,
                error_message=str(error),
            )
            record_operation_failed(
                LOGGER,
                operation="fetch_vacancy_detail",
                started_at=started_at,
                error_type=error.__class__.__name__,
                error_message=str(error),
                level=logging.WARNING,
                vacancy_id=result.vacancy_id,
                hh_vacancy_id=result.hh_vacancy_id,
                attempt=command.attempt,
                run_id=command.crawl_run_id,
                request_log_id=result.request_log_id,
                raw_payload_id=result.raw_payload_id,
                detail_fetch_attempt_id=result.detail_fetch_attempt_id,
            )
            return result

        vacancy_repository.apply_detail_update(vacancy_id=vacancy.id, detail=normalized_detail)
        captured_at = response.response_received_at or datetime.now(UTC)
        snapshot_id = vacancy_snapshot_repository.add(
            vacancy_id=vacancy.id,
            crawl_run_id=command.crawl_run_id,
            snapshot_type=VacancySnapshotType.DETAIL.value,
            captured_at=captured_at,
            detail_hash=normalized_detail.detail_hash,
            detail_payload_ref_id=raw_payload_id,
            normalized_json=normalized_detail.normalized_json,
            change_reason=command.reason,
        )
        vacancy_current_state_repository.record_detail_fetch(
            vacancy_id=vacancy.id,
            recorded_at=captured_at,
            detail_hash=normalized_detail.detail_hash,
            detail_fetch_status=DetailFetchStatus.SUCCEEDED.value,
        )
        detail_fetch_attempt_repository.finish(
            detail_fetch_attempt_id=detail_fetch_attempt_id,
            status=DetailFetchStatus.SUCCEEDED.value,
            finished_at=captured_at,
            error_message=None,
        )
    except Exception as error:
        record_operation_failed(
            LOGGER,
            operation="fetch_vacancy_detail",
            started_at=started_at,
            error_type=error.__class__.__name__,
            error_message=str(error),
            level=logging.WARNING if isinstance(error, VacancyNotFoundError) else logging.ERROR,
            vacancy_id=command.vacancy_id,
            attempt=command.attempt,
            run_id=command.crawl_run_id,
            reason=command.reason,
        )
        raise

    result = FetchVacancyDetailResult(
        vacancy_id=vacancy.id,
        hh_vacancy_id=vacancy.hh_vacancy_id,
        detail_fetch_status=DetailFetchStatus.SUCCEEDED.value,
        snapshot_id=snapshot_id,
        request_log_id=request_log_id,
        raw_payload_id=raw_payload_id,
        detail_fetch_attempt_id=detail_fetch_attempt_id,
        error_message=None,
    )
    record_operation_succeeded(
        LOGGER,
        operation="fetch_vacancy_detail",
        started_at=started_at,
        records_written={"vacancy_snapshot": 1},
        vacancy_id=result.vacancy_id,
        hh_vacancy_id=result.hh_vacancy_id,
        attempt=command.attempt,
        run_id=command.crawl_run_id,
        request_log_id=result.request_log_id,
        raw_payload_id=result.raw_payload_id,
        detail_fetch_attempt_id=result.detail_fetch_attempt_id,
        snapshot_id=result.snapshot_id,
        detail_status=result.detail_fetch_status,
    )
    return result


def _normalize_response(response: VacancyDetailResponse) -> NormalizedVacancyDetail:
    if response.status_code != 200:
        raise VacancyDetailNormalizationError(f"Unexpected status code: {response.status_code}")

    if response.error_type is not None:
        raise VacancyDetailNormalizationError(
            f"{response.error_type}: {response.error_message or 'vacancy detail request failed'}"
        )

    if not _payload_is_present(response.payload_json):
        raise VacancyDetailNormalizationError("vacancy detail payload is empty")

    return normalize_vacancy_detail(response.payload_json)


def _payload_is_present(payload_json: object | None) -> bool:
    return isinstance(payload_json, dict | list)
