from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol
from uuid import UUID

from hhru_platform.application.dto import (
    SUPPORTED_DICTIONARY_NAMES,
    DictionaryFetchResponse,
    DictionaryPersistSummary,
)
from hhru_platform.domain.entities.dictionary_sync_run import DictionarySyncRun
from hhru_platform.domain.value_objects.enums import DictionarySyncStatus
from hhru_platform.infrastructure.normalization.dictionary_normalizers import (
    DictionaryNormalizationError,
)


@dataclass(slots=True, frozen=True)
class SyncDictionaryCommand:
    dictionary_name: str

    def __post_init__(self) -> None:
        normalized_dictionary_name = self.dictionary_name.strip()
        if normalized_dictionary_name not in SUPPORTED_DICTIONARY_NAMES:
            supported = ", ".join(SUPPORTED_DICTIONARY_NAMES)
            raise ValueError(
                f"Unsupported dictionary_name {self.dictionary_name!r}. "
                f"Expected one of: {supported}."
            )

        object.__setattr__(self, "dictionary_name", normalized_dictionary_name)


@dataclass(slots=True, frozen=True)
class SyncDictionaryResult:
    dictionary_name: str
    sync_run_id: UUID
    status: str
    created_count: int
    updated_count: int
    deactivated_count: int
    source_status_code: int | None
    request_log_id: int
    raw_payload_id: int | None
    error_message: str | None


class DictionaryApiClient(Protocol):
    def fetch_dictionary(self, dictionary_name: str) -> DictionaryFetchResponse:
        """Fetch a dictionary payload from the upstream hh API."""


class DictionarySyncRunRepository(Protocol):
    def start(self, *, dictionary_name: str, status: str) -> DictionarySyncRun:
        """Persist a new dictionary sync run."""

    def finish(
        self,
        *,
        run_id: UUID,
        status: str,
        etag: str | None,
        source_status_code: int | None,
        notes: str | None,
    ) -> DictionarySyncRun:
        """Finalize an existing dictionary sync run."""


class ApiRequestLogRepository(Protocol):
    def add(
        self,
        *,
        crawl_run_id: UUID | None,
        crawl_partition_id: UUID | None,
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
        endpoint_type: str,
        entity_hh_id: str | None,
        payload_json: object,
    ) -> int:
        """Persist a raw payload row and return its identifier."""


class DictionaryStore(Protocol):
    def sync(self, dictionary_name: str, payload_json: object) -> DictionaryPersistSummary:
        """Normalize and persist dictionary data."""


def sync_dictionary(
    command: SyncDictionaryCommand,
    api_client: DictionaryApiClient,
    sync_run_repository: DictionarySyncRunRepository,
    api_request_log_repository: ApiRequestLogRepository,
    raw_api_payload_repository: RawApiPayloadRepository,
    dictionary_store: DictionaryStore,
) -> SyncDictionaryResult:
    sync_run = sync_run_repository.start(
        dictionary_name=command.dictionary_name,
        status=DictionarySyncStatus.RUNNING.value,
    )

    response = api_client.fetch_dictionary(command.dictionary_name)
    request_log_id = api_request_log_repository.add(
        crawl_run_id=None,
        crawl_partition_id=None,
        request_type="dictionary_sync",
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
            endpoint_type=f"dictionary.{command.dictionary_name}",
            entity_hh_id=None,
            payload_json=response.payload_json,
        )

    try:
        summary = _persist_dictionary_payload(command.dictionary_name, response, dictionary_store)
    except DictionaryNormalizationError as error:
        return _build_failed_result(
            command=command,
            sync_run=sync_run,
            sync_run_repository=sync_run_repository,
            request_log_id=request_log_id,
            raw_payload_id=raw_payload_id,
            response=response,
            error_message=str(error),
        )

    finished_sync_run = sync_run_repository.finish(
        run_id=sync_run.id,
        status=DictionarySyncStatus.SUCCEEDED.value,
        etag=response.etag,
        source_status_code=response.status_code,
        notes=_build_success_notes(summary),
    )
    return SyncDictionaryResult(
        dictionary_name=command.dictionary_name,
        sync_run_id=finished_sync_run.id,
        status=finished_sync_run.status,
        created_count=summary.created_count,
        updated_count=summary.updated_count,
        deactivated_count=summary.deactivated_count,
        source_status_code=finished_sync_run.source_status_code,
        request_log_id=request_log_id,
        raw_payload_id=raw_payload_id,
        error_message=None,
    )


def _persist_dictionary_payload(
    dictionary_name: str,
    response: DictionaryFetchResponse,
    dictionary_store: DictionaryStore,
) -> DictionaryPersistSummary:
    if response.status_code != 200:
        raise DictionaryNormalizationError(f"Unexpected status code: {response.status_code}")

    if response.error_type is not None:
        raise DictionaryNormalizationError(
            f"{response.error_type}: {response.error_message or 'dictionary request failed'}"
        )

    if not _payload_is_present(response.payload_json):
        raise DictionaryNormalizationError("dictionary response payload is empty")

    return dictionary_store.sync(dictionary_name, response.payload_json)


def _build_failed_result(
    *,
    command: SyncDictionaryCommand,
    sync_run: DictionarySyncRun,
    sync_run_repository: DictionarySyncRunRepository,
    request_log_id: int,
    raw_payload_id: int | None,
    response: DictionaryFetchResponse,
    error_message: str,
) -> SyncDictionaryResult:
    finished_sync_run = sync_run_repository.finish(
        run_id=sync_run.id,
        status=DictionarySyncStatus.FAILED.value,
        etag=response.etag,
        source_status_code=response.status_code or None,
        notes=error_message,
    )
    return SyncDictionaryResult(
        dictionary_name=command.dictionary_name,
        sync_run_id=finished_sync_run.id,
        status=finished_sync_run.status,
        created_count=0,
        updated_count=0,
        deactivated_count=0,
        source_status_code=finished_sync_run.source_status_code,
        request_log_id=request_log_id,
        raw_payload_id=raw_payload_id,
        error_message=error_message,
    )


def _build_success_notes(summary: DictionaryPersistSummary) -> str:
    return (
        f"created={summary.created_count}; "
        f"updated={summary.updated_count}; "
        f"deactivated={summary.deactivated_count}"
    )


def _payload_is_present(payload_json: object | None) -> bool:
    return isinstance(payload_json, dict | list)
