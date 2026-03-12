from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID, uuid4

from hhru_platform.application.commands.sync_dictionary import (
    SyncDictionaryCommand,
    sync_dictionary,
)
from hhru_platform.application.dto import DictionaryFetchResponse, DictionaryPersistSummary
from hhru_platform.domain.entities.dictionary_sync_run import DictionarySyncRun


class InMemoryDictionarySyncRunRepository:
    def __init__(self) -> None:
        self.runs: dict[UUID, DictionarySyncRun] = {}

    def start(self, *, dictionary_name: str, status: str) -> DictionarySyncRun:
        sync_run = DictionarySyncRun(
            id=uuid4(),
            dictionary_name=dictionary_name,
            status=status,
            etag=None,
            source_status_code=None,
            notes=None,
            started_at=datetime(2026, 3, 12, 12, 0, tzinfo=UTC),
            finished_at=None,
        )
        self.runs[sync_run.id] = sync_run
        return sync_run

    def finish(
        self,
        *,
        run_id: UUID,
        status: str,
        etag: str | None,
        source_status_code: int | None,
        notes: str | None,
    ) -> DictionarySyncRun:
        sync_run = self.runs[run_id]
        sync_run.status = status
        sync_run.etag = etag
        sync_run.source_status_code = source_status_code
        sync_run.notes = notes
        sync_run.finished_at = datetime(2026, 3, 12, 12, 1, tzinfo=UTC)
        return sync_run


class InMemoryApiRequestLogRepository:
    def __init__(self) -> None:
        self.records: list[dict[str, object]] = []

    def add(self, **kwargs: object) -> int:
        self.records.append(dict(kwargs))
        return len(self.records)


class InMemoryRawApiPayloadRepository:
    def __init__(self) -> None:
        self.records: list[dict[str, object]] = []

    def add(self, **kwargs: object) -> int:
        self.records.append(dict(kwargs))
        return len(self.records)


class StaticDictionaryApiClient:
    def __init__(self, response: DictionaryFetchResponse) -> None:
        self._response = response

    def fetch_dictionary(self, dictionary_name: str) -> DictionaryFetchResponse:
        assert dictionary_name == self._response.dictionary_name
        return self._response


class RecordingDictionaryStore:
    def __init__(self, summary: DictionaryPersistSummary) -> None:
        self._summary = summary
        self.calls: list[tuple[str, object]] = []

    def sync(self, dictionary_name: str, payload_json: object) -> DictionaryPersistSummary:
        self.calls.append((dictionary_name, payload_json))
        return self._summary


def test_sync_dictionary_returns_successful_summary_and_logs_payload() -> None:
    response = DictionaryFetchResponse(
        dictionary_name="areas",
        endpoint="/areas",
        method="GET",
        params_json={},
        request_headers_json={"Accept": "application/json", "User-Agent": "pytest"},
        status_code=200,
        headers={"etag": "pytest-areas-etag"},
        latency_ms=31,
        requested_at=datetime(2026, 3, 12, 12, 0, tzinfo=UTC),
        response_received_at=datetime(2026, 3, 12, 12, 0, 1, tzinfo=UTC),
        payload_json=[
            {
                "id": "113",
                "name": "Россия",
                "parent_id": None,
                "areas": [],
            }
        ],
    )
    sync_run_repository = InMemoryDictionarySyncRunRepository()
    api_request_log_repository = InMemoryApiRequestLogRepository()
    raw_api_payload_repository = InMemoryRawApiPayloadRepository()
    dictionary_store = RecordingDictionaryStore(
        DictionaryPersistSummary(created_count=1, updated_count=0, deactivated_count=0)
    )

    result = sync_dictionary(
        SyncDictionaryCommand(dictionary_name="areas"),
        api_client=StaticDictionaryApiClient(response),
        sync_run_repository=sync_run_repository,
        api_request_log_repository=api_request_log_repository,
        raw_api_payload_repository=raw_api_payload_repository,
        dictionary_store=dictionary_store,
    )

    assert result.dictionary_name == "areas"
    assert result.status == "succeeded"
    assert result.created_count == 1
    assert result.updated_count == 0
    assert result.deactivated_count == 0
    assert result.source_status_code == 200
    assert result.request_log_id == 1
    assert result.raw_payload_id == 1
    assert result.error_message is None
    assert dictionary_store.calls == [("areas", response.payload_json)]
    assert api_request_log_repository.records[0]["endpoint"] == "/areas"
    assert raw_api_payload_repository.records[0]["endpoint_type"] == "dictionary.areas"
    assert (
        sync_run_repository.runs[result.sync_run_id].notes
        == "created=1; updated=0; deactivated=0"
    )


def test_sync_dictionary_marks_run_failed_on_non_200_response() -> None:
    response = DictionaryFetchResponse(
        dictionary_name="professional_roles",
        endpoint="/professional_roles",
        method="GET",
        params_json={},
        request_headers_json={"Accept": "application/json", "User-Agent": "pytest"},
        status_code=503,
        headers={"etag": "pytest-professional-roles-etag"},
        latency_ms=44,
        requested_at=datetime(2026, 3, 12, 12, 0, tzinfo=UTC),
        response_received_at=datetime(2026, 3, 12, 12, 0, 1, tzinfo=UTC),
        payload_json={"errors": [{"type": "service_unavailable"}]},
    )
    sync_run_repository = InMemoryDictionarySyncRunRepository()
    api_request_log_repository = InMemoryApiRequestLogRepository()
    raw_api_payload_repository = InMemoryRawApiPayloadRepository()
    dictionary_store = RecordingDictionaryStore(
        DictionaryPersistSummary(created_count=0, updated_count=0, deactivated_count=0)
    )

    result = sync_dictionary(
        SyncDictionaryCommand(dictionary_name="professional_roles"),
        api_client=StaticDictionaryApiClient(response),
        sync_run_repository=sync_run_repository,
        api_request_log_repository=api_request_log_repository,
        raw_api_payload_repository=raw_api_payload_repository,
        dictionary_store=dictionary_store,
    )

    assert result.status == "failed"
    assert result.created_count == 0
    assert result.updated_count == 0
    assert result.deactivated_count == 0
    assert result.source_status_code == 503
    assert result.request_log_id == 1
    assert result.raw_payload_id == 1
    assert result.error_message == "Unexpected status code: 503"
    assert dictionary_store.calls == []
    assert sync_run_repository.runs[result.sync_run_id].notes == "Unexpected status code: 503"
