from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest

from hhru_platform.application.commands.process_list_page import (
    CrawlPartitionNotFoundError,
    ProcessListPageCommand,
    process_list_page,
)
from hhru_platform.application.dto import (
    ObservedVacancyRecord,
    StoredVacancyReference,
    VacancySearchResponse,
    VacancyUpsertResult,
)
from hhru_platform.domain.entities.crawl_partition import CrawlPartition


class InMemoryCrawlPartitionRepository:
    def __init__(self, partition: CrawlPartition | None) -> None:
        self._partition = partition

    def get(self, partition_id: UUID) -> CrawlPartition | None:
        if self._partition is None or self._partition.id != partition_id:
            return None
        return self._partition

    def mark_running(self, partition_id: UUID) -> CrawlPartition:
        assert self._partition is not None
        assert self._partition.id == partition_id
        self._partition.status = "running"
        self._partition.started_at = datetime(2026, 3, 12, 12, 1, tzinfo=UTC)
        self._partition.last_error_message = None
        return self._partition

    def record_page_processed(
        self,
        *,
        partition_id: UUID,
        pages_total_expected: int | None,
        items_seen_delta: int,
        status: str,
    ) -> CrawlPartition:
        assert self._partition is not None
        assert self._partition.id == partition_id
        self._partition.pages_total_expected = pages_total_expected
        self._partition.pages_processed += 1
        self._partition.items_seen += items_seen_delta
        self._partition.status = status
        self._partition.finished_at = datetime(2026, 3, 12, 12, 2, tzinfo=UTC)
        self._partition.last_error_message = None
        return self._partition

    def mark_failed(self, *, partition_id: UUID, error_message: str) -> CrawlPartition:
        assert self._partition is not None
        assert self._partition.id == partition_id
        self._partition.status = "failed"
        self._partition.finished_at = datetime(2026, 3, 12, 12, 2, tzinfo=UTC)
        self._partition.last_error_message = error_message
        return self._partition


class StaticVacancySearchApiClient:
    def __init__(self, response: VacancySearchResponse) -> None:
        self._response = response

    def search_vacancies(self, params_json: dict[str, object]) -> VacancySearchResponse:
        assert params_json["page"] == 0
        assert params_json["per_page"] == 2
        assert params_json["text"] == "pytest list search"
        return self._response


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


class RecordingVacancyRepository:
    def __init__(self) -> None:
        self.records: list[object] = []

    def upsert_many(self, records: list[object]) -> VacancyUpsertResult:
        self.records = list(records)
        return VacancyUpsertResult(
            created_count=len(records),
            vacancies=[
                StoredVacancyReference(
                    id=uuid4(),
                    hh_vacancy_id=record.hh_vacancy_id,
                    name_current=record.name_current,
                )
                for record in records
            ],
        )


class RecordingVacancySeenEventRepository:
    def __init__(self) -> None:
        self.observations: list[ObservedVacancyRecord] = []

    def add_many(self, **kwargs: object) -> int:
        self.observations = list(kwargs["observations"])
        return len(self.observations)


class RecordingVacancyCurrentStateRepository:
    def __init__(self) -> None:
        self.observations: list[ObservedVacancyRecord] = []

    def observe_many(self, **kwargs: object) -> int:
        self.observations = list(kwargs["observations"])
        return len(self.observations)


def test_process_list_page_persists_request_raw_and_observations() -> None:
    partition = CrawlPartition(
        id=uuid4(),
        crawl_run_id=uuid4(),
        partition_key="pytest-list-partition",
        params_json={"params": {"text": "pytest list search", "per_page": 2}},
        status="pending",
        pages_total_expected=None,
        pages_processed=0,
        items_seen=0,
        retry_count=0,
        started_at=None,
        finished_at=None,
        last_error_message=None,
        created_at=datetime(2026, 3, 12, 12, 0, tzinfo=UTC),
    )
    api_request_log_repository = InMemoryApiRequestLogRepository()
    raw_api_payload_repository = InMemoryRawApiPayloadRepository()
    vacancy_repository = RecordingVacancyRepository()
    vacancy_seen_event_repository = RecordingVacancySeenEventRepository()
    vacancy_current_state_repository = RecordingVacancyCurrentStateRepository()

    response = VacancySearchResponse(
        endpoint="/vacancies",
        method="GET",
        params_json={"page": 0, "per_page": 2, "text": "pytest list search"},
        request_headers_json={"Accept": "application/json", "User-Agent": "pytest"},
        status_code=200,
        headers={},
        latency_ms=25,
        requested_at=datetime(2026, 3, 12, 12, 1, tzinfo=UTC),
        response_received_at=datetime(2026, 3, 12, 12, 1, 1, tzinfo=UTC),
        payload_json={
            "items": [
                {
                    "id": "pytest-vacancy-1",
                    "name": "Python Engineer",
                    "area": {"id": "pytest-area", "name": "Test Area"},
                    "created_at": "2026-03-12T09:30:00+0300",
                    "published_at": "2026-03-12T10:00:00+0300",
                    "alternate_url": "https://hh.ru/vacancy/pytest-vacancy-1",
                    "employment": {"id": "full", "name": "Full"},
                    "schedule": {"id": "remote", "name": "Remote"},
                    "experience": {"id": "between1And3", "name": "1-3 years"},
                },
                {
                    "id": "pytest-vacancy-2",
                    "name": "Data Engineer",
                    "area": {"id": "pytest-area", "name": "Test Area"},
                    "created_at": "2026-03-12T09:35:00+0300",
                    "published_at": "2026-03-12T10:05:00+0300",
                    "alternate_url": "https://hh.ru/vacancy/pytest-vacancy-2",
                    "employment": {"id": "part", "name": "Part time"},
                    "schedule": {"id": "fullDay", "name": "Full day"},
                    "experience": {"id": "noExperience", "name": "No experience"},
                },
            ],
            "found": 11,
            "page": 0,
            "pages": 6,
            "per_page": 2,
        },
    )

    result = process_list_page(
        ProcessListPageCommand(partition_id=partition.id),
        crawl_partition_repository=InMemoryCrawlPartitionRepository(partition),
        api_client=StaticVacancySearchApiClient(response),
        api_request_log_repository=api_request_log_repository,
        raw_api_payload_repository=raw_api_payload_repository,
        vacancy_repository=vacancy_repository,
        vacancy_seen_event_repository=vacancy_seen_event_repository,
        vacancy_current_state_repository=vacancy_current_state_repository,
    )

    assert result.partition_id == partition.id
    assert result.partition_status == "done"
    assert result.page == 0
    assert result.pages_total_expected == 6
    assert result.vacancies_processed == 2
    assert result.vacancies_created == 2
    assert result.seen_events_created == 2
    assert result.request_log_id == 1
    assert result.raw_payload_id == 1
    assert len(result.processed_vacancies) == 2
    assert all(result.processed_vacancies)
    assert result.error_message is None
    assert api_request_log_repository.records[0]["request_type"] == "vacancy_search"
    assert raw_api_payload_repository.records[0]["endpoint_type"] == "vacancies.search"
    assert len(vacancy_repository.records) == 2
    assert [
        observation.list_position for observation in vacancy_seen_event_repository.observations
    ] == [0, 1]
    assert len(vacancy_current_state_repository.observations) == 2
    assert partition.pages_processed == 1
    assert partition.items_seen == 2
    assert partition.pages_total_expected == 6


def test_process_list_page_raises_when_partition_is_missing() -> None:
    with pytest.raises(CrawlPartitionNotFoundError):
        process_list_page(
            ProcessListPageCommand(partition_id=uuid4()),
            crawl_partition_repository=InMemoryCrawlPartitionRepository(None),
            api_client=StaticVacancySearchApiClient(
                VacancySearchResponse(
                    endpoint="/vacancies",
                    method="GET",
                    params_json={},
                    request_headers_json={"Accept": "application/json", "User-Agent": "pytest"},
                    status_code=200,
                    headers={},
                    latency_ms=0,
                    requested_at=datetime(2026, 3, 12, 12, 0, tzinfo=UTC),
                    response_received_at=datetime(2026, 3, 12, 12, 0, 1, tzinfo=UTC),
                    payload_json={"items": [], "page": 0, "pages": 0, "per_page": 20},
                )
            ),
            api_request_log_repository=InMemoryApiRequestLogRepository(),
            raw_api_payload_repository=InMemoryRawApiPayloadRepository(),
            vacancy_repository=RecordingVacancyRepository(),
            vacancy_seen_event_repository=RecordingVacancySeenEventRepository(),
            vacancy_current_state_repository=RecordingVacancyCurrentStateRepository(),
        )
