from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID, uuid4

from hhru_platform.application.commands.fetch_vacancy_detail import (
    FetchVacancyDetailCommand,
    fetch_vacancy_detail,
)
from hhru_platform.application.dto import NormalizedVacancyDetail, VacancyDetailResponse
from hhru_platform.domain.entities.vacancy import Vacancy


class InMemoryVacancyRepository:
    def __init__(self, vacancy: Vacancy | None) -> None:
        self._vacancy = vacancy
        self.updated_detail: NormalizedVacancyDetail | None = None

    def get(self, vacancy_id: UUID) -> Vacancy | None:
        if self._vacancy is None or self._vacancy.id != vacancy_id:
            return None
        return self._vacancy

    def apply_detail_update(
        self,
        *,
        vacancy_id: UUID,
        detail: NormalizedVacancyDetail,
    ) -> Vacancy:
        assert self._vacancy is not None
        assert self._vacancy.id == vacancy_id
        self.updated_detail = detail
        self._vacancy.name_current = detail.name_current
        self._vacancy.published_at = detail.published_at
        self._vacancy.created_at_hh = detail.created_at_hh
        self._vacancy.alternate_url = detail.alternate_url
        self._vacancy.employment_type_code = detail.employment_type_code
        self._vacancy.schedule_type_code = detail.schedule_type_code
        self._vacancy.experience_code = detail.experience_code
        return self._vacancy


class StaticVacancyDetailApiClient:
    def __init__(self, response: VacancyDetailResponse) -> None:
        self._response = response

    def fetch_vacancy_detail(self, hh_vacancy_id: str) -> VacancyDetailResponse:
        assert hh_vacancy_id == "pytest-detail-vacancy"
        return self._response


class InMemoryDetailFetchAttemptRepository:
    def __init__(self) -> None:
        self.records: dict[int, dict[str, object]] = {}

    def start(self, **kwargs: object) -> int:
        attempt_id = len(self.records) + 1
        self.records[attempt_id] = dict(kwargs)
        return attempt_id

    def finish(self, **kwargs: object) -> int:
        attempt_id = int(kwargs["detail_fetch_attempt_id"])
        self.records[attempt_id].update(kwargs)
        return attempt_id


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


class InMemoryVacancySnapshotRepository:
    def __init__(self) -> None:
        self.records: list[dict[str, object]] = []

    def add(self, **kwargs: object) -> int:
        self.records.append(dict(kwargs))
        return len(self.records)


class RecordingVacancyCurrentStateRepository:
    def __init__(self) -> None:
        self.records: list[dict[str, object]] = []

    def record_detail_fetch(self, **kwargs: object) -> None:
        self.records.append(dict(kwargs))


def test_fetch_vacancy_detail_persists_attempt_snapshot_raw_and_current_state() -> None:
    vacancy = Vacancy(
        id=uuid4(),
        hh_vacancy_id="pytest-detail-vacancy",
        employer_id=None,
        area_id=None,
        name_current="Old vacancy name",
        published_at=None,
        created_at_hh=None,
        archived_at_hh=None,
        alternate_url=None,
        employment_type_code=None,
        schedule_type_code=None,
        experience_code=None,
        source_type="hh_api",
    )
    detail_fetch_attempt_repository = InMemoryDetailFetchAttemptRepository()
    api_request_log_repository = InMemoryApiRequestLogRepository()
    raw_api_payload_repository = InMemoryRawApiPayloadRepository()
    vacancy_snapshot_repository = InMemoryVacancySnapshotRepository()
    vacancy_current_state_repository = RecordingVacancyCurrentStateRepository()
    vacancy_repository = InMemoryVacancyRepository(vacancy)

    response = VacancyDetailResponse(
        endpoint="/vacancies/pytest-detail-vacancy",
        method="GET",
        params_json={},
        request_headers_json={"Accept": "application/json", "User-Agent": "pytest"},
        status_code=200,
        headers={},
        latency_ms=14,
        requested_at=datetime(2026, 3, 12, 12, 0, tzinfo=UTC),
        response_received_at=datetime(2026, 3, 12, 12, 0, 1, tzinfo=UTC),
        payload_json={
            "id": "pytest-detail-vacancy",
            "name": "Senior Python Engineer",
            "description": "Detailed vacancy description",
            "alternate_url": "https://hh.ru/vacancy/pytest-detail-vacancy",
            "archived": False,
            "area": {"id": "pytest-area", "name": "Test Area"},
            "created_at": "2026-03-12T09:30:00+0300",
            "initial_created_at": "2026-03-11T09:00:00+0300",
            "employer": {
                "id": "pytest-employer-1",
                "name": "Pytest Employer One",
                "alternate_url": "https://hh.ru/employer/pytest-employer-1",
                "trusted": True,
            },
            "employment": {"id": "full", "name": "Full employment"},
            "experience": {"id": "between1And3", "name": "1-3 years"},
            "key_skills": [{"name": "Python"}, {"name": "SQL"}],
            "professional_roles": [{"id": "pytest-role-python", "name": "Programmer, developer"}],
            "published_at": "2026-03-12T10:00:00+0300",
            "salary": {"currency": "RUR", "from": 200000, "to": 300000, "gross": False},
            "schedule": {"id": "remote", "name": "Remote"},
        },
    )

    result = fetch_vacancy_detail(
        FetchVacancyDetailCommand(vacancy_id=vacancy.id),
        vacancy_repository=vacancy_repository,
        api_client=StaticVacancyDetailApiClient(response),
        detail_fetch_attempt_repository=detail_fetch_attempt_repository,
        api_request_log_repository=api_request_log_repository,
        raw_api_payload_repository=raw_api_payload_repository,
        vacancy_snapshot_repository=vacancy_snapshot_repository,
        vacancy_current_state_repository=vacancy_current_state_repository,
    )

    assert result.vacancy_id == vacancy.id
    assert result.hh_vacancy_id == "pytest-detail-vacancy"
    assert result.detail_fetch_status == "succeeded"
    assert result.snapshot_id == 1
    assert result.request_log_id == 1
    assert result.raw_payload_id == 1
    assert result.detail_fetch_attempt_id == 1
    assert result.error_message is None
    assert api_request_log_repository.records[0]["request_type"] == "vacancy_detail"
    assert raw_api_payload_repository.records[0]["endpoint_type"] == "vacancies.detail"
    assert vacancy_repository.updated_detail is not None
    assert vacancy_repository.updated_detail.name_current == "Senior Python Engineer"
    assert vacancy_repository.updated_detail.employer is not None
    assert vacancy_repository.updated_detail.employer.hh_employer_id == "pytest-employer-1"
    assert vacancy_repository.updated_detail.professional_role_hh_ids == ("pytest-role-python",)
    assert vacancy_snapshot_repository.records[0]["snapshot_type"] == "detail"
    assert vacancy_snapshot_repository.records[0]["change_reason"] == "manual_refetch"
    assert (
        vacancy_snapshot_repository.records[0]["normalized_json"]["description"]
        == "Detailed vacancy description"
    )
    assert vacancy_current_state_repository.records[0]["detail_fetch_status"] == "succeeded"
    assert vacancy_current_state_repository.records[0]["detail_hash"] is not None
    assert detail_fetch_attempt_repository.records[1]["status"] == "succeeded"
