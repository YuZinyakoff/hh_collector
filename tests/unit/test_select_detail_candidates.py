from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

from hhru_platform.application.commands.select_detail_candidates import (
    DetailFetchCandidate,
    SelectDetailCandidatesCommand,
    select_detail_candidates,
)
from hhru_platform.domain.entities.vacancy_current_state import VacancyCurrentState
from hhru_platform.domain.value_objects.enums import DetailFetchStatus


class InMemoryVacancyCurrentStateRepository:
    def __init__(self, states: list[VacancyCurrentState]) -> None:
        self._states = list(states)

    def list_by_last_seen_run_id(self, crawl_run_id: UUID) -> list[VacancyCurrentState]:
        return [state for state in self._states if state.last_seen_run_id == crawl_run_id]


class InMemoryVacancySeenEventRepository:
    def __init__(self, previous_short_hashes: dict[UUID, str]) -> None:
        self._previous_short_hashes = dict(previous_short_hashes)

    def list_latest_short_hashes_before_run(
        self,
        *,
        crawl_run_id: UUID,
        vacancy_ids: list[UUID],
    ) -> dict[UUID, str]:
        del crawl_run_id
        return {
            vacancy_id: short_hash
            for vacancy_id, short_hash in self._previous_short_hashes.items()
            if vacancy_id in vacancy_ids
        }


def test_select_detail_candidates_prioritizes_reason_order_and_applies_limit() -> None:
    crawl_run_id = uuid4()
    now = datetime(2026, 3, 19, 12, 0, tzinfo=UTC)
    first_seen_newer_id = uuid4()
    first_seen_older_id = uuid4()
    short_changed_id = uuid4()
    ttl_refresh_id = uuid4()
    fresh_id = uuid4()

    states = [
        _build_vacancy_current_state(
            vacancy_id=first_seen_older_id,
            crawl_run_id=crawl_run_id,
            last_seen_at=now - timedelta(hours=3),
            last_short_hash="hash-first-seen-older",
            last_detail_fetched_at=None,
            detail_fetch_status=DetailFetchStatus.FAILED.value,
        ),
        _build_vacancy_current_state(
            vacancy_id=first_seen_newer_id,
            crawl_run_id=crawl_run_id,
            last_seen_at=now - timedelta(hours=1),
            last_short_hash="hash-first-seen-newer",
            last_detail_fetched_at=None,
            detail_fetch_status=DetailFetchStatus.FAILED.value,
        ),
        _build_vacancy_current_state(
            vacancy_id=short_changed_id,
            crawl_run_id=crawl_run_id,
            last_seen_at=now - timedelta(hours=2),
            last_short_hash="hash-current",
            last_detail_fetched_at=now - timedelta(days=5),
            detail_fetch_status=DetailFetchStatus.SUCCEEDED.value,
        ),
        _build_vacancy_current_state(
            vacancy_id=ttl_refresh_id,
            crawl_run_id=crawl_run_id,
            last_seen_at=now - timedelta(hours=4),
            last_short_hash="hash-ttl",
            last_detail_fetched_at=now - timedelta(days=45),
            detail_fetch_status=DetailFetchStatus.SUCCEEDED.value,
        ),
        _build_vacancy_current_state(
            vacancy_id=fresh_id,
            crawl_run_id=crawl_run_id,
            last_seen_at=now - timedelta(minutes=30),
            last_short_hash="hash-fresh",
            last_detail_fetched_at=now - timedelta(days=2),
            detail_fetch_status=DetailFetchStatus.SUCCEEDED.value,
        ),
    ]

    result = select_detail_candidates(
        SelectDetailCandidatesCommand(
            crawl_run_id=crawl_run_id,
            limit=3,
            detail_refresh_ttl_days=30,
        ),
        vacancy_current_state_repository=InMemoryVacancyCurrentStateRepository(states),
        vacancy_seen_event_repository=InMemoryVacancySeenEventRepository(
            {
                short_changed_id: "hash-previous",
                ttl_refresh_id: "hash-ttl",
                fresh_id: "hash-fresh",
            }
        ),
        now=now,
    )

    assert result.observed_vacancy_count == 5
    assert result.eligible_candidates_count == 4
    assert result.first_seen_candidates == 2
    assert result.short_changed_candidates == 1
    assert result.ttl_refresh_candidates == 1
    assert result.skipped_due_to_limit == 1
    assert result.selected_candidates == (
        _candidate(first_seen_newer_id, "first_seen"),
        _candidate(first_seen_older_id, "first_seen"),
        _candidate(short_changed_id, "short_changed"),
    )


def test_select_detail_candidates_skips_recent_successful_unchanged_vacancies() -> None:
    crawl_run_id = uuid4()
    now = datetime(2026, 3, 19, 12, 0, tzinfo=UTC)
    vacancy_id = uuid4()

    result = select_detail_candidates(
        SelectDetailCandidatesCommand(
            crawl_run_id=crawl_run_id,
            limit=10,
            detail_refresh_ttl_days=30,
        ),
        vacancy_current_state_repository=InMemoryVacancyCurrentStateRepository(
            [
                _build_vacancy_current_state(
                    vacancy_id=vacancy_id,
                    crawl_run_id=crawl_run_id,
                    last_seen_at=now - timedelta(hours=1),
                    last_short_hash="hash-current",
                    last_detail_fetched_at=now - timedelta(days=3),
                    detail_fetch_status=DetailFetchStatus.SUCCEEDED.value,
                )
            ]
        ),
        vacancy_seen_event_repository=InMemoryVacancySeenEventRepository(
            {vacancy_id: "hash-current"}
        ),
        now=now,
    )

    assert result.eligible_candidates_count == 0
    assert result.selected_candidates == ()
    assert result.skipped_due_to_limit == 0
    assert result.first_seen_candidates == 0
    assert result.short_changed_candidates == 0
    assert result.ttl_refresh_candidates == 0


def _build_vacancy_current_state(
    *,
    vacancy_id: UUID,
    crawl_run_id: UUID,
    last_seen_at: datetime,
    last_short_hash: str | None,
    last_detail_fetched_at: datetime | None,
    detail_fetch_status: str,
) -> VacancyCurrentState:
    return VacancyCurrentState(
        vacancy_id=vacancy_id,
        first_seen_at=last_seen_at - timedelta(days=1),
        last_seen_at=last_seen_at,
        seen_count=3,
        consecutive_missing_runs=0,
        is_probably_inactive=False,
        last_seen_run_id=crawl_run_id,
        last_short_hash=last_short_hash,
        last_detail_hash="detail-hash" if last_detail_fetched_at is not None else None,
        last_detail_fetched_at=last_detail_fetched_at,
        detail_fetch_status=detail_fetch_status,
        updated_at=last_seen_at,
    )


def _candidate(vacancy_id: UUID, reason: str) -> DetailFetchCandidate:
    return DetailFetchCandidate(vacancy_id=vacancy_id, reason=reason)
