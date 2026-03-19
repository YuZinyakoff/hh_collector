from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol
from uuid import UUID

from hhru_platform.domain.entities.vacancy_current_state import VacancyCurrentState
from hhru_platform.domain.value_objects.enums import DetailFetchStatus

DEFAULT_DETAIL_REFRESH_TTL_DAYS = 30
DEFAULT_DETAIL_SELECTION_LIMIT = 100
DETAIL_REASON_PRIORITY = {
    "first_seen": 0,
    "short_changed": 1,
    "ttl_refresh": 2,
}


@dataclass(slots=True, frozen=True)
class DetailFetchCandidate:
    vacancy_id: UUID
    reason: str


@dataclass(slots=True, frozen=True)
class SelectDetailCandidatesCommand:
    crawl_run_id: UUID
    limit: int = DEFAULT_DETAIL_SELECTION_LIMIT
    detail_refresh_ttl_days: int = DEFAULT_DETAIL_REFRESH_TTL_DAYS

    def __post_init__(self) -> None:
        if self.limit < 0:
            raise ValueError("limit must be greater than or equal to zero")
        if self.detail_refresh_ttl_days < 1:
            raise ValueError("detail_refresh_ttl_days must be greater than or equal to one")


@dataclass(slots=True, frozen=True)
class SelectDetailCandidatesResult:
    crawl_run_id: UUID
    observed_vacancy_count: int
    eligible_candidates_count: int
    selected_candidates: tuple[DetailFetchCandidate, ...]
    skipped_due_to_limit: int
    first_seen_candidates: int
    short_changed_candidates: int
    ttl_refresh_candidates: int


class VacancyCurrentStateRepository(Protocol):
    def list_by_last_seen_run_id(self, crawl_run_id: UUID) -> list[VacancyCurrentState]:
        """Return current-state rows observed in the given run."""


class VacancySeenEventRepository(Protocol):
    def list_latest_short_hashes_before_run(
        self,
        *,
        crawl_run_id: UUID,
        vacancy_ids: list[UUID],
    ) -> dict[UUID, str]:
        """Return the latest short hash before the given run for each vacancy id."""


def select_detail_candidates(
    command: SelectDetailCandidatesCommand,
    vacancy_current_state_repository: VacancyCurrentStateRepository,
    vacancy_seen_event_repository: VacancySeenEventRepository,
    *,
    now: datetime | None = None,
) -> SelectDetailCandidatesResult:
    current_states = vacancy_current_state_repository.list_by_last_seen_run_id(command.crawl_run_id)
    vacancy_ids = [state.vacancy_id for state in current_states]
    previous_short_hashes = vacancy_seen_event_repository.list_latest_short_hashes_before_run(
        crawl_run_id=command.crawl_run_id,
        vacancy_ids=vacancy_ids,
    )
    evaluated_at = now or datetime.now(UTC)
    ttl_cutoff = evaluated_at - timedelta(days=command.detail_refresh_ttl_days)

    candidates_with_state: list[tuple[DetailFetchCandidate, VacancyCurrentState]] = []
    first_seen_candidates = 0
    short_changed_candidates = 0
    ttl_refresh_candidates = 0

    for state in current_states:
        reason = _decide_candidate_reason(
            vacancy_state=state,
            previous_short_hash=previous_short_hashes.get(state.vacancy_id),
            ttl_cutoff=ttl_cutoff,
        )
        if reason is None:
            continue

        candidates_with_state.append(
            (
                DetailFetchCandidate(vacancy_id=state.vacancy_id, reason=reason),
                state,
            )
        )
        if reason == "first_seen":
            first_seen_candidates += 1
        elif reason == "short_changed":
            short_changed_candidates += 1
        elif reason == "ttl_refresh":
            ttl_refresh_candidates += 1

    candidates_with_state.sort(
        key=lambda item: (
            DETAIL_REASON_PRIORITY[item[0].reason],
            -int(item[1].last_seen_at.timestamp()),
            str(item[0].vacancy_id),
        )
    )
    selected_candidates = tuple(
        candidate for candidate, _ in candidates_with_state[: command.limit]
    )

    return SelectDetailCandidatesResult(
        crawl_run_id=command.crawl_run_id,
        observed_vacancy_count=len(current_states),
        eligible_candidates_count=len(candidates_with_state),
        selected_candidates=selected_candidates,
        skipped_due_to_limit=max(len(candidates_with_state) - len(selected_candidates), 0),
        first_seen_candidates=first_seen_candidates,
        short_changed_candidates=short_changed_candidates,
        ttl_refresh_candidates=ttl_refresh_candidates,
    )


def _decide_candidate_reason(
    *,
    vacancy_state: VacancyCurrentState,
    previous_short_hash: str | None,
    ttl_cutoff: datetime,
) -> str | None:
    if (
        vacancy_state.last_detail_fetched_at is None
        or vacancy_state.detail_fetch_status != DetailFetchStatus.SUCCEEDED.value
    ):
        return "first_seen"

    if (
        previous_short_hash is not None
        and vacancy_state.last_short_hash is not None
        and previous_short_hash != vacancy_state.last_short_hash
    ):
        return "short_changed"

    if vacancy_state.last_detail_fetched_at <= ttl_cutoff:
        return "ttl_refresh"

    return None
