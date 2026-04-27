from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

from hhru_platform.application.commands.drain_first_detail_backlog import (
    FIRST_DETAIL_BACKLOG_REASON,
    DrainFirstDetailBacklogCommand,
    drain_first_detail_backlog,
)
from hhru_platform.application.commands.fetch_vacancy_detail import (
    FetchVacancyDetailCommand,
    FetchVacancyDetailResult,
)
from hhru_platform.domain.entities.vacancy_current_state import VacancyCurrentState
from hhru_platform.domain.value_objects.enums import DetailFetchStatus


class InMemoryVacancyCurrentStateRepository:
    def __init__(
        self,
        states: list[VacancyCurrentState],
        *,
        cooling_down_vacancy_ids: set[UUID] | None = None,
    ) -> None:
        self._states = list(states)
        self._cooling_down_vacancy_ids = cooling_down_vacancy_ids or set()

    def count_first_detail_backlog(self, *, include_inactive: bool) -> int:
        return len(
            [
                state
                for state in self._states
                if _is_first_detail_backlog_item(
                    state,
                    include_inactive=include_inactive,
                )
            ]
        )

    def count_first_detail_backlog_ready(
        self,
        *,
        include_inactive: bool,
        retry_cooldown_seconds: int,
        max_retry_cooldown_seconds: int,
        now: datetime,
    ) -> int:
        return len(
            self.list_first_detail_backlog(
                limit=len(self._states),
                include_inactive=include_inactive,
                retry_cooldown_seconds=retry_cooldown_seconds,
                max_retry_cooldown_seconds=max_retry_cooldown_seconds,
                now=now,
            )
        )

    def list_first_detail_backlog(
        self,
        *,
        limit: int,
        include_inactive: bool,
        retry_cooldown_seconds: int,
        max_retry_cooldown_seconds: int,
        now: datetime,
    ) -> list[VacancyCurrentState]:
        candidates = [
            state
            for state in self._states
            if _is_first_detail_backlog_item(
                state,
                include_inactive=include_inactive,
            )
            and (
                retry_cooldown_seconds <= 0
                or state.vacancy_id not in self._cooling_down_vacancy_ids
            )
        ]
        candidates.sort(key=lambda state: (state.first_seen_at, str(state.vacancy_id)))
        return candidates[:limit]

    def mark_detail_succeeded(self, vacancy_id: UUID, recorded_at: datetime) -> None:
        for state in self._states:
            if state.vacancy_id == vacancy_id:
                state.last_detail_fetched_at = recorded_at
                state.detail_fetch_status = DetailFetchStatus.SUCCEEDED.value
                state.last_detail_hash = f"detail-{vacancy_id}"
                return
        raise AssertionError(f"state not found: {vacancy_id}")

    def mark_detail_terminal_404(self, vacancy_id: UUID, recorded_at: datetime) -> None:
        for state in self._states:
            if state.vacancy_id == vacancy_id:
                state.last_detail_fetched_at = recorded_at
                state.detail_fetch_status = DetailFetchStatus.TERMINAL_404.value
                return
        raise AssertionError(f"state not found: {vacancy_id}")


class InMemoryDetailFetchAttemptRepository:
    def __init__(self, latest_attempt_numbers: dict[UUID, int] | None = None) -> None:
        self._latest_attempt_numbers = latest_attempt_numbers or {}

    def latest_attempt_numbers_by_vacancy_ids(
        self,
        vacancy_ids: list[UUID],
    ) -> dict[UUID, int]:
        return {
            vacancy_id: attempt
            for vacancy_id, attempt in self._latest_attempt_numbers.items()
            if vacancy_id in vacancy_ids
        }


def test_drain_first_detail_backlog_fetches_limited_batch_and_recounts() -> None:
    now = datetime(2026, 4, 25, 12, 0, tzinfo=UTC)
    vacancy_one = uuid4()
    vacancy_two = uuid4()
    vacancy_three = uuid4()
    current_state_repository = InMemoryVacancyCurrentStateRepository(
        [
            _build_state(vacancy_id=vacancy_one, first_seen_at=now - timedelta(days=3)),
            _build_state(vacancy_id=vacancy_two, first_seen_at=now - timedelta(days=2)),
            _build_state(vacancy_id=vacancy_three, first_seen_at=now - timedelta(days=1)),
        ]
    )
    attempt_repository = InMemoryDetailFetchAttemptRepository({vacancy_one: 2})
    commands: list[FetchVacancyDetailCommand] = []

    def fetch_step(command: FetchVacancyDetailCommand) -> FetchVacancyDetailResult:
        commands.append(command)
        current_state_repository.mark_detail_succeeded(command.vacancy_id, now)
        return _build_detail_result(command.vacancy_id)

    result = drain_first_detail_backlog(
        DrainFirstDetailBacklogCommand(limit=2, triggered_by="pytest"),
        vacancy_current_state_repository=current_state_repository,
        detail_fetch_attempt_repository=attempt_repository,
        fetch_vacancy_detail_step=fetch_step,
    )

    assert result.status == "succeeded"
    assert result.backlog_size_before == 3
    assert result.selected_count == 2
    assert result.detail_fetch_attempted == 2
    assert result.detail_fetch_succeeded == 2
    assert result.detail_fetch_failed == 0
    assert result.ready_backlog_size_before == 3
    assert result.cooldown_skipped_before == 0
    assert result.backlog_size_after == 1
    assert result.ready_backlog_size_after == 1
    assert [
        (
            command.vacancy_id,
            command.reason,
            command.attempt,
            command.crawl_run_id,
        )
        for command in commands
    ] == [
        (vacancy_one, FIRST_DETAIL_BACKLOG_REASON, 3, None),
        (vacancy_two, FIRST_DETAIL_BACKLOG_REASON, 1, None),
    ]


def test_drain_first_detail_backlog_continues_after_one_item_exception() -> None:
    now = datetime(2026, 4, 25, 12, 0, tzinfo=UTC)
    failing_vacancy = uuid4()
    succeeding_vacancy = uuid4()
    current_state_repository = InMemoryVacancyCurrentStateRepository(
        [
            _build_state(vacancy_id=failing_vacancy, first_seen_at=now - timedelta(days=2)),
            _build_state(vacancy_id=succeeding_vacancy, first_seen_at=now - timedelta(days=1)),
        ]
    )

    def fetch_step(command: FetchVacancyDetailCommand) -> FetchVacancyDetailResult:
        if command.vacancy_id == failing_vacancy:
            raise RuntimeError("detail endpoint unavailable")
        current_state_repository.mark_detail_succeeded(command.vacancy_id, now)
        return _build_detail_result(command.vacancy_id)

    result = drain_first_detail_backlog(
        DrainFirstDetailBacklogCommand(limit=10, triggered_by="pytest"),
        vacancy_current_state_repository=current_state_repository,
        detail_fetch_attempt_repository=InMemoryDetailFetchAttemptRepository(),
        fetch_vacancy_detail_step=fetch_step,
    )

    assert result.status == "completed_with_failures"
    assert result.backlog_size_before == 2
    assert result.selected_count == 2
    assert result.detail_fetch_succeeded == 1
    assert result.detail_fetch_failed == 1
    assert result.backlog_size_after == 1
    assert result.item_results[0].vacancy_id == failing_vacancy
    assert result.item_results[0].exception_type == "RuntimeError"
    assert result.item_results[0].error_message == "detail endpoint unavailable"
    assert result.item_results[1].vacancy_id == succeeding_vacancy
    assert result.item_results[1].succeeded is True


def test_drain_first_detail_backlog_treats_terminal_404_as_resolved() -> None:
    now = datetime(2026, 4, 25, 12, 0, tzinfo=UTC)
    terminal_vacancy = uuid4()
    current_state_repository = InMemoryVacancyCurrentStateRepository(
        [_build_state(vacancy_id=terminal_vacancy, first_seen_at=now)]
    )

    def fetch_step(command: FetchVacancyDetailCommand) -> FetchVacancyDetailResult:
        current_state_repository.mark_detail_terminal_404(command.vacancy_id, now)
        return _build_detail_result(
            command.vacancy_id,
            status=DetailFetchStatus.TERMINAL_404.value,
            snapshot_id=None,
            error_message="Unexpected status code: 404",
        )

    result = drain_first_detail_backlog(
        DrainFirstDetailBacklogCommand(limit=10, triggered_by="pytest"),
        vacancy_current_state_repository=current_state_repository,
        detail_fetch_attempt_repository=InMemoryDetailFetchAttemptRepository(),
        fetch_vacancy_detail_step=fetch_step,
    )

    assert result.status == "succeeded"
    assert result.detail_fetch_succeeded == 0
    assert result.detail_fetch_terminal == 1
    assert result.detail_fetch_failed == 0
    assert result.backlog_size_after == 0
    assert result.item_results[0].terminal is True


def test_drain_first_detail_backlog_skips_recent_failed_items_in_cooldown() -> None:
    now = datetime(2026, 4, 25, 12, 0, tzinfo=UTC)
    cooling_down_vacancy = uuid4()
    ready_vacancy = uuid4()
    current_state_repository = InMemoryVacancyCurrentStateRepository(
        [
            _build_state(vacancy_id=cooling_down_vacancy, first_seen_at=now - timedelta(days=2)),
            _build_state(vacancy_id=ready_vacancy, first_seen_at=now - timedelta(days=1)),
        ],
        cooling_down_vacancy_ids={cooling_down_vacancy},
    )
    commands: list[FetchVacancyDetailCommand] = []

    def fetch_step(command: FetchVacancyDetailCommand) -> FetchVacancyDetailResult:
        commands.append(command)
        current_state_repository.mark_detail_succeeded(command.vacancy_id, now)
        return _build_detail_result(command.vacancy_id)

    result = drain_first_detail_backlog(
        DrainFirstDetailBacklogCommand(
            limit=10,
            triggered_by="pytest",
            retry_cooldown_seconds=3600,
            max_retry_cooldown_seconds=86400,
        ),
        vacancy_current_state_repository=current_state_repository,
        detail_fetch_attempt_repository=InMemoryDetailFetchAttemptRepository(),
        fetch_vacancy_detail_step=fetch_step,
    )

    assert result.status == "succeeded"
    assert result.backlog_size_before == 2
    assert result.ready_backlog_size_before == 1
    assert result.cooldown_skipped_before == 1
    assert result.selected_count == 1
    assert result.backlog_size_after == 1
    assert result.ready_backlog_size_after == 0
    assert result.cooldown_skipped_after == 1
    assert [command.vacancy_id for command in commands] == [ready_vacancy]


def _is_first_detail_backlog_item(
    state: VacancyCurrentState,
    *,
    include_inactive: bool,
) -> bool:
    if state.is_probably_inactive and not include_inactive:
        return False
    if state.detail_fetch_status == DetailFetchStatus.TERMINAL_404.value:
        return False
    return not (
        state.last_detail_fetched_at is not None
        and state.detail_fetch_status == DetailFetchStatus.SUCCEEDED.value
    )


def _build_state(*, vacancy_id: UUID, first_seen_at: datetime) -> VacancyCurrentState:
    return VacancyCurrentState(
        vacancy_id=vacancy_id,
        first_seen_at=first_seen_at,
        last_seen_at=first_seen_at + timedelta(hours=1),
        seen_count=1,
        consecutive_missing_runs=0,
        is_probably_inactive=False,
        last_seen_run_id=uuid4(),
        last_short_hash=f"short-{vacancy_id}",
        last_detail_hash=None,
        last_detail_fetched_at=None,
        detail_fetch_status=DetailFetchStatus.NOT_REQUESTED.value,
        updated_at=first_seen_at,
    )


def _build_detail_result(
    vacancy_id: UUID,
    *,
    status: str = DetailFetchStatus.SUCCEEDED.value,
    snapshot_id: int | None = 1,
    error_message: str | None = None,
) -> FetchVacancyDetailResult:
    return FetchVacancyDetailResult(
        vacancy_id=vacancy_id,
        hh_vacancy_id=f"hh-{vacancy_id}",
        detail_fetch_status=status,
        snapshot_id=snapshot_id,
        request_log_id=2,
        raw_payload_id=3,
        detail_fetch_attempt_id=4,
        error_message=error_message,
    )
