from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest

from hhru_platform.application.commands.reconcile_run import (
    CrawlRunNotFoundError,
    ReconcileRunCommand,
    ReconcileRunResult,
    reconcile_run,
)
from hhru_platform.application.policies.reconciliation import (
    MissingRunsReconciliationPolicyV1,
)
from hhru_platform.domain.entities.crawl_partition import CrawlPartition
from hhru_platform.domain.entities.crawl_run import CrawlRun
from hhru_platform.domain.entities.vacancy_current_state import (
    VacancyCurrentState,
    VacancyCurrentStateReconciliationUpdate,
)


class InMemoryCrawlRunRepository:
    def __init__(self, crawl_run: CrawlRun | None) -> None:
        self._crawl_run = crawl_run

    def get(self, run_id: UUID) -> CrawlRun | None:
        if self._crawl_run is None or self._crawl_run.id != run_id:
            return None
        return self._crawl_run

    def complete(
        self,
        *,
        run_id: UUID,
        status: str,
        finished_at: datetime,
        partitions_done: int,
        partitions_failed: int,
        notes: str | None = None,
    ) -> CrawlRun:
        assert self._crawl_run is not None
        assert self._crawl_run.id == run_id
        self._crawl_run.status = status
        self._crawl_run.finished_at = finished_at
        self._crawl_run.partitions_done = partitions_done
        self._crawl_run.partitions_failed = partitions_failed
        self._crawl_run.notes = notes
        return self._crawl_run


class InMemoryCrawlPartitionRepository:
    def __init__(self, partitions: list[CrawlPartition]) -> None:
        self._partitions = partitions

    def list_by_run_id(self, run_id: UUID) -> list[CrawlPartition]:
        return [partition for partition in self._partitions if partition.crawl_run_id == run_id]


class InMemoryVacancySeenEventRepository:
    def __init__(self, observed_vacancy_ids: list[UUID]) -> None:
        self._observed_vacancy_ids = observed_vacancy_ids

    def count_distinct_vacancy_ids_by_run(self, crawl_run_id: UUID) -> int:
        return len(set(self._observed_vacancy_ids))

    def list_observed_vacancy_ids(
        self, *, crawl_run_id: UUID, vacancy_ids: list[UUID]
    ) -> list[UUID]:
        return list(set(vacancy_ids).intersection(self._observed_vacancy_ids))


class InMemoryVacancyCurrentStateRepository:
    def __init__(self, current_states: list[VacancyCurrentState]) -> None:
        self._current_states = {state.vacancy_id: state for state in current_states}
        self.applied_updates: list[VacancyCurrentStateReconciliationUpdate] = []

    def list_reconciliation_batch(
        self, *, after_vacancy_id: UUID | None, limit: int
    ) -> list[VacancyCurrentState]:
        return [
            self._current_states[key]
            for key in sorted(self._current_states)
            if after_vacancy_id is None or key > after_vacancy_id
        ][:limit]

    def apply_reconciliation_updates(
        self,
        *,
        updated_at: datetime,
        updates: list[VacancyCurrentStateReconciliationUpdate],
    ) -> int:
        self.applied_updates = list(updates)
        for update in updates:
            state = self._current_states[update.vacancy_id]
            state.consecutive_missing_runs = update.consecutive_missing_runs
            state.is_probably_inactive = update.is_probably_inactive
            state.last_seen_run_id = update.last_seen_run_id
            state.updated_at = updated_at
        return len(updates)


class RecordingRunTerminalStatusMetricsRecorder:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def record_run_terminal_status(self, **kwargs) -> None:
        self.calls.append(kwargs)


def _build_crawl_run() -> CrawlRun:
    return CrawlRun(
        id=uuid4(),
        run_type="weekly_sweep",
        status="created",
        started_at=datetime(2026, 3, 12, 12, 0, tzinfo=UTC),
        finished_at=None,
        triggered_by="pytest",
        config_snapshot_json={},
        partitions_total=2,
        partitions_done=0,
        partitions_failed=0,
        notes=None,
    )


def _build_partition(*, crawl_run_id: UUID, status: str, key: str) -> CrawlPartition:
    return CrawlPartition(
        id=uuid4(),
        crawl_run_id=crawl_run_id,
        partition_key=key,
        params_json={"planner_policy": "single_partition_v1"},
        status=status,
        pages_total_expected=1,
        pages_processed=1 if status in {"done", "failed"} else 0,
        items_seen=1 if status == "done" else 0,
        retry_count=0,
        started_at=datetime(2026, 3, 12, 12, 5, tzinfo=UTC),
        finished_at=datetime(2026, 3, 12, 12, 6, tzinfo=UTC)
        if status in {"done", "failed"}
        else None,
        last_error_message="boom" if status == "failed" else None,
        created_at=datetime(2026, 3, 12, 12, 4, tzinfo=UTC),
    )


def _build_current_state(
    *,
    vacancy_id: UUID,
    consecutive_missing_runs: int,
    is_probably_inactive: bool,
    last_seen_run_id: UUID | None,
) -> VacancyCurrentState:
    return VacancyCurrentState(
        vacancy_id=vacancy_id,
        first_seen_at=datetime(2026, 3, 1, 12, 0, tzinfo=UTC),
        last_seen_at=datetime(2026, 3, 11, 12, 0, tzinfo=UTC),
        seen_count=3,
        consecutive_missing_runs=consecutive_missing_runs,
        is_probably_inactive=is_probably_inactive,
        last_seen_run_id=last_seen_run_id,
        last_short_hash="short-hash",
        last_detail_hash=None,
        last_detail_fetched_at=None,
        detail_fetch_status="not_requested",
        updated_at=datetime(2026, 3, 11, 12, 0, tzinfo=UTC),
    )


def test_reconcile_run_updates_seen_and_missing_vacancy_state_and_completes_run() -> None:
    crawl_run = _build_crawl_run()
    previous_run_id = uuid4()
    seen_vacancy_id = uuid4()
    newly_inactive_vacancy_id = uuid4()
    still_active_missing_vacancy_id = uuid4()
    current_state_repository = InMemoryVacancyCurrentStateRepository(
        [
            _build_current_state(
                vacancy_id=seen_vacancy_id,
                consecutive_missing_runs=3,
                is_probably_inactive=True,
                last_seen_run_id=previous_run_id,
            ),
            _build_current_state(
                vacancy_id=newly_inactive_vacancy_id,
                consecutive_missing_runs=1,
                is_probably_inactive=False,
                last_seen_run_id=previous_run_id,
            ),
            _build_current_state(
                vacancy_id=still_active_missing_vacancy_id,
                consecutive_missing_runs=0,
                is_probably_inactive=False,
                last_seen_run_id=previous_run_id,
            ),
        ]
    )
    metrics_recorder = RecordingRunTerminalStatusMetricsRecorder()

    result = reconcile_run(
        ReconcileRunCommand(crawl_run_id=crawl_run.id),
        crawl_run_repository=InMemoryCrawlRunRepository(crawl_run),
        crawl_partition_repository=InMemoryCrawlPartitionRepository(
            [
                _build_partition(crawl_run_id=crawl_run.id, status="done", key="done"),
                _build_partition(crawl_run_id=crawl_run.id, status="failed", key="failed"),
            ]
        ),
        vacancy_seen_event_repository=InMemoryVacancySeenEventRepository([seen_vacancy_id]),
        vacancy_current_state_repository=current_state_repository,
        reconciliation_policy=MissingRunsReconciliationPolicyV1(),
        metrics_recorder=metrics_recorder,
    )

    seen_state = current_state_repository._current_states[seen_vacancy_id]
    newly_inactive_state = current_state_repository._current_states[newly_inactive_vacancy_id]
    still_active_missing_state = current_state_repository._current_states[
        still_active_missing_vacancy_id
    ]

    assert result.crawl_run_id == crawl_run.id
    assert result.observed_in_run_count == 1
    assert result.missing_updated_count == 2
    assert result.marked_inactive_count == 1
    assert result.run_status == "completed"
    assert crawl_run.status == "completed"
    assert crawl_run.finished_at is not None
    assert crawl_run.partitions_done == 1
    assert crawl_run.partitions_failed == 1
    assert seen_state.consecutive_missing_runs == 0
    assert seen_state.is_probably_inactive is False
    assert seen_state.last_seen_run_id == crawl_run.id
    assert newly_inactive_state.consecutive_missing_runs == 2
    assert newly_inactive_state.is_probably_inactive is True
    assert still_active_missing_state.consecutive_missing_runs == 1
    assert still_active_missing_state.is_probably_inactive is False
    assert metrics_recorder.calls == [
        {
            "run_type": "weekly_sweep",
            "status": "completed",
            "recorded_at": crawl_run.finished_at,
        }
    ]


def test_reconcile_run_raises_for_missing_crawl_run() -> None:
    with pytest.raises(CrawlRunNotFoundError):
        reconcile_run(
            ReconcileRunCommand(crawl_run_id=uuid4()),
            crawl_run_repository=InMemoryCrawlRunRepository(None),
            crawl_partition_repository=InMemoryCrawlPartitionRepository([]),
            vacancy_seen_event_repository=InMemoryVacancySeenEventRepository([]),
            vacancy_current_state_repository=InMemoryVacancyCurrentStateRepository([]),
            reconciliation_policy=MissingRunsReconciliationPolicyV1(),
        )


def test_reconciliation_pages_are_bounded_and_completed_run_cannot_repeat(monkeypatch) -> None:
    monkeypatch.setattr(
        "hhru_platform.application.commands.reconcile_run.RECONCILIATION_BATCH_SIZE", 2
    )
    crawl_run = _build_crawl_run()
    ids = [UUID(int=index) for index in range(1, 6)]

    class BoundedRepository(InMemoryVacancyCurrentStateRepository):
        batch_lengths: list[int] = []

        def apply_reconciliation_updates(self, *, updated_at, updates):
            assert len(updates) <= 2
            self.batch_lengths.append(len(updates))
            return super().apply_reconciliation_updates(updated_at=updated_at, updates=updates)

    repository = BoundedRepository(
        [
            _build_current_state(
                vacancy_id=key,
                consecutive_missing_runs=1,
                is_probably_inactive=False,
                last_seen_run_id=None,
            )
            for key in reversed(ids)
        ]
    )
    kwargs = dict(
        crawl_run_repository=InMemoryCrawlRunRepository(crawl_run),
        crawl_partition_repository=InMemoryCrawlPartitionRepository([]),
        vacancy_seen_event_repository=InMemoryVacancySeenEventRepository([ids[0], ids[4], ids[4]]),
        vacancy_current_state_repository=repository,
        reconciliation_policy=MissingRunsReconciliationPolicyV1(),
    )
    result = reconcile_run(ReconcileRunCommand(crawl_run_id=crawl_run.id), **kwargs)
    assert repository.batch_lengths == [2, 2, 1]
    assert result.observed_in_run_count == 2
    assert result.missing_updated_count == result.marked_inactive_count == 3
    assert [repository._current_states[key].consecutive_missing_runs for key in ids] == [
        0,
        2,
        2,
        2,
        0,
    ]
    with pytest.raises(ValueError, match="already finished"):
        reconcile_run(ReconcileRunCommand(crawl_run_id=crawl_run.id), **kwargs)
    assert repository.batch_lengths == [2, 2, 1]


def test_reconcile_empty_corpus_completes_run() -> None:
    crawl_run = _build_crawl_run()
    result = reconcile_run(
        ReconcileRunCommand(crawl_run_id=crawl_run.id),
        crawl_run_repository=InMemoryCrawlRunRepository(crawl_run),
        crawl_partition_repository=InMemoryCrawlPartitionRepository([]),
        vacancy_seen_event_repository=InMemoryVacancySeenEventRepository([]),
        vacancy_current_state_repository=InMemoryVacancyCurrentStateRepository([]),
        reconciliation_policy=MissingRunsReconciliationPolicyV1(),
    )
    assert result.observed_in_run_count == result.missing_updated_count == 0
    assert crawl_run.finished_at is not None


def test_failed_second_batch_rolls_back_and_retry_increments_missing_once(monkeypatch) -> None:
    from sqlalchemy import create_engine, select
    from sqlalchemy.orm import Session, sessionmaker

    from hhru_platform.infrastructure.db.models.vacancy_current_state import (
        VacancyCurrentState as StateModel,
    )
    from hhru_platform.infrastructure.db.repositories.vacancy_current_state_repo import (
        SqlAlchemyVacancyCurrentStateRepository,
    )
    from hhru_platform.infrastructure.db.session import session_scope

    monkeypatch.setattr(
        "hhru_platform.application.commands.reconcile_run.RECONCILIATION_BATCH_SIZE", 2
    )
    engine = create_engine("sqlite://")
    StateModel.__table__.create(engine)
    ids = [UUID(f"a0000000-0000-0000-0000-{index:012x}") for index in range(1, 6)]
    now = datetime.now(UTC)
    factory = sessionmaker(bind=engine, expire_on_commit=False)
    with factory.begin() as session:
        session.add_all(
            [
                StateModel(
                    vacancy_id=key, first_seen_at=now, last_seen_at=now, consecutive_missing_runs=0
                )
                for key in ids
            ]
        )
    run = _build_crawl_run()

    class FailingRepository(SqlAlchemyVacancyCurrentStateRepository):
        calls = 0

        def apply_reconciliation_updates(self, *, updated_at, updates):
            self.calls += 1
            if self.calls == 2:
                raise RuntimeError("injected batch failure")
            return super().apply_reconciliation_updates(updated_at=updated_at, updates=updates)

    def execute(session: Session, *, fail: bool) -> ReconcileRunResult:
        repository = (FailingRepository if fail else SqlAlchemyVacancyCurrentStateRepository)(
            session
        )
        return reconcile_run(
            ReconcileRunCommand(crawl_run_id=run.id),
            crawl_run_repository=InMemoryCrawlRunRepository(run),
            crawl_partition_repository=InMemoryCrawlPartitionRepository([]),
            vacancy_seen_event_repository=InMemoryVacancySeenEventRepository([ids[0]]),
            vacancy_current_state_repository=repository,
            reconciliation_policy=MissingRunsReconciliationPolicyV1(),
        )

    with pytest.raises(RuntimeError, match="injected batch failure"):
        with session_scope(factory) as session:
            execute(session, fail=True)
    assert run.status == "created"
    assert run.finished_at is None
    with factory() as session:
        assert list(session.scalars(select(StateModel.consecutive_missing_runs))) == [0] * 5
    with session_scope(factory) as session:
        result = execute(session, fail=False)
    assert result.missing_updated_count == 4
    with factory() as session:
        assert list(
            session.scalars(
                select(StateModel.consecutive_missing_runs).order_by(StateModel.vacancy_id)
            )
        ) == [0, 1, 1, 1, 1]
    engine.dispose()
