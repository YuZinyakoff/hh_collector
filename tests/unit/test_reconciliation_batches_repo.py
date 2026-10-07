from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from hhru_platform.domain.entities.vacancy_current_state import (
    VacancyCurrentStateReconciliationUpdate,
)
from hhru_platform.infrastructure.db.models.vacancy_current_state import VacancyCurrentState
from hhru_platform.infrastructure.db.models.vacancy_seen_event import VacancySeenEvent
from hhru_platform.infrastructure.db.repositories.vacancy_current_state_repo import (
    SqlAlchemyVacancyCurrentStateRepository,
)
from hhru_platform.infrastructure.db.repositories.vacancy_seen_event_repo import (
    SqlAlchemyVacancySeenEventRepository,
)


def test_sql_batches_bulk_updates_and_rollback() -> None:
    engine = create_engine("sqlite://")
    VacancyCurrentState.__table__.create(engine)
    VacancySeenEvent.__table__.create(engine)
    now = datetime.now(UTC)
    ids = [UUID(f"a0000000-0000-0000-0000-{index:012x}") for index in range(1, 6)]
    run_id, other_run = uuid4(), uuid4()
    with Session(engine) as session:
        session.add_all(
            [
                VacancyCurrentState(
                    vacancy_id=key,
                    first_seen_at=now,
                    last_seen_at=now,
                    consecutive_missing_runs=1,
                    last_short_hash="preserved",
                )
                for key in ids
            ]
        )
        session.add_all(
            [
                VacancySeenEvent(
                    id=index,
                    vacancy_id=key,
                    crawl_run_id=event_run,
                    crawl_partition_id=uuid4(),
                    seen_at=now,
                    short_hash="hash",
                )
                for index, (key, event_run) in enumerate(
                    [(ids[0], run_id), (ids[0], run_id), (ids[4], run_id), (ids[1], other_run)], 1
                )
            ]
        )
        session.commit()
        states = SqlAlchemyVacancyCurrentStateRepository(session)
        events = SqlAlchemyVacancySeenEventRepository(session)
        assert events.count_distinct_vacancy_ids_by_run(run_id) == 2
        assert events.list_observed_vacancy_ids(crawl_run_id=run_id, vacancy_ids=[]) == []
        assert events.list_observed_vacancy_ids(crawl_run_id=run_id, vacancy_ids=ids[:2]) == [
            ids[0]
        ]
        with pytest.raises(ValueError):
            states.list_reconciliation_batch(after_vacancy_id=None, limit=0)

        cursor = None
        visited = []
        while batch := states.list_reconciliation_batch(after_vacancy_id=cursor, limit=2):
            visited.extend(item.vacancy_id for item in batch)
            states.apply_reconciliation_updates(
                updated_at=now,
                updates=[
                    VacancyCurrentStateReconciliationUpdate(
                        vacancy_id=item.vacancy_id,
                        consecutive_missing_runs=2,
                        is_probably_inactive=True,
                        last_seen_run_id=None,
                    )
                    for item in batch
                ],
            )
            assert not session.dirty
            assert len(session.identity_map) <= 2
            cursor = batch[-1].vacancy_id
        assert visited == ids
        assert states.apply_reconciliation_updates(updated_at=now, updates=[]) == 0
        assert session.scalar(select(VacancyCurrentState.consecutive_missing_runs).limit(1)) == 2
        session.rollback()
        assert (
            list(session.scalars(select(VacancyCurrentState.consecutive_missing_runs))) == [1] * 5
        )
        assert (
            list(session.scalars(select(VacancyCurrentState.last_short_hash))) == ["preserved"] * 5
        )
        with pytest.raises(LookupError, match="not found"):
            states.apply_reconciliation_updates(
                updated_at=now,
                updates=[
                    VacancyCurrentStateReconciliationUpdate(
                        vacancy_id=uuid4(),
                        consecutive_missing_runs=1,
                        is_probably_inactive=False,
                        last_seen_run_id=None,
                    )
                ],
            )
        session.rollback()
    engine.dispose()
