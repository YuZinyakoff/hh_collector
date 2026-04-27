from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from sqlalchemy import select, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from hhru_platform.application.commands.reconcile_run import (
    ReconcileRunCommand,
    reconcile_run,
)
from hhru_platform.application.policies.reconciliation import (
    MissingRunsReconciliationPolicyV1,
)
from hhru_platform.domain.entities.vacancy_current_state import (
    VacancyCurrentState,
    VacancyCurrentStateReconciliationUpdate,
)
from hhru_platform.infrastructure.db.models.vacancy_current_state import (
    VacancyCurrentState as VacancyCurrentStateModel,
)
from hhru_platform.infrastructure.db.repositories import (
    SqlAlchemyCrawlPartitionRepository,
    SqlAlchemyCrawlRunRepository,
    SqlAlchemyVacancyCurrentStateRepository,
    SqlAlchemyVacancySeenEventRepository,
)
from hhru_platform.infrastructure.db.session import (
    create_engine_from_settings,
)

TEST_TRIGGERED_BY = "pytest-reconcile-run"


def _database_is_available() -> bool:
    engine = create_engine_from_settings()
    try:
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
    except OperationalError:
        return False
    finally:
        engine.dispose()

    return True


pytestmark = pytest.mark.skipif(
    not _database_is_available(),
    reason="PostgreSQL is not available for integration tests.",
)


def test_reconcile_run_updates_current_state_and_completes_run() -> None:
    engine = create_engine_from_settings()
    crawl_run_id = uuid4()
    crawl_partition_id = uuid4()
    seen_previous_run_id = uuid4()
    missing_previous_run_id = uuid4()
    seen_vacancy_id = uuid4()
    missing_vacancy_id = uuid4()
    seen_hh_vacancy_id = f"pytest-reconcile-seen-{seen_vacancy_id}"
    missing_hh_vacancy_id = f"pytest-reconcile-missing-{missing_vacancy_id}"

    try:
        with engine.connect() as connection:
            transaction = connection.begin()
            session = Session(
                bind=connection,
                autoflush=False,
                autocommit=False,
                expire_on_commit=False,
            )
            try:
                crawl_run_repository = SqlAlchemyCrawlRunRepository(session)
                crawl_partition_repository = SqlAlchemyCrawlPartitionRepository(session)
                vacancy_seen_event_repository = SqlAlchemyVacancySeenEventRepository(session)

                session.execute(
                    text(
                        """
                        INSERT INTO crawl_run (
                            id,
                            run_type,
                            status,
                            started_at,
                            triggered_by,
                            config_snapshot_json,
                            partitions_total,
                            partitions_done,
                            partitions_failed
                        )
                        VALUES
                            (
                                :crawl_run_id,
                                'weekly_sweep',
                                'created',
                                :started_at,
                                :triggered_by,
                                '{}'::jsonb,
                                1,
                                0,
                                0
                            ),
                            (
                                :seen_previous_run_id,
                                'weekly_sweep',
                                'completed',
                                :seen_previous_started_at,
                                'pytest-history',
                                '{}'::jsonb,
                                1,
                                1,
                                0
                            ),
                            (
                                :missing_previous_run_id,
                                'weekly_sweep',
                                'completed',
                                :missing_previous_started_at,
                                'pytest-history',
                                '{}'::jsonb,
                                1,
                                1,
                                0
                            )
                        """
                    ),
                    {
                        "crawl_run_id": crawl_run_id,
                        "started_at": datetime(2026, 3, 12, 11, 55, tzinfo=UTC),
                        "triggered_by": TEST_TRIGGERED_BY,
                        "seen_previous_run_id": seen_previous_run_id,
                        "seen_previous_started_at": datetime(2026, 3, 10, 11, 55, tzinfo=UTC),
                        "missing_previous_run_id": missing_previous_run_id,
                        "missing_previous_started_at": datetime(
                            2026,
                            3,
                            11,
                            11,
                            55,
                            tzinfo=UTC,
                        ),
                    },
                )
                session.execute(
                    text(
                        """
                        INSERT INTO crawl_partition (
                            id,
                            crawl_run_id,
                            parent_partition_id,
                            partition_key,
                            scope_key,
                            params_json,
                            status,
                            depth,
                            split_dimension,
                            split_value,
                            planner_policy_version,
                            is_terminal,
                            is_saturated,
                            coverage_status,
                            pages_total_expected,
                            pages_processed,
                            items_seen,
                            retry_count,
                            started_at,
                            finished_at,
                            created_at
                        )
                        VALUES (
                            :crawl_partition_id,
                            :crawl_run_id,
                            NULL,
                            'pytest-reconcile',
                            'pytest-reconcile',
                            '{"planner_policy":"single_partition_v1"}'::jsonb,
                            'done',
                            0,
                            NULL,
                            NULL,
                            'v1',
                            TRUE,
                            FALSE,
                            'unassessed',
                            1,
                            1,
                            1,
                            0,
                            :started_at,
                            :finished_at,
                            :created_at
                        )
                        """
                    ),
                    {
                        "crawl_partition_id": crawl_partition_id,
                        "crawl_run_id": crawl_run_id,
                        "started_at": datetime(2026, 3, 12, 11, 56, tzinfo=UTC),
                        "finished_at": datetime(2026, 3, 12, 11, 57, tzinfo=UTC),
                        "created_at": datetime(2026, 3, 12, 11, 55, tzinfo=UTC),
                    },
                )

                session.execute(
                    text(
                        """
                        INSERT INTO vacancy (id, hh_vacancy_id, name_current, source_type)
                        VALUES
                            (:seen_vacancy_id, :seen_hh_vacancy_id, 'Seen vacancy', 'hh_api'),
                            (
                                :missing_vacancy_id,
                                :missing_hh_vacancy_id,
                                'Missing vacancy',
                                'hh_api'
                            )
                        """
                    ),
                    {
                        "seen_vacancy_id": seen_vacancy_id,
                        "seen_hh_vacancy_id": seen_hh_vacancy_id,
                        "missing_vacancy_id": missing_vacancy_id,
                        "missing_hh_vacancy_id": missing_hh_vacancy_id,
                    },
                )
                session.execute(
                    text(
                        """
                        INSERT INTO vacancy_current_state (
                            vacancy_id,
                            first_seen_at,
                            last_seen_at,
                            seen_count,
                            consecutive_missing_runs,
                            is_probably_inactive,
                            last_seen_run_id,
                            detail_fetch_status,
                            updated_at
                        )
                        VALUES
                            (
                                :seen_vacancy_id,
                                :seen_first_seen_at,
                                :seen_last_seen_at,
                                3,
                                4,
                                TRUE,
                                :seen_previous_run_id,
                                'not_requested',
                                :seen_updated_at
                            ),
                            (
                                :missing_vacancy_id,
                                :missing_first_seen_at,
                                :missing_last_seen_at,
                                5,
                                1,
                                FALSE,
                                :missing_previous_run_id,
                                'not_requested',
                                :missing_updated_at
                            )
                        """
                    ),
                    {
                        "seen_vacancy_id": seen_vacancy_id,
                        "seen_first_seen_at": datetime(2026, 3, 10, 10, 0, tzinfo=UTC),
                        "seen_last_seen_at": datetime(2026, 3, 11, 10, 0, tzinfo=UTC),
                        "seen_previous_run_id": seen_previous_run_id,
                        "seen_updated_at": datetime(2026, 3, 11, 10, 0, tzinfo=UTC),
                        "missing_vacancy_id": missing_vacancy_id,
                        "missing_first_seen_at": datetime(2026, 3, 9, 10, 0, tzinfo=UTC),
                        "missing_last_seen_at": datetime(2026, 3, 11, 10, 0, tzinfo=UTC),
                        "missing_previous_run_id": missing_previous_run_id,
                        "missing_updated_at": datetime(2026, 3, 11, 10, 0, tzinfo=UTC),
                    },
                )
                session.execute(
                    text(
                        """
                        INSERT INTO vacancy_seen_event (
                            vacancy_id,
                            crawl_run_id,
                            crawl_partition_id,
                            seen_at,
                            list_position,
                            short_hash
                        )
                        VALUES (
                            :vacancy_id,
                            :crawl_run_id,
                            :crawl_partition_id,
                            :seen_at,
                            0,
                            :short_hash
                        )
                        """
                    ),
                    {
                        "vacancy_id": seen_vacancy_id,
                        "crawl_run_id": crawl_run_id,
                        "crawl_partition_id": crawl_partition_id,
                        "seen_at": datetime(2026, 3, 12, 12, 0, tzinfo=UTC),
                        "short_hash": "pytest-reconcile-short-hash",
                    },
                )

                result = reconcile_run(
                    ReconcileRunCommand(crawl_run_id=crawl_run_id),
                    crawl_run_repository=crawl_run_repository,
                    crawl_partition_repository=crawl_partition_repository,
                    vacancy_seen_event_repository=vacancy_seen_event_repository,
                    vacancy_current_state_repository=_ScopedVacancyCurrentStateRepository(
                        session=session,
                        vacancy_ids=[seen_vacancy_id, missing_vacancy_id],
                    ),
                    reconciliation_policy=MissingRunsReconciliationPolicyV1(),
                )
                session.flush()

                assert result.run_status == "completed"
                assert result.observed_in_run_count == 1

                current_state_rows = (
                    connection.execute(
                        text(
                            """
                        SELECT vacancy_id::text AS vacancy_id,
                               consecutive_missing_runs,
                               is_probably_inactive,
                               last_seen_run_id::text AS last_seen_run_id
                        FROM vacancy_current_state
                        WHERE vacancy_id IN (:seen_vacancy_id, :missing_vacancy_id)
                        ORDER BY vacancy_id
                        """
                        ),
                        {
                            "seen_vacancy_id": seen_vacancy_id,
                            "missing_vacancy_id": missing_vacancy_id,
                        },
                    )
                    .mappings()
                    .all()
                )
                crawl_run_row = (
                    connection.execute(
                        text(
                            """
                        SELECT status, finished_at, partitions_done, partitions_failed
                        FROM crawl_run
                        WHERE id = :crawl_run_id
                        """
                        ),
                        {"crawl_run_id": crawl_run_id},
                    )
                    .mappings()
                    .one()
                )

                rows_by_vacancy_id = {row["vacancy_id"]: row for row in current_state_rows}
                assert rows_by_vacancy_id[str(seen_vacancy_id)]["consecutive_missing_runs"] == 0
                assert rows_by_vacancy_id[str(seen_vacancy_id)]["is_probably_inactive"] is False
                assert rows_by_vacancy_id[str(seen_vacancy_id)]["last_seen_run_id"] == str(
                    crawl_run_id
                )
                assert rows_by_vacancy_id[str(missing_vacancy_id)]["consecutive_missing_runs"] == 2
                assert rows_by_vacancy_id[str(missing_vacancy_id)]["is_probably_inactive"] is True
                assert crawl_run_row["status"] == "completed"
                assert crawl_run_row["finished_at"] is not None
                assert crawl_run_row["partitions_done"] == 1
                assert crawl_run_row["partitions_failed"] == 0
            finally:
                session.close()
                transaction.rollback()
    finally:
        engine.dispose()


class _ScopedVacancyCurrentStateRepository:
    def __init__(self, *, session: Session, vacancy_ids: list[UUID]) -> None:
        self._session = session
        self._vacancy_ids = list(vacancy_ids)
        self._delegate = SqlAlchemyVacancyCurrentStateRepository(session)

    def list_all(self) -> list[VacancyCurrentState]:
        statement = (
            select(VacancyCurrentStateModel)
            .where(VacancyCurrentStateModel.vacancy_id.in_(tuple(self._vacancy_ids)))
            .order_by(VacancyCurrentStateModel.vacancy_id)
        )
        return [
            SqlAlchemyVacancyCurrentStateRepository._to_entity(model)
            for model in self._session.scalars(statement)
        ]

    def apply_reconciliation_updates(
        self,
        *,
        updated_at: datetime,
        updates: list[VacancyCurrentStateReconciliationUpdate],
    ) -> int:
        return self._delegate.apply_reconciliation_updates(
            updated_at=updated_at,
            updates=updates,
        )
