from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import select, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from hhru_platform.application.policies.reconciliation import MissingRunsReconciliationPolicyV1
from hhru_platform.infrastructure.db.admission import PostgresCollectionRunAdmissionController
from hhru_platform.infrastructure.db.models import (
    ApiRequestLog,
    CrawlPartition,
    CrawlRun,
    DetailFetchAttempt,
    RawApiPayload,
    Vacancy,
    VacancyCurrentState,
    VacancySeenEvent,
    VacancySnapshot,
)
from hhru_platform.infrastructure.db.repositories.housekeeping_repo import (
    SqlAlchemyHousekeepingRepository,
)
from hhru_platform.infrastructure.db.repositories.run_tree_metadata_repo import (
    SqlAlchemyRunTreeMetadataRepository,
)
from hhru_platform.infrastructure.db.repositories.vacancy_current_state_repo import (
    SqlAlchemyVacancyCurrentStateRepository,
)
from hhru_platform.infrastructure.db.session import create_engine_from_settings
from hhru_platform.infrastructure.research_archive.run_tree_metadata import (
    iter_bundle_records,
    records_digest,
    write_bundle,
)


@pytest.fixture
def retention_db():
    engine = create_engine_from_settings()
    try:
        connection = engine.connect()
    except OperationalError:
        engine.dispose()
        pytest.skip("PostgreSQL is not available for integration tests.")
    transaction = connection.begin()
    session = Session(bind=connection)
    now = datetime.now(UTC)
    old_run = CrawlRun(
        run_type="production_weekly_sweep",
        status="succeeded",
        started_at=now - timedelta(days=61),
        finished_at=now - timedelta(days=60),
    )
    recent_run = CrawlRun(
        run_type="production_weekly_sweep",
        status="succeeded",
        started_at=now - timedelta(days=2),
        finished_at=now - timedelta(days=1),
    )
    vacancy = Vacancy(hh_vacancy_id=f"retention-safety-{uuid4()}", name_current="preserved")
    session.add_all([old_run, recent_run, vacancy])
    session.flush()
    parent = CrawlPartition(
        crawl_run_id=old_run.id,
        partition_key="parent",
        scope_key="parent",
        status="split_done",
    )
    recent_partition = CrawlPartition(
        crawl_run_id=recent_run.id,
        partition_key="recent",
        scope_key="recent",
        status="done",
    )
    session.add_all([parent, recent_partition])
    session.flush()
    child = CrawlPartition(
        crawl_run_id=old_run.id,
        parent_partition_id=parent.id,
        partition_key="child",
        scope_key="child",
        status="done",
    )
    session.add(child)
    session.flush()
    request = ApiRequestLog(
        crawl_run_id=old_run.id,
        crawl_partition_id=child.id,
        request_type="vacancy_search",
        endpoint="/vacancies",
        status_code=200,
        latency_ms=1,
    )
    session.add(request)
    session.flush()
    payload = RawApiPayload(
        api_request_log_id=request.id,
        endpoint_type="vacancy_search",
        payload_json={"items": []},
        payload_hash="preserved",
    )
    session.add(payload)
    session.flush()
    session.add_all(
        [
            VacancySeenEvent(
                vacancy_id=vacancy.id,
                crawl_run_id=old_run.id,
                crawl_partition_id=child.id,
                short_hash="old",
                short_payload_ref_id=payload.id,
            ),
            VacancySeenEvent(
                vacancy_id=vacancy.id,
                crawl_run_id=recent_run.id,
                crawl_partition_id=recent_partition.id,
                short_hash="recent",
            ),
            VacancyCurrentState(
                vacancy_id=vacancy.id,
                first_seen_at=now - timedelta(days=61),
                last_seen_at=now,
                last_seen_run_id=old_run.id,
                seen_count=7,
                consecutive_missing_runs=2,
                is_probably_inactive=True,
                last_short_hash="preserved",
                detail_fetch_status="succeeded",
                last_detail_fetched_at=now,
            ),
            VacancySnapshot(
                vacancy_id=vacancy.id,
                snapshot_type="detail",
                crawl_run_id=old_run.id,
                normalized_json={"schema_version": 2},
                detail_payload_ref_id=payload.id,
            ),
            DetailFetchAttempt(
                vacancy_id=vacancy.id,
                crawl_run_id=old_run.id,
                reason="first_detail",
                status="succeeded",
            ),
        ]
    )
    session.flush()
    try:
        yield session, engine, old_run.id, recent_run.id, vacancy.id
    finally:
        session.close()
        if transaction.is_active:
            transaction.rollback()
        connection.close()
        engine.dispose()


def test_run_tree_cascade_preserves_vacancy_state_and_payloads(retention_db) -> None:
    session, _, old_id, recent_id, vacancy_id = retention_db
    repository = SqlAlchemyHousekeepingRepository(session)
    before = dict(session.execute(select(VacancyCurrentState.__table__)).mappings().one())
    assert repository.reserve_idle_collection_for_run_tree_retention()
    assert repository.lock_finished_crawl_run_ids_for_retention_bounded_by_seen_event_coverage(
        run_ids=[old_id],
        cutoff=datetime.now(UTC) - timedelta(days=30),
        max_seen_event_source_id=2**63 - 1,
    ) == [old_id]
    assert repository.delete_finished_crawl_runs([old_id]) == 1
    session.expire_all()

    after = dict(session.execute(select(VacancyCurrentState.__table__)).mappings().one())
    assert after == {**before, "last_seen_run_id": None}
    assert session.get(Vacancy, vacancy_id).name_current == "preserved"
    assert session.get(CrawlRun, recent_id).status == "succeeded"
    assert list(session.scalars(select(CrawlPartition.crawl_run_id))) == [recent_id]
    assert list(session.scalars(select(VacancySeenEvent.crawl_run_id))) == [recent_id]
    snapshot = session.scalars(select(VacancySnapshot)).one()
    assert snapshot.crawl_run_id is None
    assert snapshot.normalized_json == {"schema_version": 2}
    assert snapshot.detail_payload_ref_id is not None
    assert session.scalars(select(DetailFetchAttempt)).one().crawl_run_id is None
    request = session.scalars(select(ApiRequestLog)).one()
    assert request.crawl_run_id is None and request.crawl_partition_id is None
    assert session.scalars(select(RawApiPayload.payload_hash)).one() == "preserved"

    state = SqlAlchemyVacancyCurrentStateRepository(session).list_reconciliation_batch(
        after_vacancy_id=None,
        limit=1,
    )[0]
    policy = MissingRunsReconciliationPolicyV1()
    assert (
        policy.decide(
            vacancy_state=state,
            seen_in_run=False,
            crawl_run_id=recent_id,
        ).last_seen_run_id
        is None
    )
    seen_update = policy.decide(
        vacancy_state=state,
        seen_in_run=True,
        crawl_run_id=recent_id,
    )
    assert seen_update.last_seen_run_id == recent_id
    assert seen_update.consecutive_missing_runs == 0
    assert not seen_update.is_probably_inactive


def test_run_tree_delete_rollback_restores_cascades_and_nullified_links(retention_db) -> None:
    session, _, old_id, _, _ = retention_db
    before = dict(session.execute(select(VacancyCurrentState.__table__)).mappings().one())
    with pytest.raises(RuntimeError, match="forced failure"):
        with session.begin_nested():
            assert (
                SqlAlchemyHousekeepingRepository(session).delete_finished_crawl_runs([old_id]) == 1
            )
            raise RuntimeError("forced failure")
    session.expire_all()
    assert session.get(CrawlRun, old_id) is not None
    assert len(list(session.scalars(select(CrawlPartition)))) == 3
    assert len(list(session.scalars(select(VacancySeenEvent)))) == 2
    assert dict(session.execute(select(VacancyCurrentState.__table__)).mappings().one()) == before
    assert session.scalars(select(VacancySnapshot.crawl_run_id)).one() == old_id
    assert session.scalars(select(ApiRequestLog.crawl_partition_id)).one() is not None


def test_run_tree_guard_excludes_active_runs_and_uncovered_observations(retention_db) -> None:
    session, _, old_id, recent_id, _ = retention_db
    repository = SqlAlchemyHousekeepingRepository(session)
    cutoff = datetime.now(UTC) - timedelta(days=30)
    assert (
        repository.lock_finished_crawl_run_ids_for_retention_bounded_by_seen_event_coverage(
            run_ids=[old_id],
            cutoff=cutoff,
            max_seen_event_source_id=0,
        )
        == []
    )
    recent = session.get(CrawlRun, recent_id)
    recent.status = "created"
    recent.finished_at = None
    session.flush()
    assert not repository.reserve_idle_collection_for_run_tree_retention()


def test_run_tree_guard_serializes_with_scheduler_until_transaction_end(retention_db) -> None:
    session, engine, _, _, _ = retention_db
    controller = PostgresCollectionRunAdmissionController(lambda: Session(engine))
    lease = controller.acquire()
    assert lease is not None
    try:
        assert not SqlAlchemyHousekeepingRepository(
            session
        ).reserve_idle_collection_for_run_tree_retention()
    finally:
        lease.release()
    assert SqlAlchemyHousekeepingRepository(
        session
    ).reserve_idle_collection_for_run_tree_retention()
    assert controller.acquire() is None
    session.rollback()
    lease = controller.acquire()
    assert lease is not None
    lease.release()
    assert session.scalar(text("SELECT 1")) == 1


def test_snapshot_retention_preserves_latest_per_type_and_breaks_timestamp_ties(
    retention_db,
) -> None:
    session, _, old_id, _, vacancy_id = retention_db
    captured_at = datetime.now(UTC) - timedelta(days=60)
    older = VacancySnapshot(
        vacancy_id=vacancy_id,
        snapshot_type="short",
        crawl_run_id=old_id,
        captured_at=captured_at,
        normalized_json={"schema_version": 2},
    )
    session.add(older)
    session.flush()
    latest = VacancySnapshot(
        vacancy_id=vacancy_id,
        snapshot_type="short",
        crawl_run_id=old_id,
        captured_at=captured_at,
        normalized_json={"schema_version": 2},
    )
    session.add(latest)
    session.flush()
    repository = SqlAlchemyHousekeepingRepository(session)
    cutoff = datetime.now(UTC) - timedelta(days=30)
    assert repository.count_vacancy_snapshot_candidates(cutoff=cutoff) == 1
    candidates = repository.list_vacancy_snapshot_ids_for_retention(cutoff=cutoff, limit=10)
    assert candidates == [older.id]
    assert repository.delete_vacancy_snapshots(candidates) == 1
    session.expire_all()
    assert session.get(VacancySnapshot, latest.id) is not None
    assert list(
        session.scalars(
            select(VacancySnapshot.snapshot_type).order_by(VacancySnapshot.snapshot_type)
        )
    ) == ["detail", "short"]


def test_metadata_bundle_restores_exact_tree_into_isolated_empty_schema(retention_db, tmp_path):
    session, _, old_id, _, _ = retention_db
    repository = SqlAlchemyRunTreeMetadataRepository(session)
    bundle = write_bundle(
        output_dir=tmp_path, run_id=old_id, records=repository.iter_records(old_id)
    )
    schema = f"metadata_restore_{uuid4().hex}"
    session.execute(text(f'CREATE SCHEMA "{schema}"'))
    session.execute(text(f'SET LOCAL search_path TO "{schema}", pg_catalog'))
    CrawlRun.__table__.create(session.connection())
    CrawlPartition.__table__.create(session.connection())
    repository.restore_into_empty_tables(iter_bundle_records(bundle.directory))
    assert records_digest(repository.iter_records(old_id)) == bundle.content_sha256
    assert list(session.scalars(select(CrawlPartition.parent_partition_id))).count(None) == 1
    with pytest.raises(ValueError, match="requires empty"):
        repository.restore_into_empty_tables(iter_bundle_records(bundle.directory))
    # Fixture rollback removes this schema and all restored rows; no production restore CLI.
