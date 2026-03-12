from __future__ import annotations

from uuid import UUID

import pytest
from sqlalchemy import text
from sqlalchemy.exc import OperationalError

from hhru_platform.application.commands.create_crawl_run import (
    CreateCrawlRunCommand,
    create_crawl_run,
)
from hhru_platform.infrastructure.db.repositories.crawl_run_repo import (
    SqlAlchemyCrawlRunRepository,
)
from hhru_platform.infrastructure.db.session import (
    create_engine_from_settings,
    create_session_factory,
    session_scope,
)


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


def test_create_crawl_run_persists_record() -> None:
    engine = create_engine_from_settings()
    session_factory = create_session_factory(engine)
    triggered_by = "pytest-create-run"
    created_run_id: UUID | None = None

    try:
        with session_scope(session_factory) as session:
            repository = SqlAlchemyCrawlRunRepository(session)
            created_run = create_crawl_run(
                CreateCrawlRunCommand(
                    run_type="weekly_sweep",
                    triggered_by=triggered_by,
                ),
                repository,
            )
            created_run_id = created_run.id

        assert created_run_id is not None

        with session_scope(session_factory) as session:
            repository = SqlAlchemyCrawlRunRepository(session)
            stored_run = repository.get(created_run_id)

        assert stored_run is not None
        assert stored_run.id == created_run_id
        assert stored_run.run_type == "weekly_sweep"
        assert stored_run.triggered_by == triggered_by
        assert stored_run.status == "created"
    finally:
        with engine.begin() as connection:
            connection.execute(
                text("DELETE FROM crawl_run WHERE triggered_by = :triggered_by"),
                {"triggered_by": triggered_by},
            )
        engine.dispose()
