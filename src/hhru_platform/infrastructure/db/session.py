from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from hhru_platform.config.settings import get_settings


def create_engine_from_settings() -> Engine:
    settings = get_settings()
    return create_engine(settings.database_url, future=True)


def create_session_factory(engine: Engine | None = None) -> sessionmaker[Session]:
    return sessionmaker(
        bind=engine or create_engine_from_settings(),
        autoflush=False,
        autocommit=False,
        expire_on_commit=False,
        class_=Session,
    )


SessionLocal = create_session_factory()


@contextmanager
def session_scope(session_factory: sessionmaker[Session] | None = None) -> Iterator[Session]:
    session = (session_factory or SessionLocal)()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
