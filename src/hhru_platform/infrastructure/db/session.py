from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from hhru_platform.config.settings import get_settings


def create_engine_from_settings() -> Engine:
    settings = get_settings()
    return create_engine(settings.database_url, future=True)


SessionLocal = sessionmaker(
    bind=create_engine_from_settings(),
    autoflush=False,
    autocommit=False,
    class_=Session,
)
