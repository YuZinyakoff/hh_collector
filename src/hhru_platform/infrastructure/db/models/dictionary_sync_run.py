from datetime import datetime

from sqlalchemy import DateTime, Index, Integer, Text, desc, func
from sqlalchemy.orm import Mapped, mapped_column

from hhru_platform.infrastructure.db.base import Base
from hhru_platform.infrastructure.db.models.common import uuid_pk_column


class DictionarySyncRun(Base):
    __tablename__ = "dictionary_sync_run"
    __table_args__ = (
        Index("idx_dictionary_sync_run_name", "dictionary_name"),
        Index("idx_dictionary_sync_run_started_at", desc("started_at")),
    )

    id = uuid_pk_column()
    dictionary_name: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    etag: Mapped[str | None] = mapped_column(Text)
    source_status_code: Mapped[int | None] = mapped_column(Integer)
    notes: Mapped[str | None] = mapped_column(Text)
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
