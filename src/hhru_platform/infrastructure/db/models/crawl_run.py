from datetime import datetime
from typing import Any

from sqlalchemy import DateTime, Index, Integer, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from hhru_platform.infrastructure.db.base import Base
from hhru_platform.infrastructure.db.models.common import uuid_pk_column


class CrawlRun(Base):
    __tablename__ = "crawl_run"
    __table_args__ = (
        Index("idx_crawl_run_status", "status"),
        Index("idx_crawl_run_started_at", "started_at"),
    )

    id = uuid_pk_column()
    run_type: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    triggered_by: Mapped[str] = mapped_column(Text, nullable=False)
    config_snapshot_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    partitions_total: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    partitions_done: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    partitions_failed: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    notes: Mapped[str | None] = mapped_column(Text)
