from datetime import datetime
from typing import Any

from sqlalchemy import DateTime, ForeignKey, Index, Integer, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from hhru_platform.infrastructure.db.base import Base
from hhru_platform.infrastructure.db.models.common import uuid_pk_column


class CrawlPartition(Base):
    __tablename__ = "crawl_partition"
    __table_args__ = (
        UniqueConstraint("crawl_run_id", "partition_key", name="uq_crawl_partition_run_key"),
        Index("idx_crawl_partition_run_id", "crawl_run_id"),
        Index("idx_crawl_partition_status", "status"),
    )

    id = uuid_pk_column()
    crawl_run_id: Mapped[UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("crawl_run.id"),
        nullable=False,
    )
    partition_key: Mapped[str] = mapped_column(Text, nullable=False)
    params_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    pages_total_expected: Mapped[int | None] = mapped_column(Integer)
    pages_processed: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    items_seen: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    retry_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_error_message: Mapped[str | None] = mapped_column(Text)
