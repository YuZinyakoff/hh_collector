from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import (
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import Mapped, mapped_column

from hhru_platform.infrastructure.db.base import Base
from hhru_platform.infrastructure.db.models.common import uuid_pk_column


class CrawlPartition(Base):
    __tablename__ = "crawl_partition"
    __table_args__ = (
        UniqueConstraint("crawl_run_id", "partition_key", name="uq_crawl_partition_run_key"),
        UniqueConstraint("crawl_run_id", "scope_key", name="uq_crawl_partition_run_scope_key"),
        Index("idx_crawl_partition_run_id", "crawl_run_id"),
        Index("idx_crawl_partition_status", "status"),
        Index("idx_crawl_partition_parent_partition_id", "parent_partition_id"),
        Index("idx_crawl_partition_coverage_status", "coverage_status"),
    )

    id = uuid_pk_column()
    crawl_run_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("crawl_run.id", ondelete="CASCADE"),
        nullable=False,
    )
    parent_partition_id: Mapped[UUID | None] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("crawl_partition.id", ondelete="SET NULL"),
    )
    partition_key: Mapped[str] = mapped_column(Text, nullable=False)
    scope_key: Mapped[str] = mapped_column(Text, nullable=False)
    params_json: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        default=dict,
        server_default=text("'{}'::jsonb"),
    )
    status: Mapped[str] = mapped_column(Text, nullable=False)
    depth: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        server_default=text("0"),
    )
    split_dimension: Mapped[str | None] = mapped_column(Text)
    split_value: Mapped[str | None] = mapped_column(Text)
    planner_policy_version: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        default="v1",
        server_default=text("'v1'"),
    )
    is_terminal: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=True,
        server_default=text("true"),
    )
    is_saturated: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        server_default=text("false"),
    )
    coverage_status: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        default="unassessed",
        server_default=text("'unassessed'"),
    )
    pages_total_expected: Mapped[int | None] = mapped_column(Integer)
    pages_processed: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        server_default=text("0"),
    )
    items_seen: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        server_default=text("0"),
    )
    retry_count: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        server_default=text("0"),
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_error_message: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
