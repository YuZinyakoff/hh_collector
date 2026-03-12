from datetime import datetime
from uuid import UUID

from sqlalchemy import Boolean, DateTime, ForeignKey, Index, Integer, Text, desc, false, func, text
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import Mapped, mapped_column

from hhru_platform.infrastructure.db.base import Base


class VacancyCurrentState(Base):
    __tablename__ = "vacancy_current_state"
    __table_args__ = (
        Index("idx_vacancy_current_state_last_seen_at", desc("last_seen_at")),
        Index("idx_vacancy_current_state_inactive", "is_probably_inactive"),
        Index("idx_vacancy_current_state_detail_status", "detail_fetch_status"),
    )

    vacancy_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("vacancy.id", ondelete="CASCADE"),
        primary_key=True,
    )
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    seen_count: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=1,
        server_default=text("1"),
    )
    consecutive_missing_runs: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        server_default=text("0"),
    )
    is_probably_inactive: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        server_default=false(),
    )
    last_seen_run_id: Mapped[UUID | None] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("crawl_run.id", ondelete="SET NULL"),
    )
    last_short_hash: Mapped[str | None] = mapped_column(Text)
    last_detail_hash: Mapped[str | None] = mapped_column(Text)
    last_detail_fetched_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    detail_fetch_status: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        default="not_requested",
        server_default=text("'not_requested'"),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )
