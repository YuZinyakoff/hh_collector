from datetime import datetime

from sqlalchemy import Boolean, DateTime, ForeignKey, Index, Integer, Text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from hhru_platform.infrastructure.db.base import Base


class VacancyCurrentState(Base):
    __tablename__ = "vacancy_current_state"
    __table_args__ = (
        Index("idx_vacancy_current_state_last_seen_at", "last_seen_at"),
        Index("idx_vacancy_current_state_inactive", "is_probably_inactive"),
        Index("idx_vacancy_current_state_detail_status", "detail_fetch_status"),
    )

    vacancy_id: Mapped[UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("vacancy.id"),
        primary_key=True,
    )
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    seen_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    consecutive_missing_runs: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    is_probably_inactive: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    last_seen_run_id: Mapped[UUID | None] = mapped_column(UUID(as_uuid=True), ForeignKey("crawl_run.id"))
    last_short_hash: Mapped[str | None] = mapped_column(Text)
    last_detail_hash: Mapped[str | None] = mapped_column(Text)
    last_detail_fetched_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    detail_fetch_status: Mapped[str] = mapped_column(Text, nullable=False, default="not_requested")
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
