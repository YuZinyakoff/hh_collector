from datetime import datetime

from sqlalchemy import BigInteger, DateTime, ForeignKey, Index, Integer, Text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from hhru_platform.infrastructure.db.base import Base


class DetailFetchAttempt(Base):
    __tablename__ = "detail_fetch_attempt"
    __table_args__ = (
        Index("idx_detail_fetch_attempt_vacancy_id", "vacancy_id"),
        Index("idx_detail_fetch_attempt_status", "status"),
        Index("idx_detail_fetch_attempt_requested_at", "requested_at"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    vacancy_id: Mapped[UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("vacancy.id"), nullable=False)
    crawl_run_id: Mapped[UUID | None] = mapped_column(UUID(as_uuid=True), ForeignKey("crawl_run.id"))
    reason: Mapped[str] = mapped_column(Text, nullable=False)
    attempt: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    requested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    error_message: Mapped[str | None] = mapped_column(Text)
