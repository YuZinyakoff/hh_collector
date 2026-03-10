from datetime import datetime

from sqlalchemy import BigInteger, DateTime, ForeignKey, Index, Integer, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from hhru_platform.infrastructure.db.base import Base


class VacancySeenEvent(Base):
    __tablename__ = "vacancy_seen_event"
    __table_args__ = (
        UniqueConstraint("vacancy_id", "crawl_partition_id", "seen_at", name="uq_vse_seen"),
        Index("idx_vacancy_seen_event_vacancy_id", "vacancy_id"),
        Index("idx_vacancy_seen_event_run_id", "crawl_run_id"),
        Index("idx_vacancy_seen_event_seen_at", "seen_at"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    vacancy_id: Mapped[UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("vacancy.id"), nullable=False)
    crawl_run_id: Mapped[UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("crawl_run.id"),
        nullable=False,
    )
    crawl_partition_id: Mapped[UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("crawl_partition.id"),
        nullable=False,
    )
    seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    list_position: Mapped[int | None] = mapped_column(Integer)
    short_hash: Mapped[str] = mapped_column(Text, nullable=False)
    short_payload_ref_id: Mapped[int | None] = mapped_column(BigInteger, ForeignKey("raw_api_payload.id"))
