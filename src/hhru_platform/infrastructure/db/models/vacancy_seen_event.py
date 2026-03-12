from datetime import datetime
from uuid import UUID

from sqlalchemy import (
    BigInteger,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Text,
    UniqueConstraint,
    desc,
    func,
)
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import Mapped, mapped_column

from hhru_platform.infrastructure.db.base import Base


class VacancySeenEvent(Base):
    __tablename__ = "vacancy_seen_event"
    __table_args__ = (
        UniqueConstraint("vacancy_id", "crawl_partition_id", "seen_at", name="uq_vse_seen"),
        Index("idx_vacancy_seen_event_vacancy_id", "vacancy_id"),
        Index("idx_vacancy_seen_event_run_id", "crawl_run_id"),
        Index("idx_vacancy_seen_event_partition_id", "crawl_partition_id"),
        Index("idx_vacancy_seen_event_seen_at", desc("seen_at")),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    vacancy_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("vacancy.id", ondelete="CASCADE"),
        nullable=False,
    )
    crawl_run_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("crawl_run.id", ondelete="CASCADE"),
        nullable=False,
    )
    crawl_partition_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("crawl_partition.id", ondelete="CASCADE"),
        nullable=False,
    )
    seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    list_position: Mapped[int | None] = mapped_column(Integer)
    short_hash: Mapped[str] = mapped_column(Text, nullable=False)
    short_payload_ref_id: Mapped[int | None] = mapped_column(
        BigInteger,
        ForeignKey("raw_api_payload.id", ondelete="SET NULL"),
    )
