from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import BigInteger, DateTime, ForeignKey, Index, Text, desc, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import Mapped, mapped_column

from hhru_platform.infrastructure.db.base import Base


class VacancySnapshot(Base):
    __tablename__ = "vacancy_snapshot"
    __table_args__ = (
        Index("idx_vacancy_snapshot_vacancy_id", "vacancy_id"),
        Index("idx_vacancy_snapshot_captured_at", desc("captured_at")),
        Index("idx_vacancy_snapshot_detail_hash", "detail_hash"),
        Index(
            "idx_vacancy_snapshot_vacancy_type_captured_at",
            "vacancy_id",
            "snapshot_type",
            desc("captured_at"),
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    vacancy_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("vacancy.id", ondelete="CASCADE"),
        nullable=False,
    )
    snapshot_type: Mapped[str] = mapped_column(Text, nullable=False)
    captured_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    crawl_run_id: Mapped[UUID | None] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("crawl_run.id", ondelete="SET NULL"),
    )
    short_hash: Mapped[str | None] = mapped_column(Text)
    detail_hash: Mapped[str | None] = mapped_column(Text)
    short_payload_ref_id: Mapped[int | None] = mapped_column(
        BigInteger,
        ForeignKey("raw_api_payload.id", ondelete="SET NULL"),
    )
    detail_payload_ref_id: Mapped[int | None] = mapped_column(
        BigInteger,
        ForeignKey("raw_api_payload.id", ondelete="SET NULL"),
    )
    normalized_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    change_reason: Mapped[str | None] = mapped_column(Text)
