from datetime import datetime

from sqlalchemy import Boolean, DateTime, ForeignKey, Index, Text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from hhru_platform.infrastructure.db.base import Base
from hhru_platform.infrastructure.db.models.common import TimestampMixin, uuid_pk_column


class Employer(TimestampMixin, Base):
    __tablename__ = "employer"
    __table_args__ = (
        Index("uq_employer_hh_employer_id", "hh_employer_id", unique=True),
        Index("idx_employer_name", "name"),
    )

    id = uuid_pk_column()
    hh_employer_id: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    alternate_url: Mapped[str | None] = mapped_column(Text)
    site_url: Mapped[str | None] = mapped_column(Text)
    area_id: Mapped[UUID | None] = mapped_column(UUID(as_uuid=True), ForeignKey("area.id"))
    is_trusted: Mapped[bool | None] = mapped_column(Boolean)
    raw_first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    raw_last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
