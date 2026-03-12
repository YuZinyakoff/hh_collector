from datetime import datetime
from uuid import UUID

from sqlalchemy import Boolean, DateTime, ForeignKey, Index, Text, UniqueConstraint, func
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import Mapped, mapped_column

from hhru_platform.infrastructure.db.base import Base
from hhru_platform.infrastructure.db.models.common import TimestampMixin, uuid_pk_column


class Employer(TimestampMixin, Base):
    __tablename__ = "employer"
    __table_args__ = (
        UniqueConstraint("hh_employer_id", name="uq_employer_hh_employer_id"),
        Index("idx_employer_name", "name"),
        Index("idx_employer_area_id", "area_id"),
    )

    id = uuid_pk_column()
    hh_employer_id: Mapped[str] = mapped_column(Text, nullable=False)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    alternate_url: Mapped[str | None] = mapped_column(Text)
    site_url: Mapped[str | None] = mapped_column(Text)
    area_id: Mapped[UUID | None] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("area.id", ondelete="SET NULL"),
    )
    is_trusted: Mapped[bool | None] = mapped_column(Boolean)
    raw_first_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    raw_last_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
