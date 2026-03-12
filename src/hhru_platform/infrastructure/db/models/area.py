from uuid import UUID

from sqlalchemy import Boolean, ForeignKey, Index, Integer, Text, UniqueConstraint, true
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import Mapped, mapped_column

from hhru_platform.infrastructure.db.base import Base
from hhru_platform.infrastructure.db.models.common import TimestampMixin, uuid_pk_column


class Area(TimestampMixin, Base):
    __tablename__ = "area"
    __table_args__ = (
        UniqueConstraint("hh_area_id", name="uq_area_hh_area_id"),
        Index("idx_area_parent_area_id", "parent_area_id"),
    )

    id = uuid_pk_column()
    hh_area_id: Mapped[str] = mapped_column(Text, nullable=False)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    parent_area_id: Mapped[UUID | None] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("area.id", ondelete="SET NULL"),
    )
    level: Mapped[int | None] = mapped_column(Integer)
    path_text: Mapped[str | None] = mapped_column(Text)
    is_active: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=True,
        server_default=true(),
    )
