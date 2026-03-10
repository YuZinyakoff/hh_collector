from sqlalchemy import Boolean, ForeignKey, Integer, Text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from hhru_platform.infrastructure.db.base import Base
from hhru_platform.infrastructure.db.models.common import TimestampMixin, uuid_pk_column


class Area(TimestampMixin, Base):
    __tablename__ = "area"

    id = uuid_pk_column()
    hh_area_id: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    parent_area_id: Mapped[UUID | None] = mapped_column(UUID(as_uuid=True), ForeignKey("area.id"))
    level: Mapped[int | None] = mapped_column(Integer)
    path_text: Mapped[str | None] = mapped_column(Text)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
