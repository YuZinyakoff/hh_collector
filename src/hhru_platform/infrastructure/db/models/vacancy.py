from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Index, Text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from hhru_platform.infrastructure.db.base import Base
from hhru_platform.infrastructure.db.models.common import TimestampMixin, uuid_pk_column


class Vacancy(TimestampMixin, Base):
    __tablename__ = "vacancy"
    __table_args__ = (
        Index("uq_vacancy_hh_vacancy_id", "hh_vacancy_id", unique=True),
        Index("idx_vacancy_employer_id", "employer_id"),
        Index("idx_vacancy_area_id", "area_id"),
        Index("idx_vacancy_published_at", "published_at"),
    )

    id = uuid_pk_column()
    hh_vacancy_id: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    employer_id: Mapped[UUID | None] = mapped_column(UUID(as_uuid=True), ForeignKey("employer.id"))
    area_id: Mapped[UUID | None] = mapped_column(UUID(as_uuid=True), ForeignKey("area.id"))
    name_current: Mapped[str] = mapped_column(Text, nullable=False)
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at_hh: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    archived_at_hh: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    alternate_url: Mapped[str | None] = mapped_column(Text)
    employment_type_code: Mapped[str | None] = mapped_column(Text)
    schedule_type_code: Mapped[str | None] = mapped_column(Text)
    experience_code: Mapped[str | None] = mapped_column(Text)
    source_type: Mapped[str] = mapped_column(Text, nullable=False, default="hh_api")
