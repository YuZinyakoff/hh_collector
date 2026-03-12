from uuid import UUID

from sqlalchemy import ForeignKey, Index
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import Mapped, mapped_column

from hhru_platform.infrastructure.db.base import Base


class VacancyProfessionalRole(Base):
    __tablename__ = "vacancy_professional_role"
    __table_args__ = (Index("idx_vacancy_prof_role_role_id", "professional_role_id"),)

    vacancy_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("vacancy.id", ondelete="CASCADE"),
        primary_key=True,
    )
    professional_role_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("professional_role.id", ondelete="CASCADE"),
        primary_key=True,
    )
