from sqlalchemy import Boolean, Text, UniqueConstraint, true
from sqlalchemy.orm import Mapped, mapped_column

from hhru_platform.infrastructure.db.base import Base
from hhru_platform.infrastructure.db.models.common import TimestampMixin, uuid_pk_column


class ProfessionalRole(TimestampMixin, Base):
    __tablename__ = "professional_role"
    __table_args__ = (
        UniqueConstraint(
            "hh_professional_role_id",
            name="uq_professional_role_hh_professional_role_id",
        ),
    )

    id = uuid_pk_column()
    hh_professional_role_id: Mapped[str] = mapped_column(Text, nullable=False)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    category_name: Mapped[str | None] = mapped_column(Text)
    is_active: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=True,
        server_default=true(),
    )
