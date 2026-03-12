from datetime import datetime
from typing import Any

from sqlalchemy import BigInteger, DateTime, ForeignKey, Index, Text, desc, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from hhru_platform.infrastructure.db.base import Base


class RawApiPayload(Base):
    __tablename__ = "raw_api_payload"
    __table_args__ = (
        Index("idx_raw_api_payload_request_log_id", "api_request_log_id"),
        Index("idx_raw_api_payload_entity_hh_id", "entity_hh_id"),
        Index("idx_raw_api_payload_received_at", desc("received_at")),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    api_request_log_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("api_request_log.id", ondelete="CASCADE"),
        nullable=False,
    )
    endpoint_type: Mapped[str] = mapped_column(Text, nullable=False)
    entity_hh_id: Mapped[str | None] = mapped_column(Text)
    payload_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    payload_hash: Mapped[str] = mapped_column(Text, nullable=False)
    received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
