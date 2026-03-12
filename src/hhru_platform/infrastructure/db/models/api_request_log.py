from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import BigInteger, DateTime, ForeignKey, Index, Integer, Text, desc, func, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import Mapped, mapped_column

from hhru_platform.infrastructure.db.base import Base


class ApiRequestLog(Base):
    __tablename__ = "api_request_log"
    __table_args__ = (
        Index("idx_api_request_log_requested_at", desc("requested_at")),
        Index("idx_api_request_log_status_code", "status_code"),
        Index("idx_api_request_log_run_id", "crawl_run_id"),
        Index("idx_api_request_log_partition_id", "crawl_partition_id"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    crawl_run_id: Mapped[UUID | None] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("crawl_run.id", ondelete="SET NULL"),
    )
    crawl_partition_id: Mapped[UUID | None] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("crawl_partition.id", ondelete="SET NULL"),
    )
    request_type: Mapped[str] = mapped_column(Text, nullable=False)
    endpoint: Mapped[str] = mapped_column(Text, nullable=False)
    method: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        default="GET",
        server_default=text("'GET'"),
    )
    params_json: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        default=dict,
        server_default=text("'{}'::jsonb"),
    )
    request_headers_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    status_code: Mapped[int] = mapped_column(Integer, nullable=False)
    latency_ms: Mapped[int] = mapped_column(Integer, nullable=False)
    attempt: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=1,
        server_default=text("1"),
    )
    requested_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    response_received_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    error_type: Mapped[str | None] = mapped_column(Text)
    error_message: Mapped[str | None] = mapped_column(Text)
