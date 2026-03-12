from __future__ import annotations

from datetime import datetime
from uuid import UUID

from sqlalchemy.orm import Session

from hhru_platform.infrastructure.db.models.api_request_log import ApiRequestLog


class SqlAlchemyApiRequestLogRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def add(
        self,
        *,
        crawl_run_id: UUID | None,
        crawl_partition_id: UUID | None,
        requested_at: datetime,
        request_type: str,
        endpoint: str,
        method: str,
        params_json: dict[str, object],
        request_headers_json: dict[str, str] | None,
        status_code: int,
        latency_ms: int,
        response_received_at: datetime | None,
        error_type: str | None,
        error_message: str | None,
    ) -> int:
        request_log = ApiRequestLog(
            crawl_run_id=crawl_run_id,
            crawl_partition_id=crawl_partition_id,
            request_type=request_type,
            endpoint=endpoint,
            method=method,
            params_json=dict(params_json),
            request_headers_json=request_headers_json,
            requested_at=requested_at,
            status_code=status_code,
            latency_ms=latency_ms,
            response_received_at=response_received_at,
            error_type=error_type,
            error_message=error_message,
        )
        self._session.add(request_log)
        self._session.flush()
        return request_log.id
