from __future__ import annotations

import hashlib
import json
from typing import Any

from sqlalchemy.orm import Session

from hhru_platform.infrastructure.db.models.raw_api_payload import RawApiPayload


class SqlAlchemyRawApiPayloadRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def add(
        self,
        *,
        api_request_log_id: int,
        endpoint_type: str,
        entity_hh_id: str | None,
        payload_json: Any,
    ) -> int:
        payload = RawApiPayload(
            api_request_log_id=api_request_log_id,
            endpoint_type=endpoint_type,
            entity_hh_id=entity_hh_id,
            payload_json=payload_json,
            payload_hash=_build_payload_hash(payload_json),
        )
        self._session.add(payload)
        self._session.flush()
        return payload.id


def _build_payload_hash(payload_json: Any) -> str:
    normalized_payload = json.dumps(
        payload_json,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(normalized_payload.encode("utf-8")).hexdigest()
