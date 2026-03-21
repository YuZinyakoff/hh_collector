from __future__ import annotations

import hashlib
import json
from datetime import datetime
from uuid import UUID

SNAPSHOT_SCHEMA_VERSION = 2


def build_payload_hash(payload_json: object) -> str:
    normalized_payload = json.dumps(
        _canonicalize_json(payload_json),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(normalized_payload.encode("utf-8")).hexdigest()


def build_detail_snapshot_document(payload_json: object) -> dict[str, object]:
    payload = _require_mapping(
        payload_json=payload_json,
        message="vacancy detail payload must be an object",
    )
    return {
        "schema_version": SNAPSHOT_SCHEMA_VERSION,
        "source": "detail",
        "payload": _canonicalize_mapping(payload),
    }


def build_short_snapshot_document(
    payload_json: object,
    *,
    seen_at: datetime,
    crawl_partition_id: UUID,
    list_position: int,
    page: int | None,
    per_page: int | None,
    found: int | None,
    pages: int | None,
    search_params: dict[str, object],
) -> dict[str, object]:
    payload = _require_mapping(
        payload_json=payload_json,
        message="vacancy search item payload must be an object",
    )
    context: dict[str, object] = {
        "seen_at": seen_at.isoformat(),
        "crawl_partition_id": str(crawl_partition_id),
        "list_position": list_position,
        "search_params": _canonicalize_mapping(search_params),
    }
    if page is not None:
        context["page"] = page
    if per_page is not None:
        context["per_page"] = per_page
    if found is not None:
        context["found"] = found
    if pages is not None:
        context["pages"] = pages

    return {
        "schema_version": SNAPSHOT_SCHEMA_VERSION,
        "source": "short",
        "payload": _canonicalize_mapping(payload),
        "context": context,
    }


def has_full_snapshot_document(document_json: object | None) -> bool:
    if not isinstance(document_json, dict):
        return False
    return document_json.get("schema_version") == SNAPSHOT_SCHEMA_VERSION and isinstance(
        document_json.get("payload"),
        dict,
    )


def extract_search_item_payload(
    page_payload_json: object,
    *,
    hh_vacancy_id: str,
) -> dict[str, object] | None:
    if not isinstance(page_payload_json, dict):
        return None
    items = page_payload_json.get("items")
    if not isinstance(items, list):
        return None
    for item in items:
        if not isinstance(item, dict):
            continue
        if item.get("id") == hh_vacancy_id:
            return _canonicalize_mapping(item)
    return None


def _require_mapping(*, message: str, payload_json: object) -> dict[str, object]:
    if not isinstance(payload_json, dict):
        raise ValueError(message)
    return _canonicalize_mapping(payload_json)


def _canonicalize_mapping(payload_json: dict[str, object]) -> dict[str, object]:
    return {str(key): _canonicalize_json(value) for key, value in payload_json.items()}


def _canonicalize_json(value: object) -> object:
    if value is None or isinstance(value, str | int | float | bool):
        return value
    if isinstance(value, dict):
        return {str(key): _canonicalize_json(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_canonicalize_json(item) for item in value]
    return str(value)
