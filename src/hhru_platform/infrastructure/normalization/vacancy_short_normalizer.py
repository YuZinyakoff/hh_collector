from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Any

from hhru_platform.application.dto import (
    NormalizedVacancySearchPage,
    NormalizedVacancyShortRecord,
)
from hhru_platform.infrastructure.normalization.employer_normalizer import (
    EmployerNormalizationError,
    normalize_employer_reference,
)


class VacancySearchNormalizationError(ValueError):
    """Raised when a vacancy search payload does not match the expected shape."""


def normalize_vacancy_search_page(payload_json: object) -> NormalizedVacancySearchPage:
    if not isinstance(payload_json, dict):
        raise VacancySearchNormalizationError("vacancy search payload must be an object")

    items = payload_json.get("items")
    if not isinstance(items, list):
        raise VacancySearchNormalizationError("vacancy search payload.items must be a list")

    page = _optional_int(payload_json.get("page")) or 0
    per_page = _optional_int(payload_json.get("per_page")) or len(items)
    normalized_items = [
        _normalize_vacancy_item(item, list_position=page * max(per_page, 1) + index)
        for index, item in enumerate(items)
    ]

    return NormalizedVacancySearchPage(
        page=page,
        pages=_optional_int(payload_json.get("pages")),
        per_page=per_page,
        found=_optional_int(payload_json.get("found")),
        items=normalized_items,
    )


def _normalize_vacancy_item(
    payload: object,
    *,
    list_position: int,
) -> NormalizedVacancyShortRecord:
    if not isinstance(payload, dict):
        raise VacancySearchNormalizationError("vacancy item must be an object")
    try:
        employer = normalize_employer_reference(payload.get("employer"))
    except EmployerNormalizationError as error:
        raise VacancySearchNormalizationError(str(error)) from error

    return NormalizedVacancyShortRecord(
        hh_vacancy_id=_require_string(payload, "id"),
        name_current=_require_string(payload, "name"),
        area_hh_id=_lookup_id(payload.get("area")),
        published_at=_parse_datetime(payload.get("published_at")),
        created_at_hh=_parse_datetime(payload.get("created_at")),
        alternate_url=_optional_string(payload.get("alternate_url")),
        employment_type_code=_lookup_id(payload.get("employment")),
        schedule_type_code=_lookup_id(payload.get("schedule")),
        experience_code=_lookup_id(payload.get("experience")),
        employer=employer,
        professional_role_hh_ids=_normalize_lookup_ids(payload.get("professional_roles")),
        short_hash=_build_short_hash(payload),
        list_position=list_position,
    )


def _lookup_id(payload: object) -> str | None:
    if payload is None:
        return None
    if not isinstance(payload, dict):
        raise VacancySearchNormalizationError("lookup payload must be an object")
    return _optional_string(payload.get("id"))


def _require_string(payload: dict[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise VacancySearchNormalizationError(f"{key} must be a non-empty string")
    return value


def _optional_string(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        stripped_value = value.strip()
        return stripped_value or None
    raise VacancySearchNormalizationError("expected string-compatible value")


def _optional_int(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise VacancySearchNormalizationError("expected integer-compatible value")
    return value


def _normalize_lookup_ids(payload: object) -> tuple[str, ...]:
    if payload is None:
        return ()
    if not isinstance(payload, list):
        raise VacancySearchNormalizationError("professional_roles must be a list")

    normalized_ids: list[str] = []
    for item in payload:
        lookup_id = _lookup_id(item)
        if lookup_id is not None:
            normalized_ids.append(lookup_id)
    return tuple(dict.fromkeys(normalized_ids))


def _parse_datetime(value: object) -> datetime | None:
    string_value = _optional_string(value)
    if string_value is None:
        return None

    for format_string in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S.%f%z"):
        try:
            return datetime.strptime(string_value, format_string)
        except ValueError:
            continue

    raise VacancySearchNormalizationError(f"unsupported datetime value: {string_value!r}")


def _build_short_hash(payload: dict[str, Any]) -> str:
    normalized_payload = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(normalized_payload.encode("utf-8")).hexdigest()
