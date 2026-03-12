from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Any

from hhru_platform.application.dto import NormalizedVacancyDetail


class VacancyDetailNormalizationError(ValueError):
    """Raised when a vacancy detail payload does not match the expected shape."""


def normalize_vacancy_detail(payload_json: object) -> NormalizedVacancyDetail:
    if not isinstance(payload_json, dict):
        raise VacancyDetailNormalizationError("vacancy detail payload must be an object")

    hh_vacancy_id = _require_string(payload_json, "id")
    name_current = _require_string(payload_json, "name")
    published_at = _parse_datetime(payload_json.get("published_at"))
    created_at_hh = _parse_datetime(payload_json.get("initial_created_at"))
    if created_at_hh is None:
        created_at_hh = _parse_datetime(payload_json.get("created_at"))

    normalized_json: dict[str, object] = {
        "hh_vacancy_id": hh_vacancy_id,
        "name_current": name_current,
        "description": _optional_string(payload_json.get("description")),
        "branded_description": _optional_string(payload_json.get("branded_description")),
        "alternate_url": _optional_string(payload_json.get("alternate_url")),
        "archived": _optional_bool(payload_json.get("archived")),
        "area": _normalize_area(payload_json.get("area")),
        "employer": _normalize_employer(payload_json.get("employer")),
        "employment_type_code": _lookup_id(payload_json.get("employment")),
        "schedule_type_code": _lookup_id(payload_json.get("schedule")),
        "experience_code": _lookup_id(payload_json.get("experience")),
        "professional_role_hh_ids": _normalize_lookup_ids(payload_json.get("professional_roles")),
        "key_skill_names": _normalize_key_skills(payload_json.get("key_skills")),
        "salary": _optional_mapping(payload_json.get("salary")),
        "salary_range": _optional_mapping(payload_json.get("salary_range")),
        "published_at": published_at.isoformat() if published_at is not None else None,
        "created_at_hh": created_at_hh.isoformat() if created_at_hh is not None else None,
    }

    return NormalizedVacancyDetail(
        hh_vacancy_id=hh_vacancy_id,
        name_current=name_current,
        area_hh_id=_lookup_id(payload_json.get("area")),
        published_at=published_at,
        created_at_hh=created_at_hh,
        alternate_url=_optional_string(payload_json.get("alternate_url")),
        employment_type_code=_lookup_id(payload_json.get("employment")),
        schedule_type_code=_lookup_id(payload_json.get("schedule")),
        experience_code=_lookup_id(payload_json.get("experience")),
        normalized_json=normalized_json,
        detail_hash=_build_detail_hash(normalized_json),
    )


def _normalize_area(payload: object) -> dict[str, object] | None:
    if payload is None:
        return None
    if not isinstance(payload, dict):
        raise VacancyDetailNormalizationError("area must be an object")
    return {
        "hh_area_id": _optional_string(payload.get("id")),
        "name": _optional_string(payload.get("name")),
    }


def _normalize_employer(payload: object) -> dict[str, object] | None:
    if payload is None:
        return None
    if not isinstance(payload, dict):
        raise VacancyDetailNormalizationError("employer must be an object")
    return {
        "hh_employer_id": _optional_string(payload.get("id")),
        "name": _optional_string(payload.get("name")),
        "alternate_url": _optional_string(payload.get("alternate_url")),
    }


def _normalize_lookup_ids(payload: object) -> list[str]:
    if payload is None:
        return []
    if not isinstance(payload, list):
        raise VacancyDetailNormalizationError("professional_roles must be a list")
    normalized_ids: list[str] = []
    for item in payload:
        lookup_id = _lookup_id(item)
        if lookup_id is not None:
            normalized_ids.append(lookup_id)
    return normalized_ids


def _normalize_key_skills(payload: object) -> list[str]:
    if payload is None:
        return []
    if not isinstance(payload, list):
        raise VacancyDetailNormalizationError("key_skills must be a list")
    normalized_names: list[str] = []
    for item in payload:
        if not isinstance(item, dict):
            raise VacancyDetailNormalizationError("key_skill item must be an object")
        name = _optional_string(item.get("name"))
        if name is not None:
            normalized_names.append(name)
    return normalized_names


def _lookup_id(payload: object) -> str | None:
    if payload is None:
        return None
    if not isinstance(payload, dict):
        raise VacancyDetailNormalizationError("lookup payload must be an object")
    return _optional_string(payload.get("id"))


def _require_string(payload: dict[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise VacancyDetailNormalizationError(f"{key} must be a non-empty string")
    return value


def _optional_string(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        stripped_value = value.strip()
        return stripped_value or None
    raise VacancyDetailNormalizationError("expected string-compatible value")


def _optional_bool(value: object) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    raise VacancyDetailNormalizationError("expected boolean-compatible value")


def _optional_mapping(value: object) -> dict[str, object] | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise VacancyDetailNormalizationError("expected object-compatible value")
    return dict(value)


def _parse_datetime(value: object) -> datetime | None:
    string_value = _optional_string(value)
    if string_value is None:
        return None

    for format_string in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S.%f%z"):
        try:
            return datetime.strptime(string_value, format_string)
        except ValueError:
            continue

    raise VacancyDetailNormalizationError(f"unsupported datetime value: {string_value!r}")


def _build_detail_hash(payload: dict[str, object]) -> str:
    normalized_payload = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(normalized_payload.encode("utf-8")).hexdigest()
