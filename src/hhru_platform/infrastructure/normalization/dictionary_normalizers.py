from __future__ import annotations

from dataclasses import dataclass
from typing import Any


class DictionaryNormalizationError(ValueError):
    """Raised when a dictionary payload does not match the expected shape."""


@dataclass(slots=True, frozen=True)
class NormalizedAreaRecord:
    hh_area_id: str
    name: str
    parent_hh_area_id: str | None
    level: int
    path_text: str


@dataclass(slots=True, frozen=True)
class NormalizedProfessionalRoleRecord:
    hh_professional_role_id: str
    name: str
    category_name: str


def normalize_areas(payload_json: object) -> list[NormalizedAreaRecord]:
    if not isinstance(payload_json, list):
        raise DictionaryNormalizationError("areas payload must be a list")

    records_by_id: dict[str, NormalizedAreaRecord] = {}
    for node in payload_json:
        _walk_area_node(node, level=0, path=(), records_by_id=records_by_id)

    return list(records_by_id.values())


def normalize_professional_roles(payload_json: object) -> list[NormalizedProfessionalRoleRecord]:
    if not isinstance(payload_json, dict):
        raise DictionaryNormalizationError("professional_roles payload must be an object")

    categories = payload_json.get("categories")
    if not isinstance(categories, list):
        raise DictionaryNormalizationError("professional_roles.categories must be a list")

    records_by_id: dict[str, NormalizedProfessionalRoleRecord] = {}
    for category in categories:
        if not isinstance(category, dict):
            raise DictionaryNormalizationError("professional_roles category must be an object")

        category_name = _require_string(category, "name")
        roles = category.get("roles")
        if not isinstance(roles, list):
            raise DictionaryNormalizationError("professional_roles category.roles must be a list")

        for role in roles:
            if not isinstance(role, dict):
                raise DictionaryNormalizationError("professional role must be an object")

            hh_professional_role_id = _require_string(role, "id")
            record = NormalizedProfessionalRoleRecord(
                hh_professional_role_id=hh_professional_role_id,
                name=_require_string(role, "name"),
                category_name=category_name,
            )
            records_by_id[hh_professional_role_id] = record

    return list(records_by_id.values())


def _walk_area_node(
    node: object,
    *,
    level: int,
    path: tuple[str, ...],
    records_by_id: dict[str, NormalizedAreaRecord],
) -> None:
    if not isinstance(node, dict):
        raise DictionaryNormalizationError("area node must be an object")

    hh_area_id = _require_string(node, "id")
    name = _require_string(node, "name")
    parent_hh_area_id = _optional_string(node.get("parent_id"))
    children = node.get("areas")
    if not isinstance(children, list):
        raise DictionaryNormalizationError("area.areas must be a list")

    current_path = (*path, name)
    records_by_id[hh_area_id] = NormalizedAreaRecord(
        hh_area_id=hh_area_id,
        name=name,
        parent_hh_area_id=parent_hh_area_id,
        level=level,
        path_text=" / ".join(current_path),
    )

    for child in children:
        _walk_area_node(
            child,
            level=level + 1,
            path=current_path,
            records_by_id=records_by_id,
        )


def _require_string(payload: dict[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise DictionaryNormalizationError(f"{key} must be a non-empty string")
    return value


def _optional_string(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        stripped_value = value.strip()
        return stripped_value or None
    raise DictionaryNormalizationError("optional string field has invalid type")
