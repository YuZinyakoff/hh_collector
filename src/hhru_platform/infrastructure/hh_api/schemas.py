from __future__ import annotations

from typing import TypedDict


class HHAreaPayload(TypedDict, total=False):
    areas: list[HHAreaPayload]
    id: str
    lat: float | None
    lng: float | None
    name: str
    parent_id: str | None
    utc_offset: str


class HHProfessionalRolePayload(TypedDict):
    accept_incomplete_resumes: bool
    id: str
    is_default: bool
    name: str
    search_deprecated: bool
    select_deprecated: bool


class HHProfessionalRoleCategoryPayload(TypedDict):
    id: str
    name: str
    roles: list[HHProfessionalRolePayload]


class HHProfessionalRolesResponse(TypedDict):
    categories: list[HHProfessionalRoleCategoryPayload]
