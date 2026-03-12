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


class HHLookupPayload(TypedDict, total=False):
    id: str
    name: str


class HHAreaReferencePayload(TypedDict, total=False):
    id: str
    name: str
    url: str


class HHEmployerShortPayload(TypedDict, total=False):
    id: str
    name: str
    alternate_url: str | None
    trusted: bool


class HHVacancyShortPayload(TypedDict, total=False):
    id: str
    name: str
    alternate_url: str | None
    archived: bool
    area: HHAreaReferencePayload | None
    created_at: str
    employer: HHEmployerShortPayload | None
    employment: HHLookupPayload | None
    experience: HHLookupPayload | None
    professional_roles: list[HHLookupPayload]
    published_at: str
    schedule: HHLookupPayload | None


class HHVacancySearchResponse(TypedDict, total=False):
    items: list[HHVacancyShortPayload]
    found: int
    page: int
    pages: int
    per_page: int
