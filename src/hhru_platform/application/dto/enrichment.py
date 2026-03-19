from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class NormalizedEmployerReference:
    hh_employer_id: str
    name: str | None
    alternate_url: str | None
    site_url: str | None
    area_hh_id: str | None
    is_trusted: bool | None
