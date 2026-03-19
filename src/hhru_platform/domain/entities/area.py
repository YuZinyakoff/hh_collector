from dataclasses import dataclass
from uuid import UUID


@dataclass(slots=True)
class Area:
    id: UUID
    hh_area_id: str
    name: str
    parent_area_id: UUID | None = None
    level: int | None = None
    path_text: str | None = None
    is_active: bool = True
