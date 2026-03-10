from dataclasses import dataclass
from uuid import UUID


@dataclass(slots=True)
class Area:
    id: UUID
    hh_area_id: str
    name: str
