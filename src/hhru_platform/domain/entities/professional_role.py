from dataclasses import dataclass
from uuid import UUID


@dataclass(slots=True)
class ProfessionalRole:
    id: UUID
    hh_professional_role_id: str
    name: str
