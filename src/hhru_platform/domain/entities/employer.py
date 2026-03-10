from dataclasses import dataclass
from uuid import UUID


@dataclass(slots=True)
class Employer:
    id: UUID
    hh_employer_id: str
    name: str
