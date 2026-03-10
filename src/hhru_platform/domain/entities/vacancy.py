from dataclasses import dataclass
from uuid import UUID


@dataclass(slots=True)
class Vacancy:
    id: UUID
    hh_vacancy_id: str
    name_current: str
