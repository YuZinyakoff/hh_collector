from dataclasses import dataclass
from datetime import datetime
from uuid import UUID


@dataclass(slots=True)
class Vacancy:
    id: UUID
    hh_vacancy_id: str
    employer_id: UUID | None
    area_id: UUID | None
    name_current: str
    published_at: datetime | None
    created_at_hh: datetime | None
    archived_at_hh: datetime | None
    alternate_url: str | None
    employment_type_code: str | None
    schedule_type_code: str | None
    experience_code: str | None
    source_type: str
