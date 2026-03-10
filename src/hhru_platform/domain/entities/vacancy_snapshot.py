from dataclasses import dataclass
from datetime import datetime
from uuid import UUID


@dataclass(slots=True)
class VacancySnapshot:
    vacancy_id: UUID
    snapshot_type: str
    captured_at: datetime
