from dataclasses import dataclass
from datetime import datetime
from uuid import UUID


@dataclass(slots=True)
class VacancyCurrentState:
    vacancy_id: UUID
    first_seen_at: datetime
    last_seen_at: datetime
