from dataclasses import dataclass
from datetime import datetime
from uuid import UUID


@dataclass(slots=True)
class VacancySeenEvent:
    vacancy_id: UUID
    crawl_run_id: UUID
    crawl_partition_id: UUID
    seen_at: datetime
