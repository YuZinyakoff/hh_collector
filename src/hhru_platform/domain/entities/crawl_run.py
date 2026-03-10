from dataclasses import dataclass
from datetime import datetime
from uuid import UUID


@dataclass(slots=True)
class CrawlRun:
    id: UUID
    run_type: str
    status: str
    started_at: datetime
