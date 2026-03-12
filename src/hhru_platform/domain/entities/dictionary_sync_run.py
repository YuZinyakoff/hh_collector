from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from uuid import UUID


@dataclass(slots=True)
class DictionarySyncRun:
    id: UUID
    dictionary_name: str
    status: str
    etag: str | None
    source_status_code: int | None
    notes: str | None
    started_at: datetime
    finished_at: datetime | None
