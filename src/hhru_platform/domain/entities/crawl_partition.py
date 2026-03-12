from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID


@dataclass(slots=True)
class CrawlPartition:
    id: UUID
    crawl_run_id: UUID
    partition_key: str
    params_json: dict[str, Any]
    status: str
    pages_total_expected: int | None
    pages_processed: int
    items_seen: int
    retry_count: int
    started_at: datetime | None
    finished_at: datetime | None
    last_error_message: str | None
    created_at: datetime
