from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID


@dataclass(slots=True)
class CrawlRun:
    id: UUID
    run_type: str
    status: str
    started_at: datetime
    finished_at: datetime | None
    triggered_by: str
    config_snapshot_json: dict[str, Any]
    partitions_total: int
    partitions_done: int
    partitions_failed: int
    notes: str | None
