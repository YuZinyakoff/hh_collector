from dataclasses import dataclass
from uuid import UUID


@dataclass(slots=True)
class CrawlPartition:
    id: UUID
    crawl_run_id: UUID
    partition_key: str
    status: str
