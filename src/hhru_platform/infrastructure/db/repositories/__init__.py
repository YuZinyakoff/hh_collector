"""Database repositories."""

from hhru_platform.infrastructure.db.repositories.crawl_partition_repo import (
    SqlAlchemyCrawlPartitionRepository,
)
from hhru_platform.infrastructure.db.repositories.crawl_run_repo import SqlAlchemyCrawlRunRepository

__all__ = ["SqlAlchemyCrawlPartitionRepository", "SqlAlchemyCrawlRunRepository"]
