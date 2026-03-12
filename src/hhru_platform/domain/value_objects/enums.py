from enum import StrEnum


class CrawlRunStatus(StrEnum):
    CREATED = "created"


class CrawlPartitionStatus(StrEnum):
    PENDING = "pending"
