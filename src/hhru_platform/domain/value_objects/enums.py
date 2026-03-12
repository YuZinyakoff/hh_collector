from enum import StrEnum


class CrawlRunStatus(StrEnum):
    CREATED = "created"


class CrawlPartitionStatus(StrEnum):
    PENDING = "pending"


class DictionarySyncStatus(StrEnum):
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
