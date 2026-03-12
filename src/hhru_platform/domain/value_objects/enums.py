from enum import StrEnum


class CrawlRunStatus(StrEnum):
    CREATED = "created"


class CrawlPartitionStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    DONE = "done"
    FAILED = "failed"


class DictionarySyncStatus(StrEnum):
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
