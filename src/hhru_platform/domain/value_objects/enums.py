from enum import StrEnum


class CrawlRunStatus(StrEnum):
    CREATED = "created"
    COMPLETED = "completed"


class CrawlPartitionStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    DONE = "done"
    FAILED = "failed"


class DictionarySyncStatus(StrEnum):
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class DetailFetchStatus(StrEnum):
    NOT_REQUESTED = "not_requested"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class VacancySnapshotType(StrEnum):
    DETAIL = "detail"
