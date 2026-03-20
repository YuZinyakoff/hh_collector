from enum import StrEnum


class CrawlRunStatus(StrEnum):
    CREATED = "created"
    COMPLETED = "completed"
    SUCCEEDED = "succeeded"
    COMPLETED_WITH_DETAIL_ERRORS = "completed_with_detail_errors"
    COMPLETED_WITH_UNRESOLVED = "completed_with_unresolved"
    FAILED = "failed"


class CrawlPartitionStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    SPLIT_REQUIRED = "split_required"
    SPLIT_DONE = "split_done"
    DONE = "done"
    FAILED = "failed"
    UNRESOLVED = "unresolved"


class CrawlPartitionCoverageStatus(StrEnum):
    UNASSESSED = "unassessed"
    COVERED = "covered"
    SATURATED = "saturated"
    SPLIT = "split"
    UNRESOLVED = "unresolved"


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
