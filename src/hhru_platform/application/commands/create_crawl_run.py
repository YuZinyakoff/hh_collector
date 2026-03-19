from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Protocol

from hhru_platform.domain.entities.crawl_run import CrawlRun
from hhru_platform.domain.value_objects.enums import CrawlRunStatus
from hhru_platform.infrastructure.observability.operations import (
    log_operation_started,
    record_operation_failed,
    record_operation_succeeded,
)

LOGGER = logging.getLogger(__name__)


@dataclass(slots=True, frozen=True)
class CreateCrawlRunCommand:
    run_type: str
    triggered_by: str

    def __post_init__(self) -> None:
        normalized_run_type = self.run_type.strip()
        normalized_triggered_by = self.triggered_by.strip()

        if not normalized_run_type:
            raise ValueError("run_type must not be empty")

        if not normalized_triggered_by:
            raise ValueError("triggered_by must not be empty")

        object.__setattr__(self, "run_type", normalized_run_type)
        object.__setattr__(self, "triggered_by", normalized_triggered_by)


class CrawlRunRepository(Protocol):
    def add(self, *, run_type: str, status: str, triggered_by: str) -> CrawlRun:
        """Persist and return a newly created crawl run."""


def create_crawl_run(command: CreateCrawlRunCommand, repository: CrawlRunRepository) -> CrawlRun:
    started_at = log_operation_started(
        LOGGER,
        operation="create_crawl_run",
        run_type=command.run_type,
        triggered_by=command.triggered_by,
    )
    try:
        crawl_run = repository.add(
            run_type=command.run_type,
            status=CrawlRunStatus.CREATED.value,
            triggered_by=command.triggered_by,
        )
    except Exception as error:
        record_operation_failed(
            LOGGER,
            operation="create_crawl_run",
            started_at=started_at,
            error_type=error.__class__.__name__,
            error_message=str(error),
            run_type=command.run_type,
            triggered_by=command.triggered_by,
        )
        raise

    record_operation_succeeded(
        LOGGER,
        operation="create_crawl_run",
        started_at=started_at,
        records_written={"crawl_run": 1},
        run_id=crawl_run.id,
        run_type=crawl_run.run_type,
        triggered_by=crawl_run.triggered_by,
        run_status=crawl_run.status,
    )
    return crawl_run
