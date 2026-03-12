from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from hhru_platform.application.commands.create_crawl_run import (
    CreateCrawlRunCommand,
    create_crawl_run,
)
from hhru_platform.domain.entities.crawl_run import CrawlRun


class InMemoryCrawlRunRepository:
    def __init__(self) -> None:
        self.created: list[CrawlRun] = []

    def add(self, *, run_type: str, status: str, triggered_by: str) -> CrawlRun:
        crawl_run = CrawlRun(
            id=uuid4(),
            run_type=run_type,
            status=status,
            started_at=datetime(2026, 3, 12, 12, 0, tzinfo=UTC),
            finished_at=None,
            triggered_by=triggered_by,
            config_snapshot_json={},
            partitions_total=0,
            partitions_done=0,
            partitions_failed=0,
            notes=None,
        )
        self.created.append(crawl_run)
        return crawl_run


def test_create_crawl_run_returns_created_entity() -> None:
    repository = InMemoryCrawlRunRepository()

    result = create_crawl_run(
        CreateCrawlRunCommand(run_type="weekly_sweep", triggered_by="cli"),
        repository,
    )

    assert result.run_type == "weekly_sweep"
    assert result.triggered_by == "cli"
    assert result.status == "created"
    assert repository.created == [result]


def test_create_crawl_run_command_normalizes_inputs() -> None:
    command = CreateCrawlRunCommand(run_type="  weekly_sweep  ", triggered_by="  cli  ")

    assert command.run_type == "weekly_sweep"
    assert command.triggered_by == "cli"
