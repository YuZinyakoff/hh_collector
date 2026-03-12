from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import uuid4

from hhru_platform.domain.entities.crawl_partition import CrawlPartition
from hhru_platform.domain.entities.crawl_run import CrawlRun
from hhru_platform.interfaces.cli.main import main


def test_cli_help_returns_zero(monkeypatch, capsys) -> None:
    monkeypatch.setattr("sys.argv", ["hhru-platform"])
    exit_code = main()
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "health-check" in captured.out
    assert "create-run" in captured.out
    assert "plan-run" in captured.out


def test_create_run_cli_prints_created_run(monkeypatch, capsys) -> None:
    created_run = CrawlRun(
        id=uuid4(),
        run_type="weekly_sweep",
        status="created",
        started_at=datetime(2026, 3, 12, 12, 0, tzinfo=UTC),
        finished_at=None,
        triggered_by="cli",
        config_snapshot_json={},
        partitions_total=0,
        partitions_done=0,
        partitions_failed=0,
        notes=None,
    )

    @contextmanager
    def fake_session_scope():
        yield object()

    def fake_create_crawl_run(command, repository) -> CrawlRun:
        assert command.run_type == "weekly_sweep"
        assert command.triggered_by == "cli"
        return created_run

    class FakeCrawlRunRepository:
        def __init__(self, session: object) -> None:
            self.session = session

    monkeypatch.setattr(
        "hhru_platform.interfaces.cli.commands.run.session_scope",
        fake_session_scope,
    )
    monkeypatch.setattr(
        "hhru_platform.interfaces.cli.commands.run.SqlAlchemyCrawlRunRepository",
        FakeCrawlRunRepository,
    )
    monkeypatch.setattr(
        "hhru_platform.interfaces.cli.commands.run.create_crawl_run",
        fake_create_crawl_run,
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "hhru-platform",
            "create-run",
            "--run-type",
            "weekly_sweep",
            "--triggered-by",
            "cli",
        ],
    )

    exit_code = main()
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "created crawl_run" in captured.out
    assert f"id={created_run.id}" in captured.out
    assert "status=created" in captured.out


def test_plan_run_cli_prints_planned_partitions(monkeypatch, capsys) -> None:
    created_partition = CrawlPartition(
        id=uuid4(),
        crawl_run_id=uuid4(),
        partition_key="global-default",
        params_json={"planner_policy": "single_partition_v1"},
        status="pending",
        pages_total_expected=None,
        pages_processed=0,
        items_seen=0,
        retry_count=0,
        started_at=None,
        finished_at=None,
        last_error_message=None,
        created_at=datetime(2026, 3, 12, 12, 5, tzinfo=UTC),
    )

    @contextmanager
    def fake_session_scope():
        yield object()

    def fake_plan_sweep(command, crawl_run_repository, crawl_partition_repository, planner_policy):
        assert command.crawl_run_id == created_partition.crawl_run_id
        assert planner_policy.__class__.__name__ == "SinglePartitionPlannerPolicyV1"
        return SimpleNamespace(
            crawl_run_id=created_partition.crawl_run_id,
            created_partitions=[created_partition],
            partitions=[created_partition],
        )

    class FakeCrawlRunRepository:
        def __init__(self, session: object) -> None:
            self.session = session

    class FakeCrawlPartitionRepository:
        def __init__(self, session: object) -> None:
            self.session = session

    monkeypatch.setattr(
        "hhru_platform.interfaces.cli.commands.partition.session_scope",
        fake_session_scope,
    )
    monkeypatch.setattr(
        "hhru_platform.interfaces.cli.commands.partition.SqlAlchemyCrawlRunRepository",
        FakeCrawlRunRepository,
    )
    monkeypatch.setattr(
        "hhru_platform.interfaces.cli.commands.partition.SqlAlchemyCrawlPartitionRepository",
        FakeCrawlPartitionRepository,
    )
    monkeypatch.setattr(
        "hhru_platform.interfaces.cli.commands.partition.plan_sweep",
        fake_plan_sweep,
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "hhru-platform",
            "plan-run",
            "--run-id",
            str(created_partition.crawl_run_id),
        ],
    )

    exit_code = main()
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "planned crawl partitions" in captured.out
    assert f"run_id={created_partition.crawl_run_id}" in captured.out
    assert "partitions_created=1" in captured.out
    assert f"key={created_partition.partition_key}" in captured.out
