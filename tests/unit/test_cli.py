from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime
from uuid import uuid4

from hhru_platform.domain.entities.crawl_run import CrawlRun
from hhru_platform.interfaces.cli.main import main


def test_cli_help_returns_zero(monkeypatch, capsys) -> None:
    monkeypatch.setattr("sys.argv", ["hhru-platform"])
    exit_code = main()
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "health-check" in captured.out
    assert "create-run" in captured.out


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
