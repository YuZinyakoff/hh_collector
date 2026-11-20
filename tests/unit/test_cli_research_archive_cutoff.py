from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from hhru_platform.application.commands.export_research_archive import (
    ExportResearchArchiveCommand,
    ExportResearchArchiveResult,
)
from hhru_platform.interfaces.cli.commands import research_archive as cli
from hhru_platform.interfaces.cli.main import build_parser


@pytest.mark.parametrize(
    "value",
    ["2026-11-11T13:00:00Z", "2026-11-11T13:00:00+00:00", "2026-11-11T16:00:00+03:00"],
)
def test_archive_cutoff_parser_normalizes_timezone(value: str) -> None:
    args = build_parser().parse_args(
        ["export-research-archive", "--incremental", "--settled-before", value]
    )
    assert args.settled_before == datetime(2026, 11, 11, 13, tzinfo=UTC)
    assert args.settled_before.tzinfo is UTC


@pytest.mark.parametrize("value", ["not-a-date", "2026-11-11", "2026-11-11T13:00:00"])
def test_archive_cutoff_parser_rejects_invalid_or_naive_times(value: str) -> None:
    with pytest.raises(SystemExit) as error:
        build_parser().parse_args(
            ["export-research-archive", "--incremental", "--settled-before", value]
        )
    assert error.value.code == 2


def test_fixed_archive_cutoff_does_not_move_as_batches_age() -> None:
    fixed = datetime(2026, 11, 11, 13, tzinfo=UTC)
    for hour in (14, 15, 18):
        assert (
            cli._export_settled_before(
                incremental=True,
                delay_hours=24,
                fixed_cutoff=fixed,
                now=datetime(2026, 11, 12, hour, tzinfo=UTC),
            )
            == fixed
        )


def test_relative_archive_cutoff_keeps_existing_manual_cli_default() -> None:
    now = datetime(2026, 11, 12, 14, tzinfo=UTC)
    assert cli._export_settled_before(
        incremental=True, delay_hours=24, fixed_cutoff=None, now=now
    ) == now - timedelta(days=1)
    assert (
        cli._export_settled_before(incremental=False, delay_hours=24, fixed_cutoff=None, now=now)
        is None
    )


@pytest.mark.parametrize("offset_hours", [0, 1, 23])
def test_fixed_archive_cutoff_cannot_bypass_safety_delay(offset_hours: int) -> None:
    now = datetime(2026, 11, 12, 14, tzinfo=UTC)
    with pytest.raises(ValueError, match="must not be newer"):
        cli._export_settled_before(
            incremental=True,
            delay_hours=24,
            fixed_cutoff=now - timedelta(hours=offset_hours),
            now=now,
        )


def test_fixed_archive_cutoff_accepts_exact_delay_boundary() -> None:
    now = datetime(2026, 11, 12, 14, tzinfo=UTC)
    fixed = now - timedelta(days=1)
    assert (
        cli._export_settled_before(incremental=True, delay_hours=24, fixed_cutoff=fixed, now=now)
        == fixed
    )


@pytest.mark.parametrize("incremental", [True, False])
def test_invalid_archive_cutoff_is_rejected_before_opening_database(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    incremental: bool,
) -> None:
    arguments = ["export-research-archive", "--settled-before", "2999-01-01T00:00:00Z"]
    if incremental:
        arguments.append("--incremental")
    args = build_parser().parse_args(arguments)
    monkeypatch.setattr(cli, "get_settings", lambda: SimpleNamespace())

    def unexpected_session() -> None:
        pytest.fail("invalid cutoff must not open a database session")

    monkeypatch.setattr(cli, "session_scope", unexpected_session)
    assert args.handler(args) == 2
    expected = "must not be newer" if incremental else "requires --incremental"
    assert expected in capsys.readouterr().err


@pytest.mark.parametrize("mode", ["fixed", "relative", "full"])
def test_archive_cli_passes_cutoff_to_export_and_report(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    mode: str,
) -> None:
    fixed = datetime.now(UTC) - timedelta(days=2)
    arguments = ["export-research-archive"]
    if mode != "full":
        arguments.append("--incremental")
    if mode == "fixed":
        arguments.extend(["--settled-before", fixed.isoformat()])
    args = build_parser().parse_args(arguments)
    commands: list[ExportResearchArchiveCommand] = []
    monkeypatch.setattr(
        cli, "get_settings", lambda: SimpleNamespace(db_name="test", research_archive_dir=tmp_path)
    )
    monkeypatch.setattr(cli, "_git_revision", lambda: "unit-test")

    @contextmanager
    def session_scope() -> Iterator[object]:
        yield object()

    def export(
        command: ExportResearchArchiveCommand, **kwargs: object
    ) -> ExportResearchArchiveResult:
        commands.append(command)
        return ExportResearchArchiveResult(
            status="succeeded",
            archive_dir=command.archive_dir,
            schema_version="research-archive-v1",
            archive_kind=command.archive_kind,
            triggered_by=command.triggered_by,
            created_at=datetime.now(UTC),
            incremental=command.incremental,
            settled_before=command.settled_before,
            checkpoint_file=None,
            summaries=(),
        )

    monkeypatch.setattr(cli, "session_scope", session_scope)
    monkeypatch.setattr(cli, "SqlAlchemyResearchArchiveRepository", lambda session: object())
    monkeypatch.setattr(cli, "export_research_archive", export)
    before = datetime.now(UTC)
    assert args.handler(args) == 0
    after = datetime.now(UTC)
    assert len(commands) == 1
    cutoff = commands[0].settled_before
    if mode == "fixed":
        assert cutoff == fixed
    elif mode == "relative":
        assert cutoff is not None
        assert before - timedelta(days=1) <= cutoff <= after - timedelta(days=1)
    else:
        assert cutoff is None
    output = capsys.readouterr().out
    assert f"settled_before={cutoff.isoformat() if cutoff else '-'}" in output
