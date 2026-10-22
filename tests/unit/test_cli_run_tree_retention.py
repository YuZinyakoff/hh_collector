from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from hhru_platform.config.settings import Settings
from hhru_platform.interfaces.cli.main import main


@pytest.mark.parametrize(
    "extra_args, expected",
    [
        ([], (1, 100_000)),
        (
            ["--run-tree-delete-limit", "2", "--max-run-tree-cascade-rows", "2500000"],
            (2, 2_500_000),
        ),
    ],
)
def test_cli_passes_run_tree_budgets_to_apply(monkeypatch, extra_args, expected) -> None:
    commands = []

    @contextmanager
    def session_scope():
        yield object()

    def apply(command, **kwargs):
        commands.append(command)
        return SimpleNamespace()

    module = "hhru_platform.interfaces.cli.commands.research_archive"
    monkeypatch.setattr(f"{module}.session_scope", session_scope)
    monkeypatch.setattr(f"{module}._ensure_research_archive_s3_backend", lambda settings: None)
    monkeypatch.setattr(f"{module}.apply_research_archive_housekeeping", apply)
    monkeypatch.setattr(f"{module}._print_housekeeping_apply_result", lambda result: None)
    monkeypatch.setattr(
        f"{module}.get_settings",
        lambda: Settings(
            _env_file=None,
            research_archive_offsite_backend="s3",
            research_archive_offsite_s3_endpoint_url="https://s3.example.test",
            research_archive_offsite_s3_bucket="test-bucket",
        ),
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "hhru-platform",
            "apply-research-archive-housekeeping",
            "--apply",
            *extra_args,
        ],
    )
    assert main() == 0
    assert len(commands) == 1
    assert (commands[0].run_tree_delete_limit, commands[0].max_run_tree_cascade_rows) == expected


@pytest.mark.parametrize("flag", ["--run-tree-delete-limit", "--max-run-tree-cascade-rows"])
def test_cli_rejects_zero_run_tree_budget(monkeypatch, flag) -> None:
    monkeypatch.setattr(
        "sys.argv",
        [
            "hhru-platform",
            "apply-research-archive-housekeeping",
            "--apply",
            flag,
            "0",
        ],
    )
    with pytest.raises(SystemExit) as error:
        main()
    assert error.value.code == 2
