from contextlib import contextmanager
from types import SimpleNamespace
from uuid import uuid4

from hhru_platform.interfaces.cli.commands import run_tree_metadata
from hhru_platform.interfaces.cli.main import build_parser


def test_metadata_export_cli_sets_read_only_snapshot_before_reading(monkeypatch, tmp_path, capsys):
    statements = []
    run_id = uuid4()

    class Session:
        def execute(self, statement):
            statements.append(str(statement))

    @contextmanager
    def session_scope():
        yield Session()

    def export(**kwargs):
        assert statements[0] == "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"
        assert kwargs["run_id"] == run_id
        return SimpleNamespace(run_id=run_id, directory=tmp_path, partition_count=2)

    monkeypatch.setattr(run_tree_metadata, "session_scope", session_scope)
    monkeypatch.setattr(run_tree_metadata, "export_run_tree_metadata", export)
    args = build_parser().parse_args(
        [
            "export-run-tree-metadata",
            "--run-id",
            str(run_id),
            "--output-dir",
            str(tmp_path),
        ]
    )
    assert args.handler(args) == 0
    assert "partition_count=2" in capsys.readouterr().out


def test_metadata_verification_cli_fails_closed_for_missing_bundle(tmp_path, capsys):
    args = build_parser().parse_args(["verify-run-tree-metadata", "--bundle-dir", str(tmp_path)])
    assert args.handler(args) == 1
    assert capsys.readouterr().err


def test_metadata_sync_cli_is_registered_without_accessing_services(tmp_path):
    args = build_parser().parse_args(["sync-run-tree-metadata", "--bundle-dir", str(tmp_path)])
    assert args.handler is run_tree_metadata.handle_sync
    assert args.bundle_dir == tmp_path
