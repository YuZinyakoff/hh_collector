from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pytest


def _load_module() -> ModuleType:
    script_path = (
        Path(__file__).resolve().parents[2] / "scripts" / "ops" / "collect_payload_inventory.py"
    )
    spec = importlib.util.spec_from_file_location("collect_payload_inventory", script_path)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_build_payload_inventory_queries_uses_bounded_json_samples() -> None:
    module = _load_module()

    queries = module.build_payload_inventory_queries(sample_rows=123, top_limit=7)

    names = [query.name for query in queries]
    assert "raw payload endpoint summary" in names
    assert "raw vacancy detail top-level keys sampled" in names
    assert "raw vacancy search item top-level keys sampled" in names
    assert "vacancy snapshot raw reference summary" in names
    combined_sql = "\n".join(query.sql.lower() for query in queries)
    assert "raw_api_payload" in combined_sql
    assert "vacancy_snapshot" in combined_sql
    assert "limit 123" in combined_sql
    assert "limit 7" in combined_sql


def test_main_skip_db_prints_read_only_header(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    module = _load_module()

    result = module.main(["--repo-root", str(tmp_path), "--skip-db"])

    captured = capsys.readouterr()
    assert result == 0
    assert "=== payload inventory ===" in captured.out
    assert "read_only=yes" in captured.out
    assert f"repo_root={tmp_path}" in captured.out


def test_psql_uses_statement_timeout_env(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_module()
    calls: list[tuple[list[str], Path]] = []

    def fake_run_and_print(args: list[str], *, cwd: Path) -> None:
        calls.append((list(args), cwd))

    monkeypatch.setattr(module, "run_and_print", fake_run_and_print)

    module.psql(tmp_path, "select 1;", statement_timeout_ms=42)

    assert len(calls) == 1
    args, cwd = calls[0]
    assert cwd == tmp_path
    assert args[:6] == [
        "docker",
        "compose",
        "exec",
        "-T",
        "-e",
        "PGOPTIONS=-c statement_timeout=42",
    ]
    assert args[6:9] == ["postgres", "psql", "-U"]
    assert "-v" in args
    assert "ON_ERROR_STOP=1" in args
