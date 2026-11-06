from __future__ import annotations

import gzip
import hashlib
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from hhru_platform.infrastructure.observability.search_quality_audit import payload_digest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts/analysis/audit_search_quality.py"


@pytest.fixture
def audit_script():
    spec = importlib.util.spec_from_file_location("audit_search_quality_cli", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def write_reference(tmp_path: Path) -> Path:
    reference = tmp_path / "areas.json"
    reference.write_text(
        json.dumps(
            {
                "captured_at": "2026-11-07T00:00:00Z",
                "areas": [
                    {"hh_area_id": "144", "parent_hh_area_id": None, "name": "Autopoligon"},
                    {"hh_area_id": "1", "parent_hh_area_id": None, "name": "Moscow"},
                ],
            }
        )
    )
    return reference


def write_chunk(root: Path, name: str, rows: list[dict]) -> Path:
    directory = root / "v1/bronze/raw_api_payload/request_type=vacancy_search"
    directory.mkdir(parents=True, exist_ok=True)
    chunk = directory / f"{name}.jsonl.gz"
    with gzip.open(chunk, "wt") as handle:
        for row in rows:
            handle.write(json.dumps(row) + "\n")
    manifest = directory / f"{name}.manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "data_file": str(chunk.relative_to(root)),
                "data_sha256": hashlib.sha256(chunk.read_bytes()).hexdigest(),
            }
        )
    )
    return manifest


def archive_row() -> dict:
    payload = {
        "items": [{"id": "1", "name": "Engineer", "area": {"id": "1"}}],
        "found": 1,
        "pages": 1,
    }
    return {
        "request_type": "vacancy_search",
        "raw_api_payload_id": 123,
        "api_request_log_id": 234,
        "status_code": 200,
        "crawl_run_id": "00000000-0000-0000-0000-000000000001",
        "params_json": {"area": "144"},
        "payload_json": payload,
        "payload_hash": payload_digest(payload),
    }


def test_archive_end_to_end_checks_hash_and_deduplicates_without_db_or_http(audit_script, tmp_path):
    root, output = tmp_path / "archive", tmp_path / "report"
    write_chunk(root, "one", [archive_row(), archive_row()])
    code = audit_script.main(
        [
            "--archive-dir",
            str(root),
            "--area-reference",
            str(write_reference(tmp_path)),
            "--output-dir",
            str(output),
        ]
    )
    assert code == 0
    report = json.loads((output / "summary.json").read_text())
    assert report["status"] == "evidence_collected"
    assert report["live_http_requests"] == 0
    assert report["counts"]["responses"] == 1
    assert report["counts"]["area_outside_current_tree"] == 1
    assert report["counts"]["payload_hash_mismatch"] == 0
    assert report["archive_counts"]["duplicate_archive_raw_ids"] == 1
    assert output.stat().st_mode & 0o077 == 0
    assert "payload_json" not in (output / "evidence.jsonl").read_text()
    with pytest.raises(FileExistsError):
        audit_script.AuditReport(output)


def test_archive_checksums_missing_files_limits_and_path_traversal(audit_script, tmp_path):
    root, output = tmp_path / "archive", tmp_path / "report"
    bad = write_chunk(root, "bad", [archive_row()])
    document = json.loads(bad.read_text())
    document["data_sha256"] = "wrong"
    bad.write_text(json.dumps(document))
    missing = write_chunk(root, "missing", [archive_row()])
    (root / json.loads(missing.read_text())["data_file"]).unlink()
    code = audit_script.main(
        [
            "--archive-dir",
            str(root),
            "--area-reference",
            str(write_reference(tmp_path)),
            "--output-dir",
            str(output),
        ]
    )
    assert code == 0  # Evidence collection completed, not a passing integrity verdict.
    report = json.loads((output / "summary.json").read_text())
    assert report["archive_counts"]["chunk_checksum_mismatch"] == 1
    assert report["archive_counts"]["local_chunks_missing"] == 1
    assert report["counts"].get("responses", 0) == 0
    with pytest.raises(ValueError, match="escapes"):
        audit_script.safe_chunk_path(root, "../private")
    with pytest.raises(ValueError, match="escapes"):
        audit_script.safe_chunk_path(root, "/etc/passwd")


def test_row_limit_and_run_filter_are_explicit(audit_script, tmp_path):
    root, output = tmp_path / "archive", tmp_path / "report"
    row = archive_row()
    write_chunk(root, "one", [row, {**row, "raw_api_payload_id": 456}])
    assert (
        audit_script.main(
            [
                "--archive-dir",
                str(root),
                "--area-reference",
                str(write_reference(tmp_path)),
                "--output-dir",
                str(output),
                "--max-rows",
                "1",
                "--run-id",
                "00000000-0000-0000-0000-000000000002",
            ]
        )
        == 0
    )
    report = json.loads((output / "summary.json").read_text())
    assert report["archive_counts"]["rows_read"] == 1
    assert report["archive_counts"]["rows_outside_selected_runs"] == 1
    assert report["archive_limits"]["max_rows"] == 1


def test_failure_report_is_partial_and_never_prints_exception_secrets(
    audit_script, tmp_path, monkeypatch, capsys
):
    def fail(*args):
        raise RuntimeError("postgresql://user:SECRET@host/database")

    monkeypatch.setattr(audit_script, "collect_database", fail)
    output = tmp_path / "report"
    assert audit_script.main(["--database", "--output-dir", str(output)]) == 2
    assert "SECRET" not in capsys.readouterr().out
    assert "SECRET" not in (output / "evidence.jsonl").read_text()
    assert json.loads((output / "summary.json").read_text())["status"] == "incomplete"


def test_database_report_includes_unselected_strata_missing_raw_and_provenance(
    audit_script, tmp_path, monkeypatch
):
    import sqlalchemy

    import hhru_platform.config.settings as settings
    import hhru_platform.infrastructure.observability.search_quality_audit_db as db

    class Reader:
        def __init__(self, *args, **kwargs):
            pass

        def areas(self):
            return [{"hh_area_id": "1", "parent_hh_area_id": None}]

        def runs(self, **kwargs):
            return [{"id": "run", "status": "succeeded"}]

        def partitions(self, *args, **kwargs):
            return [
                {
                    "id": str(i),
                    "crawl_run_id": "run",
                    "coverage_status": "covered",
                    "params_json": {"params": {"area": str(i)}},
                }
                for i in range(1, 4)
            ]

        def responses(self, partition_id, **kwargs):
            return [{**archive_row(), "raw_api_payload_id": None}, archive_row()]

        def derivatives(self, *args):
            return {"seen": [], "snapshots": []}

    monkeypatch.setattr(db, "SearchAuditReader", Reader)
    monkeypatch.setattr(sqlalchemy, "create_engine", lambda *args, **kwargs: MagicMock())
    monkeypatch.setattr(settings, "get_settings", lambda: SimpleNamespace(database_url="not-used"))
    output = tmp_path / "report"
    assert (
        audit_script.main(["--database", "--output-dir", str(output), "--max-partitions", "2"]) == 0
    )
    report = json.loads((output / "summary.json").read_text())
    assert report["sampling"]["strata_available"] == 3
    assert report["sampling"]["strata_selected"] == 2
    assert sum(s["partitions_selected"] == 0 for s in report["strata"]) == 1
    assert report["counts"]["raw_missing"] == 2
    assert report["derivative_checks"]["seen_absent_pages"] == 2


def test_metadata_cap_does_not_silently_audit_uuid_prefix(audit_script, tmp_path, monkeypatch):
    import sqlalchemy

    import hhru_platform.config.settings as settings
    import hhru_platform.infrastructure.observability.search_quality_audit_db as db

    reader = MagicMock()
    reader.areas.return_value = []
    reader.runs.return_value = [{"id": "run"}]
    reader.partitions.return_value = [{}, {}]
    monkeypatch.setattr(db, "SearchAuditReader", lambda *args, **kwargs: reader)
    monkeypatch.setattr(sqlalchemy, "create_engine", lambda *args, **kwargs: MagicMock())
    monkeypatch.setattr(settings, "get_settings", lambda: SimpleNamespace(database_url="not-used"))
    output = tmp_path / "report"
    assert (
        audit_script.main(
            ["--database", "--output-dir", str(output), "--metadata-limit-per-run", "1"]
        )
        == 2
    )
    reader.responses.assert_not_called()


def test_archive_duplicate_with_same_claimed_hash_but_different_body_is_reported(
    audit_script, tmp_path
):
    root, output = tmp_path / "archive", tmp_path / "report"
    row = archive_row()
    altered = {**row, "payload_json": {"items": [], "found": 0}}
    write_chunk(root, "one", [row, altered])
    assert (
        audit_script.main(
            [
                "--archive-dir",
                str(root),
                "--area-reference",
                str(write_reference(tmp_path)),
                "--output-dir",
                str(output),
            ]
        )
        == 0
    )
    report = json.loads((output / "summary.json").read_text())
    assert report["archive_counts"]["duplicate_archive_raw_hash_conflicts"] == 1


def test_archive_manifest_and_compressed_size_caps(audit_script, tmp_path):
    root, output = tmp_path / "archive", tmp_path / "report"
    write_chunk(root, "one", [archive_row()])
    write_chunk(root, "two", [archive_row()])
    assert (
        audit_script.main(
            [
                "--archive-dir",
                str(root),
                "--area-reference",
                str(write_reference(tmp_path)),
                "--output-dir",
                str(output),
                "--max-manifests",
                "1",
                "--max-chunk-bytes",
                "1",
            ]
        )
        == 0
    )
    report = json.loads((output / "summary.json").read_text())
    assert report["local_manifests_available"] == 2
    assert report["archive_counts"]["manifests_selected"] == 1
    assert report["archive_counts"]["chunks_over_size_limit"] == 1


def test_archive_address_support_uses_reference_names_without_clearing_area_signal(
    audit_script, tmp_path
):
    root, output = tmp_path / "archive", tmp_path / "report"
    row = archive_row()
    row["params_json"]["area"] = "1"
    row["payload_json"]["items"][0].update(area={"id": "144"}, address={"city": "Moscow"})
    row["payload_hash"] = payload_digest(row["payload_json"])
    write_chunk(root, "one", [row])
    assert (
        audit_script.main(
            [
                "--archive-dir",
                str(root),
                "--area-reference",
                str(write_reference(tmp_path)),
                "--output-dir",
                str(output),
            ]
        )
        == 0
    )
    report = json.loads((output / "summary.json").read_text())
    assert report["report_schema"] == "search-quality-audit-v2"
    assert report["counts"]["area_outside_current_tree"] == 1
    assert report["counts"]["area_outside_address_supports_requested_area"] == 1
