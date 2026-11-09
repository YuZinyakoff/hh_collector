from __future__ import annotations

import gzip
import hashlib
import importlib.util
import json
from copy import deepcopy
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


FULL_RUN = "00000000-0000-0000-0000-000000000001"


@pytest.fixture
def full_database_audit(audit_script, tmp_path, monkeypatch):
    import sqlalchemy

    import hhru_platform.config.settings as settings
    import hhru_platform.infrastructure.observability.search_quality_audit_db as db

    params = {
        "area": "1",
        "per_page": 20,
        "describe_arguments": "true",
        "date_from": "2026-11-02T00:00:00Z",
        "date_to": "2026-11-03T00:00:00Z",
    }
    partitions = [
        {
            "id": "p1",
            "crawl_run_id": FULL_RUN,
            "status": "done",
            "coverage_status": "covered",
            "params_json": {"params": params},
            "pages_processed": 3,
            "pages_total_expected": 3,
        },
        {
            "id": "p2",
            "crawl_run_id": FULL_RUN,
            "status": "done",
            "coverage_status": "covered",
            "params_json": {"params": {"area": "1", "describe_arguments": "true"}},
            "pages_processed": 1,
            "pages_total_expected": 0,
        },
    ]
    rows = {
        "p1": [
            {
                "api_request_log_id": 1,
                "crawl_run_id": FULL_RUN,
                "crawl_partition_id": "p1",
                "params_json": {**params, "page": 0},
                "status_code": 0,
                "error_type": "URLError",
                "raw_api_payload_id": None,
            }
        ],
        "p2": [],
    }
    for page in range(3):
        payload = {
            "items": [
                {
                    "id": str(page),
                    "name": "PRIVATE-NAME",
                    "area": {"id": "1"},
                    "published_at": "2026-11-09T09:34:11+0300"
                    if page == 1
                    else "2026-11-02T06:45:00Z",
                }
            ],
            "page": page,
            "pages": 3,
            "found": 41,
            "per_page": 20,
            "arguments": [{"argument": "area", "value": "1"}],
        }
        rows["p1"].append(
            {
                "api_request_log_id": page + 2,
                "raw_api_payload_id": page + 102,
                "crawl_run_id": FULL_RUN,
                "crawl_partition_id": "p1",
                "status_code": 200,
                "params_json": {**params, "page": page},
                "payload_json": payload,
                "payload_hash": payload_digest(payload),
            }
        )
    payload = {
        "items": [],
        "page": 0,
        "pages": 0,
        "found": 0,
        "arguments": [{"argument": "area", "value": "1"}],
    }
    rows["p2"].append(
        {
            "api_request_log_id": 5,
            "raw_api_payload_id": 105,
            "crawl_run_id": FULL_RUN,
            "crawl_partition_id": "p2",
            "status_code": 200,
            "params_json": {"area": "1", "page": 0, "describe_arguments": "true"},
            "payload_json": payload,
            "payload_hash": payload_digest(payload),
        }
    )

    class Reader:
        def __init__(self):
            self.inventory = {
                "requests": 5,
                "max_request_id": 5,
                "http_200_requests": 4,
                "raw_rows": 4,
                "missing_200_raw": 0,
                "multiple_raw_requests": 0,
                "request_run_mismatches": 0,
                "partition_run_mismatches": 0,
            }
            self.after_inventory = None
            self.inventory_calls = 0
            self.body_calls = 0
            self.run = {
                "id": FULL_RUN,
                "status": "succeeded",
                "finished_at": "2026-11-09T10:00:00Z",
            }
            self.rows = rows
            self.partitions_data = partitions
            self.seen_rows = [
                {
                    "short_payload_ref_id": row["raw_api_payload_id"],
                    "crawl_partition_id": row["crawl_partition_id"],
                    "seen_rows": len(row["payload_json"]["items"]),
                }
                for rs in rows.values()
                for row in rs
                if row.get("payload_json", {}).get("items")
            ]

        def areas(self):
            return [{"hh_area_id": "1", "parent_hh_area_id": None, "name": "Moscow"}]

        def runs(self, **kwargs):
            return [deepcopy(self.run)]

        def partitions(self, *args, **kwargs):
            return deepcopy(self.partitions_data)

        def request_inventory(self, *args):
            self.inventory_calls += 1
            return deepcopy(
                self.after_inventory
                if self.inventory_calls > 1 and self.after_inventory
                else self.inventory
            )

        def all_responses(self, partition_id, **kwargs):
            assert kwargs["through_request_id"] == 5
            self.body_calls += 1
            yield from deepcopy(self.rows[partition_id])

        def seen_inventory(self, *args, **kwargs):
            return deepcopy(self.seen_rows)

        def responses(self, *args, **kwargs):
            raise AssertionError("all mode must not use endpoint sampling")

        def derivatives(self, raw_id):
            row = next(
                r for rs in self.rows.values() for r in rs if r.get("raw_api_payload_id") == raw_id
            )
            return {
                "seen": [
                    {
                        "hh_vacancy_id": item["id"],
                        "short_hash": payload_digest(item),
                        "crawl_run_id": row["crawl_run_id"],
                        "crawl_partition_id": row["crawl_partition_id"],
                    }
                    for item in row["payload_json"]["items"]
                ],
                "snapshots": [],
            }

    reader = Reader()
    monkeypatch.setattr(db, "SearchAuditReader", lambda *args, **kwargs: reader)
    monkeypatch.setattr(sqlalchemy, "create_engine", lambda *args, **kwargs: MagicMock())
    monkeypatch.setattr(settings, "get_settings", lambda: SimpleNamespace(database_url="not-used"))
    output = tmp_path / "full-report"

    def run(extra=None):
        code = audit_script.main(
            [
                "--database",
                "--all-responses",
                "--run-id",
                FULL_RUN,
                "--output-dir",
                str(output),
                "--max-partitions",
                "10",
                "--max-requests",
                "10",
                "--response-batch-size",
                "2",
            ]
            + (extra or [])
        )
        return code, json.loads((output / "summary.json").read_text()), output

    return reader, run


def test_all_response_audit_visits_middle_pages_and_checks_progress(full_database_audit):
    reader, run = full_database_audit
    code, report, output = run()
    assert code == 0 and reader.body_calls == 2
    assert report["sampling"]["is_full_response_audit"] is True
    assert report["all_request_rows_visited"] is True
    assert report["requests_checked"] == report["counts"]["responses"] == 5
    assert (
        report["counts"]["payload_hash_checked"] == report["counts"]["area_arguments_checked"] == 4
    )
    assert report["counts"]["outside_window_over_300s"] == 1  # Only the middle page is anomalous.
    assert report["counts"]["raw_missing"] == 1
    assert report["page_accounting"]["partitions_checked"] == 2
    assert report["page_accounting"]["processed_page_count_mismatches"] == 0
    assert report["page_accounting"]["covered_expected_page_count_mismatches"] == 0
    assert report["derivative_checks"]["derivative_pages_checked"] == 1
    assert report["derivative_checks"]["raw_item_ids_without_seen"] == 0
    assert report["observation_count_checks"]["response_pages_checked"] == 4
    assert report["observation_count_checks"]["response_count_mismatches"] == 0
    assert report["observation_count_checks"]["unreconciled_seen_rows"] == 0
    assert "PRIVATE-NAME" not in (output / "evidence.jsonl").read_text()


@pytest.mark.parametrize("extra", [["--max-requests", "4"], ["--max-partitions", "1"]])
def test_all_response_caps_fail_before_payload_scan(full_database_audit, extra):
    reader, run = full_database_audit
    code, report, _ = run(extra)
    assert code == 2 and report["status"] == "incomplete"
    assert report["all_request_rows_visited"] is False
    assert reader.body_calls == 0


@pytest.mark.parametrize(
    "key", ["multiple_raw_requests", "request_run_mismatches", "partition_run_mismatches"]
)
def test_all_response_reference_anomalies_fail_closed(full_database_audit, key):
    reader, run = full_database_audit
    reader.inventory[key] = 1
    code, report, _ = run()
    assert code == 2 and report["status"] == "incomplete" and reader.body_calls == 0


def test_changed_source_cannot_be_called_full(full_database_audit):
    reader, run = full_database_audit
    reader.after_inventory = {**reader.inventory, "requests": 6}
    code, report, _ = run()
    assert code == 2 and report["all_request_rows_visited"] is False
    assert report["sampling"]["is_full_response_audit"] is False


@pytest.mark.parametrize("kind", ["oversize", "missing"])
def test_omitted_successful_body_is_not_complete_content(full_database_audit, kind):
    reader, run = full_database_audit
    row = reader.rows["p1"][2]
    if kind == "oversize":
        row.update(payload_json=None, payload_omitted=True)
    else:
        row.update(payload_json=None, raw_api_payload_id=None)
        reader.inventory.update(missing_200_raw=1, raw_rows=3)
    code, report, _ = run()
    assert code == 0  # Evidence collection is not an integrity verdict.
    assert report["all_request_rows_visited"] is True
    assert report["sampling"]["is_full_response_audit"] is False
    assert report["successful_response_content_omissions"] == 1


def test_duplicate_request_and_active_run_are_not_silently_truncated(full_database_audit):
    reader, run = full_database_audit
    reader.rows["p1"].append(reader.rows["p1"][1])
    code, report, _ = run()
    assert code == 2 and report["sampling"]["is_full_response_audit"] is False


def test_running_source_is_rejected_before_audit(full_database_audit):
    reader, run = full_database_audit
    reader.run.update(status="created", finished_at=None)
    code, report, _ = run()
    assert code == 2 and report["status"] == "incomplete" and reader.body_calls == 0


def test_all_response_count_check_detects_missing_and_misattributed_seen(full_database_audit):
    reader, run = full_database_audit
    reader.seen_rows[0]["seen_rows"] = 0
    reader.seen_rows[1]["crawl_partition_id"] = "wrong-partition"
    code, report, _ = run()
    assert code == 0 and report["sampling"]["is_full_response_audit"] is True
    assert report["observation_count_checks"]["response_count_mismatches"] == 2
    assert report["observation_count_checks"]["unreconciled_seen_rows"] == 1


@pytest.mark.parametrize("changed", ["partitions", "seen"])
def test_inventory_change_after_content_scan_prevents_completion(
    full_database_audit, monkeypatch, changed
):
    reader, run = full_database_audit
    method = "partitions" if changed == "partitions" else "seen_inventory"
    original = getattr(reader, method)
    calls = 0

    def changing(*args, **kwargs):
        nonlocal calls
        calls += 1
        result = original(*args, **kwargs)
        if calls > 1:
            result[0]["pages_processed" if changed == "partitions" else "seen_rows"] = 99
        return result

    monkeypatch.setattr(reader, method, changing)
    code, report, _ = run()
    assert code == 2 and report["all_request_rows_visited"] is False
    assert report["incomplete_reason"] == "source_changed_or_requests_not_fully_traversed"


def test_seen_group_budget_fails_before_body_scan(full_database_audit):
    reader, run = full_database_audit
    reader.seen_rows = reader.seen_rows * 8
    code, report, _ = run()
    assert code == 2 and reader.body_calls == 0
    assert report["incomplete_reason"] == "seen_group_budget_exceeded"


def test_report_retains_aggregates_not_response_examples(audit_script, tmp_path):
    report = audit_script.AuditReport(tmp_path / "aggregate-report")
    for _ in range(3):
        report.inspect(archive_row(), {"1": None, "144": None})
    assert len(report.groups) == 1 and report.response_count == 3
    assert "examples" not in next(iter(report.groups.values()))
    report.finish(status="evidence_collected")
    assert report.metadata["counts"]["responses"] == 3


@pytest.mark.parametrize(
    "args",
    [
        ["--archive-dir", "archive", "--area-reference", "areas"],
        ["--database"],
        ["--database", "--run-id", FULL_RUN, "--response-batch-size", "51"],
        ["--database", "--run-id", FULL_RUN, "--response-batch-size", "50"],
    ],
)
def test_all_response_cli_rejects_ambiguous_or_oversized_sources(audit_script, args):
    with pytest.raises(SystemExit):
        audit_script.parse_args(args + ["--all-responses", "--output-dir", "report"])
