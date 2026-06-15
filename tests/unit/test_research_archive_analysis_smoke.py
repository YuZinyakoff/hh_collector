from __future__ import annotations

import gzip
import hashlib
import json
import subprocess
import sys
from pathlib import Path


def test_research_archive_dataframe_smoke_reads_local_archive_summary(tmp_path: Path) -> None:
    archive_dir = tmp_path / "archive"
    output_dir = tmp_path / "analysis"
    data_file = (
        archive_dir
        / "v1"
        / "silver"
        / "vacancy_snapshot"
        / "snapshot_type=short"
        / "year=2026"
        / "month=06"
        / "day=01"
        / "20260601T000000Z-chunk-000001.jsonl.gz"
    )
    manifest_file = data_file.with_suffix("").with_suffix(".manifest.json")
    data_file.parent.mkdir(parents=True)

    rows = [
        {
            "archive_schema_version": "research-archive-v1",
            "dataset": "vacancy_snapshot",
            "snapshot_id": 1,
            "vacancy_id": 10,
            "hh_vacancy_id": "100",
            "snapshot_type": "short",
            "captured_at": "2026-06-01T00:00:00+00:00",
        },
        {
            "archive_schema_version": "research-archive-v1",
            "dataset": "vacancy_snapshot",
            "snapshot_id": 2,
            "vacancy_id": 11,
            "hh_vacancy_id": "101",
            "snapshot_type": "short",
            "captured_at": "2026-06-01T00:01:00+00:00",
        },
    ]
    with gzip.open(data_file, "wt", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row) + "\n")

    data_sha256 = _sha256(data_file)
    manifest_payload = {
        "archive_schema_version": "research-archive-v1",
        "archive_kind": "production",
        "dataset": "vacancy_snapshot",
        "dataset_key": "silver/vacancy_snapshot",
        "layer": "silver",
        "row_count": len(rows),
        "data_file": data_file.relative_to(archive_dir).as_posix(),
        "data_size_bytes": data_file.stat().st_size,
        "data_sha256": data_sha256,
    }
    manifest_file.write_text(json.dumps(manifest_payload), encoding="utf-8")

    result = subprocess.run(
        [
            sys.executable,
            "scripts/analysis/research_archive_dataframe_smoke.py",
            "--archive-dir",
            str(archive_dir),
            "--output-dir",
            str(output_dir),
            "--summary-only",
        ],
        check=True,
        cwd=Path(__file__).resolve().parents[2],
        text=True,
        capture_output=True,
    )

    assert "status=succeeded" in result.stdout
    summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["selected_manifest_count"] == 1
    assert summary["sample_row_count"] == 2
    assert summary["rows_by_dataset"] == {"silver/vacancy_snapshot": 2}
    assert summary["rows_by_day"] == {"2026-06-01": 2}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
