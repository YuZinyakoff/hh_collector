#!/usr/bin/env python3
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import sys
from collections import Counter
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from hhru_platform.config.settings import get_settings  # noqa: E402
from hhru_platform.infrastructure.backup.s3_backup_offsite_uploader import (  # noqa: E402
    S3BackupOffsiteUploader,
)

OBSERVED_AT_FIELDS = (
    "received_at",
    "requested_at",
    "captured_at",
    "seen_at",
    "updated_at",
    "finished_at",
)


@dataclass(slots=True, frozen=True)
class ArchiveEntry:
    dataset_key: str
    manifest_file: Path
    data_file: Path
    row_count: int
    data_size_bytes: int
    data_sha256: str


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    archive_dir = args.archive_dir
    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    entries = _load_entries(
        archive_dir=archive_dir,
        download_from_s3=args.download_from_s3,
        datasets=tuple(args.dataset),
        max_manifests=args.max_manifests,
        max_chunk_bytes=args.max_chunk_bytes,
    )
    if not entries:
        raise SystemExit("no matching research archive manifests found")

    if args.download_from_s3:
        _download_entries_from_s3(entries=entries, archive_dir=archive_dir)

    rows = _read_rows(entries=entries, max_rows=args.max_rows)
    if not rows:
        raise SystemExit("selected archive chunks contained no rows")

    summary = _write_summary(rows=rows, entries=entries, output_dir=output_dir)
    if not args.summary_only:
        _write_dataframe_outputs(rows=rows, output_dir=output_dir)

    print(f"status=succeeded output_dir={output_dir}")
    print(f"selected_manifest_count={len(entries)}")
    print(f"sample_row_count={len(rows)}")
    for dataset, count in sorted(summary["rows_by_dataset"].items()):
        print(f"dataset_summary dataset={dataset} row_count={count}")
    return 0


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a tiny DataFrame-ready sample from research archive JSONL.GZ chunks.",
    )
    parser.add_argument(
        "--archive-dir",
        type=Path,
        default=Path(".state/archive/research-production-v2"),
        help="Local research archive root. With --download-from-s3, files are downloaded here.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path(".state/analysis/research-archive-smoke"),
        help="Directory for CSV, JSON summary and PNG outputs.",
    )
    parser.add_argument(
        "--dataset",
        action="append",
        default=[],
        help="Dataset key filter, e.g. bronze/raw_api_payload. Can be repeated.",
    )
    parser.add_argument("--max-manifests", type=int, default=5)
    parser.add_argument("--max-rows", type=int, default=5000)
    parser.add_argument(
        "--max-chunk-bytes",
        type=int,
        default=25 * 1024 * 1024,
        help="Skip selected chunks larger than this safety limit.",
    )
    parser.add_argument(
        "--download-from-s3",
        action="store_true",
        help="Download inventory, manifests and chunks from configured research archive S3.",
    )
    parser.add_argument(
        "--summary-only",
        action="store_true",
        help="Do not import pandas/matplotlib; write only JSON summary.",
    )
    args = parser.parse_args(argv)
    if args.max_manifests < 1:
        parser.error("--max-manifests must be greater than zero")
    if args.max_rows < 1:
        parser.error("--max-rows must be greater than zero")
    if args.max_chunk_bytes < 1:
        parser.error("--max-chunk-bytes must be greater than zero")
    return args


def _load_entries(
    *,
    archive_dir: Path,
    download_from_s3: bool,
    datasets: tuple[str, ...],
    max_manifests: int,
    max_chunk_bytes: int,
) -> list[ArchiveEntry]:
    if download_from_s3:
        _download_inventory_from_s3(archive_dir=archive_dir)

    inventory_file = archive_dir / "v1" / "inventory" / "archive-inventory.jsonl"
    if inventory_file.is_file():
        entries = _load_entries_from_inventory(
            archive_dir=archive_dir,
            inventory_file=inventory_file,
        )
    else:
        entries = _load_entries_from_local_manifests(archive_dir=archive_dir)

    filtered = [
        entry
        for entry in entries
        if (not datasets or entry.dataset_key in datasets)
        and entry.row_count > 0
        and entry.data_size_bytes <= max_chunk_bytes
    ]
    return sorted(filtered, key=lambda entry: (entry.data_size_bytes, entry.dataset_key))[
        :max_manifests
    ]


def _load_entries_from_inventory(*, archive_dir: Path, inventory_file: Path) -> list[ArchiveEntry]:
    entries: list[ArchiveEntry] = []
    with inventory_file.open("rt", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            payload = json.loads(line)
            entries.append(
                ArchiveEntry(
                    dataset_key=str(payload["dataset_key"]),
                    manifest_file=archive_dir / str(payload["manifest_file"]),
                    data_file=archive_dir / str(payload["data_file"]),
                    row_count=int(payload["row_count"]),
                    data_size_bytes=int(payload["data_size_bytes"]),
                    data_sha256=str(payload["data_sha256"]),
                )
            )
    return entries


def _load_entries_from_local_manifests(*, archive_dir: Path) -> list[ArchiveEntry]:
    entries: list[ArchiveEntry] = []
    for manifest_file in sorted((archive_dir / "v1").glob("**/*.manifest.json")):
        payload = json.loads(manifest_file.read_text(encoding="utf-8"))
        entries.append(
            _entry_from_manifest(
                archive_dir=archive_dir,
                manifest_file=manifest_file,
                payload=payload,
            )
        )
    return entries


def _entry_from_manifest(
    *,
    archive_dir: Path,
    manifest_file: Path,
    payload: dict[str, Any],
) -> ArchiveEntry:
    data_file = Path(str(payload["data_file"]))
    if not data_file.is_absolute():
        data_file = archive_dir / data_file
    return ArchiveEntry(
        dataset_key=str(payload["dataset_key"]),
        manifest_file=manifest_file,
        data_file=data_file,
        row_count=int(payload["row_count"]),
        data_size_bytes=int(payload["data_size_bytes"]),
        data_sha256=str(payload["data_sha256"]),
    )


def _download_inventory_from_s3(*, archive_dir: Path) -> None:
    uploader = _build_research_archive_s3_uploader()
    inventory_file = archive_dir / "v1" / "inventory" / "archive-inventory.jsonl"
    uploader.download_file(
        local_file=inventory_file,
        remote_path="v1/inventory/archive-inventory.jsonl",
    )


def _download_entries_from_s3(*, entries: Iterable[ArchiveEntry], archive_dir: Path) -> None:
    uploader = _build_research_archive_s3_uploader()
    for entry in entries:
        manifest_remote_path = entry.manifest_file.relative_to(archive_dir).as_posix()
        data_remote_path = entry.data_file.relative_to(archive_dir).as_posix()
        if not entry.manifest_file.is_file():
            uploader.download_file(local_file=entry.manifest_file, remote_path=manifest_remote_path)
        if not entry.data_file.is_file():
            uploader.download_file(local_file=entry.data_file, remote_path=data_remote_path)
        _verify_downloaded_entry(entry)


def _build_research_archive_s3_uploader() -> S3BackupOffsiteUploader:
    settings = get_settings()
    endpoint_url = (
        settings.research_archive_offsite_s3_endpoint_url.strip()
        or settings.backup_offsite_s3_endpoint_url.strip()
    )
    bucket = (
        settings.research_archive_offsite_s3_bucket.strip()
        or settings.backup_offsite_s3_bucket.strip()
    )
    region = (
        settings.research_archive_offsite_s3_region.strip()
        or settings.backup_offsite_s3_region.strip()
        or "ru-1"
    )
    access_key_id = (
        settings.research_archive_offsite_s3_access_key_id
        or settings.backup_offsite_s3_access_key_id
        or ""
    ).strip()
    secret_access_key = (
        settings.research_archive_offsite_s3_secret_access_key
        or settings.backup_offsite_s3_secret_access_key
        or ""
    ).strip()
    if not endpoint_url or not bucket or not access_key_id or not secret_access_key:
        raise ValueError(
            "research archive S3 settings are incomplete; configure HHRU_RESEARCH_ARCHIVE_"
            "OFFSITE_S3_* or HHRU_BACKUP_OFFSITE_S3_*"
        )
    return S3BackupOffsiteUploader.with_credentials(
        endpoint_url=endpoint_url,
        bucket=bucket,
        key_prefix=settings.research_archive_offsite_root,
        region_name=region,
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
    )


def _verify_downloaded_entry(entry: ArchiveEntry) -> None:
    if entry.data_file.stat().st_size != entry.data_size_bytes:
        raise ValueError(f"downloaded size mismatch: {entry.data_file}")
    data_sha256 = _sha256(entry.data_file)
    if data_sha256 != entry.data_sha256:
        raise ValueError(f"downloaded sha256 mismatch: {entry.data_file}")


def _read_rows(*, entries: Iterable[ArchiveEntry], max_rows: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for entry in entries:
        _verify_downloaded_entry(entry)
        with gzip.open(entry.data_file, "rt", encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                row = json.loads(line)
                row["_archive_dataset_key"] = entry.dataset_key
                row["_archive_data_file"] = entry.data_file.as_posix()
                rows.append(row)
                if len(rows) >= max_rows:
                    return rows
    return rows


def _write_summary(
    *,
    rows: list[dict[str, Any]],
    entries: list[ArchiveEntry],
    output_dir: Path,
) -> dict[str, Any]:
    rows_by_dataset = Counter(
        str(row.get("_archive_dataset_key", row.get("dataset", "-"))) for row in rows
    )
    rows_by_day = Counter(_observed_day(row) for row in rows)
    rows_by_day.pop("-", None)
    summary = {
        "selected_manifest_count": len(entries),
        "selected_manifests": [entry.manifest_file.as_posix() for entry in entries],
        "sample_row_count": len(rows),
        "rows_by_dataset": dict(sorted(rows_by_dataset.items())),
        "rows_by_day": dict(sorted(rows_by_day.items())),
    }
    (output_dir / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return summary


def _write_dataframe_outputs(*, rows: list[dict[str, Any]], output_dir: Path) -> None:
    try:
        import matplotlib.pyplot as plt
        import pandas as pd
    except ImportError as error:
        raise SystemExit(
            "pandas/matplotlib are required for DataFrame outputs; install with "
            "`python -m pip install -e .[analysis]` or rerun with --summary-only"
        ) from error

    dataframe = pd.json_normalize(rows, sep=".")
    dataframe.to_csv(output_dir / "sample_rows.csv", index=False)
    dataset_counts = dataframe["_archive_dataset_key"].value_counts().sort_index()
    dataset_counts.rename_axis("dataset").rename("row_count").to_csv(
        output_dir / "rows_by_dataset.csv"
    )

    figure, axis = plt.subplots(figsize=(10, 4))
    dataset_counts.plot(kind="bar", ax=axis)
    axis.set_title("Research Archive Sample Rows By Dataset")
    axis.set_xlabel("Dataset")
    axis.set_ylabel("Rows")
    figure.tight_layout()
    figure.savefig(output_dir / "rows_by_dataset.png", dpi=160)
    plt.close(figure)


def _observed_day(row: dict[str, Any]) -> str:
    for field in OBSERVED_AT_FIELDS:
        value = row.get(field)
        if isinstance(value, str) and len(value) >= 10:
            return value[:10]
    return "-"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


if __name__ == "__main__":
    raise SystemExit(main())
