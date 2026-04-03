from __future__ import annotations

import gzip
import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from enum import Enum
from pathlib import Path
from typing import Any
from uuid import UUID


@dataclass(slots=True, frozen=True)
class RetentionArchiveFileSummary:
    archive_file: Path
    manifest_file: Path
    archive_size_bytes: int
    archive_sha256: str
    record_count: int


class LocalRetentionArchiveStore:
    def write_records(
        self,
        *,
        archive_dir: Path,
        target: str,
        evaluated_at: datetime,
        records: Sequence[Mapping[str, Any]],
        metadata: Mapping[str, Any],
    ) -> RetentionArchiveFileSummary:
        normalized_evaluated_at = evaluated_at.astimezone(UTC)
        target_dir = (
            archive_dir
            / target
            / normalized_evaluated_at.strftime("%Y")
            / normalized_evaluated_at.strftime("%m")
        )
        target_dir.mkdir(parents=True, exist_ok=True)

        archive_basename = (
            f"{normalized_evaluated_at.strftime('%Y%m%dT%H%M%SZ')}-{target}-{len(records)}"
        )
        archive_file = target_dir / f"{archive_basename}.jsonl.gz"
        manifest_file = target_dir / f"{archive_basename}.manifest.json"

        with gzip.open(archive_file, "wt", encoding="utf-8") as handle:
            for record in records:
                handle.write(
                    json.dumps(
                        dict(record),
                        ensure_ascii=True,
                        sort_keys=True,
                        default=_json_default,
                    )
                )
                handle.write("\n")

        archive_bytes = archive_file.read_bytes()
        archive_sha256 = hashlib.sha256(archive_bytes).hexdigest()
        archive_size_bytes = len(archive_bytes)
        manifest_payload = {
            "target": target,
            "evaluated_at": normalized_evaluated_at.isoformat(),
            "record_count": len(records),
            "archive_file": str(archive_file),
            "archive_size_bytes": archive_size_bytes,
            "archive_sha256": archive_sha256,
            "metadata": dict(metadata),
        }
        manifest_file.write_text(
            json.dumps(
                manifest_payload,
                ensure_ascii=True,
                indent=2,
                sort_keys=True,
                default=_json_default,
            )
            + "\n",
            encoding="utf-8",
        )
        return RetentionArchiveFileSummary(
            archive_file=archive_file,
            manifest_file=manifest_file,
            archive_size_bytes=archive_size_bytes,
            archive_sha256=archive_sha256,
            record_count=len(records),
        )


def _json_default(value: object) -> str | float | int | bool | None:
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, UUID | Path | Enum):
        return str(value)
    return str(value)
