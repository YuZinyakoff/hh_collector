from __future__ import annotations

import json
from pathlib import Path


class LocalResearchArchiveCursorStore:
    def latest_source_id(
        self,
        *,
        archive_dir: Path,
        dataset: str,
        archive_kind: str,
    ) -> int | None:
        latest_source_id: int | None = None
        for manifest_file in sorted((archive_dir / "v1").rglob("*.manifest.json")):
            payload = json.loads(manifest_file.read_text(encoding="utf-8"))
            if payload.get("dataset_key") != dataset:
                continue
            if payload.get("archive_kind") != archive_kind:
                continue
            source_max_id = payload.get("source_max_id")
            if source_max_id is None:
                continue
            try:
                normalized_source_id = int(source_max_id)
            except (TypeError, ValueError) as error:
                raise ValueError(
                    f"manifest {manifest_file} has non-numeric source_max_id: "
                    f"{source_max_id!r}"
                ) from error
            latest_source_id = max(latest_source_id or 0, normalized_source_id)
        return latest_source_id
