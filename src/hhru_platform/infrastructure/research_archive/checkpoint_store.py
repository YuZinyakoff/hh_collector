from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

RESEARCH_ARCHIVE_CHECKPOINT_SCHEMA_VERSION = "research-archive-checkpoint-v1"


@dataclass(slots=True, frozen=True)
class ResearchArchiveCheckpointDataset:
    dataset: str
    source_id_before: int
    source_id_after: int
    chunk_count: int
    row_count: int
    manifest_files: tuple[Path, ...]


@dataclass(slots=True, frozen=True)
class ResearchArchiveCheckpoint:
    checkpoint_file: Path
    archive_kind: str
    created_at: datetime
    settled_before: datetime
    triggered_by: str
    datasets: tuple[ResearchArchiveCheckpointDataset, ...]


class LocalResearchArchiveCheckpointStore:
    def write_checkpoint(
        self,
        *,
        archive_dir: Path,
        archive_kind: str,
        created_at: datetime,
        settled_before: datetime,
        triggered_by: str,
        datasets: tuple[ResearchArchiveCheckpointDataset, ...],
    ) -> Path:
        archive_root = archive_dir.resolve()
        checkpoint_dir = (
            archive_root
            / "v1"
            / "checkpoints"
            / f"archive_kind={_safe_path_component(archive_kind)}"
        )
        checkpoint_dir.mkdir(parents=True, exist_ok=True)
        checkpoint_file = checkpoint_dir / (
            f"{created_at.astimezone(UTC).strftime('%Y%m%dT%H%M%S%fZ')}.checkpoint.json"
        )
        if checkpoint_file.exists():
            raise FileExistsError(f"research archive checkpoint already exists: {checkpoint_file}")

        checkpoint_file.write_text(
            json.dumps(
                {
                    "checkpoint_schema_version": RESEARCH_ARCHIVE_CHECKPOINT_SCHEMA_VERSION,
                    "archive_kind": archive_kind,
                    "created_at": created_at.astimezone(UTC).isoformat(),
                    "settled_before": settled_before.astimezone(UTC).isoformat(),
                    "triggered_by": triggered_by,
                    "datasets": [
                        {
                            "dataset": dataset.dataset,
                            "source_id_before": dataset.source_id_before,
                            "source_id_after": dataset.source_id_after,
                            "chunk_count": dataset.chunk_count,
                            "row_count": dataset.row_count,
                            "manifest_files": [
                                str(manifest_file.resolve().relative_to(archive_root))
                                for manifest_file in dataset.manifest_files
                            ],
                        }
                        for dataset in datasets
                    ],
                },
                ensure_ascii=True,
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        return checkpoint_file

    def load_checkpoints(
        self,
        *,
        archive_dir: Path,
        archive_kind: str,
    ) -> tuple[ResearchArchiveCheckpoint, ...]:
        archive_root = archive_dir.resolve()
        checkpoint_root = archive_root / "v1" / "checkpoints"
        checkpoints = [
            checkpoint
            for checkpoint_file in sorted(checkpoint_root.rglob("*.checkpoint.json"))
            if (
                checkpoint := self._load_checkpoint(
                    archive_root=archive_root,
                    checkpoint_file=checkpoint_file,
                )
            ).archive_kind
            == archive_kind
        ]
        return tuple(
            sorted(
                checkpoints,
                key=lambda checkpoint: (checkpoint.created_at, str(checkpoint.checkpoint_file)),
            )
        )

    def _load_checkpoint(
        self,
        *,
        archive_root: Path,
        checkpoint_file: Path,
    ) -> ResearchArchiveCheckpoint:
        payload = json.loads(checkpoint_file.read_text(encoding="utf-8"))
        if (
            payload.get("checkpoint_schema_version")
            != RESEARCH_ARCHIVE_CHECKPOINT_SCHEMA_VERSION
        ):
            raise ValueError(
                f"unsupported research archive checkpoint schema in {checkpoint_file}"
            )
        created_at = _parse_datetime(payload.get("created_at"), field="created_at")
        settled_before = _parse_datetime(
            payload.get("settled_before"),
            field="settled_before",
        )
        return ResearchArchiveCheckpoint(
            checkpoint_file=checkpoint_file,
            archive_kind=str(payload["archive_kind"]),
            created_at=created_at,
            settled_before=settled_before,
            triggered_by=str(payload["triggered_by"]),
            datasets=tuple(
                ResearchArchiveCheckpointDataset(
                    dataset=str(dataset_payload["dataset"]),
                    source_id_before=int(dataset_payload["source_id_before"]),
                    source_id_after=int(dataset_payload["source_id_after"]),
                    chunk_count=int(dataset_payload["chunk_count"]),
                    row_count=int(dataset_payload["row_count"]),
                    manifest_files=tuple(
                        _resolve_archive_file(
                            archive_root=archive_root,
                            relative_file=Path(str(manifest_file)),
                        )
                        for manifest_file in dataset_payload["manifest_files"]
                    ),
                )
                for dataset_payload in payload["datasets"]
            ),
        )


def _parse_datetime(value: object, *, field: str) -> datetime:
    parsed = datetime.fromisoformat(str(value))
    if parsed.utcoffset() is None:
        raise ValueError(f"research archive checkpoint {field} must be timezone-aware")
    return parsed.astimezone(UTC)


def _safe_path_component(value: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9_.-]+", "_", value.strip())
    return normalized or "unknown"


def _resolve_archive_file(*, archive_root: Path, relative_file: Path) -> Path:
    if relative_file.is_absolute():
        raise ValueError(f"research archive checkpoint file path must be relative: {relative_file}")
    resolved_file = (archive_root / relative_file).resolve()
    try:
        resolved_file.relative_to(archive_root)
    except ValueError as error:
        raise ValueError(
            f"research archive checkpoint file escapes archive root: {relative_file}"
        ) from error
    return resolved_file
