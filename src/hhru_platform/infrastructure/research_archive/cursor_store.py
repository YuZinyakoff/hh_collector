from __future__ import annotations

from pathlib import Path

from hhru_platform.infrastructure.research_archive.checkpoint_store import (
    LocalResearchArchiveCheckpointStore,
)


class LocalResearchArchiveCursorStore:
    def __init__(
        self,
        *,
        checkpoint_store: LocalResearchArchiveCheckpointStore | None = None,
    ) -> None:
        self._checkpoint_store = checkpoint_store or LocalResearchArchiveCheckpointStore()

    def latest_source_id(
        self,
        *,
        archive_dir: Path,
        dataset: str,
        archive_kind: str,
    ) -> int | None:
        latest_source_id: int | None = None
        for checkpoint in self._checkpoint_store.load_checkpoints(
            archive_dir=archive_dir,
            archive_kind=archive_kind,
        ):
            for checkpoint_dataset in checkpoint.datasets:
                if checkpoint_dataset.dataset != dataset:
                    continue
                latest_source_id = max(
                    latest_source_id or 0,
                    checkpoint_dataset.source_id_after,
                )
        return latest_source_id
