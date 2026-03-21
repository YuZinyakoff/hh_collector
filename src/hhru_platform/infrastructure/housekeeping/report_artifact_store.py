from __future__ import annotations

import shutil
from datetime import datetime
from pathlib import Path


class LocalReportArtifactStore:
    def count_candidates(self, *, root_dir: Path, cutoff: datetime) -> int:
        return len(self.list_candidates(root_dir=root_dir, cutoff=cutoff, limit=None))

    def list_candidates(
        self,
        *,
        root_dir: Path,
        cutoff: datetime,
        limit: int | None,
    ) -> list[Path]:
        if not root_dir.exists() or not root_dir.is_dir():
            return []

        candidates: list[tuple[float, Path]] = []
        cutoff_timestamp = cutoff.timestamp()
        for path in root_dir.iterdir():
            if not path.exists():
                continue
            stat = path.stat()
            if stat.st_mtime >= cutoff_timestamp:
                continue
            candidates.append((stat.st_mtime, path))

        candidates.sort(key=lambda item: (item[0], item[1].name))
        paths = [path for _, path in candidates]
        if limit is None:
            return paths
        return paths[:limit]

    def delete_candidates(self, paths: list[Path]) -> int:
        deleted = 0
        for path in paths:
            if not path.exists():
                continue
            if path.is_dir():
                shutil.rmtree(path)
            else:
                path.unlink()
            deleted += 1
        return deleted
