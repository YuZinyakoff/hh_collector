from __future__ import annotations

import logging
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from hhru_platform.infrastructure.observability.operations import (
    log_operation_started,
    record_operation_failed,
    record_operation_succeeded,
)
from hhru_platform.infrastructure.research_archive import (
    ResearchArchiveCheckpointDataset,
    ResearchArchiveChunkSummary,
)

LOGGER = logging.getLogger(__name__)

RESEARCH_ARCHIVE_SCHEMA_VERSION = "research-archive-v1"
RESEARCH_ARCHIVE_STATUS_SUCCEEDED = "succeeded"

DATASET_RAW_API_PAYLOAD = "bronze/raw_api_payload"
DATASET_API_REQUEST_LOG = "silver/api_request_log"
DATASET_VACANCY = "silver/vacancy"
DATASET_VACANCY_SNAPSHOT = "silver/vacancy_snapshot"
DATASET_VACANCY_SEEN_EVENT = "silver/vacancy_seen_event"
DATASET_VACANCY_CURRENT_STATE = "silver/vacancy_current_state"
DATASET_DETAIL_FETCH_ATTEMPT = "silver/detail_fetch_attempt"

INCREMENTAL_RESEARCH_ARCHIVE_DATASETS = (
    DATASET_RAW_API_PAYLOAD,
    DATASET_API_REQUEST_LOG,
    DATASET_VACANCY_SNAPSHOT,
    DATASET_VACANCY_SEEN_EVENT,
    DATASET_DETAIL_FETCH_ATTEMPT,
)
DEFAULT_RESEARCH_ARCHIVE_DATASETS = (
    DATASET_RAW_API_PAYLOAD,
    DATASET_API_REQUEST_LOG,
    DATASET_VACANCY,
    DATASET_VACANCY_SNAPSHOT,
    DATASET_VACANCY_SEEN_EVENT,
    DATASET_VACANCY_CURRENT_STATE,
    DATASET_DETAIL_FETCH_ATTEMPT,
)
SUPPORTED_RESEARCH_ARCHIVE_DATASETS = DEFAULT_RESEARCH_ARCHIVE_DATASETS


@dataclass(slots=True, frozen=True)
class ExportResearchArchiveCommand:
    archive_dir: Path = Path(".state/archive/research")
    datasets: tuple[str, ...] = DEFAULT_RESEARCH_ARCHIVE_DATASETS
    chunk_size: int = 100_000
    batch_size: int = 10_000
    limit_per_dataset: int | None = None
    archive_kind: str = "tool_validation"
    triggered_by: str = "export-research-archive"
    source_database: str = "unknown"
    source_git_revision: str = "unknown"
    source_command: str = "export-research-archive"
    created_at: datetime | None = None
    incremental: bool = False
    settled_before: datetime | None = None

    def __post_init__(self) -> None:
        normalized_triggered_by = self.triggered_by.strip()
        normalized_archive_kind = self.archive_kind.strip()
        normalized_source_database = self.source_database.strip()
        normalized_source_git_revision = self.source_git_revision.strip()
        normalized_source_command = self.source_command.strip()
        normalized_datasets = tuple(dataset.strip() for dataset in self.datasets)

        if not normalized_triggered_by:
            raise ValueError("triggered_by must not be empty")
        if not normalized_archive_kind:
            raise ValueError("archive_kind must not be empty")
        if not normalized_source_database:
            raise ValueError("source_database must not be empty")
        if not normalized_source_git_revision:
            raise ValueError("source_git_revision must not be empty")
        if not normalized_source_command:
            raise ValueError("source_command must not be empty")
        if not normalized_datasets:
            raise ValueError("datasets must not be empty")
        unsupported = sorted(set(normalized_datasets) - set(SUPPORTED_RESEARCH_ARCHIVE_DATASETS))
        if unsupported:
            supported = ", ".join(SUPPORTED_RESEARCH_ARCHIVE_DATASETS)
            raise ValueError(
                f"unsupported research archive datasets: {', '.join(unsupported)}; "
                f"supported: {supported}"
            )
        if self.chunk_size < 1:
            raise ValueError("chunk_size must be greater than or equal to one")
        if self.batch_size < 1:
            raise ValueError("batch_size must be greater than or equal to one")
        if self.limit_per_dataset is not None and self.limit_per_dataset < 1:
            raise ValueError("limit_per_dataset must be greater than or equal to one")
        if self.incremental:
            unsupported_incremental = sorted(
                set(normalized_datasets) - set(INCREMENTAL_RESEARCH_ARCHIVE_DATASETS)
            )
            if unsupported_incremental:
                supported = ", ".join(INCREMENTAL_RESEARCH_ARCHIVE_DATASETS)
                raise ValueError(
                    "incremental research archive export supports only append-only "
                    f"datasets: {supported}; unsupported: {', '.join(unsupported_incremental)}"
                )
            if self.settled_before is None:
                raise ValueError("settled_before is required for incremental export")
        elif self.settled_before is not None:
            raise ValueError("settled_before requires incremental export")
        if self.settled_before is not None and self.settled_before.utcoffset() is None:
            raise ValueError("settled_before must be timezone-aware")

        object.__setattr__(self, "archive_dir", Path(self.archive_dir))
        object.__setattr__(self, "datasets", normalized_datasets)
        object.__setattr__(self, "triggered_by", normalized_triggered_by)
        object.__setattr__(self, "archive_kind", normalized_archive_kind)
        object.__setattr__(self, "source_database", normalized_source_database)
        object.__setattr__(self, "source_git_revision", normalized_source_git_revision)
        object.__setattr__(self, "source_command", normalized_source_command)
        if self.settled_before is not None:
            object.__setattr__(self, "settled_before", self.settled_before.astimezone(UTC))


@dataclass(slots=True, frozen=True)
class ResearchArchiveDatasetSummary:
    dataset: str
    chunk_count: int
    row_count: int
    data_size_bytes: int
    manifest_files: tuple[Path, ...]
    data_files: tuple[Path, ...]
    source_id_before: int | None
    source_id_after: int | None


@dataclass(slots=True, frozen=True)
class ExportResearchArchiveResult:
    status: str
    archive_dir: Path
    schema_version: str
    archive_kind: str
    triggered_by: str
    created_at: datetime
    incremental: bool
    settled_before: datetime | None
    checkpoint_file: Path | None
    summaries: tuple[ResearchArchiveDatasetSummary, ...]

    @property
    def total_chunk_count(self) -> int:
        return sum(summary.chunk_count for summary in self.summaries)

    @property
    def total_row_count(self) -> int:
        return sum(summary.row_count for summary in self.summaries)

    @property
    def total_data_size_bytes(self) -> int:
        return sum(summary.data_size_bytes for summary in self.summaries)


class ResearchArchiveRepository(Protocol):
    def iter_dataset_records(
        self,
        *,
        dataset: str,
        batch_size: int,
        limit: int | None,
        after_source_id: int | None,
        settled_before: datetime | None,
    ) -> Iterable[Mapping[str, Any]]:
        """Yield archive-ready records for one dataset."""


class ResearchArchiveCursorStore(Protocol):
    def latest_source_id(
        self,
        *,
        archive_dir: Path,
        dataset: str,
        archive_kind: str,
    ) -> int | None:
        """Return the latest locally archived numeric source id for one dataset."""


class ResearchArchiveStore(Protocol):
    def write_dataset(
        self,
        *,
        archive_dir: Path,
        schema_version: str,
        dataset: str,
        records: Iterable[Mapping[str, Any]],
        chunk_size: int,
        created_at: datetime,
        archive_kind: str,
        source_database: str,
        source_git_revision: str,
        source_command: str,
        triggered_by: str,
    ) -> tuple[ResearchArchiveChunkSummary, ...]:
        """Write one dataset into archive chunks and return chunk summaries."""


class ResearchArchiveCheckpointStore(Protocol):
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
        """Persist one incremental export checkpoint after chunks are written."""


def export_research_archive(
    command: ExportResearchArchiveCommand,
    *,
    research_archive_repository: ResearchArchiveRepository,
    research_archive_store: ResearchArchiveStore,
    research_archive_cursor_store: ResearchArchiveCursorStore | None = None,
    research_archive_checkpoint_store: ResearchArchiveCheckpointStore | None = None,
) -> ExportResearchArchiveResult:
    started_at = log_operation_started(
        LOGGER,
        operation="export_research_archive",
        archive_dir=str(command.archive_dir),
        datasets=",".join(command.datasets),
        archive_kind=command.archive_kind,
        incremental=command.incremental,
        settled_before=command.settled_before.isoformat() if command.settled_before else "-",
        triggered_by=command.triggered_by,
    )
    created_at = command.created_at or datetime.now(UTC)
    if command.incremental and research_archive_cursor_store is None:
        raise ValueError("research_archive_cursor_store is required for incremental export")
    if command.incremental and research_archive_checkpoint_store is None:
        raise ValueError("research_archive_checkpoint_store is required for incremental export")

    try:
        summaries = tuple(
            _export_dataset(
                dataset=dataset,
                command=command,
                created_at=created_at,
                research_archive_repository=research_archive_repository,
                research_archive_store=research_archive_store,
                research_archive_cursor_store=research_archive_cursor_store,
            )
            for dataset in command.datasets
        )
        checkpoint_file = _write_checkpoint(
            command=command,
            created_at=created_at,
            summaries=summaries,
            research_archive_checkpoint_store=research_archive_checkpoint_store,
        )
    except Exception as error:
        record_operation_failed(
            LOGGER,
            operation="export_research_archive",
            started_at=started_at,
            error_type=error.__class__.__name__,
            error_message=str(error),
            archive_dir=str(command.archive_dir),
            triggered_by=command.triggered_by,
        )
        raise

    result = ExportResearchArchiveResult(
        status=RESEARCH_ARCHIVE_STATUS_SUCCEEDED,
        archive_dir=command.archive_dir,
        schema_version=RESEARCH_ARCHIVE_SCHEMA_VERSION,
        archive_kind=command.archive_kind,
        triggered_by=command.triggered_by,
        created_at=created_at,
        incremental=command.incremental,
        settled_before=command.settled_before,
        checkpoint_file=checkpoint_file,
        summaries=summaries,
    )
    record_operation_succeeded(
        LOGGER,
        operation="export_research_archive",
        started_at=started_at,
        archive_dir=str(result.archive_dir),
        triggered_by=result.triggered_by,
        archive_kind=result.archive_kind,
        incremental=result.incremental,
        settled_before=result.settled_before.isoformat() if result.settled_before else "-",
        total_chunk_count=result.total_chunk_count,
        total_row_count=result.total_row_count,
        total_data_size_bytes=result.total_data_size_bytes,
        checkpoint_file=str(result.checkpoint_file or "-"),
    )
    return result


def _export_dataset(
    *,
    dataset: str,
    command: ExportResearchArchiveCommand,
    created_at: datetime,
    research_archive_repository: ResearchArchiveRepository,
    research_archive_store: ResearchArchiveStore,
    research_archive_cursor_store: ResearchArchiveCursorStore | None,
) -> ResearchArchiveDatasetSummary:
    source_id_before = _source_id_before(
        dataset=dataset,
        command=command,
        research_archive_cursor_store=research_archive_cursor_store,
    )
    records = research_archive_repository.iter_dataset_records(
        dataset=dataset,
        batch_size=command.batch_size,
        limit=command.limit_per_dataset,
        after_source_id=source_id_before,
        settled_before=command.settled_before,
    )
    chunks = research_archive_store.write_dataset(
        archive_dir=command.archive_dir,
        schema_version=RESEARCH_ARCHIVE_SCHEMA_VERSION,
        dataset=dataset,
        records=records,
        chunk_size=command.chunk_size,
        created_at=created_at,
        archive_kind=command.archive_kind,
        source_database=command.source_database,
        source_git_revision=command.source_git_revision,
        source_command=command.source_command,
        triggered_by=command.triggered_by,
    )
    return ResearchArchiveDatasetSummary(
        dataset=dataset,
        chunk_count=len(chunks),
        row_count=sum(chunk.row_count for chunk in chunks),
        data_size_bytes=sum(chunk.data_size_bytes for chunk in chunks),
        manifest_files=tuple(chunk.manifest_file for chunk in chunks),
        data_files=tuple(chunk.data_file for chunk in chunks),
        source_id_before=source_id_before,
        source_id_after=_source_id_after(
            source_id_before=source_id_before,
            chunks=chunks,
        ),
    )


def _source_id_before(
    *,
    dataset: str,
    command: ExportResearchArchiveCommand,
    research_archive_cursor_store: ResearchArchiveCursorStore | None,
) -> int | None:
    if not command.incremental:
        return None
    if research_archive_cursor_store is None:
        raise ValueError("research_archive_cursor_store is required for incremental export")
    return (
        research_archive_cursor_store.latest_source_id(
            archive_dir=command.archive_dir,
            dataset=dataset,
            archive_kind=command.archive_kind,
        )
        or 0
    )


def _source_id_after(
    *,
    source_id_before: int | None,
    chunks: tuple[ResearchArchiveChunkSummary, ...],
) -> int | None:
    if source_id_before is None:
        return None
    source_ids = [
        int(chunk.source_max_id)
        for chunk in chunks
        if chunk.source_max_id is not None
    ]
    return max([source_id_before, *source_ids])


def _write_checkpoint(
    *,
    command: ExportResearchArchiveCommand,
    created_at: datetime,
    summaries: tuple[ResearchArchiveDatasetSummary, ...],
    research_archive_checkpoint_store: ResearchArchiveCheckpointStore | None,
) -> Path | None:
    if not command.incremental:
        return None
    if research_archive_checkpoint_store is None:
        raise ValueError("research_archive_checkpoint_store is required for incremental export")
    if command.settled_before is None:
        raise ValueError("settled_before is required for incremental export")
    return research_archive_checkpoint_store.write_checkpoint(
        archive_dir=command.archive_dir,
        archive_kind=command.archive_kind,
        created_at=created_at,
        settled_before=command.settled_before,
        triggered_by=command.triggered_by,
        datasets=tuple(
            ResearchArchiveCheckpointDataset(
                dataset=summary.dataset,
                source_id_before=summary.source_id_before or 0,
                source_id_after=summary.source_id_after or 0,
                chunk_count=summary.chunk_count,
                row_count=summary.row_count,
                manifest_files=summary.manifest_files,
            )
            for summary in summaries
        ),
    )
