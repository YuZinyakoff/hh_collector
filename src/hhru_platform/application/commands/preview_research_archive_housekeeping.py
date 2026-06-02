from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Protocol
from uuid import UUID

from hhru_platform.application.commands.audit_research_archive_coverage import (
    AuditResearchArchiveCoverageCommand,
    AuditResearchArchiveCoverageResult,
    ResearchArchiveCheckpointStore,
    ResearchArchiveCheckpointVerificationReceiptStore,
    ResearchArchiveOffsiteVerificationReceiptStore,
    audit_research_archive_coverage,
)
from hhru_platform.application.commands.run_housekeeping import (
    TARGET_DETAIL_FETCH_ATTEMPT,
    TARGET_RAW_API_PAYLOAD,
    TARGET_VACANCY_SNAPSHOT,
)
from hhru_platform.infrastructure.observability.operations import (
    log_operation_started,
    record_operation_failed,
    record_operation_succeeded,
)

LOGGER = logging.getLogger(__name__)

RESEARCH_ARCHIVE_HOUSEKEEPING_PREVIEW_STATUS_READY = "ready"
RESEARCH_ARCHIVE_HOUSEKEEPING_PREVIEW_STATUS_BLOCKED = "blocked"
DATASET_RAW_API_PAYLOAD = "bronze/raw_api_payload"
DATASET_VACANCY_SNAPSHOT = "silver/vacancy_snapshot"
DATASET_VACANCY_SEEN_EVENT = "silver/vacancy_seen_event"
DATASET_DETAIL_FETCH_ATTEMPT = "silver/detail_fetch_attempt"


@dataclass(slots=True, frozen=True)
class PreviewResearchArchiveHousekeepingCommand:
    archive_dir: Path = Path(".state/archive/research")
    archive_kind: str = "production"
    offsite_url: str = ""
    offsite_root: str = "/hhru-platform/research-archive"
    raw_api_payload_retention_days: int = 90
    vacancy_snapshot_retention_days: int = 0
    detail_fetch_attempt_retention_days: int = 180
    finished_crawl_run_retention_days: int = 90
    delete_limit_per_target: int = 10_000
    triggered_by: str = "preview-research-archive-housekeeping"
    evaluated_at: datetime | None = None

    def __post_init__(self) -> None:
        normalized_archive_kind = self.archive_kind.strip()
        normalized_triggered_by = self.triggered_by.strip()
        if not normalized_archive_kind:
            raise ValueError("archive_kind must not be empty")
        if not normalized_triggered_by:
            raise ValueError("triggered_by must not be empty")
        if self.raw_api_payload_retention_days < 0:
            raise ValueError("raw_api_payload_retention_days must be greater than or equal to zero")
        if self.vacancy_snapshot_retention_days < 0:
            raise ValueError(
                "vacancy_snapshot_retention_days must be greater than or equal to zero"
            )
        if self.detail_fetch_attempt_retention_days < 0:
            raise ValueError(
                "detail_fetch_attempt_retention_days must be greater than or equal to zero"
            )
        if self.finished_crawl_run_retention_days < 0:
            raise ValueError(
                "finished_crawl_run_retention_days must be greater than or equal to zero"
            )
        if self.delete_limit_per_target < 1:
            raise ValueError("delete_limit_per_target must be greater than or equal to one")
        object.__setattr__(self, "archive_dir", Path(self.archive_dir))
        object.__setattr__(self, "archive_kind", normalized_archive_kind)
        object.__setattr__(self, "triggered_by", normalized_triggered_by)


@dataclass(slots=True, frozen=True)
class ResearchArchiveHousekeepingPreviewSummary:
    target: str
    dataset: str
    retention_days: int
    cutoff: datetime | None
    source_id_covered: int
    candidate_count: int
    action_count: int
    selected_min_id: int | None
    selected_max_id: int | None
    enabled: bool

    @property
    def limited(self) -> bool:
        return self.action_count < self.candidate_count


@dataclass(slots=True, frozen=True)
class PreviewResearchArchiveHousekeepingResult:
    status: str
    archive_dir: Path
    archive_kind: str
    triggered_by: str
    evaluated_at: datetime
    coverage: AuditResearchArchiveCoverageResult
    summaries: tuple[ResearchArchiveHousekeepingPreviewSummary, ...]
    run_tree_summary: ResearchArchiveHousekeepingRunTreePreviewSummary

    @property
    def ready(self) -> bool:
        return self.status == RESEARCH_ARCHIVE_HOUSEKEEPING_PREVIEW_STATUS_READY

    @property
    def total_candidates(self) -> int:
        return sum(summary.candidate_count for summary in self.summaries)

    @property
    def total_action_count(self) -> int:
        return sum(summary.action_count for summary in self.summaries)


class ResearchArchiveHousekeepingPreviewRepository(Protocol):
    def count_raw_api_payload_candidates(
        self,
        *,
        cutoff: datetime,
        max_source_id: int | None = None,
    ) -> int:
        """Count old raw payloads bounded by verified archive coverage."""

    def list_raw_api_payload_ids_for_retention(
        self,
        *,
        cutoff: datetime,
        limit: int,
        max_source_id: int | None = None,
    ) -> list[int]:
        """List old raw payload ids bounded by verified archive coverage."""

    def count_vacancy_snapshot_candidates(
        self,
        *,
        cutoff: datetime,
        max_source_id: int | None = None,
    ) -> int:
        """Count old vacancy snapshots bounded by verified archive coverage."""

    def list_vacancy_snapshot_ids_for_retention(
        self,
        *,
        cutoff: datetime,
        limit: int,
        max_source_id: int | None = None,
    ) -> list[int]:
        """List old vacancy snapshot ids bounded by verified archive coverage."""

    def count_detail_fetch_attempt_candidates(
        self,
        *,
        cutoff: datetime,
        max_source_id: int | None = None,
    ) -> int:
        """Count old detail attempts bounded by verified archive coverage."""

    def list_detail_fetch_attempt_ids_for_retention(
        self,
        *,
        cutoff: datetime,
        limit: int,
        max_source_id: int | None = None,
    ) -> list[int]:
        """List old detail attempt ids bounded by verified archive coverage."""

    def count_finished_crawl_run_candidates(self, *, cutoff: datetime) -> int:
        """Count old finished crawl runs before archive coverage filtering."""

    def count_finished_crawl_run_candidates_blocked_by_seen_event_coverage(
        self,
        *,
        cutoff: datetime,
        max_seen_event_source_id: int,
    ) -> int:
        """Count old finished runs that still own unarchived seen events."""

    def list_finished_crawl_run_ids_for_retention_bounded_by_seen_event_coverage(
        self,
        *,
        cutoff: datetime,
        limit: int,
        max_seen_event_source_id: int,
    ) -> list[UUID]:
        """List old finished runs whose seen events are covered by the archive."""

    def count_crawl_partitions_for_run_ids(self, run_ids: Sequence[UUID]) -> int:
        """Count partitions that would cascade-delete with selected runs."""

    def count_vacancy_seen_events_for_run_ids(self, run_ids: Sequence[UUID]) -> int:
        """Count seen events that would cascade-delete with selected runs."""


CountCandidatesStep = Callable[..., int]
ListIdentifiersStep = Callable[..., list[int]]


def preview_research_archive_housekeeping(
    command: PreviewResearchArchiveHousekeepingCommand,
    *,
    housekeeping_repository: ResearchArchiveHousekeepingPreviewRepository,
    checkpoint_store: ResearchArchiveCheckpointStore,
    receipt_store: ResearchArchiveOffsiteVerificationReceiptStore,
    checkpoint_receipt_store: ResearchArchiveCheckpointVerificationReceiptStore,
) -> PreviewResearchArchiveHousekeepingResult:
    started_at = log_operation_started(
        LOGGER,
        operation="preview_research_archive_housekeeping",
        archive_dir=str(command.archive_dir),
        archive_kind=command.archive_kind,
        offsite_url=command.offsite_url,
        offsite_root=command.offsite_root,
        triggered_by=command.triggered_by,
    )
    evaluated_at = command.evaluated_at or datetime.now(UTC)
    summaries: tuple[ResearchArchiveHousekeepingPreviewSummary, ...]
    try:
        coverage = audit_research_archive_coverage(
            AuditResearchArchiveCoverageCommand(
                archive_dir=command.archive_dir,
                archive_kind=command.archive_kind,
                offsite_url=command.offsite_url,
                offsite_root=command.offsite_root,
                triggered_by=command.triggered_by,
            ),
            checkpoint_store=checkpoint_store,
            receipt_store=receipt_store,
            checkpoint_receipt_store=checkpoint_receipt_store,
        )
        if coverage.complete:
            summaries = (
                _preview_target(
                    target=TARGET_RAW_API_PAYLOAD,
                    dataset=DATASET_RAW_API_PAYLOAD,
                    retention_days=command.raw_api_payload_retention_days,
                    evaluated_at=evaluated_at,
                    delete_limit=command.delete_limit_per_target,
                    coverage=coverage,
                    count_step=housekeeping_repository.count_raw_api_payload_candidates,
                    list_step=housekeeping_repository.list_raw_api_payload_ids_for_retention,
                ),
                _preview_target(
                    target=TARGET_VACANCY_SNAPSHOT,
                    dataset=DATASET_VACANCY_SNAPSHOT,
                    retention_days=command.vacancy_snapshot_retention_days,
                    evaluated_at=evaluated_at,
                    delete_limit=command.delete_limit_per_target,
                    coverage=coverage,
                    count_step=housekeeping_repository.count_vacancy_snapshot_candidates,
                    list_step=housekeeping_repository.list_vacancy_snapshot_ids_for_retention,
                ),
                _preview_target(
                    target=TARGET_DETAIL_FETCH_ATTEMPT,
                    dataset=DATASET_DETAIL_FETCH_ATTEMPT,
                    retention_days=command.detail_fetch_attempt_retention_days,
                    evaluated_at=evaluated_at,
                    delete_limit=command.delete_limit_per_target,
                    coverage=coverage,
                    count_step=housekeeping_repository.count_detail_fetch_attempt_candidates,
                    list_step=(
                        housekeeping_repository.list_detail_fetch_attempt_ids_for_retention
                    ),
                ),
            )
            run_tree_summary = _preview_run_tree(
                retention_days=command.finished_crawl_run_retention_days,
                evaluated_at=evaluated_at,
                delete_limit=command.delete_limit_per_target,
                coverage=coverage,
                housekeeping_repository=housekeeping_repository,
            )
        else:
            summaries = ()
            run_tree_summary = _blocked_run_tree_summary()
    except Exception as error:
        record_operation_failed(
            LOGGER,
            operation="preview_research_archive_housekeeping",
            started_at=started_at,
            error_type=error.__class__.__name__,
            error_message=str(error),
            archive_dir=str(command.archive_dir),
            archive_kind=command.archive_kind,
            triggered_by=command.triggered_by,
        )
        raise

    result = PreviewResearchArchiveHousekeepingResult(
        status=(
            RESEARCH_ARCHIVE_HOUSEKEEPING_PREVIEW_STATUS_READY
            if coverage.complete
            else RESEARCH_ARCHIVE_HOUSEKEEPING_PREVIEW_STATUS_BLOCKED
        ),
        archive_dir=command.archive_dir,
        archive_kind=command.archive_kind,
        triggered_by=command.triggered_by,
        evaluated_at=evaluated_at,
        coverage=coverage,
        summaries=summaries,
        run_tree_summary=run_tree_summary,
    )
    record_operation_succeeded(
        LOGGER,
        operation="preview_research_archive_housekeeping",
        started_at=started_at,
        archive_dir=str(result.archive_dir),
        archive_kind=result.archive_kind,
        triggered_by=result.triggered_by,
        preview_status=result.status,
        coverage_status=result.coverage.status,
        total_candidates=result.total_candidates,
        total_action_count=result.total_action_count,
        run_tree_candidate_count=result.run_tree_summary.candidate_count,
        run_tree_coverage_safe_candidate_count=(
            result.run_tree_summary.coverage_safe_candidate_count
        ),
        run_tree_action_count=result.run_tree_summary.action_count,
        run_tree_coverage_blocked_candidate_count=(
            result.run_tree_summary.coverage_blocked_candidate_count
        ),
        run_tree_selected_partition_count=(
            result.run_tree_summary.selected_partition_count
        ),
        run_tree_selected_vacancy_seen_event_count=(
            result.run_tree_summary.selected_vacancy_seen_event_count
        ),
    )
    return result


def _preview_target(
    *,
    target: str,
    dataset: str,
    retention_days: int,
    evaluated_at: datetime,
    delete_limit: int,
    coverage: AuditResearchArchiveCoverageResult,
    count_step: CountCandidatesStep,
    list_step: ListIdentifiersStep,
) -> ResearchArchiveHousekeepingPreviewSummary:
    source_id_covered = _source_id_covered(coverage=coverage, dataset=dataset)
    if retention_days == 0:
        return ResearchArchiveHousekeepingPreviewSummary(
            target=target,
            dataset=dataset,
            retention_days=0,
            cutoff=None,
            source_id_covered=source_id_covered,
            candidate_count=0,
            action_count=0,
            selected_min_id=None,
            selected_max_id=None,
            enabled=False,
        )

    cutoff = evaluated_at - timedelta(days=retention_days)
    candidate_count = count_step(
        cutoff=cutoff,
        max_source_id=source_id_covered,
    )
    identifiers = tuple(
        list_step(
            cutoff=cutoff,
            limit=delete_limit,
            max_source_id=source_id_covered,
        )
    )
    return ResearchArchiveHousekeepingPreviewSummary(
        target=target,
        dataset=dataset,
        retention_days=retention_days,
        cutoff=cutoff,
        source_id_covered=source_id_covered,
        candidate_count=candidate_count,
        action_count=len(identifiers),
        selected_min_id=min(identifiers, default=None),
        selected_max_id=max(identifiers, default=None),
        enabled=True,
    )


def _source_id_covered(
    *,
    coverage: AuditResearchArchiveCoverageResult,
    dataset: str,
) -> int:
    for summary in coverage.summaries:
        if summary.dataset == dataset:
            return summary.source_id_covered
    raise ValueError(f"coverage summary not found for dataset: {dataset}")


@dataclass(slots=True, frozen=True)
class ResearchArchiveHousekeepingRunTreePreviewSummary:
    retention_days: int
    cutoff: datetime | None
    seen_event_source_id_covered: int
    candidate_count: int
    coverage_safe_candidate_count: int
    coverage_blocked_candidate_count: int
    action_count: int
    selected_partition_count: int
    selected_vacancy_seen_event_count: int
    enabled: bool

    @property
    def limited(self) -> bool:
        return self.action_count < self.coverage_safe_candidate_count


def _preview_run_tree(
    *,
    retention_days: int,
    evaluated_at: datetime,
    delete_limit: int,
    coverage: AuditResearchArchiveCoverageResult,
    housekeeping_repository: ResearchArchiveHousekeepingPreviewRepository,
) -> ResearchArchiveHousekeepingRunTreePreviewSummary:
    source_id_covered = _source_id_covered(
        coverage=coverage,
        dataset=DATASET_VACANCY_SEEN_EVENT,
    )
    if retention_days == 0:
        return ResearchArchiveHousekeepingRunTreePreviewSummary(
            retention_days=0,
            cutoff=None,
            seen_event_source_id_covered=source_id_covered,
            candidate_count=0,
            coverage_safe_candidate_count=0,
            coverage_blocked_candidate_count=0,
            action_count=0,
            selected_partition_count=0,
            selected_vacancy_seen_event_count=0,
            enabled=False,
        )

    cutoff = evaluated_at - timedelta(days=retention_days)
    candidate_count = housekeeping_repository.count_finished_crawl_run_candidates(
        cutoff=cutoff
    )
    blocked_candidate_count = (
        housekeeping_repository.count_finished_crawl_run_candidates_blocked_by_seen_event_coverage(
            cutoff=cutoff,
            max_seen_event_source_id=source_id_covered,
        )
    )
    run_ids = (
        housekeeping_repository.list_finished_crawl_run_ids_for_retention_bounded_by_seen_event_coverage(
            cutoff=cutoff,
            limit=delete_limit,
            max_seen_event_source_id=source_id_covered,
        )
    )
    return ResearchArchiveHousekeepingRunTreePreviewSummary(
        retention_days=retention_days,
        cutoff=cutoff,
        seen_event_source_id_covered=source_id_covered,
        candidate_count=candidate_count,
        coverage_safe_candidate_count=candidate_count - blocked_candidate_count,
        coverage_blocked_candidate_count=blocked_candidate_count,
        action_count=len(run_ids),
        selected_partition_count=housekeeping_repository.count_crawl_partitions_for_run_ids(
            run_ids
        ),
        selected_vacancy_seen_event_count=(
            housekeeping_repository.count_vacancy_seen_events_for_run_ids(run_ids)
        ),
        enabled=True,
    )


def _blocked_run_tree_summary() -> ResearchArchiveHousekeepingRunTreePreviewSummary:
    return ResearchArchiveHousekeepingRunTreePreviewSummary(
        retention_days=0,
        cutoff=None,
        seen_event_source_id_covered=0,
        candidate_count=0,
        coverage_safe_candidate_count=0,
        coverage_blocked_candidate_count=0,
        action_count=0,
        selected_partition_count=0,
        selected_vacancy_seen_event_count=0,
        enabled=False,
    )
