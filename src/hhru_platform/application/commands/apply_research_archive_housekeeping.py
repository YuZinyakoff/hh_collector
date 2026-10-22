from __future__ import annotations

import logging
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Protocol, TypeVar
from uuid import UUID

from hhru_platform.application.commands.audit_research_archive_coverage import (
    ResearchArchiveCheckpointStore,
    ResearchArchiveCheckpointVerificationReceiptStore,
    ResearchArchiveOffsiteVerificationReceiptStore,
)
from hhru_platform.application.commands.preview_research_archive_housekeeping import (
    PreviewResearchArchiveHousekeepingCommand,
    PreviewResearchArchiveHousekeepingResult,
    ResearchArchiveHousekeepingPreviewRepository,
    ResearchArchiveHousekeepingPreviewSummary,
    preview_research_archive_housekeeping,
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
from hhru_platform.infrastructure.research_archive.run_tree_metadata import (
    records_digest,
    verify_offsite_receipt,
)

LOGGER = logging.getLogger(__name__)

RESEARCH_ARCHIVE_HOUSEKEEPING_APPLY_STATUS_SUCCEEDED = "succeeded"
RESEARCH_ARCHIVE_HOUSEKEEPING_PRODUCTION_KIND = "production"
RESEARCH_ARCHIVE_HOUSEKEEPING_PRODUCTION_OFFSITE_ROOT = "/hhru-platform/research-archive"
IdentifierT = TypeVar("IdentifierT", int, UUID)


@dataclass(slots=True, frozen=True)
class ApplyResearchArchiveHousekeepingCommand:
    archive_dir: Path = Path(".state/archive/research")
    archive_kind: str = RESEARCH_ARCHIVE_HOUSEKEEPING_PRODUCTION_KIND
    offsite_url: str = ""
    offsite_root: str = "/hhru-platform/research-archive"
    raw_api_payload_retention_days: int = 90
    vacancy_snapshot_retention_days: int = 0
    detail_fetch_attempt_retention_days: int = 180
    finished_crawl_run_retention_days: int = 90
    delete_limit_per_target: int = 10_000
    run_tree_delete_limit: int = 1
    max_run_tree_cascade_rows: int = 100_000
    run_tree_metadata_dir: Path | None = None
    confirmed_apply: bool = False
    triggered_by: str = "apply-research-archive-housekeeping"
    evaluated_at: datetime | None = None

    def __post_init__(self) -> None:
        normalized_archive_kind = self.archive_kind.strip()
        normalized_offsite_root = self.offsite_root.strip().rstrip("/")
        normalized_triggered_by = self.triggered_by.strip()
        if normalized_archive_kind != RESEARCH_ARCHIVE_HOUSEKEEPING_PRODUCTION_KIND:
            raise ValueError("archive_kind must be production for destructive housekeeping")
        if normalized_offsite_root != RESEARCH_ARCHIVE_HOUSEKEEPING_PRODUCTION_OFFSITE_ROOT:
            raise ValueError(
                "offsite_root must be /hhru-platform/research-archive for destructive housekeeping"
            )
        if not self.confirmed_apply:
            raise ValueError("--apply confirmation is required for destructive housekeeping")
        if not normalized_triggered_by:
            raise ValueError("triggered_by must not be empty")
        if self.run_tree_delete_limit < 1:
            raise ValueError("run_tree_delete_limit must be greater than zero")
        if self.max_run_tree_cascade_rows < 1:
            raise ValueError("max_run_tree_cascade_rows must be greater than zero")
        object.__setattr__(self, "archive_dir", Path(self.archive_dir))
        object.__setattr__(self, "archive_kind", normalized_archive_kind)
        object.__setattr__(self, "offsite_root", normalized_offsite_root)
        object.__setattr__(self, "triggered_by", normalized_triggered_by)
        self.to_preview_command()

    def to_preview_command(self) -> PreviewResearchArchiveHousekeepingCommand:
        return PreviewResearchArchiveHousekeepingCommand(
            archive_dir=self.archive_dir,
            archive_kind=self.archive_kind,
            offsite_url=self.offsite_url,
            offsite_root=self.offsite_root,
            raw_api_payload_retention_days=self.raw_api_payload_retention_days,
            vacancy_snapshot_retention_days=self.vacancy_snapshot_retention_days,
            detail_fetch_attempt_retention_days=self.detail_fetch_attempt_retention_days,
            finished_crawl_run_retention_days=self.finished_crawl_run_retention_days,
            delete_limit_per_target=self.delete_limit_per_target,
            triggered_by=self.triggered_by,
            evaluated_at=self.evaluated_at,
        )


@dataclass(slots=True, frozen=True)
class ResearchArchiveHousekeepingApplyTargetSummary:
    target: str
    dataset: str
    source_id_covered: int
    action_count: int
    deleted_count: int
    selected_min_id: int | None
    selected_max_id: int | None
    enabled: bool


@dataclass(slots=True, frozen=True)
class ResearchArchiveHousekeepingApplyRunTreeSummary:
    action_count: int
    deleted_run_count: int
    cascade_partition_count: int
    cascade_vacancy_seen_event_count: int
    enabled: bool


@dataclass(slots=True, frozen=True)
class ApplyResearchArchiveHousekeepingResult:
    status: str
    archive_dir: Path
    archive_kind: str
    triggered_by: str
    evaluated_at: datetime
    preview: PreviewResearchArchiveHousekeepingResult
    summaries: tuple[ResearchArchiveHousekeepingApplyTargetSummary, ...]
    run_tree_summary: ResearchArchiveHousekeepingApplyRunTreeSummary

    @property
    def direct_deleted_count(self) -> int:
        return sum(summary.deleted_count for summary in self.summaries) + (
            self.run_tree_summary.deleted_run_count
        )

    @property
    def total_deleted_count(self) -> int:
        return (
            self.direct_deleted_count
            + self.run_tree_summary.cascade_partition_count
            + self.run_tree_summary.cascade_vacancy_seen_event_count
        )


class ResearchArchiveHousekeepingApplyRepository(
    ResearchArchiveHousekeepingPreviewRepository,
    Protocol,
):
    def reserve_idle_collection_for_run_tree_retention(self) -> bool:
        """Reserve collection admission until transaction end; reject active runs."""

    def iter_run_tree_metadata_records(self, run_id: UUID) -> Iterable[Mapping[str, Any]]:
        """Stream exact locked root and partition metadata in deterministic order."""

    def delete_raw_api_payloads(self, payload_ids: Sequence[int]) -> int:
        """Delete exact verified raw payload ids."""

    def delete_vacancy_snapshots(self, snapshot_ids: Sequence[int]) -> int:
        """Delete exact verified vacancy snapshot ids."""

    def delete_detail_fetch_attempts(self, attempt_ids: Sequence[int]) -> int:
        """Delete exact verified detail attempt ids."""

    def lock_finished_crawl_run_ids_for_retention_bounded_by_seen_event_coverage(
        self,
        *,
        run_ids: Sequence[UUID],
        cutoff: datetime,
        max_seen_event_source_id: int,
    ) -> list[UUID]:
        """Lock exact safe run roots and recheck seen-event coverage."""

    def delete_finished_crawl_runs(self, run_ids: Sequence[UUID]) -> int:
        """Delete exact locked finished crawl runs."""


@dataclass(slots=True, frozen=True)
class _TargetApplyPlan:
    target: str
    dataset: str
    source_id_covered: int
    identifiers: tuple[int, ...]
    enabled: bool


def apply_research_archive_housekeeping(
    command: ApplyResearchArchiveHousekeepingCommand,
    *,
    housekeeping_repository: ResearchArchiveHousekeepingApplyRepository,
    checkpoint_store: ResearchArchiveCheckpointStore,
    receipt_store: ResearchArchiveOffsiteVerificationReceiptStore,
    checkpoint_receipt_store: ResearchArchiveCheckpointVerificationReceiptStore,
) -> ApplyResearchArchiveHousekeepingResult:
    started_at = log_operation_started(
        LOGGER,
        operation="apply_research_archive_housekeeping",
        archive_dir=str(command.archive_dir),
        archive_kind=command.archive_kind,
        offsite_url=command.offsite_url,
        offsite_root=command.offsite_root,
        triggered_by=command.triggered_by,
    )
    try:
        if command.finished_crawl_run_retention_days > 0 and not (
            housekeeping_repository.reserve_idle_collection_for_run_tree_retention()
        ):
            raise RuntimeError("run-tree retention requires idle collection admission")
        preview = preview_research_archive_housekeeping(
            command.to_preview_command(),
            housekeeping_repository=housekeeping_repository,
            checkpoint_store=checkpoint_store,
            receipt_store=receipt_store,
            checkpoint_receipt_store=checkpoint_receipt_store,
        )
        if not preview.ready:
            raise RuntimeError("verified research archive coverage is incomplete")

        plans = tuple(
            _build_target_plan(
                summary,
                housekeeping_repository=housekeeping_repository,
                delete_limit=command.delete_limit_per_target,
            )
            for summary in preview.summaries
        )
        metadata = None
        if preview.run_tree_summary.enabled:
            if command.run_tree_metadata_dir is None:
                raise RuntimeError("run-tree retention requires --run-tree-metadata-dir")
            metadata = verify_offsite_receipt(
                command.run_tree_metadata_dir,
                offsite_url=command.offsite_url,
                offsite_root=command.offsite_root,
            )
        # The verified bundle is the explicit allowlist, not a fresh oldest-run selection.
        run_ids = (metadata.run_id,) if metadata is not None else ()
        locked_run_ids = _lock_run_tree_ids(
            preview=preview,
            housekeeping_repository=housekeeping_repository,
            run_ids=run_ids,
        )
        cascade_partition_count = housekeeping_repository.count_crawl_partitions_for_run_ids(
            locked_run_ids
        )
        cascade_seen_event_count = housekeeping_repository.count_vacancy_seen_events_for_run_ids(
            locked_run_ids
        )
        cascade_rows = cascade_partition_count + cascade_seen_event_count
        if cascade_rows > command.max_run_tree_cascade_rows:
            raise RuntimeError(
                "run-tree cascade row budget exceeded: "
                f"{cascade_rows} > {command.max_run_tree_cascade_rows}; "
                "review the preview before explicitly raising --max-run-tree-cascade-rows"
            )
        if (
            metadata is not None
            and records_digest(
                housekeeping_repository.iter_run_tree_metadata_records(metadata.run_id)
            )
            != metadata.content_sha256
        ):
            raise RuntimeError("run-tree metadata changed since verified export")

        summaries = tuple(
            _apply_target_plan(plan, housekeeping_repository=housekeeping_repository)
            for plan in plans
        )
        deleted_run_count = _delete_exact(
            target="crawl_run",
            identifiers=locked_run_ids,
            delete_step=housekeeping_repository.delete_finished_crawl_runs,
        )
    except Exception as error:
        record_operation_failed(
            LOGGER,
            operation="apply_research_archive_housekeeping",
            started_at=started_at,
            error_type=error.__class__.__name__,
            error_message=str(error),
            archive_dir=str(command.archive_dir),
            archive_kind=command.archive_kind,
            triggered_by=command.triggered_by,
        )
        raise

    result = ApplyResearchArchiveHousekeepingResult(
        status=RESEARCH_ARCHIVE_HOUSEKEEPING_APPLY_STATUS_SUCCEEDED,
        archive_dir=command.archive_dir,
        archive_kind=command.archive_kind,
        triggered_by=command.triggered_by,
        evaluated_at=preview.evaluated_at,
        preview=preview,
        summaries=summaries,
        run_tree_summary=ResearchArchiveHousekeepingApplyRunTreeSummary(
            action_count=len(locked_run_ids),
            deleted_run_count=deleted_run_count,
            cascade_partition_count=cascade_partition_count,
            cascade_vacancy_seen_event_count=cascade_seen_event_count,
            enabled=preview.run_tree_summary.enabled,
        ),
    )
    record_operation_succeeded(
        LOGGER,
        operation="apply_research_archive_housekeeping",
        started_at=started_at,
        archive_dir=str(result.archive_dir),
        archive_kind=result.archive_kind,
        triggered_by=result.triggered_by,
        direct_deleted_count=result.direct_deleted_count,
        total_deleted_count=result.total_deleted_count,
        deleted_run_count=result.run_tree_summary.deleted_run_count,
        cascade_partition_count=result.run_tree_summary.cascade_partition_count,
        cascade_vacancy_seen_event_count=(result.run_tree_summary.cascade_vacancy_seen_event_count),
    )
    return result


def _build_target_plan(
    summary: ResearchArchiveHousekeepingPreviewSummary,
    *,
    housekeeping_repository: ResearchArchiveHousekeepingApplyRepository,
    delete_limit: int,
) -> _TargetApplyPlan:
    if not summary.enabled:
        return _TargetApplyPlan(
            target=summary.target,
            dataset=summary.dataset,
            source_id_covered=summary.source_id_covered,
            identifiers=(),
            enabled=False,
        )
    if summary.cutoff is None:
        raise RuntimeError(f"enabled target has no cutoff: {summary.target}")

    list_step = {
        TARGET_RAW_API_PAYLOAD: (housekeeping_repository.list_raw_api_payload_ids_for_retention),
        TARGET_VACANCY_SNAPSHOT: (housekeeping_repository.list_vacancy_snapshot_ids_for_retention),
        TARGET_DETAIL_FETCH_ATTEMPT: (
            housekeeping_repository.list_detail_fetch_attempt_ids_for_retention
        ),
    }.get(summary.target)
    if list_step is None:
        raise RuntimeError(f"unsupported research archive housekeeping target: {summary.target}")
    identifiers = tuple(
        list_step(
            cutoff=summary.cutoff,
            limit=delete_limit,
            max_source_id=summary.source_id_covered,
        )
    )
    return _TargetApplyPlan(
        target=summary.target,
        dataset=summary.dataset,
        source_id_covered=summary.source_id_covered,
        identifiers=identifiers,
        enabled=True,
    )


def _lock_run_tree_ids(
    *,
    preview: PreviewResearchArchiveHousekeepingResult,
    housekeeping_repository: ResearchArchiveHousekeepingApplyRepository,
    run_ids: tuple[UUID, ...],
) -> tuple[UUID, ...]:
    if not run_ids:
        return ()
    summary = preview.run_tree_summary
    if summary.cutoff is None:
        raise RuntimeError("enabled crawl_run target has no cutoff")
    locked_run_ids = tuple(
        housekeeping_repository.lock_finished_crawl_run_ids_for_retention_bounded_by_seen_event_coverage(
            run_ids=run_ids,
            cutoff=summary.cutoff,
            max_seen_event_source_id=summary.seen_event_source_id_covered,
        )
    )
    if len(locked_run_ids) != len(run_ids) or set(locked_run_ids) != set(run_ids):
        raise RuntimeError("crawl_run retention selection changed before lock acquisition")
    return run_ids


def _apply_target_plan(
    plan: _TargetApplyPlan,
    *,
    housekeeping_repository: ResearchArchiveHousekeepingApplyRepository,
) -> ResearchArchiveHousekeepingApplyTargetSummary:
    delete_step = {
        TARGET_RAW_API_PAYLOAD: housekeeping_repository.delete_raw_api_payloads,
        TARGET_VACANCY_SNAPSHOT: housekeeping_repository.delete_vacancy_snapshots,
        TARGET_DETAIL_FETCH_ATTEMPT: housekeeping_repository.delete_detail_fetch_attempts,
    }.get(plan.target)
    if delete_step is None:
        raise RuntimeError(f"unsupported research archive housekeeping target: {plan.target}")
    deleted_count = _delete_exact(
        target=plan.target,
        identifiers=plan.identifiers,
        delete_step=delete_step,
    )
    return ResearchArchiveHousekeepingApplyTargetSummary(
        target=plan.target,
        dataset=plan.dataset,
        source_id_covered=plan.source_id_covered,
        action_count=len(plan.identifiers),
        deleted_count=deleted_count,
        selected_min_id=min(plan.identifiers, default=None),
        selected_max_id=max(plan.identifiers, default=None),
        enabled=plan.enabled,
    )


def _delete_exact(
    *,
    target: str,
    identifiers: Sequence[IdentifierT],
    delete_step: Callable[[Sequence[IdentifierT]], int],
) -> int:
    deleted_count = delete_step(identifiers)
    if deleted_count != len(identifiers):
        raise RuntimeError(
            f"{target} delete count mismatch: expected {len(identifiers)}, got {deleted_count}"
        )
    return deleted_count
