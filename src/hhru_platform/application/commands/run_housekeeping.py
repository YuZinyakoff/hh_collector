from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Protocol
from uuid import UUID

from hhru_platform.infrastructure.observability.operations import (
    log_operation_started,
    record_operation_failed,
    record_operation_succeeded,
)

LOGGER = logging.getLogger(__name__)

HOUSEKEEPING_STATUS_SUCCEEDED = "succeeded"
HOUSEKEEPING_STATUS_FAILED = "failed"
HOUSEKEEPING_MODE_DRY_RUN = "dry_run"
HOUSEKEEPING_MODE_EXECUTE = "execute"
TARGET_RAW_API_PAYLOAD = "raw_api_payload"
TARGET_VACANCY_SNAPSHOT = "vacancy_snapshot"
TARGET_CRAWL_RUN = "crawl_run"
TARGET_CRAWL_PARTITION = "crawl_partition"
TARGET_DETAIL_FETCH_ATTEMPT = "detail_fetch_attempt"
TARGET_DETAIL_PAYLOAD_STUDY_ARTIFACT = "detail_payload_study_artifact"


@dataclass(slots=True, frozen=True)
class HousekeepingRetentionPolicy:
    raw_api_payload_retention_days: int
    vacancy_snapshot_retention_days: int
    finished_crawl_run_retention_days: int
    detail_fetch_attempt_retention_days: int
    report_artifact_retention_days: int
    report_artifact_dir: Path = Path(".state/reports/detail-payload-study")
    delete_limit_per_target: int = 10_000

    def __post_init__(self) -> None:
        for field_name in (
            "raw_api_payload_retention_days",
            "vacancy_snapshot_retention_days",
            "finished_crawl_run_retention_days",
            "detail_fetch_attempt_retention_days",
            "report_artifact_retention_days",
        ):
            value = getattr(self, field_name)
            if value < 0:
                raise ValueError(f"{field_name} must be greater than or equal to zero")
        if self.delete_limit_per_target < 1:
            raise ValueError("delete_limit_per_target must be greater than or equal to one")

        object.__setattr__(self, "report_artifact_dir", Path(self.report_artifact_dir))


@dataclass(slots=True, frozen=True)
class RunHousekeepingCommand:
    retention_policy: HousekeepingRetentionPolicy
    execute: bool = False
    triggered_by: str = "run-housekeeping"
    evaluated_at: datetime | None = None

    def __post_init__(self) -> None:
        normalized_triggered_by = self.triggered_by.strip()
        if not normalized_triggered_by:
            raise ValueError("triggered_by must not be empty")
        object.__setattr__(self, "triggered_by", normalized_triggered_by)

    @property
    def mode(self) -> str:
        if self.execute:
            return HOUSEKEEPING_MODE_EXECUTE
        return HOUSEKEEPING_MODE_DRY_RUN


@dataclass(slots=True, frozen=True)
class HousekeepingTargetSummary:
    target: str
    item_type: str
    retention_days: int
    cutoff: datetime | None
    candidate_count: int
    action_count: int
    deleted_count: int
    enabled: bool

    @property
    def limited(self) -> bool:
        return self.action_count < self.candidate_count


@dataclass(slots=True, frozen=True)
class RunHousekeepingResult:
    status: str
    mode: str
    triggered_by: str
    evaluated_at: datetime
    summaries: tuple[HousekeepingTargetSummary, ...]

    @property
    def total_candidates(self) -> int:
        return sum(summary.candidate_count for summary in self.summaries)

    @property
    def total_action_count(self) -> int:
        return sum(summary.action_count for summary in self.summaries)

    @property
    def total_deleted(self) -> int:
        return sum(summary.deleted_count for summary in self.summaries)


class HousekeepingRepository(Protocol):
    def count_raw_api_payload_candidates(self, *, cutoff: datetime) -> int:
        """Count raw_api_payload rows eligible for retention cleanup."""

    def list_raw_api_payload_ids_for_retention(
        self,
        *,
        cutoff: datetime,
        limit: int,
    ) -> list[int]:
        """List raw_api_payload ids eligible for cleanup, oldest first."""

    def delete_raw_api_payloads(self, payload_ids: Sequence[int]) -> int:
        """Delete raw_api_payload rows by id."""

    def count_vacancy_snapshot_candidates(self, *, cutoff: datetime) -> int:
        """Count vacancy_snapshot rows eligible for retention cleanup."""

    def list_vacancy_snapshot_ids_for_retention(
        self,
        *,
        cutoff: datetime,
        limit: int,
    ) -> list[int]:
        """List vacancy_snapshot ids eligible for cleanup, oldest first."""

    def delete_vacancy_snapshots(self, snapshot_ids: Sequence[int]) -> int:
        """Delete vacancy_snapshot rows by id."""

    def count_detail_fetch_attempt_candidates(self, *, cutoff: datetime) -> int:
        """Count detail_fetch_attempt rows eligible for retention cleanup."""

    def list_detail_fetch_attempt_ids_for_retention(
        self,
        *,
        cutoff: datetime,
        limit: int,
    ) -> list[int]:
        """List detail_fetch_attempt ids eligible for cleanup, oldest first."""

    def delete_detail_fetch_attempts(self, attempt_ids: Sequence[int]) -> int:
        """Delete detail_fetch_attempt rows by id."""

    def count_finished_crawl_run_candidates(self, *, cutoff: datetime) -> int:
        """Count finished crawl_run rows eligible for retention cleanup."""

    def list_finished_crawl_run_ids_for_retention(
        self,
        *,
        cutoff: datetime,
        limit: int,
    ) -> list[UUID]:
        """List finished crawl_run ids eligible for cleanup, oldest first."""

    def delete_finished_crawl_runs(self, run_ids: Sequence[UUID]) -> int:
        """Delete finished crawl_run rows by id."""

    def count_crawl_partition_candidates_for_finished_runs(self, *, cutoff: datetime) -> int:
        """Count crawl_partition rows that would be cascade-deleted with old crawl_run rows."""

    def count_crawl_partitions_for_run_ids(self, run_ids: Sequence[UUID]) -> int:
        """Count crawl_partition rows for the selected crawl_run ids."""


class ReportArtifactStore(Protocol):
    def count_candidates(self, *, root_dir: Path, cutoff: datetime) -> int:
        """Count local report artifacts eligible for retention cleanup."""

    def list_candidates(
        self,
        *,
        root_dir: Path,
        cutoff: datetime,
        limit: int | None,
    ) -> list[Path]:
        """List local report artifacts eligible for cleanup, oldest first."""

    def delete_candidates(self, paths: list[Path]) -> int:
        """Delete selected local report artifacts."""


class HousekeepingMetricsRecorder(Protocol):
    def record_housekeeping_run(
        self,
        *,
        mode: str,
        status: str,
        recorded_at: datetime,
    ) -> None:
        """Persist one housekeeping run lifecycle outcome."""

    def set_housekeeping_last_action_count(
        self,
        *,
        target: str,
        mode: str,
        count: int,
    ) -> None:
        """Persist the last housekeeping affected count for a target."""

    def record_housekeeping_deleted(
        self,
        *,
        target: str,
        count: int,
    ) -> None:
        """Persist cumulative housekeeping deletions for a target."""


CountRetentionCandidatesStep = Callable[..., int]
ListRetentionIdentifiersStep = Callable[..., Sequence[int | UUID | Path]]
DeleteRetentionIdentifiersStep = Callable[..., int]


@dataclass(slots=True)
class _RetentionPlan:
    target: str
    item_type: str
    retention_days: int
    cutoff: datetime | None
    candidate_count: int
    identifiers: Sequence[int | UUID | Path]
    enabled: bool

    @property
    def action_count(self) -> int:
        return len(self.identifiers)


def run_housekeeping(
    command: RunHousekeepingCommand,
    *,
    housekeeping_repository: HousekeepingRepository,
    report_artifact_store: ReportArtifactStore,
    metrics_recorder: HousekeepingMetricsRecorder | None = None,
) -> RunHousekeepingResult:
    started_at = log_operation_started(
        LOGGER,
        operation="run_housekeeping",
        mode=command.mode,
        triggered_by=command.triggered_by,
        execute=command.execute,
        delete_limit_per_target=command.retention_policy.delete_limit_per_target,
    )
    evaluated_at = command.evaluated_at or datetime.now(UTC)

    try:
        plans = (
            _plan_raw_api_payload_retention(
                housekeeping_repository=housekeeping_repository,
                policy=command.retention_policy,
                evaluated_at=evaluated_at,
            ),
            _plan_vacancy_snapshot_retention(
                housekeeping_repository=housekeeping_repository,
                policy=command.retention_policy,
                evaluated_at=evaluated_at,
            ),
            _plan_detail_fetch_attempt_retention(
                housekeeping_repository=housekeeping_repository,
                policy=command.retention_policy,
                evaluated_at=evaluated_at,
            ),
            *_plan_finished_run_tree_retention(
                housekeeping_repository=housekeeping_repository,
                policy=command.retention_policy,
                evaluated_at=evaluated_at,
            ),
            _plan_report_artifact_retention(
                report_artifact_store=report_artifact_store,
                policy=command.retention_policy,
                evaluated_at=evaluated_at,
            ),
        )

        raw_summary = _execute_simple_plan(
            plan=plans[0],
            execute=command.execute,
            delete_step=housekeeping_repository.delete_raw_api_payloads,
        )
        snapshot_summary = _execute_simple_plan(
            plan=plans[1],
            execute=command.execute,
            delete_step=housekeeping_repository.delete_vacancy_snapshots,
        )
        detail_attempt_summary = _execute_simple_plan(
            plan=plans[2],
            execute=command.execute,
            delete_step=housekeeping_repository.delete_detail_fetch_attempts,
        )
        run_summary = _execute_simple_plan(
            plan=plans[3],
            execute=command.execute,
            delete_step=housekeeping_repository.delete_finished_crawl_runs,
        )
        partition_summary = _execute_partition_plan(
            plan=plans[4],
            deleted_count=(
                plans[4].action_count
                if command.execute and run_summary.deleted_count == run_summary.action_count
                else 0
            ),
        )
        artifact_summary = _execute_simple_plan(
            plan=plans[5],
            execute=command.execute,
            delete_step=report_artifact_store.delete_candidates,
        )
        summaries = (
            raw_summary,
            snapshot_summary,
            detail_attempt_summary,
            run_summary,
            partition_summary,
            artifact_summary,
        )
    except Exception as error:
        if metrics_recorder is not None:
            metrics_recorder.record_housekeeping_run(
                mode=command.mode,
                status=HOUSEKEEPING_STATUS_FAILED,
                recorded_at=evaluated_at,
            )
        record_operation_failed(
            LOGGER,
            operation="run_housekeeping",
            started_at=started_at,
            error_type=error.__class__.__name__,
            error_message=str(error),
            mode=command.mode,
            triggered_by=command.triggered_by,
            evaluated_at=evaluated_at.isoformat(),
        )
        raise

    result = RunHousekeepingResult(
        status=HOUSEKEEPING_STATUS_SUCCEEDED,
        mode=command.mode,
        triggered_by=command.triggered_by,
        evaluated_at=evaluated_at,
        summaries=summaries,
    )
    if metrics_recorder is not None:
        metrics_recorder.record_housekeeping_run(
            mode=result.mode,
            status=result.status,
            recorded_at=result.evaluated_at,
        )
        for summary in result.summaries:
            metrics_recorder.set_housekeeping_last_action_count(
                target=summary.target,
                mode=result.mode,
                count=(
                    summary.action_count
                    if result.mode == HOUSEKEEPING_MODE_DRY_RUN
                    else summary.deleted_count
                ),
            )
            if result.mode == HOUSEKEEPING_MODE_EXECUTE:
                metrics_recorder.record_housekeeping_deleted(
                    target=summary.target,
                    count=summary.deleted_count,
                )

    record_operation_succeeded(
        LOGGER,
        operation="run_housekeeping",
        started_at=started_at,
        mode=result.mode,
        triggered_by=result.triggered_by,
        evaluated_at=result.evaluated_at.isoformat(),
        total_candidates=result.total_candidates,
        total_action_count=result.total_action_count,
        total_deleted=result.total_deleted,
        summary_fields=_summary_fields(result.summaries),
    )
    return result


def _plan_raw_api_payload_retention(
    *,
    housekeeping_repository: HousekeepingRepository,
    policy: HousekeepingRetentionPolicy,
    evaluated_at: datetime,
) -> _RetentionPlan:
    return _plan_sequence_target(
        target=TARGET_RAW_API_PAYLOAD,
        item_type="rows",
        retention_days=policy.raw_api_payload_retention_days,
        delete_limit=policy.delete_limit_per_target,
        evaluated_at=evaluated_at,
        count_step=housekeeping_repository.count_raw_api_payload_candidates,
        list_step=housekeeping_repository.list_raw_api_payload_ids_for_retention,
    )


def _plan_vacancy_snapshot_retention(
    *,
    housekeeping_repository: HousekeepingRepository,
    policy: HousekeepingRetentionPolicy,
    evaluated_at: datetime,
) -> _RetentionPlan:
    return _plan_sequence_target(
        target=TARGET_VACANCY_SNAPSHOT,
        item_type="rows",
        retention_days=policy.vacancy_snapshot_retention_days,
        delete_limit=policy.delete_limit_per_target,
        evaluated_at=evaluated_at,
        count_step=housekeeping_repository.count_vacancy_snapshot_candidates,
        list_step=housekeeping_repository.list_vacancy_snapshot_ids_for_retention,
    )


def _plan_detail_fetch_attempt_retention(
    *,
    housekeeping_repository: HousekeepingRepository,
    policy: HousekeepingRetentionPolicy,
    evaluated_at: datetime,
) -> _RetentionPlan:
    return _plan_sequence_target(
        target=TARGET_DETAIL_FETCH_ATTEMPT,
        item_type="rows",
        retention_days=policy.detail_fetch_attempt_retention_days,
        delete_limit=policy.delete_limit_per_target,
        evaluated_at=evaluated_at,
        count_step=housekeeping_repository.count_detail_fetch_attempt_candidates,
        list_step=housekeeping_repository.list_detail_fetch_attempt_ids_for_retention,
    )


def _plan_finished_run_tree_retention(
    *,
    housekeeping_repository: HousekeepingRepository,
    policy: HousekeepingRetentionPolicy,
    evaluated_at: datetime,
) -> tuple[_RetentionPlan, _RetentionPlan]:
    retention_days = policy.finished_crawl_run_retention_days
    if retention_days == 0:
        disabled_plan = _RetentionPlan(
            target=TARGET_CRAWL_RUN,
            item_type="rows",
            retention_days=0,
            cutoff=None,
            candidate_count=0,
            identifiers=(),
            enabled=False,
        )
        return disabled_plan, _RetentionPlan(
            target=TARGET_CRAWL_PARTITION,
            item_type="rows",
            retention_days=0,
            cutoff=None,
            candidate_count=0,
            identifiers=(),
            enabled=False,
        )

    cutoff = evaluated_at - timedelta(days=retention_days)
    run_candidate_count = housekeeping_repository.count_finished_crawl_run_candidates(
        cutoff=cutoff
    )
    run_ids = housekeeping_repository.list_finished_crawl_run_ids_for_retention(
        cutoff=cutoff,
        limit=policy.delete_limit_per_target,
    )
    partition_candidate_count = (
        housekeeping_repository.count_crawl_partition_candidates_for_finished_runs(
            cutoff=cutoff
        )
    )
    partition_action_count = housekeeping_repository.count_crawl_partitions_for_run_ids(run_ids)
    return (
        _RetentionPlan(
            target=TARGET_CRAWL_RUN,
            item_type="rows",
            retention_days=retention_days,
            cutoff=cutoff,
            candidate_count=run_candidate_count,
            identifiers=tuple(run_ids),
            enabled=True,
        ),
        _RetentionPlan(
            target=TARGET_CRAWL_PARTITION,
            item_type="rows",
            retention_days=retention_days,
            cutoff=cutoff,
            candidate_count=partition_candidate_count,
            identifiers=tuple(range(partition_action_count)),
            enabled=True,
        ),
    )


def _plan_report_artifact_retention(
    *,
    report_artifact_store: ReportArtifactStore,
    policy: HousekeepingRetentionPolicy,
    evaluated_at: datetime,
) -> _RetentionPlan:
    retention_days = policy.report_artifact_retention_days
    if retention_days == 0:
        return _RetentionPlan(
            target=TARGET_DETAIL_PAYLOAD_STUDY_ARTIFACT,
            item_type="files",
            retention_days=0,
            cutoff=None,
            candidate_count=0,
            identifiers=(),
            enabled=False,
        )

    cutoff = evaluated_at - timedelta(days=retention_days)
    candidate_count = report_artifact_store.count_candidates(
        root_dir=policy.report_artifact_dir,
        cutoff=cutoff,
    )
    candidates = report_artifact_store.list_candidates(
        root_dir=policy.report_artifact_dir,
        cutoff=cutoff,
        limit=policy.delete_limit_per_target,
    )
    return _RetentionPlan(
        target=TARGET_DETAIL_PAYLOAD_STUDY_ARTIFACT,
        item_type="files",
        retention_days=retention_days,
        cutoff=cutoff,
        candidate_count=candidate_count,
        identifiers=tuple(candidates),
        enabled=True,
    )


def _plan_sequence_target(
    *,
    target: str,
    item_type: str,
    retention_days: int,
    delete_limit: int,
    evaluated_at: datetime,
    count_step: CountRetentionCandidatesStep,
    list_step: ListRetentionIdentifiersStep,
) -> _RetentionPlan:
    if retention_days == 0:
        return _RetentionPlan(
            target=target,
            item_type=item_type,
            retention_days=0,
            cutoff=None,
            candidate_count=0,
            identifiers=(),
            enabled=False,
        )

    cutoff = evaluated_at - timedelta(days=retention_days)
    candidate_count = int(count_step(cutoff=cutoff))
    identifiers = tuple(list_step(cutoff=cutoff, limit=delete_limit))
    return _RetentionPlan(
        target=target,
        item_type=item_type,
        retention_days=retention_days,
        cutoff=cutoff,
        candidate_count=candidate_count,
        identifiers=identifiers,
        enabled=True,
    )


def _execute_simple_plan(
    *,
    plan: _RetentionPlan,
    execute: bool,
    delete_step: DeleteRetentionIdentifiersStep,
) -> HousekeepingTargetSummary:
    deleted_count = 0
    if execute and plan.enabled and plan.identifiers:
        deleted_count = int(delete_step(plan.identifiers))
    return HousekeepingTargetSummary(
        target=plan.target,
        item_type=plan.item_type,
        retention_days=plan.retention_days,
        cutoff=plan.cutoff,
        candidate_count=plan.candidate_count,
        action_count=plan.action_count,
        deleted_count=deleted_count,
        enabled=plan.enabled,
    )


def _execute_partition_plan(
    *,
    plan: _RetentionPlan,
    deleted_count: int,
) -> HousekeepingTargetSummary:
    return HousekeepingTargetSummary(
        target=plan.target,
        item_type=plan.item_type,
        retention_days=plan.retention_days,
        cutoff=plan.cutoff,
        candidate_count=plan.candidate_count,
        action_count=plan.action_count,
        deleted_count=deleted_count,
        enabled=plan.enabled,
    )


def _summary_fields(
    summaries: tuple[HousekeepingTargetSummary, ...],
) -> dict[str, object]:
    fields: dict[str, object] = {}
    for summary in summaries:
        fields[f"{summary.target}_candidate_count"] = summary.candidate_count
        fields[f"{summary.target}_action_count"] = summary.action_count
        fields[f"{summary.target}_deleted_count"] = summary.deleted_count
        fields[f"{summary.target}_retention_days"] = summary.retention_days
        fields[f"{summary.target}_enabled"] = summary.enabled
    return fields
