from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Protocol
from uuid import UUID

from hhru_platform.application.commands.process_list_page import (
    ProcessListPageCommand,
    ProcessListPageResult,
)
from hhru_platform.application.commands.split_partition import (
    SplitPartitionCommand,
    SplitPartitionResult,
)
from hhru_platform.application.policies.list_engine import (
    PartitionSaturationPolicyV1,
    SaturationDecision,
)
from hhru_platform.application.policies.planner import PLANNER_POLICY_VERSION_V2
from hhru_platform.domain.entities.crawl_partition import CrawlPartition
from hhru_platform.domain.value_objects.enums import (
    CrawlPartitionCoverageStatus,
    CrawlPartitionStatus,
)
from hhru_platform.infrastructure.observability.operations import (
    log_operation_started,
    record_operation_failed,
    record_operation_succeeded,
)

LOGGER = logging.getLogger(__name__)


RUNNABLE_PARTITION_STATUSES = {
    CrawlPartitionStatus.PENDING.value,
    CrawlPartitionStatus.RUNNING.value,
    CrawlPartitionStatus.DONE.value,
}
FAILED_PARTITION_STATUSES = {
    CrawlPartitionStatus.FAILED.value,
    CrawlPartitionStatus.UNRESOLVED.value,
}


class CrawlPartitionNotFoundError(LookupError):
    def __init__(self, partition_id: UUID) -> None:
        super().__init__(f"crawl_partition not found: {partition_id}")
        self.partition_id = partition_id


class UnsupportedPartitionExecutionError(ValueError):
    def __init__(self, partition_id: UUID, message: str) -> None:
        super().__init__(
            f"crawl_partition {partition_id} is not runnable for list engine v2: {message}"
        )
        self.partition_id = partition_id


@dataclass(slots=True, frozen=True)
class ProcessPartitionV2Command:
    partition_id: UUID


@dataclass(slots=True, frozen=True)
class ProcessPartitionV2Result:
    partition_id: UUID
    crawl_run_id: UUID
    final_partition_status: str
    final_coverage_status: str
    saturated: bool
    page_results: tuple[ProcessListPageResult, ...]
    split_result: SplitPartitionResult | None
    saturation_reason: str | None
    error_message: str | None

    @property
    def status(self) -> str:
        if self.final_partition_status in FAILED_PARTITION_STATUSES:
            return "failed"
        return "succeeded"

    @property
    def pages_attempted(self) -> int:
        return len(self.page_results)

    @property
    def pages_processed(self) -> int:
        return sum(1 for result in self.page_results if not _is_failed_page_result(result))

    @property
    def vacancies_found(self) -> int:
        unique_vacancy_ids: set[UUID] = set()
        for result in self.page_results:
            if _is_failed_page_result(result):
                continue
            unique_vacancy_ids.update(vacancy.id for vacancy in result.processed_vacancies)
        return len(unique_vacancy_ids)

    @property
    def vacancies_created(self) -> int:
        return sum(
            result.vacancies_created
            for result in self.page_results
            if not _is_failed_page_result(result)
        )

    @property
    def seen_events_created(self) -> int:
        return sum(
            result.seen_events_created
            for result in self.page_results
            if not _is_failed_page_result(result)
        )

    @property
    def children_created_count(self) -> int:
        if self.split_result is None:
            return 0
        return len(self.split_result.created_children)

    @property
    def children_total_count(self) -> int:
        if self.split_result is None:
            return 0
        return len(self.split_result.children)


class CrawlPartitionRepository(Protocol):
    def get(self, partition_id: UUID) -> CrawlPartition | None:
        """Return a crawl partition by id."""

    def mark_pending(self, partition_id: UUID) -> CrawlPartition:
        """Mark a partition as pending for another page."""

    def mark_covered(self, partition_id: UUID) -> CrawlPartition:
        """Mark a leaf partition as fully covered."""


class ProcessListPageStep(Protocol):
    def __call__(self, command: ProcessListPageCommand) -> ProcessListPageResult:
        """Process one search page for a partition."""


class SplitPartitionStep(Protocol):
    def __call__(self, command: SplitPartitionCommand) -> SplitPartitionResult:
        """Split one saturated partition into child scopes."""


def process_partition_v2(
    command: ProcessPartitionV2Command,
    crawl_partition_repository: CrawlPartitionRepository,
    process_list_page_step: ProcessListPageStep,
    split_partition_step: SplitPartitionStep,
    saturation_policy: PartitionSaturationPolicyV1,
) -> ProcessPartitionV2Result:
    started_at = log_operation_started(
        LOGGER,
        operation="process_partition_v2",
        partition_id=command.partition_id,
    )
    try:
        partition = crawl_partition_repository.get(command.partition_id)
        if partition is None:
            raise CrawlPartitionNotFoundError(command.partition_id)
        _validate_partition_is_runnable(partition)

        saturation_decision = saturation_policy.decide(
            pages_total_expected=partition.pages_total_expected,
        )
        if partition.pages_processed > 0 and saturation_decision.is_saturated:
            split_result = split_partition_step(SplitPartitionCommand(partition_id=partition.id))
            result = ProcessPartitionV2Result(
                partition_id=partition.id,
                crawl_run_id=partition.crawl_run_id,
                final_partition_status=split_result.parent_partition.status,
                final_coverage_status=split_result.parent_partition.coverage_status,
                saturated=True,
                page_results=(),
                split_result=split_result,
                saturation_reason=saturation_decision.reason,
                error_message=split_result.resolution_message
                if split_result.parent_partition.status == CrawlPartitionStatus.UNRESOLVED.value
                else None,
            )
            return _finalize_process_partition_v2_result(
                result=result,
                started_at=started_at,
            )

        page_results: list[ProcessListPageResult] = []
        total_pages_expected = _normalize_total_pages(
            pages_total_expected=partition.pages_total_expected,
            completed_pages=partition.pages_processed,
        )
        next_page = partition.pages_processed
        first_saturation_decision: SaturationDecision | None = None

        if partition.pages_processed == 0:
            first_page_result = process_list_page_step(
                ProcessListPageCommand(partition_id=partition.id, page=0)
            )
            page_results.append(first_page_result)
            if _is_failed_page_result(first_page_result):
                failed_result = ProcessPartitionV2Result(
                    partition_id=partition.id,
                    crawl_run_id=partition.crawl_run_id,
                    final_partition_status=first_page_result.partition_status,
                    final_coverage_status=CrawlPartitionCoverageStatus.UNASSESSED.value,
                    saturated=False,
                    page_results=tuple(page_results),
                    split_result=None,
                    saturation_reason=None,
                    error_message=first_page_result.error_message,
                )
                return _finalize_process_partition_v2_result(
                    result=failed_result,
                    started_at=started_at,
                )

            first_saturation_decision = saturation_policy.decide(
                pages_total_expected=first_page_result.pages_total_expected,
            )
            if first_saturation_decision.is_saturated:
                split_result = split_partition_step(
                    SplitPartitionCommand(partition_id=partition.id)
                )
                saturated_result = ProcessPartitionV2Result(
                    partition_id=partition.id,
                    crawl_run_id=partition.crawl_run_id,
                    final_partition_status=split_result.parent_partition.status,
                    final_coverage_status=split_result.parent_partition.coverage_status,
                    saturated=True,
                    page_results=tuple(page_results),
                    split_result=split_result,
                    saturation_reason=first_saturation_decision.reason,
                    error_message=split_result.resolution_message
                    if split_result.parent_partition.status == CrawlPartitionStatus.UNRESOLVED.value
                    else None,
                )
                return _finalize_process_partition_v2_result(
                    result=saturated_result,
                    started_at=started_at,
                )

            total_pages_expected = _normalize_total_pages(
                pages_total_expected=first_page_result.pages_total_expected,
                completed_pages=1,
            )
            next_page = 1
            if next_page < total_pages_expected:
                crawl_partition_repository.mark_pending(partition.id)

        while next_page < total_pages_expected:
            page_result = process_list_page_step(
                ProcessListPageCommand(partition_id=partition.id, page=next_page)
            )
            page_results.append(page_result)
            if _is_failed_page_result(page_result):
                failed_result = ProcessPartitionV2Result(
                    partition_id=partition.id,
                    crawl_run_id=partition.crawl_run_id,
                    final_partition_status=page_result.partition_status,
                    final_coverage_status=CrawlPartitionCoverageStatus.UNASSESSED.value,
                    saturated=False,
                    page_results=tuple(page_results),
                    split_result=None,
                    saturation_reason=(
                        first_saturation_decision.reason
                        if first_saturation_decision is not None
                        else None
                    ),
                    error_message=page_result.error_message,
                )
                return _finalize_process_partition_v2_result(
                    result=failed_result,
                    started_at=started_at,
                )

            next_page += 1
            if next_page < total_pages_expected:
                crawl_partition_repository.mark_pending(partition.id)

        covered_partition = crawl_partition_repository.mark_covered(partition.id)
    except Exception as error:
        record_operation_failed(
            LOGGER,
            operation="process_partition_v2",
            started_at=started_at,
            error_type=error.__class__.__name__,
            error_message=str(error),
            partition_id=command.partition_id,
        )
        raise

    result = ProcessPartitionV2Result(
        partition_id=covered_partition.id,
        crawl_run_id=covered_partition.crawl_run_id,
        final_partition_status=covered_partition.status,
        final_coverage_status=covered_partition.coverage_status,
        saturated=False,
        page_results=tuple(page_results),
        split_result=None,
        saturation_reason=(
            first_saturation_decision.reason if first_saturation_decision is not None else None
        ),
        error_message=None,
    )
    return _finalize_process_partition_v2_result(result=result, started_at=started_at)


def _normalize_total_pages(*, pages_total_expected: int | None, completed_pages: int) -> int:
    normalized_completed_pages = max(completed_pages, 0)
    if pages_total_expected is None:
        return max(normalized_completed_pages, 1)
    return max(pages_total_expected, normalized_completed_pages, 1)


def _validate_partition_is_runnable(partition: CrawlPartition) -> None:
    if partition.planner_policy_version != PLANNER_POLICY_VERSION_V2:
        raise UnsupportedPartitionExecutionError(
            partition.id,
            f"planner_policy_version={partition.planner_policy_version}",
        )
    if not partition.is_terminal:
        raise UnsupportedPartitionExecutionError(partition.id, "partition is not terminal")
    if partition.coverage_status == CrawlPartitionCoverageStatus.COVERED.value:
        raise UnsupportedPartitionExecutionError(partition.id, "partition is already covered")
    if partition.status not in RUNNABLE_PARTITION_STATUSES:
        raise UnsupportedPartitionExecutionError(
            partition.id,
            f"status={partition.status}",
        )


def _finalize_process_partition_v2_result(
    *,
    result: ProcessPartitionV2Result,
    started_at,
) -> ProcessPartitionV2Result:
    if result.status == "failed":
        record_operation_failed(
            LOGGER,
            operation="process_partition_v2",
            started_at=started_at,
            error_type="ProcessPartitionV2ResultFailed",
            error_message=result.error_message or result.final_partition_status,
            partition_id=result.partition_id,
            run_id=result.crawl_run_id,
            final_partition_status=result.final_partition_status,
            final_coverage_status=result.final_coverage_status,
            pages_attempted=result.pages_attempted,
            pages_processed=result.pages_processed,
            saturated=result.saturated,
            children_created=result.children_created_count,
        )
        return result

    record_operation_succeeded(
        LOGGER,
        operation="process_partition_v2",
        started_at=started_at,
        records_written={
            "vacancy": result.vacancies_found,
            "vacancy_seen_event": result.seen_events_created,
            "crawl_partition": result.children_created_count,
        },
        partition_id=result.partition_id,
        run_id=result.crawl_run_id,
        final_partition_status=result.final_partition_status,
        final_coverage_status=result.final_coverage_status,
        pages_attempted=result.pages_attempted,
        pages_processed=result.pages_processed,
        vacancies_found=result.vacancies_found,
        vacancies_created=result.vacancies_created,
        seen_events_created=result.seen_events_created,
        saturated=result.saturated,
        saturation_reason=result.saturation_reason,
        children_created=result.children_created_count,
        children_total=result.children_total_count,
    )
    return result


def _is_failed_page_result(result: ProcessListPageResult) -> bool:
    return (
        result.error_message is not None
        or result.partition_status == CrawlPartitionStatus.FAILED.value
    )
